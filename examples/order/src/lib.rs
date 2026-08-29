//! The `order` example service.
//!
//! `order` owns orders. It does **not** own products, but it needs product state
//! on its own request path — to refuse an order for a discontinued or
//! out-of-stock item, and to render a product without a synchronous call to the
//! service that owns it. That is what the kafkaman cache is for.
//!
//! Two directions of propagation meet here, and they deliberately demonstrate
//! different things:
//!
//! - **inbound `ProductSnapshot`** is declared with `cache::<T>()` and needs no
//!   application code at all. kafkaman creates the received and cache tables,
//!   runs the ingester and dispatcher, and upserts the snapshot — the whole role
//!   is one line in [`service::start`].
//! - **outbound `OrderSnapshot`** is enqueued in the same transaction as the
//!   business write, so an accepted order and its announcement commit or fail
//!   together.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod boot;
pub mod http;
pub mod service;

use kafkaman::InstrumentDb;
use std::sync::Arc;

use example_contracts::{OrderSnapshot, OrderStatus, ProductSnapshot, ProductStatus};
use kafkaman::sqlx::{CacheTable, Error as KafkamanError, ResolvedConfig};
use kafkaman::{Envelope, IdempotencyIdentity};
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub use boot::{BoxError, RunningService, ServiceOptions};
pub use http::build_router;
pub use service::start;

/// Everything a request handler needs.
///
/// `product_cache` is the qualified name of the kafkaman-owned cache table, read
/// once at boot. kafkaman deliberately ships no typed key-value getter for a
/// cache: the table is the API, because the interesting reads are joins and
/// aggregates that a `get(key)` shape would turn into N+1 round trips.
#[derive(Clone, Debug)]
pub struct AppState {
    pub pool: PgPool,
    pub cfg: Arc<ResolvedConfig>,
    pub product_cache: Arc<str>,
}

impl AppState {
    /// Build the state, resolving the cache table name once.
    pub fn new(pool: PgPool, cfg: Arc<ResolvedConfig>) -> Result<Self, KafkamanError> {
        let product_cache = CacheTable::for_message::<ProductSnapshot>(&cfg)?.qualified_name();
        Ok(Self {
            pool,
            cfg,
            product_cache: product_cache.into(),
        })
    }
}

/// One order, as this service stores and reports it.
#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct OrderRecord {
    pub order_id: Uuid,
    pub product_id: Uuid,
    pub quantity: i64,
    // See the note on `CachedProduct::status`: this enum is a string on the
    // wire, so that is what the schema says.
    #[schema(value_type = String, example = "Placed")]
    pub status: OrderStatus,
    /// Monotonic per order, bumped on every state change.
    ///
    /// It is the *idempotency* identity of the snapshot, not its convergence
    /// ordinal — kafkaman takes the ordinal from the Kafka offset. Deriving the
    /// idempotency key from `(order_id, version)` means a retried enqueue of one
    /// state is deduplicated at the consumer, while a genuinely new state always
    /// gets a fresh key. Deriving it from the payload instead would look
    /// equivalent and is not: an entity that returns to a previous state would
    /// re-derive an already-seen key and its snapshot would be silently dropped
    /// as a duplicate.
    pub version: i64,
}

/// The current cached state of a product this service does not own.
#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct CachedProduct {
    pub product_id: Uuid,
    pub name: String,
    pub price_cents: i64,
    // `ProductStatus` carries `#[serde(from = "String", into = "String")]` so
    // that an unrecognised variant survives a round trip rather than failing
    // ingest. `value_type = String` therefore describes the wire exactly, and
    // keeps `example-contracts` free of a schema dependency.
    #[schema(value_type = String, example = "Available")]
    pub status: ProductStatus,
    pub available: i64,
    /// Where the cached state came from.
    ///
    /// Exposed because it is the only externally visible sign that convergence
    /// has advanced. Offsets are comparable within one topic and partition, and
    /// the guarded upsert applies a record only when it is strictly newer, so a
    /// rising `applied_offset` means "this row moved", independent of whether
    /// any field the caller looks at happened to change.
    pub applied_partition: i32,
    pub applied_offset: i64,
}

impl CachedProduct {
    /// Whether `quantity` units may be ordered.
    ///
    /// Both conditions matter. A discontinued product with stock on hand must
    /// still be unorderable, which is why the snapshot carries state rather than
    /// only a counter.
    pub fn is_orderable(&self, quantity: i64) -> bool {
        self.status == ProductStatus::Available && self.available >= quantity
    }
}

/// Namespace for order snapshot idempotency digests, versioned so the
/// derivation can change later without colliding with keys already stored.
pub const ORDER_SNAPSHOT_IDEMPOTENCY_NAMESPACE: &str = "example-order:order-snapshot:v1";

/// The snapshot to publish for an order's current state.
pub fn order_snapshot_envelope(
    order: &OrderRecord,
) -> Result<Envelope<OrderSnapshot>, KafkamanError> {
    let identity = IdempotencyIdentity::derive(
        ORDER_SNAPSHOT_IDEMPOTENCY_NAMESPACE,
        (order.order_id, order.version),
    )?;
    let envelope = Envelope::new(OrderSnapshot {
        order_id: order.order_id,
        product_id: order.product_id,
        quantity: order.quantity,
        status: order.status.clone(),
    })
    .try_with_idempotency_key(identity)?;
    Ok(envelope)
}

/// Create the business tables this service owns.
///
/// Separate from the kafkaman changelog on purpose: kafkaman migrates its own
/// tables and does not want ownership of yours.
pub async fn ensure_business_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            product_id UUID NOT NULL,
            quantity BIGINT NOT NULL CHECK (quantity > 0),
            status TEXT NOT NULL,
            version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Read one product out of the kafkaman cache.
///
/// Takes an executor rather than a pool so a request handler can read it inside
/// the same transaction that writes the order: the acceptance decision and the
/// order row are then one atomic unit against a single snapshot of the cache.
pub async fn cached_product<'c, E>(
    executor: E,
    cache_table: &str,
    product_id: Uuid,
) -> Result<Option<CachedProduct>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let sql = format!(
        "SELECT payload, applied_partition, applied_offset
           FROM {cache_table}
          WHERE entity_key = $1 AND deleted = false"
    );
    let Some(row) = sqlx::query(&sql)
        .bind(product_id.to_string())
        .fetch_optional(executor)
        .instrument_db(kafkaman::db_span!(
            "SELECT",
            cache_table,
            "read cached product"
        ))
        .await?
    else {
        return Ok(None);
    };

    let payload: serde_json::Value = row.try_get("payload")?;
    let snapshot: ProductSnapshot =
        serde_json::from_value(payload).map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
    Ok(Some(CachedProduct {
        product_id: snapshot.product_id,
        name: snapshot.name,
        price_cents: snapshot.price_cents,
        status: snapshot.status,
        available: snapshot.available,
        applied_partition: row.try_get("applied_partition")?,
        applied_offset: row.try_get("applied_offset")?,
    }))
}

/// Decode an `orders` row.
fn order_from_row(row: &sqlx::postgres::PgRow) -> Result<OrderRecord, sqlx::Error> {
    let status: String = row.try_get("status")?;
    Ok(OrderRecord {
        order_id: row.try_get("order_id")?,
        product_id: row.try_get("product_id")?,
        quantity: row.try_get("quantity")?,
        status: OrderStatus::from(status),
        version: row.try_get("version")?,
    })
}

/// The columns every order read and write projects, so the shapes cannot drift.
const ORDER_COLUMNS: &str = "order_id, product_id, quantity, status, version";
