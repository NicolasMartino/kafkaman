//! The `product` example service.
//!
//! `product` owns products: presentation, status, and stock on hand. It
//! publishes a [`ProductSnapshot`] whenever any of that changes, and it consumes
//! [`OrderSnapshot`] so it can tell consumers how much is still available.
//!
//! The handler here is the interesting half of the example. Where `order`'s
//! inbound handler is empty, this one *derives*: it recomputes availability from
//! its own converged order cache and republishes the product through
//! [`enqueue_on_connection`], consuming and producing in a single transaction.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod boot;
pub mod http;
pub mod service;
pub mod service_manual;

use std::sync::Arc;

use example_contracts::{
    OrderSnapshot, ProductSnapshot, ProductStatus, ORDER_STATUS_FULFILLED_WIRE,
};
use kafkaman::sqlx::{
    enqueue_on_connection, CacheTable, Error as KafkamanError, MessageRouter, ResolvedConfig,
};
use kafkaman::{Envelope, HandlerCtx, IdempotencyIdentity};
use serde::Serialize;
use sqlx::{PgConnection, PgPool, Row};
use tracing::Instrument;
use uuid::Uuid;

pub use boot::{start, start_with, BootMode, BoxError, RunningService, ServiceOptions};
pub use http::build_router;

/// Everything a request handler needs.
#[derive(Clone, Debug)]
pub struct AppState {
    pub pool: PgPool,
    pub cfg: Arc<ResolvedConfig>,
}

/// One product, as this service stores and reports it.
///
/// `on_hand` is in this struct and deliberately *not* in [`ProductSnapshot`]:
/// stock on hand is `product`'s private business, and a consumer that received
/// it could only misuse it, because it has no orders cache to subtract from.
#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct ProductRecord {
    pub product_id: Uuid,
    pub name: String,
    pub price_cents: i64,
    // `ProductStatus` already travels as a plain string — it carries
    // `#[serde(from = "String", into = "String")]` so an unrecognised variant
    // survives a round trip. Declaring `String` here describes the wire
    // accurately *and* keeps `example-contracts` free of a schema dependency it
    // has no other reason to carry.
    #[schema(value_type = String, example = "Available")]
    pub status: ProductStatus,
    pub on_hand: i64,
    pub available: i64,
    /// Monotonic per product; see the note on `example_order::OrderRecord::version`.
    pub version: i64,
}

/// Namespace for product snapshot idempotency digests, versioned so the
/// derivation can change later without colliding with keys already stored.
pub const PRODUCT_SNAPSHOT_IDEMPOTENCY_NAMESPACE: &str = "example-product:product-snapshot:v1";

/// The columns every product read and write projects, so the shapes cannot drift.
pub(crate) const PRODUCT_COLUMNS: &str =
    "product_id, name, price_cents, status, on_hand, available, version";

/// The snapshot to publish for a product's current state.
pub fn product_snapshot_envelope(
    product: &ProductRecord,
) -> Result<Envelope<ProductSnapshot>, KafkamanError> {
    let identity = IdempotencyIdentity::derive(
        PRODUCT_SNAPSHOT_IDEMPOTENCY_NAMESPACE,
        (product.product_id, product.version),
    )?;
    let envelope = Envelope::new(ProductSnapshot {
        product_id: product.product_id,
        name: product.name.clone(),
        price_cents: product.price_cents,
        status: product.status.clone(),
        available: product.available,
    })
    .try_with_idempotency_key(identity)?;
    Ok(envelope)
}

/// The dispatch router for everything this service consumes.
///
/// The handler closure captures the resolved config and the cache table name so
/// neither has to be rebuilt per message.
pub fn dispatch_router(cfg: Arc<ResolvedConfig>) -> Result<MessageRouter, KafkamanError> {
    let order_cache: Arc<str> = CacheTable::for_message::<OrderSnapshot>(&cfg)?
        .qualified_name()
        .into();
    Ok(
        MessageRouter::new().handler::<OrderSnapshot>(move |conn, _meta, order| {
            let cfg = Arc::clone(&cfg);
            let order_cache = Arc::clone(&order_cache);
            Box::pin(async move { apply_order_snapshot(conn, &cfg, &order_cache, &order).await })
        }),
    )
}

/// Recompute one product's availability from converged order state and
/// republish it.
///
/// # Deriving rather than decrementing
///
/// This handler does not subtract `order.quantity` from stock. It recomputes:
///
/// ```text
/// available = on_hand − SUM(quantity over cached fulfilled orders for this product)
/// ```
///
/// A decrementing handler would be correct only because the handler and the
/// processed-mark share a transaction — a property that has to be argued and
/// asserted. Recomputing is idempotent by construction: apply the same snapshot
/// twenty times and the cache still converges to one row per order, so the sum
/// cannot double-count. It is also what makes cancellation free: a cancelled
/// order simply stops matching the filter, and the count comes back with no
/// compensating logic anywhere.
///
/// Filtering at query time is load-bearing rather than convenient. Caching only
/// fulfilled orders would break convergence — an order going Fulfilled →
/// Cancelled would leave a stale Fulfilled row in the cache forever — and it is
/// not even expressible: kafkaman upserts every consumed message into the cache
/// inside `dispatch_once`, with no ingest-time filter hook.
///
/// # Reading its own cache is safe here, deliberately
///
/// The `SUM` below includes the order currently being dispatched, with no
/// exclusion and no add-back, because `dispatch_once` applies the cache upsert
/// *before* it calls this handler. Registering through `handle` is what buys
/// that: the incoming snapshot is already the cache's current row for its
/// entity, so the query sees the new status rather than the previous one.
///
/// The ordering is a decision, not an accident — see
/// `wiki/decisions/dispatch-handler-ordering.decision.md`. An earlier version of
/// this function ran before the upsert and had to exclude `entity_key <> $3` and
/// add the incoming order back by hand, which double-counted the moment anyone
/// forgot either half.
pub(crate) async fn recompute_availability(
    conn: &mut PgConnection,
    order_cache: &str,
    order: &OrderSnapshot,
) -> Result<Option<ProductRecord>, KafkamanError> {
    // `SUM` over `bigint` returns `numeric` in PostgreSQL, so the cast back to
    // `bigint` is required, not decorative.
    let sql = format!(
        "SELECT COALESCE(SUM((payload->>'quantity')::bigint), 0)::bigint
           FROM {order_cache}
          WHERE deleted = false
            AND payload->>'product_id' = $1
            AND payload->>'status' = $2"
    );
    let fulfilled: i64 = sqlx::query_scalar(&sql)
        .bind(order.product_id.to_string())
        .bind(ORDER_STATUS_FULFILLED_WIRE)
        .fetch_one(&mut *conn)
        .instrument(kafkaman::db_span!(
            "SELECT",
            order_cache,
            "sum fulfilled orders from cache",
        ))
        .await?;

    let sql = format!(
        "UPDATE products
            SET available = GREATEST(on_hand - $1, 0), version = version + 1, updated_at = now()
          WHERE product_id = $2
      RETURNING {PRODUCT_COLUMNS}"
    );
    let Some(row) = sqlx::query(&sql)
        .bind(fulfilled)
        .bind(order.product_id)
        .fetch_optional(&mut *conn)
        .instrument(kafkaman::db_span!(
            "UPDATE",
            "products",
            "recompute product availability",
        ))
        .await?
    else {
        // An order for a product this service has never heard of. Not an error
        // to retry: the row will not appear by waiting, and failing would spend
        // the attempt budget and then park a permanently stuck row in the DLQ.
        tracing::warn!(
            product_id = %order.product_id,
            order_id = %order.order_id,
            "order references an unknown product; nothing to recompute"
        );
        return Ok(None);
    };
    Ok(Some(product_from_row(&row)?))
}

/// Derive availability and republish, through the runtime builder's handler
/// context.
///
/// Consume-and-produce in one transaction: the recomputed product and the
/// announcement of it commit together, or neither does. [`HandlerCtx::enqueue`]
/// writes to the dispatch transaction's own connection, which is what makes that
/// true without anything having to be reconciled afterwards.
///
/// The republish is unconditional rather than change-detecting, and that is
/// load-bearing rather than lazy. A snapshot repeating a value is harmless — the
/// consumer's guarded upsert converges to the same state — while always
/// republishing keeps the propagation *observable*, so "this order was accounted
/// for" is something a consumer can wait on. It is also why this service does
/// not use `handle_before` to skip the recompute when fulfilled-ness did not
/// change: the skip would be correct arithmetically and would delete the event
/// the end-to-end test waits for.
pub async fn derive_availability(
    order: &OrderSnapshot,
    cx: &mut HandlerCtx<'_>,
) -> Result<(), KafkamanError> {
    let order_cache = cx.cache_table::<OrderSnapshot>()?.qualified_name();
    let Some(product) = recompute_availability(cx.conn(), &order_cache, order).await? else {
        return Ok(());
    };
    cx.enqueue(&product_snapshot_envelope(&product)?).await
}

/// Derive availability and republish, on the low-level path.
///
/// The same work as [`derive_availability`], reached through
/// [`enqueue_on_connection`] rather than a handler context. Kept because
/// `service_manual.rs` registers an ordinary [`MessageRouter`] handler, which is
/// handed a bare connection.
async fn apply_order_snapshot(
    conn: &mut PgConnection,
    cfg: &ResolvedConfig,
    order_cache: &str,
    order: &OrderSnapshot,
) -> Result<(), KafkamanError> {
    let Some(product) = recompute_availability(conn, order_cache, order).await? else {
        return Ok(());
    };
    let envelope = product_snapshot_envelope(&product)?;
    enqueue_on_connection(conn, cfg, &envelope).await?;
    Ok(())
}

/// Create the business tables this service owns.
pub async fn ensure_business_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS products (
            product_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
            status TEXT NOT NULL,
            on_hand BIGINT NOT NULL CHECK (on_hand >= 0),
            available BIGINT NOT NULL CHECK (available >= 0),
            version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Decode a `products` row.
pub(crate) fn product_from_row(row: &sqlx::postgres::PgRow) -> Result<ProductRecord, sqlx::Error> {
    let status: String = row.try_get("status")?;
    Ok(ProductRecord {
        product_id: row.try_get("product_id")?,
        name: row.try_get("name")?,
        price_cents: row.try_get("price_cents")?,
        status: ProductStatus::from(status),
        on_hand: row.try_get("on_hand")?,
        available: row.try_get("available")?,
        version: row.try_get("version")?,
    })
}
