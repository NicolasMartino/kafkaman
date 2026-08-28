//! The HTTP surface of the `product` service.
//!
//! Small on purpose: this service's interesting behaviour is in its dispatch
//! handler, not its endpoints. What the endpoints demonstrate is that every
//! write to an owned entity enqueues its snapshot in the *same* transaction, so
//! a committed product and its announcement cannot come apart.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use example_contracts::ProductStatus;
use kafkaman::sqlx::enqueue;
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    product_from_row, product_snapshot_envelope, AppState, ProductRecord, PRODUCT_COLUMNS,
};

/// The OpenAPI description of this service, derived from the handlers below.
///
/// Derived rather than hand-written on purpose. A checked-in spec is a second
/// source of truth, and this repository has already been bitten once by an
/// artifact that nothing exercised: it drifts silently and is believed anyway.
/// Generating it from the same annotations that document the handlers means a
/// renamed route or a changed response type cannot leave the spec behind.
#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "product",
        description = "Owns products. Publishes ProductSnapshot, and derives \
                       availability from its own cache of orders.",
    ),
    paths(health, list_products, create_product, read_product, discontinue),
    components(schemas(CreateProductRequest, ProductRecord, ErrorBody)),
    tags((name = "products", description = "Entities this service owns.")),
)]
struct ApiDoc;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/health", get(health))
        .route("/products", get(list_products).post(create_product))
        .route("/products/{product_id}", get(read_product))
        .route("/products/{product_id}/discontinue", post(discontinue))
        .with_state(state)
        .layer(kafkaman::axum::CorrelationLayer::new())
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateProductRequest {
    /// Supplied by the caller when it wants a retry to be a no-op rather than a
    /// second product.
    pub product_id: Option<Uuid>,
    pub name: String,
    pub price_cents: i64,
    pub on_hand: i64,
    /// Defaults to [`ProductStatus::Available`]. A real catalogue would start
    /// products in `Draft` and publish them separately; the example keeps the
    /// lifecycle to the transition that consumers actually have to react to.
    #[schema(value_type = Option<String>, example = "Available")]
    pub status: Option<ProductStatus>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct ErrorBody {
    error: String,
}

/// What can go wrong on this service's write paths.
#[derive(Debug)]
pub enum ProductError {
    InvalidRequest(String),
    NotFound,
    DuplicateProduct,
    /// Anything else: logged in full, reported opaquely, so the response body
    /// never carries schema names or connection details.
    Internal(String),
}

impl IntoResponse for ProductError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::InvalidRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound => (StatusCode::NOT_FOUND, "no such product".to_owned()),
            Self::DuplicateProduct => (
                StatusCode::CONFLICT,
                "a product with this id already exists".to_owned(),
            ),
            Self::Internal(detail) => {
                tracing::error!(error = %detail, "product request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_owned(),
                )
            }
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

/// Wrap any error as [`ProductError::Internal`] with a stage label, so the
/// operator log says which step failed instead of only what the driver said.
fn internal(stage: &str) -> impl Fn(sqlx::Error) -> ProductError + '_ {
    move |err| ProductError::Internal(format!("{stage}: {err}"))
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

/// Liveness only.
///
/// Deliberately checks nothing: it answers "this process is serving", which is
/// meaningful because the listener binds last, after migrations have applied
/// and every loop is spawned.
#[utoipa::path(
    get, path = "/health", tag = "products",
    responses((status = 204, description = "The process is serving.")),
)]
async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Create a product and announce it in the same transaction.
#[utoipa::path(
    post, path = "/products", tag = "products",
    request_body = CreateProductRequest,
    responses(
        (status = 201, description = "Created, and its snapshot enqueued.", body = ProductRecord),
        (status = 400, description = "Empty name, or a negative price or stock.", body = ErrorBody),
        (status = 409, description = "This product_id already exists.", body = ErrorBody),
    ),
)]
async fn create_product(
    State(state): State<AppState>,
    Json(request): Json<CreateProductRequest>,
) -> Result<(StatusCode, Json<ProductRecord>), ProductError> {
    if request.name.trim().is_empty() {
        return Err(ProductError::InvalidRequest(
            "name must not be empty".to_owned(),
        ));
    }
    if request.on_hand < 0 || request.price_cents < 0 {
        return Err(ProductError::InvalidRequest(
            "on_hand and price_cents must not be negative".to_owned(),
        ));
    }

    let product = ProductRecord {
        product_id: request.product_id.unwrap_or_else(Uuid::new_v4),
        name: request.name.trim().to_owned(),
        price_cents: request.price_cents,
        status: request.status.unwrap_or(ProductStatus::Available),
        on_hand: request.on_hand,
        // No orders exist for a product that has just been created, so the
        // derived value and the stock on hand start out equal.
        available: request.on_hand,
        version: 1,
    };

    let mut tx = state
        .pool
        .begin()
        .instrument(kafkaman::db_span!(
            "BEGIN",
            "products",
            "open product write transaction",
        ))
        .await
        .map_err(internal("begin"))?;
    sqlx::query(
        "INSERT INTO products (product_id, name, price_cents, status, on_hand, available, version)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(product.product_id)
    .bind(&product.name)
    .bind(product.price_cents)
    .bind(String::from(product.status.clone()))
    .bind(product.on_hand)
    .bind(product.available)
    .bind(product.version)
    .execute(&mut *tx)
    .instrument(kafkaman::db_span!("INSERT", "products", "insert product"))
    .await
    .map_err(|err| {
        if is_unique_violation(&err) {
            ProductError::DuplicateProduct
        } else {
            ProductError::Internal(format!("insert product: {err}"))
        }
    })?;

    let envelope = product_snapshot_envelope(&product)
        .map_err(|err| ProductError::Internal(format!("build snapshot: {err}")))?;
    enqueue(&mut tx, &state.cfg, &envelope)
        .await
        .map_err(|err| ProductError::Internal(format!("enqueue snapshot: {err}")))?;

    tx.commit()
        .instrument(kafkaman::db_span!(
            "COMMIT",
            "products",
            "commit product write transaction",
        ))
        .await
        .map_err(internal("commit"))?;
    Ok((StatusCode::CREATED, Json(product)))
}

/// List every product this service owns.
#[utoipa::path(
    get, path = "/products", tag = "products",
    responses((status = 200, description = "All products this service owns.", body = Vec<ProductRecord>)),
)]
async fn list_products(
    State(state): State<AppState>,
) -> Result<Json<Vec<ProductRecord>>, ProductError> {
    let rows = sqlx::query(&format!(
        "SELECT {PRODUCT_COLUMNS} FROM products ORDER BY created_at, product_id"
    ))
    .fetch_all(&state.pool)
    .instrument(kafkaman::db_span!("SELECT", "products", "list products"))
    .await
    .map_err(internal("list products"))?;

    let products = rows
        .iter()
        .map(product_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal("decode products"))?;
    Ok(Json(products))
}

/// Read a product from this service's own tables.
#[utoipa::path(
    get, path = "/products/{product_id}", tag = "products",
    params(("product_id" = Uuid, Path, description = "The product to read.")),
    responses(
        (status = 200, body = ProductRecord),
        (status = 404, description = "No such product.", body = ErrorBody),
    ),
)]
async fn read_product(
    State(state): State<AppState>,
    Path(product_id): Path<Uuid>,
) -> Result<Json<ProductRecord>, ProductError> {
    let row = sqlx::query(&format!(
        "SELECT {PRODUCT_COLUMNS} FROM products WHERE product_id = $1"
    ))
    .bind(product_id)
    .fetch_optional(&state.pool)
    .instrument(kafkaman::db_span!("SELECT", "products", "read product"))
    .await
    .map_err(internal("read product"))?
    .ok_or(ProductError::NotFound)?;
    Ok(Json(
        product_from_row(&row).map_err(internal("decode product"))?,
    ))
}

/// Take a product out of circulation without deleting it.
///
/// Soft state, not a tombstone: the entity keeps flowing with a terminal status,
/// so every consumer converges on "exists, not orderable" rather than having to
/// notice an absence.
#[utoipa::path(
    post, path = "/products/{product_id}/discontinue", tag = "products",
    params(("product_id" = Uuid, Path, description = "The product to withdraw.")),
    responses(
        (status = 200, description = "Withdrawn, or already withdrawn.", body = ProductRecord),
        (status = 404, description = "No such product.", body = ErrorBody),
    ),
)]
async fn discontinue(
    State(state): State<AppState>,
    Path(product_id): Path<Uuid>,
) -> Result<Json<ProductRecord>, ProductError> {
    let mut tx = state
        .pool
        .begin()
        .instrument(kafkaman::db_span!(
            "BEGIN",
            "products",
            "open product write transaction",
        ))
        .await
        .map_err(internal("begin"))?;

    let row = sqlx::query(&format!(
        "SELECT {PRODUCT_COLUMNS} FROM products WHERE product_id = $1 FOR UPDATE"
    ))
    .bind(product_id)
    .fetch_optional(&mut *tx)
    .instrument(kafkaman::db_span!("SELECT", "products", "lock product"))
    .await
    .map_err(internal("read product"))?
    .ok_or(ProductError::NotFound)?;
    let current = product_from_row(&row).map_err(internal("decode product"))?;

    // A repeated request is a no-op, not a conflict, and must not enqueue a
    // second snapshot of state nothing changed.
    if current.status == ProductStatus::Discontinued {
        tx.commit()
            .instrument(kafkaman::db_span!(
                "COMMIT",
                "products",
                "commit product no-op transaction",
            ))
            .await
            .map_err(internal("commit"))?;
        return Ok(Json(current));
    }

    let row = sqlx::query(&format!(
        "UPDATE products
            SET status = $1, version = version + 1, updated_at = now()
          WHERE product_id = $2
      RETURNING {PRODUCT_COLUMNS}"
    ))
    .bind(String::from(ProductStatus::Discontinued))
    .bind(product_id)
    .fetch_one(&mut *tx)
    .instrument(kafkaman::db_span!(
        "UPDATE",
        "products",
        "discontinue product"
    ))
    .await
    .map_err(internal("update product"))?;
    let updated = product_from_row(&row).map_err(internal("decode product"))?;

    let envelope = product_snapshot_envelope(&updated)
        .map_err(|err| ProductError::Internal(format!("build snapshot: {err}")))?;
    enqueue(&mut tx, &state.cfg, &envelope)
        .await
        .map_err(|err| ProductError::Internal(format!("enqueue snapshot: {err}")))?;

    tx.commit()
        .instrument(kafkaman::db_span!(
            "COMMIT",
            "products",
            "commit product write transaction",
        ))
        .await
        .map_err(internal("commit"))?;
    Ok(Json(updated))
}
