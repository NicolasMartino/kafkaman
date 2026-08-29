//! The HTTP surface of the `order` service.
//!
//! Every endpoint here is either a write that must announce itself
//! (`POST /orders`, the lifecycle transitions) or a read served entirely from
//! local state (`GET /products/{id}`). There is no outbound HTTP call to
//! `product` anywhere in this file, which is the property the cache exists to
//! provide.

use kafkaman::InstrumentDb;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use example_contracts::{OrderStatus, ProductStatus};
use kafkaman::axum::{admin_router, redrive_router, AdminState};
use kafkaman::sqlx::enqueue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    cached_product, order_from_row, order_snapshot_envelope, AppState, CachedProduct, OrderRecord,
    ORDER_COLUMNS,
};

/// The OpenAPI description of this service, derived from the handlers below.
///
/// The two tags are the point rather than decoration: `orders` is what this
/// service owns and may write, `cache` is what it serves from another service's
/// state without ever calling it.
#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "order",
        description = "Owns orders. Admits them against a local cache of \
                       products, with no synchronous call to the service that \
                       owns them.",
    ),
    paths(
        health,
        list_orders,
        create_order,
        read_order,
        fulfil_order,
        cancel_order,
        read_cached_product,
    ),
    components(schemas(CreateOrderRequest, OrderRecord, CachedProduct, ErrorBody)),
    tags(
        (name = "orders", description = "Entities this service owns."),
        (name = "cache", description = "Entities another service owns, served locally."),
    ),
)]
struct ApiDoc;

pub fn build_router(state: AppState) -> Router {
    let operator = operator_routes(&state);
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/health", get(health))
        .route("/orders", get(list_orders).post(create_order))
        .route("/orders/{order_id}", get(read_order))
        .route("/orders/{order_id}/fulfil", post(fulfil_order))
        .route("/orders/{order_id}/cancel", post(cancel_order))
        .route("/products/{product_id}", get(read_cached_product))
        .with_state(state)
        // After `with_state`, so the already-stated operator router nests
        // without the two state types having to unify.
        .nest("/internal/kafkaman", operator)
        .layer(kafkaman::axum::CorrelationLayer::new())
}

/// The operator routes kafkaman ships, mounted where an operator can reach them.
///
/// Unauthenticated, which is acceptable here and nowhere else: a disposable
/// local stack on a private compose network with ports published to loopback.
/// [`admin_router`]'s own `# Security` section is the canonical account of what
/// these routes expose and how a real deployment should mount them; this is one
/// of two example services and the warning belongs in the library, not copied
/// into each of them.
fn operator_routes(state: &AppState) -> Router {
    let admin = AdminState::new(state.pool.clone(), Arc::clone(&state.cfg));
    admin_router(admin.clone()).merge(redrive_router(admin))
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateOrderRequest {
    /// Supplied by the caller when it wants a retry to be a no-op rather than a
    /// second order.
    pub order_id: Option<Uuid>,
    pub product_id: Uuid,
    pub quantity: i64,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct ErrorBody {
    error: String,
}

/// What can go wrong on this service's write paths.
///
/// Modelled as a type rather than a `String` so each cause maps to the right
/// status code, and so the response body never carries the raw database error —
/// which leaks schema names, and sometimes connection details, to the caller.
#[derive(Debug)]
pub enum OrderError {
    /// The request was well-formed JSON but not a usable order.
    InvalidRequest(String),
    /// No order with this id.
    NotFound,
    /// The same order id was already accepted.
    DuplicateOrder,
    /// This service's cache holds no row for the product yet.
    ///
    /// Distinct from "unknown product": the product may exist and simply not
    /// have propagated. Reported as 409 rather than 404 because retrying later
    /// can succeed.
    ProductNotCached,
    /// The cached product state forbids the order.
    NotOrderable(String),
    /// The requested lifecycle transition is not legal from the current state.
    IllegalTransition(String),
    /// Anything else: logged in full, reported opaquely.
    Internal(String),
}

impl IntoResponse for OrderError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::InvalidRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound => (StatusCode::NOT_FOUND, "no such order".to_owned()),
            Self::DuplicateOrder => (
                StatusCode::CONFLICT,
                "an order with this id already exists".to_owned(),
            ),
            Self::ProductNotCached => (
                StatusCode::CONFLICT,
                "product has not propagated to this service yet".to_owned(),
            ),
            Self::NotOrderable(message) => (StatusCode::CONFLICT, message),
            Self::IllegalTransition(message) => (StatusCode::CONFLICT, message),
            Self::Internal(detail) => {
                // The detail goes to the operator, not the caller.
                tracing::error!(error = %detail, "order request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_owned(),
                )
            }
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

/// Wrap any error as [`OrderError::Internal`] with a stage label, so the
/// operator log says which step failed instead of only what the driver said.
fn internal(stage: &str) -> impl Fn(sqlx::Error) -> OrderError + '_ {
    move |err| OrderError::Internal(format!("{stage}: {err}"))
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

/// Liveness only; see the note on `product`'s equivalent.
#[utoipa::path(
    get, path = "/health", tag = "orders",
    responses((status = 204, description = "The process is serving.")),
)]
async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Admit an order against cached product state alone.
///
/// The two 409s are different failures and worth distinguishing: the product
/// may simply not have propagated yet, in which case retrying later succeeds;
/// or it propagated and forbids the order.
#[utoipa::path(
    post, path = "/orders", tag = "orders",
    request_body = CreateOrderRequest,
    responses(
        (status = 201, description = "Accepted, and its snapshot enqueued.", body = OrderRecord),
        (status = 400, description = "Quantity was not greater than zero, or the \
                                      body was not valid JSON.", body = ErrorBody),
        // Axum's `Json` extractor answers before any handler code runs, and
        // with three statuses this route would otherwise not admit to. A client
        // generated from this spec has to handle them: they are what it gets for
        // a field it spelled wrong.
        (status = 415, description = "Content-Type was not application/json.", body = ErrorBody),
        (status = 422, description = "Well-formed JSON that does not match the \
                                      request schema — an unknown, missing, or \
                                      wrongly typed field.", body = ErrorBody),
        (status = 409, description = "Not yet propagated, a duplicate order_id, or \
                                      the cached product forbids it.", body = ErrorBody),
    ),
)]
async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<OrderRecord>), OrderError> {
    if request.quantity <= 0 {
        return Err(OrderError::InvalidRequest(
            "quantity must be greater than zero".to_owned(),
        ));
    }
    let order_id = request.order_id.unwrap_or_else(Uuid::new_v4);

    let mut tx = state
        .pool
        .begin()
        .instrument_db(kafkaman::db_span!(
            "BEGIN",
            "orders",
            "open order write transaction"
        ))
        .await
        .map_err(internal("begin"))?;

    // The admission decision and the order row are taken against one snapshot of
    // the cache, inside the transaction that writes the order.
    let product = cached_product(&mut *tx, &state.product_cache, request.product_id)
        .await
        .map_err(internal("read product cache"))?
        .ok_or(OrderError::ProductNotCached)?;

    if product.status != ProductStatus::Available {
        return Err(OrderError::NotOrderable(format!(
            "product is not available: status is {}",
            String::from(product.status)
        )));
    }
    if product.available < request.quantity {
        return Err(OrderError::NotOrderable(format!(
            "product has {} available, requested {}",
            product.available, request.quantity
        )));
    }

    let order = OrderRecord {
        order_id,
        product_id: request.product_id,
        quantity: request.quantity,
        status: OrderStatus::Placed,
        version: 1,
    };

    sqlx::query(
        "INSERT INTO orders (order_id, product_id, quantity, status, version)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(order.order_id)
    .bind(order.product_id)
    .bind(order.quantity)
    .bind(String::from(order.status.clone()))
    .bind(order.version)
    .execute(&mut *tx)
    .instrument_db(kafkaman::db_span!("INSERT", "orders", "insert order"))
    .await
    .map_err(|err| {
        if is_unique_violation(&err) {
            OrderError::DuplicateOrder
        } else {
            OrderError::Internal(format!("insert order: {err}"))
        }
    })?;

    let envelope = order_snapshot_envelope(&order)
        .map_err(|err| OrderError::Internal(format!("build snapshot: {err}")))?;
    enqueue(&mut tx, &state.cfg, &envelope)
        .await
        .map_err(|err| OrderError::Internal(format!("enqueue snapshot: {err}")))?;

    tx.commit()
        .instrument_db(kafkaman::db_span!(
            "COMMIT",
            "orders",
            "commit order write transaction",
        ))
        .await
        .map_err(internal("commit"))?;
    Ok((StatusCode::CREATED, Json(order)))
}

/// List every order this service owns.
#[utoipa::path(
    get, path = "/orders", tag = "orders",
    responses((status = 200, description = "All orders this service owns.", body = Vec<OrderRecord>)),
)]
async fn list_orders(State(state): State<AppState>) -> Result<Json<Vec<OrderRecord>>, OrderError> {
    let rows = sqlx::query(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders ORDER BY created_at, order_id"
    ))
    .fetch_all(&state.pool)
    .instrument_db(kafkaman::db_span!("SELECT", "orders", "list orders"))
    .await
    .map_err(internal("list orders"))?;

    let orders = rows
        .iter()
        .map(order_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal("decode orders"))?;
    Ok(Json(orders))
}

/// Read an order from this service's own tables.
#[utoipa::path(
    get, path = "/orders/{order_id}", tag = "orders",
    params(("order_id" = Uuid, Path, description = "The order to read.")),
    responses(
        (status = 200, body = OrderRecord),
        (status = 404, description = "No such order.", body = ErrorBody),
    ),
)]
async fn read_order(
    State(state): State<AppState>,
    Path(order_id): Path<Uuid>,
) -> Result<Json<OrderRecord>, OrderError> {
    let row = sqlx::query(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders WHERE order_id = $1"
    ))
    .bind(order_id)
    .fetch_optional(&state.pool)
    .instrument_db(kafkaman::db_span!("SELECT", "orders", "read order"))
    .await
    .map_err(internal("read order"))?
    .ok_or(OrderError::NotFound)?;
    Ok(Json(
        order_from_row(&row).map_err(internal("decode order"))?,
    ))
}

/// Fulfil an order, which is what eventually reduces availability at `product`.
#[utoipa::path(
    post, path = "/orders/{order_id}/fulfil", tag = "orders",
    params(("order_id" = Uuid, Path, description = "The order to fulfil.")),
    responses(
        (status = 200, description = "Fulfilled, or already fulfilled.", body = OrderRecord),
        (status = 404, description = "No such order.", body = ErrorBody),
        (status = 409, description = "Illegal transition from the current status.", body = ErrorBody),
    ),
)]
async fn fulfil_order(
    state: State<AppState>,
    order_id: Path<Uuid>,
) -> Result<Json<OrderRecord>, OrderError> {
    transition(state, order_id, OrderStatus::Fulfilled).await
}

/// Cancel an order, including one already fulfilled.
///
/// The interesting case: availability at `product` is restored with no
/// compensating write anywhere, because it is recomputed from cached state
/// rather than decremented.
#[utoipa::path(
    post, path = "/orders/{order_id}/cancel", tag = "orders",
    params(("order_id" = Uuid, Path, description = "The order to cancel.")),
    responses(
        (status = 200, description = "Cancelled, or already cancelled.", body = OrderRecord),
        (status = 404, description = "No such order.", body = ErrorBody),
        (status = 409, description = "Illegal transition from the current status.", body = ErrorBody),
    ),
)]
async fn cancel_order(
    state: State<AppState>,
    order_id: Path<Uuid>,
) -> Result<Json<OrderRecord>, OrderError> {
    transition(state, order_id, OrderStatus::Cancelled).await
}

/// Whether an order may move from `from` to `to`.
///
/// Cancelling a fulfilled order is legal, and it is the interesting case: it
/// gives back availability at `product` with no compensating logic anywhere,
/// because availability there is recomputed from cached state rather than
/// decremented.
fn is_legal_transition(from: &OrderStatus, to: &OrderStatus) -> bool {
    matches!(
        (from, to),
        (OrderStatus::Placed, OrderStatus::Fulfilled)
            | (OrderStatus::Placed, OrderStatus::Cancelled)
            | (OrderStatus::Fulfilled, OrderStatus::Cancelled)
    )
}

async fn transition(
    State(state): State<AppState>,
    Path(order_id): Path<Uuid>,
    target: OrderStatus,
) -> Result<Json<OrderRecord>, OrderError> {
    let mut tx = state
        .pool
        .begin()
        .instrument_db(kafkaman::db_span!(
            "BEGIN",
            "orders",
            "open order write transaction"
        ))
        .await
        .map_err(internal("begin"))?;

    // `FOR UPDATE` so two concurrent transitions of one order serialize. Without
    // it both could read `Placed`, both bump to version 2, and the second insert
    // would derive an idempotency key the first already used — the newer state
    // would then be dropped as a duplicate at every consumer.
    let row = sqlx::query(&format!(
        "SELECT {ORDER_COLUMNS} FROM orders WHERE order_id = $1 FOR UPDATE"
    ))
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .instrument_db(kafkaman::db_span!("SELECT", "orders", "lock order"))
    .await
    .map_err(internal("read order"))?
    .ok_or(OrderError::NotFound)?;
    let current = order_from_row(&row).map_err(internal("decode order"))?;

    // Re-applying the state an order is already in is a no-op, not a conflict:
    // a retried request must not be an error, and must not enqueue a second
    // snapshot of state nothing changed.
    if current.status == target {
        tx.commit()
            .instrument_db(kafkaman::db_span!(
                "COMMIT",
                "orders",
                "commit order no-op transaction",
            ))
            .await
            .map_err(internal("commit"))?;
        return Ok(Json(current));
    }
    if !is_legal_transition(&current.status, &target) {
        return Err(OrderError::IllegalTransition(format!(
            "cannot move order from {} to {}",
            String::from(current.status),
            String::from(target)
        )));
    }

    let row = sqlx::query(&format!(
        "UPDATE orders
            SET status = $1, version = version + 1, updated_at = now()
          WHERE order_id = $2
      RETURNING {ORDER_COLUMNS}"
    ))
    .bind(String::from(target))
    .bind(order_id)
    .fetch_one(&mut *tx)
    .instrument_db(kafkaman::db_span!("UPDATE", "orders", "transition order"))
    .await
    .map_err(internal("update order"))?;
    let updated = order_from_row(&row).map_err(internal("decode order"))?;

    let envelope = order_snapshot_envelope(&updated)
        .map_err(|err| OrderError::Internal(format!("build snapshot: {err}")))?;
    enqueue(&mut tx, &state.cfg, &envelope)
        .await
        .map_err(|err| OrderError::Internal(format!("enqueue snapshot: {err}")))?;

    tx.commit()
        .instrument_db(kafkaman::db_span!(
            "COMMIT",
            "orders",
            "commit order write transaction",
        ))
        .await
        .map_err(internal("commit"))?;
    Ok(Json(updated))
}

/// Serve a product this service does not own, entirely from its local cache.
///
/// There is no outbound call to `product` on this path — which is the whole
/// point of the cache, and the reason a 409 here means "not propagated yet"
/// rather than "not found".
#[utoipa::path(
    get, path = "/products/{product_id}", tag = "cache",
    params(("product_id" = Uuid, Path, description = "The product to read from cache.")),
    responses(
        (status = 200, description = "The converged view, with the Kafka offset it \
                                      was applied at.", body = CachedProduct),
        (status = 409, description = "Nothing has been applied for this product yet; \
                                      retrying later can succeed.", body = ErrorBody),
    ),
)]
async fn read_cached_product(
    State(state): State<AppState>,
    Path(product_id): Path<Uuid>,
) -> Result<Json<CachedProduct>, OrderError> {
    cached_product(&state.pool, &state.product_cache, product_id)
        .await
        .map_err(internal("read product cache"))?
        .map(Json)
        .ok_or(OrderError::ProductNotCached)
}
