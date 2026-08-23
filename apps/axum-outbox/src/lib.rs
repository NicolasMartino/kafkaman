//! A worked example: an HTTP handler that writes a business row and an outbox
//! event in one transaction.
//!
//! The point of the example is that the two writes share the caller's
//! transaction, so a crash between them is impossible — there is no "between".
pub mod changelog;

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use kafkaman::sqlx::{enqueue, ResolvedConfig};
use kafkaman::{Envelope, IdempotencyIdentity, KafkaMessage};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AppState {
    pub pool: PgPool,
    pub cfg: Arc<ResolvedConfig>,
}

#[derive(Debug, Deserialize)]
pub struct CreateOrderRequest {
    pub order_id: Option<Uuid>,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct CreateOrderResponse {
    pub order_id: Uuid,
    pub message_id: Uuid,
}

#[derive(Debug, Serialize)]
pub struct OrderCreated {
    pub order_id: Uuid,
    pub description: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.to_string())
    }

    fn entity_key(&self) -> String {
        self.order_id.to_string()
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/orders", post(create_order))
        .with_state(state)
}

pub async fn ensure_business_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Namespace for order-created idempotency digests. Versioned so the derivation
/// can change later without colliding with keys already stored.
const ORDER_CREATED_IDEMPOTENCY_NAMESPACE: &str = "axum-outbox:order-created:v1";

/// What can go wrong handling `POST /orders`.
///
/// Modelled as a type rather than a `String` so each cause maps to the right
/// status code, and so the response body never carries the raw database error —
/// which leaks schema names, and sometimes connection details, to the caller.
#[derive(Debug)]
pub enum CreateOrderError {
    /// The same order id was already accepted.
    DuplicateOrder,
    /// The request was well-formed JSON but not a usable order.
    InvalidRequest(String),
    /// Anything else: logged in full, reported opaquely.
    Internal(String),
}

impl IntoResponse for CreateOrderError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::DuplicateOrder => (
                StatusCode::CONFLICT,
                "an order with this id already exists".to_owned(),
            ),
            Self::InvalidRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Internal(detail) => {
                // The detail goes to the operator, not the caller.
                tracing::error!(error = %detail, "create_order failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_owned(),
                )
            }
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<Json<CreateOrderResponse>, CreateOrderError> {
    if request.description.trim().is_empty() {
        return Err(CreateOrderError::InvalidRequest(
            "description must not be empty".to_owned(),
        ));
    }
    let order_id = request.order_id.unwrap_or_else(Uuid::new_v4);
    // The order id is this entity's business identity, so a retried POST that
    // carries the same order id derives the same key and is deduplicated rather
    // than enqueued twice. The namespace scopes the digest to this message type,
    // so an unrelated type deriving from the same uuid cannot collide with it.
    let identity = IdempotencyIdentity::derive(ORDER_CREATED_IDEMPOTENCY_NAMESPACE, order_id)
        .map_err(|err| CreateOrderError::Internal(format!("derive idempotency identity: {err}")))?;
    let event = Envelope::new(OrderCreated {
        order_id,
        description: request.description.clone(),
    })
    .try_with_idempotency_key(identity)
    .map_err(|err| CreateOrderError::Internal(format!("attach idempotency identity: {err}")))?;
    let message_id = event.message_id;

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|err| CreateOrderError::Internal(format!("begin transaction: {err}")))?;

    sqlx::query("INSERT INTO orders (order_id, description) VALUES ($1, $2)")
        .bind(order_id)
        .bind(&request.description)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            if is_unique_violation(&err) {
                CreateOrderError::DuplicateOrder
            } else {
                CreateOrderError::Internal(format!("insert order: {err}"))
            }
        })?;

    enqueue(&mut tx, &state.cfg, &event)
        .await
        .map_err(|err| CreateOrderError::Internal(format!("enqueue event: {err}")))?;

    tx.commit()
        .await
        .map_err(|err| CreateOrderError::Internal(format!("commit transaction: {err}")))?;

    Ok(Json(CreateOrderResponse {
        order_id,
        message_id,
    }))
}
