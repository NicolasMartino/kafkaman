pub mod changelog;

use std::sync::Arc;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use kafkaman::sqlx::{enqueue, ResolvedConfig};
use kafkaman::{Envelope, KafkaMessage};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
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

async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<Json<CreateOrderResponse>, String> {
    let order_id = request.order_id.unwrap_or_else(Uuid::new_v4);
    let event = Envelope::new(OrderCreated {
        order_id,
        description: request.description.clone(),
    });
    let message_id = event.message_id;

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|err| format!("begin transaction: {err}"))?;

    sqlx::query("INSERT INTO orders (order_id, description) VALUES ($1, $2)")
        .bind(order_id)
        .bind(&request.description)
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("insert order: {err}"))?;

    enqueue(&mut tx, &state.cfg, &event)
        .await
        .map_err(|err| format!("enqueue event: {err}"))?;

    tx.commit()
        .await
        .map_err(|err| format!("commit transaction: {err}"))?;

    Ok(Json(CreateOrderResponse {
        order_id,
        message_id,
    }))
}
