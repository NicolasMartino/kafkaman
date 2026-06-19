mod changelog;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use kafkaman_core::{Envelope, KafkaMessage};
use kafkaman_rdkafka::RdkafkaPublisher;
use kafkaman_sqlx::{enqueue, migrate, OutboxTable, ResolvedConfig};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    cfg: Arc<ResolvedConfig>,
}

#[derive(Debug, Deserialize)]
struct CreateOrderRequest {
    order_id: Option<Uuid>,
    description: String,
}

#[derive(Debug, Serialize)]
struct CreateOrderResponse {
    order_id: Uuid,
    message_id: Uuid,
}

#[derive(Debug, Serialize)]
pub struct OrderCreated {
    order_id: Uuid,
    description: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the example Postgres database");
    let brokers =
        std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must point to Kafka/Redpanda");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    ensure_business_schema(&pool).await?;

    let cfg =
        ResolvedConfig::default().with_message(OrderCreated::descriptor().expect("valid message"));
    migrate(&pool, &cfg, &changelog::changelog()).await?;

    let table = OutboxTable::for_message::<OrderCreated>(&cfg)?;
    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_pool = pool.clone();
    let relay_cfg = cfg.relay.clone();

    let worker = tokio::spawn(async move {
        kafkaman_worker::run(worker_pool, publisher, table, relay_cfg, worker_shutdown).await
    });

    let app = Router::new()
        .route("/orders", post(create_order))
        .with_state(AppState {
            pool,
            cfg: Arc::new(cfg),
        });

    let addr: SocketAddr = "0.0.0.0:3000".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        })
        .await?;

    worker.await??;
    Ok(())
}

async fn ensure_business_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
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
