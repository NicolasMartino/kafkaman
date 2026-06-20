use std::net::SocketAddr;
use std::sync::Arc;

use axum_outbox::{build_router, changelog, ensure_business_schema, AppState, OrderCreated};
use kafkaman::config::Config;
use kafkaman::sqlx::{migrate, MigrationContext, OutboxTable, ResolvedConfig};
use kafkaman::{worker, KafkaMessage};
use kafkaman_rdkafka::RdkafkaPublisher;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must point to the example Postgres database");
    let brokers =
        std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must point to Kafka/Redpanda");

    // Discover and fully resolve config before any database work, so a missing or
    // mistyped required key (or an invalid retry policy) fails fast without first
    // opening a pool or touching the business schema.
    let cfg_file = Config::discover()?;
    let cfg = ResolvedConfig::from_config(
        cfg_file.as_ref(),
        [OrderCreated::descriptor().expect("valid message")],
    )?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    ensure_business_schema(&pool).await?;

    let migration_report = migrate(
        &pool,
        &cfg,
        &MigrationContext::from_env(),
        &changelog::changelog(),
    )
    .await?;
    tracing::info!(?migration_report, "kafkaman migrations converged");

    let table = OutboxTable::for_message::<OrderCreated>(&cfg)?;
    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_pool = pool.clone();
    let relay_cfg = cfg.relay.clone();

    let worker = tokio::spawn(async move {
        worker::run(worker_pool, publisher, table, relay_cfg, worker_shutdown).await
    });

    let app = build_router(AppState {
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
