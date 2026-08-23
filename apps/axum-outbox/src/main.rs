use std::net::SocketAddr;
use std::sync::Arc;

use axum_outbox::{build_router, changelog, ensure_business_schema, AppState, OrderCreated};
use kafkaman::config::Config;
use kafkaman::rdkafka::RdkafkaPublisher;
use kafkaman::sqlx::{migrate, MigrationContext, OutboxTable, ResolvedConfig};
use kafkaman::{worker, KafkaMessage};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Read a required environment variable, reporting the variable name rather than
/// panicking with a backtrace.
fn required_env(name: &str, purpose: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} must be set: {purpose}").into())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Without a subscriber every `tracing::` call in this binary — and in the
    // relay worker it supervises — is silently discarded.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let database_url = required_env("DATABASE_URL", "points to the example Postgres database")?;
    let brokers = required_env("KAFKA_BROKERS", "points to Kafka/Redpanda")?;
    let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_owned())
        .parse()?;

    // Discover and fully resolve config before any database work, so a missing or
    // mistyped required key (or an invalid retry policy) fails fast without first
    // opening a pool or touching the business schema.
    let cfg_file = Config::discover()?;
    let cfg = ResolvedConfig::from_config(cfg_file.as_ref(), [OrderCreated::descriptor()?])?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    ensure_business_schema(&pool).await?;

    let migration_report = migrate(
        &pool,
        &cfg,
        &MigrationContext::from_env(),
        &changelog::changelog()?,
    )
    .await?;
    tracing::info!(?migration_report, "kafkaman migrations converged");

    let table = OutboxTable::for_message::<OrderCreated>(&cfg)?;
    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_pool = pool.clone();
    let relay_cfg = cfg.relay.clone();

    let mut worker = tokio::spawn(async move {
        worker::run(worker_pool, publisher, table, relay_cfg, worker_shutdown).await
    });

    let app = build_router(AppState {
        pool,
        cfg: Arc::new(cfg),
    });

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "serving");

    // Supervise the relay rather than only joining it at shutdown: if the worker
    // task dies (or panics), the process must stop serving requests it can no
    // longer publish, instead of silently accepting writes into an outbox that
    // nothing is draining.
    let server = axum::serve(listener, app).with_graceful_shutdown({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        }
    });

    tokio::select! {
        result = server => result?,
        joined = &mut worker => {
            shutdown.cancel();
            tracing::error!("relay worker exited before shutdown was requested");
            joined??;
            return Err("relay worker exited unexpectedly".into());
        }
    }

    shutdown.cancel();
    worker.await??;
    Ok(())
}
