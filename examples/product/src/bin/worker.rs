//! Worker entry point for the `product` example service.
//!
//! This binary declares the same message roles as the HTTP service, but starts
//! only kafkaman's background pipeline. It does not bind an HTTP listener.

use example_contracts::{OrderSnapshot, ProductSnapshot};
use example_product::{derive_availability, ensure_business_schema, BoxError};
use kafkaman::config::Config;
use kafkaman::{RuntimeBuilder, Subsystems};
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

/// Read a required environment variable, reporting the variable name rather than
/// panicking with a backtrace.
fn required_env(name: &str, purpose: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} must be set: {purpose}").into())
}

#[derive(Debug)]
struct WorkerOptions {
    database_url: String,
    brokers: String,
    consumer_group: String,
    config: Option<Config>,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let options = WorkerOptions {
        database_url: required_env("DATABASE_URL", "points to this service's Postgres database")?,
        brokers: required_env("KAFKA_BROKERS", "points to Kafka/Redpanda")?,
        consumer_group: std::env::var("KAFKA_CONSUMER_GROUP")
            .unwrap_or_else(|_| "product-service".to_owned()),
        // Discovery walks up from the current directory, which is why this
        // binary is meant to be run from `examples/product`.
        config: Config::discover()?,
    };

    let telemetry = kafkaman_otel::init("kafkaman-example-product-worker")?;
    let result = run(options).await;
    let flushed = telemetry.shutdown();

    if let (Err(_), Err(flush)) = (&result, &flushed) {
        tracing::error!(error = %flush, "the telemetry flush failed as well");
    }

    result?;
    flushed.map_err(Into::into)
}

async fn run(options: WorkerOptions) -> Result<(), BoxError> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&options.database_url)
        .await?;
    ensure_business_schema(&pool).await?;

    let mut builder = RuntimeBuilder::new()
        .pool(pool)
        .brokers(&options.brokers)
        .consumer_group(&options.consumer_group)
        .subsystems(Subsystems::PIPELINE)
        .publish::<ProductSnapshot>()
        .handle::<OrderSnapshot, _>(|order, mut cx| {
            Box::pin(async move { derive_availability(&order, &mut cx).await })
        });
    if let Some(config) = options.config {
        builder = builder.config(config);
    }
    let runtime = builder.build().await?;

    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to wait for Ctrl-C");
        }
        stop.cancel();
    });

    let result = runtime.run(shutdown).await;
    signal_task.abort();
    result.map_err(Into::into)
}
