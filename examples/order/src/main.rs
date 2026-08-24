//! Binary entry point for the `order` example service.
//!
//! Run it from this directory, so `Config::discover()` finds the `kafkaman.toml`
//! sitting next to `Cargo.toml`:
//!
//! ```text
//! cd examples/order
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/order_service \
//! KAFKA_BROKERS=localhost:9092 \
//! cargo run
//! ```

use std::net::SocketAddr;

use example_order::{start, BoxError, ServiceOptions};
use kafkaman::config::Config;

/// Read a required environment variable, reporting the variable name rather than
/// panicking with a backtrace.
fn required_env(name: &str, purpose: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} must be set: {purpose}").into())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Without a subscriber every `tracing::` call in this binary — and in the
    // three kafkaman loops it supervises — is silently discarded.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let database_url = required_env("DATABASE_URL", "points to this service's Postgres database")?;
    let brokers = required_env("KAFKA_BROKERS", "points to Kafka/Redpanda")?;
    let bind: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3001".to_owned())
        .parse()?;
    let consumer_group =
        std::env::var("KAFKA_CONSUMER_GROUP").unwrap_or_else(|_| "order-service".to_owned());

    let mut service = start(ServiceOptions {
        database_url,
        brokers,
        bind,
        consumer_group,
        // Discovery walks up from the current directory, which is why this
        // binary is meant to be run from `examples/order`.
        config: Config::discover()?,
    })
    .await?;

    // Supervise rather than only joining at shutdown: if a loop dies the process
    // must stop, instead of serving requests whose effects nothing will carry.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        result = service.wait() => {
            result?;
            return Err("a kafkaman loop exited before shutdown was requested".into());
        }
    }

    service.shutdown().await?;
    Ok(())
}
