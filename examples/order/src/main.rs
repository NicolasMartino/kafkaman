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
    // Assembled before telemetry is installed, so a missing variable or an
    // unparseable address exits while there is still nothing to flush.
    // `Config::discover` only walks the filesystem looking for `kafkaman.toml` —
    // it emits no events — so nothing is lost by running it ahead of the
    // subscriber either.
    let options = ServiceOptions {
        database_url: required_env("DATABASE_URL", "points to this service's Postgres database")?,
        brokers: required_env("KAFKA_BROKERS", "points to Kafka/Redpanda")?,
        bind: std::env::var("BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3001".to_owned())
            .parse::<SocketAddr>()?,
        consumer_group: std::env::var("KAFKA_CONSUMER_GROUP")
            .unwrap_or_else(|_| "order-service".to_owned()),
        // Discovery walks up from the current directory, which is why this
        // binary is meant to be run from `examples/order`.
        config: Config::discover()?,
    };

    // Before any kafkaman loop exists: instruments are created as each loop is
    // built, and one created ahead of the meter provider is bound to the no-op
    // provider for the life of the process.
    let telemetry = example_telemetry::init("kafkaman-example-order")?;

    // Every way out of `run` — a boot failure, a dead loop, or Ctrl-C — comes
    // back through here, so the flush covers all of them. A process that exits
    // without flushing loses the telemetry explaining why it exited.
    let result = run(options).await;
    let telemetry_result = telemetry.shutdown();

    result?;
    telemetry_result
}

/// Own the service lifecycle, so `main` can own the telemetry lifecycle around it.
async fn run(options: ServiceOptions) -> Result<(), BoxError> {
    let mut service = start(options).await?;

    // Supervise rather than only joining at shutdown: if a loop dies the process
    // must stop, instead of serving requests whose effects nothing will carry.
    //
    // The arm below is a `match` rather than a `?`. `?` inside a `select!` arm
    // returns from this function on the spot, skipping the drain underneath it —
    // and with it the telemetry flush — on exactly the failure this supervision
    // exists to report.
    let wait_result: Result<(), BoxError> = tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        result = service.wait() => match result {
            Ok(()) => Err("a kafkaman loop exited before shutdown was requested".into()),
            Err(err) => Err(err.into()),
        },
    };

    let shutdown_result: Result<(), BoxError> = service.shutdown().await.map_err(Into::into);

    // The loop's own error first: the drain failing afterwards is a consequence
    // worth logging, not the thing that went wrong.
    wait_result?;
    shutdown_result
}
