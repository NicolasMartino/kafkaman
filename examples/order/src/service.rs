//! Booting the `order` service.
//!
//! Factored out of `main.rs` so the `distributed-cache` test can start a real
//! instance in-process on an ephemeral port, driving exactly the code path the
//! binary does.

use std::sync::Arc;

use example_contracts::{OrderSnapshot, ProductSnapshot};
use kafkaman::RuntimeBuilder;
use sqlx::postgres::PgPoolOptions;

use crate::boot::{BoxError, RunningService, ServiceOptions};
use crate::{build_router, ensure_business_schema, AppState};

/// Declare what this service does with each message type, then serve.
///
/// Two roles is the whole of it. `publish::<OrderSnapshot>()` means an outbox
/// table and a relay; `cache::<ProductSnapshot>()` means a received table, a
/// cache table, an ingester, a dispatcher, and a no-op handler. Deriving that
/// from the declaration is what the builder is for — a service author should not
/// have to remember it, restate it in a changelog, or number the changesets.
pub async fn start(options: ServiceOptions) -> Result<RunningService, BoxError> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&options.database_url)
        .await?;
    ensure_business_schema(&pool).await?; // business tables stay the host's

    let mut builder = RuntimeBuilder::new()
        .pool(pool.clone())
        .brokers(&options.brokers)
        .consumer_group(&options.consumer_group)
        .publish::<OrderSnapshot>()
        .cache::<ProductSnapshot>();
    // Left unset when absent so `build()` reports it, rather than this function
    // inventing a worse message.
    if let Some(config) = options.config {
        builder = builder.config(config);
    }
    let runtime = builder.build().await?;

    let state = AppState::new(pool, Arc::clone(runtime.context().config()))?;
    let listener = tokio::net::TcpListener::bind(options.bind).await?;
    let service = kafkaman::axum::serve(listener, build_router(state))
        .with_runtime(runtime)
        .spawn()?;

    tracing::info!(addr = %service.addr(), "order service listening");
    Ok(service)
}
