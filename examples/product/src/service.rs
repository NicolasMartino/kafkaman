//! Booting the `product` service through the runtime builder.
//!
//! The blessed path. Compare `service_manual.rs`, which produces an observably
//! identical runtime out of the low-level primitives and is roughly three times
//! the size.

use std::sync::Arc;

use example_contracts::{OrderSnapshot, ProductSnapshot};
use kafkaman::RuntimeBuilder;
use sqlx::postgres::PgPoolOptions;

use crate::boot::{BoxError, RunningService, ServiceOptions};
use crate::{build_router, derive_availability, ensure_business_schema, AppState};

/// Declare what this service does with each message type, then serve.
///
/// `publish::<ProductSnapshot>()` means an outbox table and a relay.
/// `handle::<OrderSnapshot>` means a received table, a cache table, an ingester,
/// a dispatcher, and the derivation below — which runs *after* the cache upsert,
/// so it sees the incoming order already applied.
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
        .publish::<ProductSnapshot>()
        .handle::<OrderSnapshot, _>(|order, mut cx| {
            Box::pin(async move { derive_availability(&order, &mut cx).await })
        });
    // Left unset when absent so `build()` reports it, rather than this function
    // inventing a worse message.
    if let Some(config) = options.config {
        builder = builder.config(config);
    }
    let runtime = builder.build().await?;

    let state = AppState {
        pool,
        cfg: Arc::clone(runtime.context().config()),
    };
    let listener = tokio::net::TcpListener::bind(options.bind).await?;
    let service = kafkaman::axum::serve(listener, build_router(state))
        .with_runtime(runtime)
        .spawn()?;

    tracing::info!(addr = %service.addr(), "product service listening");
    Ok(RunningService::Builder(service))
}
