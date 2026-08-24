//! Booting the `product` service by hand, out of the low-level primitives.
//!
//! # Why this file exists
//!
//! `RuntimeBuilder` is the default service UX, not a closed framework. Some
//! users will need hand-written migrations, custom supervision, or a topology
//! the builder does not express, and the low-level API stays public and
//! supported for them.
//!
//! Documenting that is cheap and proves nothing. So `Services::start` in
//! `tests/distributed-cache` is parameterised over the boot mode and runs the
//! whole end-to-end suite against *both* paths: if the escape hatch stops
//! producing an equivalent runtime, a test says so.
//!
//! `product` rather than `order`, deliberately. It exercises the hard path — a
//! post-upsert handler, consume-then-produce, and the availability derivation —
//! whereas `order`'s `cache::<T>()` is trivially equivalent to a no-op handler
//! and would prove almost nothing.
//!
//! # What the comparison shows
//!
//! Everything below is derived by [`crate::service`] from two role declarations:
//! the changelog and its version numbers, the three table handles, the topic
//! convergence call, the migration, the publisher and consumer, the subscribe,
//! the four spawned loops, the cancellation token, and the drain.

use std::sync::Arc;

use example_contracts::{OrderSnapshot, ProductSnapshot};
use kafkaman::rdkafka::{converge_topics, RdkafkaConsumer, RdkafkaPublisher, TopicAdmin};
use kafkaman::sqlx::{
    migrate, try_changelog, Changeset, CreateCacheTable, CreateOutboxTable, CreateReceivedTable,
    InitSchema, MigrationContext, OutboxTable, ReceivedTable, ResolvedConfig,
};
use kafkaman::{worker, KafkaMessage};
use sqlx::postgres::PgPoolOptions;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::boot::{BoxError, ManualService, RunningService, ServiceOptions};
use crate::{build_router, dispatch_router, ensure_business_schema, AppState};

/// The kafkaman changelog, written out and numbered by hand.
///
/// This is what the builder replaces. The numbers are the reason: they are the
/// durable identity of every schema change in `changelog_history`, and choosing
/// them at the call site makes *registration order* that identity. Two branches
/// that both add a table pick the same next integer and collide; a service that
/// reorders its declarations renumbers changesets that already applied.
///
/// A database migrated by this changelog and later booted through the builder
/// keeps these history rows and gains the generated ones. Nothing breaks —
/// every create is `IF NOT EXISTS` — but the history carries both, which is
/// worth knowing before switching a live service over.
fn changelog() -> Result<Vec<Box<dyn Changeset>>, BoxError> {
    Ok(try_changelog![
        InitSchema,
        CreateOutboxTable::new(2, ProductSnapshot::descriptor()?),
        CreateReceivedTable::new(3, OrderSnapshot::descriptor()?),
        CreateCacheTable::new(4, OrderSnapshot::descriptor()?),
    ]?)
}

/// Assemble the same runtime `service::start` declares, by hand.
pub async fn start(options: ServiceOptions) -> Result<RunningService, BoxError> {
    // Both types are registered: `ProductSnapshot` because this service
    // publishes it, `OrderSnapshot` because it consumes it. Registration is what
    // makes `OutboxTable::for_message`, `ReceivedTable::for_message` and
    // `CacheTable::for_message` resolve, and what lets `[retry.messages.*]`
    // reject a policy for a type nobody handles.
    let cfg = ResolvedConfig::from_config(
        options.config.as_ref(),
        [ProductSnapshot::descriptor()?, OrderSnapshot::descriptor()?],
    )?;

    // Convergence before any loop starts: a topic carrying entity snapshots must
    // be compacted whichever end of it this service is on, and boot is the last
    // moment at which refusing to start is still cheap. `verify` by default, so
    // this never creates anything.
    let admin = TopicAdmin::from_brokers(&options.brokers)?;
    converge_topics(&admin, cfg.topics, cfg.messages()).await?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&options.database_url)
        .await?;

    ensure_business_schema(&pool).await?;
    let report = migrate(&pool, &cfg, &MigrationContext::from_env(), &changelog()?).await?;
    tracing::info!(
        applied = report.applied_count(),
        "kafkaman schema converged"
    );

    let cfg = Arc::new(cfg);
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // 1. Relay: outbox rows -> Kafka.
    let outbox = OutboxTable::for_message::<ProductSnapshot>(&cfg)?;
    let publisher = RdkafkaPublisher::from_brokers(&options.brokers)?;
    {
        let pool = pool.clone();
        let relay_cfg = cfg.relay.clone();
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            worker::run(pool, publisher, outbox, relay_cfg, shutdown)
                .await
                .map_err(|err| Box::new(err) as BoxError)
        });
    }

    // 2. Ingester: Kafka -> received table, offsets committed only after the
    //    durable write.
    let consumer = RdkafkaConsumer::from_brokers(&options.brokers, &options.consumer_group)?;
    consumer.subscribe(&[OrderSnapshot::TOPIC])?;
    {
        let pool = pool.clone();
        let cfg = Arc::clone(&cfg);
        let shutdown = shutdown.clone();
        let retry_delay = cfg.relay.retry_after;
        tasks.spawn(async move {
            consumer
                .run_ingester::<OrderSnapshot>(&pool, &cfg, retry_delay, shutdown)
                .await
                .map(|_| ())
                .map_err(|err| Box::new(err) as BoxError)
        });
    }

    // 3. Dispatcher: received table -> cache upsert -> deriving handler ->
    //    republish, all in one transaction.
    let received = ReceivedTable::for_message::<OrderSnapshot>(&cfg)?;
    let router = dispatch_router(Arc::clone(&cfg))?;
    {
        let pool = pool.clone();
        let poll_interval = cfg.relay.poll_interval;
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            worker::run_dispatcher(pool, received, router, poll_interval, shutdown)
                .await
                .map_err(|err| Box::new(err) as BoxError)
        });
    }

    // 4. HTTP.
    let listener = tokio::net::TcpListener::bind(options.bind).await?;
    let addr = listener.local_addr()?;
    let app = build_router(AppState {
        pool,
        cfg: Arc::clone(&cfg),
    });
    {
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
                .map_err(|err| Box::new(err) as BoxError)
        });
    }

    tracing::info!(%addr, "product service listening (hand-wired)");
    Ok(RunningService::Manual(ManualService {
        addr,
        shutdown,
        tasks,
    }))
}
