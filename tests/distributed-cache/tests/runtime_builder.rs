//! What `RuntimeBuilder::build()` does against real infrastructure, and — just
//! as load-bearing — what it does *not* do.
//!
//! # Why one test function
//!
//! One PostgreSQL and one Redpanda per test function, and these assertions share
//! a cluster deliberately: they are cheap, they are ordered, and the interesting
//! ones are about the *absence* of side effects, which is only meaningful
//! against a broker and database that the earlier assertions have already used.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use distributed_cache_tests::{service_config, Cluster, TestResult};
use kafkaman::sqlx::ResolvedConfig;
use kafkaman::{BuildError, CancellationToken, KafkaMessage, RuntimeBuilder, Subsystems};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// The two entity types, declared here rather than pulled from
/// `example-contracts`.
///
/// This package deliberately does not depend on the examples' Kafka contract —
/// its sibling suite speaks the services' HTTP surface and must not be able to
/// compile against a shape only the wire has. Nothing about assembling a runtime
/// needs the real payloads either; what has to match the shipped services is the
/// message type and topic *names*, because the provisioner creates those topics
/// and `examples/order/kafkaman.toml` names those types under
/// `[retry.messages]`.
#[derive(Serialize, Deserialize)]
struct OrderSnapshot;

impl KafkaMessage for OrderSnapshot {
    const MESSAGE_TYPE: &'static str = "order_snapshot";
    const TOPIC: &'static str = "orders";

    fn entity_key(&self) -> String {
        "order".to_owned()
    }
}

#[derive(Serialize, Deserialize)]
struct ProductSnapshot;

impl KafkaMessage for ProductSnapshot {
    const MESSAGE_TYPE: &'static str = "product_snapshot";
    const TOPIC: &'static str = "products";

    fn entity_key(&self) -> String {
        "product".to_owned()
    }
}

#[tokio::test]
async fn the_builder_converges_topics_migrates_and_starts_nothing_until_told() -> TestResult {
    let cluster = Cluster::start().await?;

    convergence_runs_before_the_migration(&cluster).await?;

    cluster.provision_topics().await?;

    let pool = the_generated_changelog_creates_every_table_the_roles_imply(&cluster).await?;
    building_twice_is_idempotent(&cluster).await?;
    build_starts_no_loops(&cluster).await?;
    subsystem_selection_starts_only_selected_loops(&cluster).await?;
    empty_subsystem_selection_starts_no_loops(&cluster).await?;
    a_pre_cancelled_runtime_drains_without_working(&cluster).await?;
    the_router_is_a_plain_message_router(&cluster).await?;

    drop(pool);
    Ok(())
}

// ---------------------------------------------------------------------------

async fn open_pool(url: &str) -> TestResult<PgPool> {
    // `migrate` holds an advisory-lock connection plus a changeset connection,
    // so two is the floor rather than a comfort margin.
    Ok(PgPoolOptions::new().max_connections(4).connect(url).await?)
}

/// The order service's roles: publishes orders, caches products.
fn order_roles(pool: PgPool, brokers: &str, config: kafkaman::config::Config) -> RuntimeBuilder {
    RuntimeBuilder::new()
        .config(config)
        .pool(pool)
        .brokers(brokers)
        .consumer_group("runtime-builder-test")
        .publish::<OrderSnapshot>()
        .cache::<ProductSnapshot>()
}

async fn table_exists(pool: &PgPool, schema: &str, table: &str) -> TestResult<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.tables
             WHERE table_schema = $1 AND table_name = $2
         )",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await?)
}

// ---------------------------------------------------------------------------

/// Boot is the last moment at which refusing to start is still cheap, so
/// convergence runs before the migration — and a service pointed at an
/// unprovisioned broker must fail there rather than after creating its tables.
async fn convergence_runs_before_the_migration(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_unprovisioned").await?;
    let pool = open_pool(&url).await?;

    let error = order_roles(pool.clone(), cluster.brokers(), service_config("order")?)
        .build()
        .await
        .expect_err("a verify-mode boot against a broker with no topics must fail");

    assert!(matches!(error, BuildError::Topics(_)), "{error:?}");
    let rendered = error.to_string();
    assert!(rendered.contains("orders"), "{rendered}");
    assert!(
        rendered.contains("before any loop started"),
        "the error must say why it fired at boot: {rendered}"
    );

    // And nothing was created on the way to that failure.
    let cfg = ResolvedConfig::from_config(
        Some(&service_config("order")?),
        [OrderSnapshot::descriptor()?, ProductSnapshot::descriptor()?],
    )?;
    assert!(
        !table_exists(&pool, cfg.schema.as_str(), "outbox_order_snapshot").await?,
        "a service that refused to start must not leave tables behind"
    );

    Ok(())
}

/// The generated changesets implied by the roles, without anyone having
/// numbered them by hand.
async fn the_generated_changelog_creates_every_table_the_roles_imply(
    cluster: &Cluster,
) -> TestResult<PgPool> {
    let url = cluster.create_database("builder_migrated").await?;
    let pool = open_pool(&url).await?;

    let runtime = order_roles(pool.clone(), cluster.brokers(), service_config("order")?)
        .build()
        .await?;

    let schema = runtime.context().config().schema.as_str().to_owned();
    for table in [
        "outbox_order_snapshot",
        "received_product_snapshot",
        "cache_product_snapshot",
    ] {
        assert!(
            table_exists(&pool, &schema, table).await?,
            "`{table}` should have been created from the declared roles"
        );
    }
    // The mirror image is *not* created: this service publishes orders and
    // caches products, so it has no order cache and no product outbox.
    for absent in ["cache_order_snapshot", "outbox_product_snapshot"] {
        assert!(
            !table_exists(&pool, &schema, absent).await?,
            "`{absent}` is not implied by these roles and must not exist"
        );
    }

    // Every generated changeset is recorded, plus `InitSchema` at version 1.
    // Four: `InitSchema`, and one create apiece for the order outbox, the
    // product received table, and the product cache. One per table, because V1
    // ships no upgrade changesets — see the V1 legacy-removal compat note.
    let versions: Vec<i64> = sqlx::query_scalar(&format!(
        "SELECT version FROM {schema}.changelog_history ORDER BY version"
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(versions.len(), 4, "got {versions:?}");
    assert_eq!(versions[0], 1, "InitSchema must sort first");
    assert!(
        versions[1..].iter().all(|version| *version >= 1_000),
        "generated versions must stay out of the reserved range: {versions:?}"
    );

    Ok(pool)
}

/// Adopting the builder is not a one-way door and re-running it is not a
/// migration event: every generated create is `IF NOT EXISTS`, and the second
/// build finds its own history rows already applied.
async fn building_twice_is_idempotent(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_twice").await?;
    let pool = open_pool(&url).await?;

    for _ in 0..2 {
        order_roles(pool.clone(), cluster.brokers(), service_config("order")?)
            .build()
            .await?;
    }

    let schema = "kafkaman";
    let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {schema}.changelog_history"))
        .fetch_one(&pool)
        .await?;
    assert_eq!(rows, 4, "the second build must not duplicate history rows");
    Ok(())
}

/// `build()` assembles; `into_tasks()` starts. Keeping them apart is what lets a
/// test build a runtime without a broker connection going live, and it keeps the
/// point at which telemetry instruments would bind next to a call the host
/// writes rather than hidden inside `build()`.
async fn build_starts_no_loops(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_no_loops").await?;
    let pool = open_pool(&url).await?;

    let runtime = order_roles(pool.clone(), cluster.brokers(), service_config("order")?)
        .build()
        .await?;

    // Nothing has been claimed, because nothing is running. A dispatcher or a
    // relay would have polled at least once inside this window.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let claimed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM kafkaman.received_product_snapshot WHERE status <> 'pending'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(claimed, 0);

    let tasks = runtime.into_tasks()?;
    assert_eq!(
        tasks.len(),
        4,
        "one relay for the published type, one ingester and one dispatcher for the \
         consumed one, plus the queue-depth sampler that covers both — and no \
         purger, because these roles declare no `[retention]`"
    );
    tasks.shutdown().await?;
    Ok(())
}

/// Worker-role binaries can keep the declarative role surface while naming the
/// loops this process owns. Here the service still publishes orders and caches
/// products, but it starts only the relay and dispatcher: no Kafka consumer, no
/// purger, and no queue sampler.
async fn subsystem_selection_starts_only_selected_loops(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_subsystems").await?;
    let pool = open_pool(&url).await?;

    let runtime = order_roles(pool, cluster.brokers(), service_config("order")?)
        .subsystems(Subsystems::RELAY | Subsystems::DISPATCH)
        .build()
        .await?;

    let tasks = runtime.into_tasks()?;
    assert_eq!(
        tasks.len(),
        2,
        "only the selected relay and dispatcher loops should start"
    );
    tasks.shutdown().await?;
    Ok(())
}

/// An empty selector is useful for migration-only probes: roles still converge
/// topics and schema, but the resulting runtime owns no background work.
async fn empty_subsystem_selection_starts_no_loops(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_empty_subsystems").await?;
    let pool = open_pool(&url).await?;

    let runtime = order_roles(pool, cluster.brokers(), service_config("order")?)
        .subsystems(Subsystems::empty())
        .build()
        .await?;

    let tasks = runtime.into_tasks()?;
    assert_eq!(tasks.len(), 0, "no selected subsystem should mean no loops");
    tasks.shutdown().await?;
    Ok(())
}

/// A runtime handed an already-cancelled token must notice before doing a
/// batch's worth of work, and `run` must return rather than hang or exit.
async fn a_pre_cancelled_runtime_drains_without_working(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_cancelled").await?;
    let pool = open_pool(&url).await?;

    let runtime = order_roles(pool.clone(), cluster.brokers(), service_config("order")?)
        .build()
        .await?;

    let shutdown = CancellationToken::new();
    shutdown.cancel();

    // The deadline is the assertion: `run` returning at all is what proves the
    // loops observed the token instead of settling into their poll intervals.
    tokio::time::timeout(Duration::from_secs(20), runtime.run(shutdown))
        .await
        .expect("a pre-cancelled runtime must drain promptly")?;
    Ok(())
}

/// Role registration yields an ordinary `MessageRouter`. This is the structural
/// proof that a future wrapping hook — a Tower-style layer, whenever that
/// surface is actually designed — needs no change to roles at all.
async fn the_router_is_a_plain_message_router(cluster: &Cluster) -> TestResult {
    let url = cluster.create_database("builder_router").await?;
    let pool = open_pool(&url).await?;

    let runtime = order_roles(pool.clone(), cluster.brokers(), service_config("order")?)
        .build()
        .await?;

    // Hand-wrapped, exactly as an external layer would have to.
    let wrapped = runtime.router().clone();
    let rendered = format!("{wrapped:?}");
    assert!(
        rendered.contains("product_snapshot"),
        "`cache::<T>()` must install a handler, so the type can never report a \
         missing handler: {rendered}"
    );
    Ok(())
}
