use kafkaman_config::Config;
use kafkaman_core::{Envelope, KafkaMessage, ReceiveStatus, RetentionClass};
use kafkaman_sqlx::{
    changelog, dispatch_once, migrate, CacheTable, CreateReceivedTable, InitSchema, MessageRouter,
    MigrationContext, Replay,
};
use kafkaman_test::Harness;
use kafkaman_worker::BoxError;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Notify;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn entity_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn retry_test_config(schema: &str, max_attempts: u32) -> Config {
    Config::from_str(&format!(
        r#"
        [database]
        schema = "{schema}"

        [relay]
        worker_id = "worker-entity-test"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "50ms"

        [retry.defaults]
        max_attempts = {max_attempts}
        initial_backoff = "2s"
        max_backoff = "5s"
        multiplier = 2.0
        errors_limit = 20
        dlq = "table"
        "#
    ))
    .expect("retry test config is valid")
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProductSnapshot {
    product_id: String,
    name: String,
}

impl KafkaMessage for ProductSnapshot {
    const MESSAGE_TYPE: &'static str = "product_snapshot";
    const TOPIC: &'static str = "products";

    fn partition_key(&self) -> Option<String> {
        Some(self.product_id.clone())
    }

    fn entity_key(&self, _message_id: uuid::Uuid) -> String {
        self.product_id.clone()
    }

    fn retention_class() -> RetentionClass {
        RetentionClass::Compact
    }
}

#[tokio::test]
async fn retry_after_newer_applied_does_not_regress_cache() -> TestResult {
    let _test_guard = entity_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let old = product("p-1", "old").with_idempotency_key("idem-p1-old");
    let new = product("p-1", "new").with_idempotency_key("idem-p1-new");
    assert!(harness.insert_received(&old, 0, 10, Some(b"p-1")).await?);
    assert!(harness.insert_received(&new, 0, 11, Some(b"p-1")).await?);

    let router = MessageRouter::new().handler::<ProductSnapshot>(|_conn, meta, _msg| {
        Box::pin(async move {
            if meta.source_offset == 10 && meta.attempts == 0 {
                Err(kafkaman_sqlx::Error::Handler(
                    "transient old state".to_owned(),
                ))
            } else {
                Ok(())
            }
        })
    });

    let now = OffsetDateTime::now_utc();
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.failed, 1);

    let second = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(second.processed, 1);
    assert_cache_state(&harness, "p-1", "new", 11).await?;

    let retried = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + TimeDuration::seconds(10),
    )
    .await?;
    assert_eq!(retried.processed, 1);
    assert_cache_state(&harness, "p-1", "new", 11).await?;

    Ok(())
}

#[tokio::test]
async fn redrive_after_newer_applied_does_not_regress_cache() -> TestResult {
    let _test_guard = entity_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let schema = format!("kafkaman_entity_{}", uuid::Uuid::new_v4().simple());
    let harness =
        Harness::connect_with_config(&database_url, retry_test_config(&schema, 1)).await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let old = product("p-2", "old").with_idempotency_key("idem-p2-old");
    let new = product("p-2", "new").with_idempotency_key("idem-p2-new");
    assert!(harness.insert_received(&old, 0, 20, Some(b"p-2")).await?);
    assert!(harness.insert_received(&new, 0, 21, Some(b"p-2")).await?);

    let router = MessageRouter::new().handler::<ProductSnapshot>(|_conn, meta, _msg| {
        Box::pin(async move {
            if meta.source_offset == 20 && meta.attempts == 0 {
                Err(kafkaman_sqlx::Error::Handler(
                    "terminal old state".to_owned(),
                ))
            } else {
                Ok(())
            }
        })
    });

    let now = OffsetDateTime::now_utc();
    let failed = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(failed.failed, 1);
    let old_row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("idem-p2-old")
        .await?;
    assert_eq!(old_row.status, ReceiveStatus::Failed);

    let newer = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(newer.processed, 1);
    assert_cache_state(&harness, "p-2", "new", 21).await?;

    let cfg = harness.config();
    let ctx = MigrationContext::default().with_context("prod");
    let redrive = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, ProductSnapshot::descriptor()?),
        Replay::received::<ProductSnapshot>(30_000)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(10)
            .contexts(&["prod"]),
    ];
    migrate(harness.pool(), &cfg, &ctx, &redrive).await?;

    let redriven = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(redriven.processed, 1);
    assert_cache_state(&harness, "p-2", "new", 21).await?;

    Ok(())
}

#[tokio::test]
async fn concurrent_dispatch_of_two_states_converges_to_newer() -> TestResult {
    let _test_guard = entity_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let old = product("p-3", "old").with_idempotency_key("idem-p3-old");
    let new = product("p-3", "new").with_idempotency_key("idem-p3-new");
    assert!(harness.insert_received(&old, 0, 30, Some(b"p-3")).await?);
    assert!(harness.insert_received(&new, 0, 31, Some(b"p-3")).await?);

    let old_started = Arc::new(Notify::new());
    let release_old = Arc::new(Notify::new());
    let old_started_for_handler = Arc::clone(&old_started);
    let release_old_for_handler = Arc::clone(&release_old);
    let router = Arc::new(MessageRouter::new().handler::<ProductSnapshot>(
        move |_conn, meta, _msg| {
            let old_started = Arc::clone(&old_started_for_handler);
            let release_old = Arc::clone(&release_old_for_handler);
            Box::pin(async move {
                if meta.source_offset == 30 {
                    old_started.notify_one();
                    release_old.notified().await;
                }
                Ok(())
            })
        },
    ));

    let now = OffsetDateTime::now_utc();
    let pool_for_old = harness.pool().clone();
    let table_for_old = table.clone();
    let router_for_old = Arc::clone(&router);
    let old_dispatch = tokio::spawn(async move {
        dispatch_once(&pool_for_old, &table_for_old, router_for_old.as_ref(), now).await
    });

    old_started.notified().await;

    let pool_for_new = harness.pool().clone();
    let table_for_new = table.clone();
    let router_for_new = Arc::clone(&router);
    let new_dispatch = tokio::spawn(async move {
        dispatch_once(&pool_for_new, &table_for_new, router_for_new.as_ref(), now).await
    });

    let new_stats = new_dispatch.await??;
    assert_eq!(new_stats.claimed, 1);
    assert_eq!(new_stats.processed, 1);
    assert_cache_state(&harness, "p-3", "new", 31).await?;

    release_old.notify_one();
    let old_stats = old_dispatch.await??;
    assert_eq!(old_stats.claimed, 1);
    assert_eq!(old_stats.processed, 1);
    assert_cache_state(&harness, "p-3", "new", 31).await?;

    Ok(())
}

fn product(product_id: &str, name: &str) -> Envelope<ProductSnapshot> {
    Envelope::new(ProductSnapshot {
        product_id: product_id.to_owned(),
        name: name.to_owned(),
    })
}

async fn assert_cache_state(
    harness: &Harness,
    entity_key: &str,
    name: &str,
    applied_offset: i64,
) -> TestResult {
    let cache = CacheTable::for_message::<ProductSnapshot>(&harness.config())?;
    let sql = format!(
        "SELECT payload, applied_offset FROM {} WHERE entity_key = $1",
        cache.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(entity_key)
        .fetch_one(harness.pool())
        .await?;
    let payload: serde_json::Value = row.try_get("payload")?;
    let offset: i64 = row.try_get("applied_offset")?;
    assert_eq!(payload["name"], name);
    assert_eq!(offset, applied_offset);
    Ok(())
}

async fn start_postgres() -> Result<(ContainerAsync<GenericImage>, String), BoxError> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await?;
    let host = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(port).await?;
    let database_url = format!("postgres://postgres:postgres@{host}:{host_port}/postgres");

    Ok((container, database_url))
}
