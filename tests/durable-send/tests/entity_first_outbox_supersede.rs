use std::time::Duration;

use kafkaman_core::{Envelope, KafkaMessage, MarkOutcome, OutboxStatus, RetentionClass};
use kafkaman_sqlx::{
    claim_batch, enqueue, mark_published, migrate, AddOutboxEntityKey, Changeset,
    CreateOutboxTable, InitSchema, MigrationContext, OutboxTable,
};
use kafkaman_test::Harness;
use kafkaman_worker::BoxError;
use serde::Serialize;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn supersede_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[derive(Clone, Debug, Serialize)]
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
async fn first_concurrent_enqueues_for_entity_serialize() -> TestResult {
    let _test_guard = supersede_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let cfg = harness.config();

    let old = product("p-1", "old").with_idempotency_key("idem-p1-old");
    let new = product("p-1", "new").with_idempotency_key("idem-p1-new");

    let mut first_tx = harness.pool().begin().await?;
    enqueue(&mut first_tx, &cfg, &old).await?;

    let pool_for_new = harness.pool().clone();
    let cfg_for_new = cfg.clone();
    let new_for_task = new.clone();
    let new_enqueue = tokio::spawn(async move {
        let mut tx = pool_for_new.begin().await?;
        enqueue(&mut tx, &cfg_for_new, &new_for_task).await?;
        tx.commit().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    tokio::task::yield_now().await;
    first_tx.commit().await?;
    timeout(Duration::from_secs(5), new_enqueue).await??;

    let old_row = harness
        .outbox_row::<ProductSnapshot>(old.message_id)
        .await?;
    let new_row = harness
        .outbox_row::<ProductSnapshot>(new.message_id)
        .await?;
    assert_eq!(old_row.status, OutboxStatus::Superseded);
    assert_eq!(new_row.status, OutboxStatus::Pending);
    assert_eq!(old_row.entity_key.as_deref(), Some("p-1"));
    assert_eq!(new_row.entity_key.as_deref(), Some("p-1"));

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].row.message_id, new.message_id);

    Ok(())
}

#[tokio::test]
async fn supersede_collapses_queued_updates() -> TestResult {
    let _test_guard = supersede_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;

    let first = product("p-2", "first").with_idempotency_key("idem-p2-first");
    let second = product("p-2", "second").with_idempotency_key("idem-p2-second");
    let third = product("p-2", "third").with_idempotency_key("idem-p2-third");

    harness.enqueue(&first).await?;
    harness.enqueue(&second).await?;
    harness.enqueue(&third).await?;

    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(first.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(second.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(third.message_id)
            .await?
            .status,
        OutboxStatus::Pending
    );

    let stats = harness.relay_once::<ProductSnapshot>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    let records = harness.published_on(ProductSnapshot::TOPIC);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].message_id, third.message_id);
    assert_eq!(records[0].payload["name"], "third");

    Ok(())
}

#[tokio::test]
async fn publishing_entity_blocks_newer_pending_claim_until_published() -> TestResult {
    let _test_guard = supersede_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;

    let first = product("p-3", "first").with_idempotency_key("idem-p3-first");
    let second = product("p-3", "second").with_idempotency_key("idem-p3-second");

    harness.enqueue(&first).await?;
    let mut tx = harness.pool().begin().await?;
    let first_claim = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].row.message_id, first.message_id);

    harness.enqueue(&second).await?;
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(first.message_id)
            .await?
            .status,
        OutboxStatus::Publishing
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(second.message_id)
            .await?
            .status,
        OutboxStatus::Pending
    );

    let mut blocked_tx = harness.pool().begin().await?;
    let blocked_claim = claim_batch(
        &mut blocked_tx,
        &table,
        "worker-b",
        Duration::from_secs(30),
        10,
    )
    .await?;
    blocked_tx.commit().await?;
    assert!(
        blocked_claim.is_empty(),
        "newer entity row must wait while an older row is publishing"
    );

    let outcome = mark_published(
        harness.pool(),
        &table,
        first.message_id,
        first_claim[0].claim_id,
    )
    .await?;
    assert_eq!(outcome, MarkOutcome::Updated);

    let mut next_tx = harness.pool().begin().await?;
    let next_claim = claim_batch(
        &mut next_tx,
        &table,
        "worker-b",
        Duration::from_secs(30),
        10,
    )
    .await?;
    next_tx.commit().await?;
    assert_eq!(next_claim.len(), 1);
    assert_eq!(next_claim[0].row.message_id, second.message_id);

    Ok(())
}

#[tokio::test]
async fn add_outbox_entity_key_upgrades_legacy_outbox_table() -> TestResult {
    let _test_guard = supersede_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = ProductSnapshot::descriptor()?;
    let table = OutboxTable::new(cfg.schema.clone(), descriptor.clone())?;

    let legacy_ddl = format!(
        "CREATE TABLE {} (
            message_id UUID PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'Pending',
            topic TEXT NOT NULL,
            correlation_id UUID NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        table.qualified_name()
    );
    sqlx::query(&legacy_ddl).execute(pool).await?;
    sqlx::query(&format!(
        "INSERT INTO {}.\"changelog_history\" (version, name) VALUES (2, 'create_outbox_table')",
        cfg.schema.quoted()
    ))
    .execute(pool)
    .await?;

    assert_eq!(entity_key_columns(pool, &cfg).await?, 0);

    let changesets: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(2, descriptor.clone())),
        Box::new(AddOutboxEntityKey::new(3, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(entity_key_columns(pool, &cfg).await?, 1);
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(entity_key_columns(pool, &cfg).await?, 1);

    Ok(())
}

fn product(product_id: &str, name: &str) -> Envelope<ProductSnapshot> {
    Envelope::new(ProductSnapshot {
        product_id: product_id.to_owned(),
        name: name.to_owned(),
    })
}

async fn entity_key_columns(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'outbox_product_snapshot'
           AND column_name = 'entity_key'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
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
