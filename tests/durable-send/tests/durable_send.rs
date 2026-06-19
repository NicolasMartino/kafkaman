use std::time::Duration;

use async_trait::async_trait;
use kafkaman_core::{
    ClaimedOutboxRow, Envelope, KafkaMessage, MarkOutcome, OutboxStatus, PublishAck,
};
use kafkaman_sqlx::{
    claim_batch, mark_publish_failed, mark_published, migrate, Changeset, CreateOutboxTable,
    InitSchema,
};
use kafkaman_test::Harness;
use kafkaman_worker::{BoxError, Publisher};
use serde::Serialize;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::OffsetDateTime;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug, Serialize)]
struct OrderCreated {
    order_id: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }
}

#[derive(Clone, Debug, Serialize)]
struct InvoiceCreated {
    invoice_id: String,
}

impl KafkaMessage for InvoiceCreated {
    const MESSAGE_TYPE: &'static str = "invoice_created";
    const TOPIC: &'static str = "invoices";

    fn partition_key(&self) -> Option<String> {
        Some(self.invoice_id.clone())
    }
}

#[tokio::test]
async fn migrate_is_idempotent_and_template_generalizes() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;

    harness.outbox_table::<OrderCreated>().await?;
    harness.outbox_table::<InvoiceCreated>().await?;
    let cfg = harness.config();
    let changesets = changelog_for_two_messages()?;

    migrate(harness.pool(), &cfg, &changesets).await?;
    migrate(harness.pool(), &cfg, &changesets).await?;

    let table_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.tables
         WHERE table_schema = $1
           AND table_name IN ('outbox_order_created', 'outbox_invoice_created')",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(table_count, 2);

    let history_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {}.{}",
        cfg.schema.quoted(),
        "changelog_history"
    ))
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(history_count, 3);

    Ok(())
}

#[tokio::test]
async fn durable_send_publishes_record_and_marks_row() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-1".to_owned(),
    });
    let message_id = event.message_id;

    harness.enqueue(&event).await?;
    let stats = harness.relay_once::<OrderCreated>().await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    let published = harness.published_on(OrderCreated::TOPIC);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message_id, message_id);
    assert_eq!(published[0].key.as_deref(), Some("order-1"));

    Ok(())
}

#[tokio::test]
async fn publish_error_requeues_row_with_last_error() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-error".to_owned(),
    });
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let cfg = harness.config();
    let table = harness.outbox_table::<OrderCreated>().await?;
    let stats =
        kafkaman_worker::relay_once(harness.pool(), &FailingPublisher, &table, &cfg.relay).await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.failed, 1);

    let row = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(row.status, OutboxStatus::Pending);
    assert_eq!(row.attempts, 1);
    assert!(row.claim_id.is_none());
    assert!(row.claimed_by.is_none());
    assert!(row.claim_expires_at.is_none());
    assert!(row
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("synthetic publish failure")));

    Ok(())
}

#[tokio::test]
async fn stale_claim_cannot_mark_row_published() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-stale".to_owned(),
    });
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let outcome = mark_published(harness.pool(), &table, message_id, Uuid::new_v4()).await?;
    assert_eq!(outcome, MarkOutcome::StaleClaim);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Publishing)
        .await?;

    let outcome = mark_published(harness.pool(), &table, message_id, claimed[0].claim_id).await?;
    assert_eq!(outcome, MarkOutcome::Updated);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    Ok(())
}

#[tokio::test]
async fn ack_before_mark_republishes_after_claim_lease_expiry() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-dup".to_owned(),
    });
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let publisher = harness.publisher();

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "manual-crash", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    publisher.publish(&claimed[0]).await?;
    assert_eq!(publisher.records_on(OrderCreated::TOPIC).len(), 1);

    let expire_sql = format!(
        "UPDATE {} SET claim_expires_at = now() - interval '1 second' WHERE message_id = $1",
        table.qualified_name()
    );
    sqlx::query(&expire_sql)
        .bind(message_id)
        .execute(harness.pool())
        .await?;

    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;
    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 2);

    Ok(())
}

#[tokio::test]
async fn mark_publish_failed_rejects_stale_claim() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-failed-stale".to_owned(),
    });
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let outcome = mark_publish_failed(
        harness.pool(),
        &table,
        message_id,
        Uuid::new_v4(),
        "wrong claim",
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(outcome, MarkOutcome::StaleClaim);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Publishing)
        .await?;

    Ok(())
}

#[derive(Debug)]
struct FailingPublisher;

#[async_trait]
impl Publisher for FailingPublisher {
    async fn publish(&self, _row: &ClaimedOutboxRow) -> Result<PublishAck, BoxError> {
        Err("synthetic publish failure".into())
    }
}

fn changelog_for_two_messages() -> Result<Vec<Box<dyn Changeset>>, kafkaman_sqlx::Error> {
    Ok(vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(2, OrderCreated::descriptor()?)),
        Box::new(CreateOutboxTable::new(3, InvoiceCreated::descriptor()?)),
    ])
}

async fn start_postgres() -> Result<(ContainerAsync<GenericImage>, String), BoxError> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "kafkaman_test")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(port).await?;
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/kafkaman_test");

    Ok((container, database_url))
}
