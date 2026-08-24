#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

// Test cases, split by theme; the helpers they share follow.
mod claim_lease;
mod config_validation;
mod idempotency_and_headers;
mod inspection;
mod migrations;
mod relay_and_publish;
mod replay;

use durable_send_tests::{idem_key, postgres, start_harness, TestResult};
use std::time::Duration;

use async_trait::async_trait;
use kafkaman_core::{
    ClaimedOutboxRow, Envelope, KafkaMessage, MarkOutcome, OutboxStatus, PublishAck, SqlIdentifier,
};
use kafkaman_sqlx::{
    changelog, claim_batch, enqueue, mark_publish_failed, mark_published, migrate, migrate_dry_run,
    outbox_status_summary, outbox_stuck_rows, AddIdempotencyKey, Changeset, CreateOutboxTable,
    InitSchema, MigrationAction, MigrationContext, OutboxTable, Replay,
};
use kafkaman_test::{EnvelopeTestExt, Harness};
use kafkaman_worker::{BoxError, Publisher};
use serde::Serialize;
use time::OffsetDateTime;
use uuid::Uuid;

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

    fn entity_key(&self) -> String {
        self.order_id.clone()
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

    fn entity_key(&self) -> String {
        self.invoice_id.clone()
    }
}

async fn idempotency_key_columns(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'outbox_order_created'
           AND column_name = 'idempotency_key'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
}

async fn status_count(
    pool: &sqlx::PgPool,
    table: &OutboxTable,
    status: OutboxStatus,
) -> Result<i64, sqlx::Error> {
    let sql = format!(
        "SELECT COUNT(*)::BIGINT FROM {} WHERE status = $1",
        table.qualified_name()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .bind(status.as_str())
        .fetch_one(pool)
        .await
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
    Ok(changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
        CreateOutboxTable::new(3, InvoiceCreated::descriptor()?),
    ])
}
