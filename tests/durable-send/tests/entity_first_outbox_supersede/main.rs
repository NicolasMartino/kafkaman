#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

// Test cases, split by theme; the helpers they share follow.
mod migration;
mod publish_ordering;
mod supersede_on_enqueue;

use std::time::Duration;

use durable_send_tests::{start_harness, ProductSnapshot, TestResult};
use kafkaman_core::{Envelope, KafkaMessage, MarkOutcome, OutboxStatus};
use kafkaman_sqlx::{
    claim_batch, enqueue, mark_publish_failed, mark_published, migrate, AddOutboxEntityKey,
    Changeset, CreateOutboxTable, InitSchema, MigrationContext, OutboxTable,
};
use kafkaman_test::EnvelopeTestExt;
use tokio::time::timeout;

fn product(product_id: &str, name: &str) -> Envelope<ProductSnapshot> {
    ProductSnapshot::envelope(product_id, name)
}
