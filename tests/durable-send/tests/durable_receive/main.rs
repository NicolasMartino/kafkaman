#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

// Test cases, split by theme; the helpers they share follow.
mod atomic_outbox;
mod dispatch_failure_fallback;
mod dispatch_handler_failure;
mod dispatch_handler_panic;
mod dispatch_retry_schedule;
mod dispatch_success;
mod dispatcher_loop;
mod dlq_inspection;
mod failure_metadata_migration;
mod idempotent_redelivery;
mod insert_and_harness;
mod redrive;
mod redrive_filters;
mod retry_budget;

use durable_send_tests::{
    idem_key, recreate_effect_table, retry_test_config, start_harness, start_harness_with_config,
    TestResult,
};
use kafkaman_core::{
    DispatcherConfig, Envelope, KafkaMessage, OutboxStatus, ReceiveStatus, ReceivedError,
    ReceivedFailureKind, ReceivedMeta,
};
use kafkaman_sqlx::{
    changelog, dispatch_once, enqueue_on_connection, insert_received_with_outcome, migrate,
    migrate_dry_run, received_failed_count, received_failed_rows, received_status_summary,
    received_stuck_rows, redrive_received, AddReceivedFailedIndex, AddReceivedFailureMetadata,
    Changeset, CreateReceivedTable, InitSchema, MessageRouter, MigrationAction, MigrationContext,
    ReceivedFailureFilter, ReceivedInsertOutcome, ReceivedTable, Replay,
};
use kafkaman_test::{dispatch_once_with_hooks, DispatchTestHooks, EnvelopeTestExt};
use kafkaman_worker::run_dispatcher;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;

fn receive_test_lock() -> &'static Mutex<()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Build a stored `errors` JSONB array the way production failure accounting
/// does (serde-serialized `ReceivedError`), so it round-trips back through
/// `ReceivedRow` deserialization in assertions.
fn failure_errors_json(kind: ReceivedFailureKind, count: usize) -> serde_json::Value {
    failure_errors_json_at(kind, count, OffsetDateTime::now_utc())
}

fn failure_errors_json_at(
    kind: ReceivedFailureKind,
    count: usize,
    occurred_at: OffsetDateTime,
) -> serde_json::Value {
    let errors = (0..count)
        .map(|idx| ReceivedError::new(kind, format!("boom-{idx}"), occurred_at, None))
        .collect::<Vec<_>>();
    serde_json::to_value(errors).expect("serialize received errors")
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct OrderAccepted {
    order_id: String,
}

impl KafkaMessage for OrderAccepted {
    const MESSAGE_TYPE: &'static str = "order_accepted";
    const TOPIC: &'static str = "accepted-orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }

    fn entity_key(&self) -> String {
        self.order_id.clone()
    }
}

/// Assert a scheduled retry falls inside the equal-jitter window for a base
/// backoff of `base_secs`: `[now + base/2, now + base]`.
fn assert_retry_within(
    next_attempt_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
    base_secs: i64,
) {
    let retry_at = next_attempt_at.expect("a retryable row must be scheduled");
    let earliest = now + time::Duration::seconds(base_secs) / 2;
    let latest = now + time::Duration::seconds(base_secs);
    assert!(
        retry_at >= earliest && retry_at <= latest,
        "retry at {retry_at} outside jitter window [{earliest}, {latest}]"
    );
}
