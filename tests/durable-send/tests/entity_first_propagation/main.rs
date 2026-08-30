#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

// Test cases, split by theme; the helpers they share follow.
mod cache_apply;
mod dispatch_ordering;
mod entity_key_resolution;
mod retry_and_redrive;

use durable_send_tests::{
    retry_test_config, start_harness, start_harness_with_config, unique_schema, KeylessProduct,
    ProductSnapshot, RegionalProduct, TestResult,
};
use kafkaman_core::{KafkaMessage, ReceiveStatus};
use kafkaman_sqlx::{
    changelog, dispatch_once, migrate, migrate_dry_run, CacheTable, Changeset, CreateReceivedTable,
    HandlerFlow, InitSchema, MessageRouter, MigrationAction, MigrationContext, ReceivedTable,
    Replay,
};
use kafkaman_test::{EnvelopeTestExt, Harness};
use sqlx::Row;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Notify;

fn product(product_id: &str, name: &str) -> kafkaman_core::Envelope<ProductSnapshot> {
    ProductSnapshot::envelope(product_id, name)
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

/// Force the cache row for `entity_key` to look as though it was applied from a
/// different topic, which is what a cache left behind by a topic rebuild looks
/// like. The offset is set deliberately high so that the ordinary guard could
/// never accept the next record on offset alone.
async fn strand_cache_on_retired_topic(
    harness: &Harness,
    entity_key: &str,
    retired_topic: &str,
    applied_offset: i64,
) -> TestResult {
    let cache = CacheTable::for_message::<ProductSnapshot>(&harness.config())?;
    let sql = format!(
        "UPDATE {} SET applied_topic = $2, applied_offset = $3 WHERE entity_key = $1",
        cache.qualified_name()
    );
    let affected = sqlx::query(&sql)
        .bind(entity_key)
        .bind(retired_topic)
        .bind(applied_offset)
        .execute(harness.pool())
        .await?
        .rows_affected();
    assert_eq!(
        affected, 1,
        "stranding must hit exactly one cache row, or the test proves nothing"
    );
    Ok(())
}

/// Rewrite a received row's source topic, standing in for a record that was
/// ingested from a topic other than the one its type declares.
///
/// Targets the row by `source_offset` rather than by idempotency key: the stored
/// key is a digest of the supplied source, not the string handed to
/// `with_idempotency_key`, so matching on that silently updates nothing. The
/// row-count assertion is here because that failure mode is invisible - the test
/// still runs, against a record that was never restamped.
async fn restamp_received_source_topic(
    harness: &Harness,
    source_offset: i64,
    source_topic: &str,
) -> TestResult {
    let table = ReceivedTable::for_message::<ProductSnapshot>(&harness.config())?;
    let sql = format!(
        "UPDATE {} SET source_topic = $2 WHERE source_offset = $1",
        table.qualified_name()
    );
    let affected = sqlx::query(&sql)
        .bind(source_offset)
        .bind(source_topic)
        .execute(harness.pool())
        .await?
        .rows_affected();
    assert_eq!(
        affected, 1,
        "restamping must hit exactly one received row, or the test proves nothing"
    );
    Ok(())
}
