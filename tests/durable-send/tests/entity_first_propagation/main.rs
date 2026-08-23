#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

// Test cases, split by theme; the helpers they share follow.
mod cache_apply;
mod entity_key_resolution;
mod migration;
mod retry_and_redrive;

use durable_send_tests::{
    retry_test_config, start_harness, start_harness_with_config, unique_schema, KeylessProduct,
    ProductSnapshot, RegionalProduct, TestResult,
};
use kafkaman_core::{KafkaMessage, ReceiveStatus};
use kafkaman_sqlx::{
    changelog, dispatch_once, migrate, migrate_dry_run, AddReceivedEntityKey, CacheTable,
    Changeset, CreateCacheTable, CreateReceivedTable, InitSchema, MessageRouter, MigrationAction,
    MigrationContext, ReceivedTable, Replay,
};
use kafkaman_test::{EnvelopeTestExt, Harness};
use sqlx::Row;
use std::sync::Arc;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Notify;

fn product(product_id: &str, name: &str) -> kafkaman_core::Envelope<ProductSnapshot> {
    ProductSnapshot::envelope(product_id, name)
}

async fn received_entity_key_columns(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'received_product_snapshot'
           AND column_name = 'entity_key'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
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
