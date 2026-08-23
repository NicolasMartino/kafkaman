//! Outbox retention: what a purge sweep reclaims, and — more importantly — what it
//! must never touch. See the outbox retention decision.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use durable_send_tests::{start_harness, ProductSnapshot, TestResult};
use kafkaman_core::{KafkaMessage, OutboxStatus, PurgeConfig};
use kafkaman_sqlx::{
    migrate, purge_outbox_once, AddOutboxRetentionIndex, Changeset, CreateOutboxTable, InitSchema,
    MigrationContext, OutboxTable, ResolvedConfig,
};
use sqlx::PgPool;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);

fn config(older_than: Duration, batch_size: i64, include_failed: bool) -> PurgeConfig {
    PurgeConfig {
        older_than,
        batch_size,
        poll_interval: Duration::from_secs(60),
        include_failed,
    }
}

/// Insert a row directly in a chosen status and age. Going through `enqueue` would
/// only ever produce `Pending` rows created now, which is the one state retention
/// is not about.
async fn seed_row(
    pool: &PgPool,
    table: &OutboxTable,
    status: OutboxStatus,
    age_days: i64,
) -> TestResult<Uuid> {
    let message_id = Uuid::new_v4();
    sqlx::query(&format!(
        "INSERT INTO {} (
             message_id, status, attempts, next_attempt_at, topic, entity_key,
             correlation_id, headers, payload, occurred_at, created_at
         ) VALUES ($1, $2, 0, now(), 'products', 'p-1', gen_random_uuid(),
                  '{{}}'::jsonb, '{{}}'::jsonb, now(), now() - ($3 * interval '1 day'))",
        table.qualified_name()
    ))
    .bind(message_id)
    .bind(status.as_str())
    .bind(age_days as f64)
    .execute(pool)
    .await?;
    Ok(message_id)
}

async fn surviving(pool: &PgPool, table: &OutboxTable) -> TestResult<Vec<String>> {
    let mut rows: Vec<String> =
        sqlx::query_scalar(&format!("SELECT status FROM {}", table.qualified_name()))
            .fetch_all(pool)
            .await?;
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn retention_reclaims_terminal_rows_and_spares_everything_else() -> TestResult {
    // The whole risk in a retention sweep is deleting one row too many. A row a
    // worker still intends to act on is unrecoverable, and `Failed` rows are the
    // invalid-send audit trail — the one terminal class with no successor carrying
    // the same information.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let pool = harness.pool();

    for status in [
        OutboxStatus::Pending,
        OutboxStatus::Publishing,
        OutboxStatus::Published,
        OutboxStatus::Superseded,
        OutboxStatus::Failed,
    ] {
        seed_row(pool, &table, status, 30).await?;
    }

    let stats = purge_outbox_once(pool, &table, &config(WEEK, 100, false)).await?;
    assert_eq!(
        stats.deleted, 2,
        "only Published and Superseded are reclaimable"
    );
    assert_eq!(
        surviving(pool, &table).await?,
        vec!["Failed", "Pending", "Publishing"],
        "a claimable row or an audit row must never be reclaimed"
    );

    Ok(())
}

#[tokio::test]
async fn retention_spares_rows_inside_the_window() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let pool = harness.pool();

    seed_row(pool, &table, OutboxStatus::Published, 1).await?;
    seed_row(pool, &table, OutboxStatus::Published, 30).await?;

    let stats = purge_outbox_once(pool, &table, &config(WEEK, 100, false)).await?;
    assert_eq!(stats.deleted, 1);
    assert_eq!(surviving(pool, &table).await?, vec!["Published"]);

    Ok(())
}

#[tokio::test]
async fn retention_batches_are_bounded_and_converge() -> TestResult {
    // The bound is the point of the API. One unbounded DELETE over a table that has
    // grown for months holds locks for its duration and writes a WAL volume
    // proportional to the whole backlog — the outage retention exists to avoid.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let pool = harness.pool();

    for _ in 0..5 {
        seed_row(pool, &table, OutboxStatus::Published, 30).await?;
    }

    let cfg = config(WEEK, 2, false);
    let mut batches = Vec::new();
    loop {
        let stats = purge_outbox_once(pool, &table, &cfg).await?;
        batches.push(stats.deleted);
        if stats.deleted == 0 {
            break;
        }
    }

    assert_eq!(
        batches,
        vec![2, 2, 1, 0],
        "batches must be capped, then converge"
    );
    assert!(surviving(pool, &table).await?.is_empty());

    Ok(())
}

#[tokio::test]
async fn retention_reclaims_failed_rows_only_on_opt_in() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let pool = harness.pool();

    seed_row(pool, &table, OutboxStatus::Failed, 30).await?;

    assert_eq!(
        purge_outbox_once(pool, &table, &config(WEEK, 100, false))
            .await?
            .deleted,
        0,
        "the audit trail survives by default"
    );
    assert_eq!(
        purge_outbox_once(pool, &table, &config(WEEK, 100, true))
            .await?
            .deleted,
        1,
        "and is reclaimable only when asked for explicitly"
    );

    Ok(())
}

#[tokio::test]
async fn retention_rejects_a_config_that_would_delete_live_rows() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;

    // Zero retention deletes a row the instant it goes terminal, destroying the
    // operational record while an incident is still being diagnosed. Rejected at
    // the call, not just in the loop, so a direct caller cannot bypass it.
    let err = purge_outbox_once(harness.pool(), &table, &config(Duration::ZERO, 100, false))
        .await
        .expect_err("zero retention must be rejected");
    assert!(err.to_string().contains("older_than"), "{err}");

    Ok(())
}

#[tokio::test]
async fn add_outbox_retention_index_upgrades_a_legacy_table() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    // Migrate first: `OutboxTable::new` only builds a name, it creates nothing.
    harness.outbox_table::<ProductSnapshot>().await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = ProductSnapshot::descriptor()?;

    // Reproduce a table created before retention existed by dropping the index the
    // current DDL adds. Looked up rather than named: the generated name is bounded
    // and hashed for long table names, so hardcoding it would be wrong for exactly
    // the tables that need it most.
    let index: String = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes
          WHERE schemaname = $1 AND indexname LIKE '%_retention'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    sqlx::query(&format!("DROP INDEX {}.\"{index}\"", cfg.schema.quoted()))
        .execute(pool)
        .await?;
    assert_eq!(retention_indexes(pool, &cfg).await?, 0);

    let changesets: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(2, descriptor.clone())),
        Box::new(AddOutboxRetentionIndex::new(3, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(retention_indexes(pool, &cfg).await?, 1);

    // Convergence, not just application: re-running a changelog is the property the
    // whole migration engine rests on.
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(retention_indexes(pool, &cfg).await?, 1);

    Ok(())
}

async fn retention_indexes(pool: &PgPool, cfg: &ResolvedConfig) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes
          WHERE schemaname = $1 AND indexname LIKE '%_retention'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
}

#[tokio::test]
async fn the_purger_loop_drains_a_backlog_then_stops_on_cancellation() -> TestResult {
    // The loop, not just the batch. `purge_outbox_once` is bounded on purpose, so
    // something has to keep calling it — and that something must also notice a
    // shutdown, or a deploy waits out a full poll interval for a task that has
    // nothing left to do.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let pool = harness.pool();

    for _ in 0..5 {
        seed_row(pool, &table, OutboxStatus::Published, 30).await?;
    }
    seed_row(pool, &table, OutboxStatus::Pending, 30).await?;

    // A poll interval far longer than the test: reaching it would mean the loop
    // stopped draining, and the timeout below would fail rather than hang.
    let shutdown = CancellationToken::new();
    let purger = tokio::spawn(kafkaman_worker::run_purger(
        pool.clone(),
        table.clone(),
        config(WEEK, 2, false),
        shutdown.clone(),
    ));

    // Batches of 2 against 5 reclaimable rows: the loop must run four times
    // rather than sleeping between them.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if surviving(pool, &table).await? == vec!["Pending"] {
                return Ok::<(), durable_send_tests::BoxError>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), purger).await???;

    // The claimable row is untouched: draining fast must not mean draining more.
    assert_eq!(surviving(pool, &table).await?, vec!["Pending"]);

    Ok(())
}
