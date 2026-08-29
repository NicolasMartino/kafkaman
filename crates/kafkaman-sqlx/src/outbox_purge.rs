//! Reclaiming disk from outbox rows nothing will ever act on again.

use kafkaman_core::InstrumentDb;
use kafkaman_core::{OutboxStatus, PurgeConfig, PurgeStats};
use sqlx::PgPool;

use crate::{OutboxTable, Result};

/// Reclaim one bounded batch of terminal outbox rows, returning what it deleted.
///
/// One batch, not the whole backlog: the caller loops. This is the same contract as
/// [`claim_batch`](crate::claim_batch) and [`dispatch_once`](crate::dispatch_once), and it is not merely stylistic. A single
/// unbounded `DELETE` over a table that has grown for months holds locks for the
/// duration and writes a write-ahead log proportional to the entire backlog, which
/// is precisely the outage a retention sweep is supposed to prevent.
///
/// Only the outbox is reclaimable. The received table is the only durable record of
/// what was done, and its dedupe window *is* its retention window — purging a
/// processed row lets a Kafka redelivery re-run the handler. Cache tables are the
/// state. See the outbox retention decision.
///
/// Age is measured from `created_at` against the *database* clock, matching claim
/// and retry eligibility, so retention is immune to skew between the worker host
/// and PostgreSQL.
///
/// `FOR UPDATE SKIP LOCKED` keeps a sweep from blocking, or being blocked by, a
/// relay working the same table: a row another transaction holds is left for the
/// next batch rather than waited on.
// Stays in the debug tier: a 60s sweep against a retention measured in days.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn purge_outbox_once(
    pool: &PgPool,
    table: &OutboxTable,
    cfg: &PurgeConfig,
) -> Result<PurgeStats> {
    cfg.validate()?;

    let mut statuses = vec![
        OutboxStatus::Published.sql_literal(),
        OutboxStatus::Superseded.sql_literal(),
    ];
    if cfg.include_failed {
        statuses.push(OutboxStatus::Failed.sql_literal());
    }

    let sql = format!(
        "WITH victims AS (
             SELECT message_id FROM {name}
             WHERE status IN ({statuses})
               AND created_at < now() - make_interval(secs => $1)
             ORDER BY created_at
             LIMIT $2
             FOR UPDATE SKIP LOCKED
         )
         DELETE FROM {name} AS target
         USING victims
         WHERE target.message_id = victims.message_id",
        name = table.qualified_name(),
        statuses = statuses.join(", "),
    );

    // The one statement on the durable path that had no span at any tier, so a
    // retention sweep was invisible however `RUST_LOG` was set. `db_poll_span!`
    // rather than `db_span!` for the reason the seven scheduler statements use
    // it: this runs on `poll_interval` (60s by default) against an `older_than`
    // measured in days, so on nearly every run it deletes nothing and would be
    // most of what an idle service exported.
    let result = sqlx::query(&sql)
        .bind(cfg.older_than.as_secs_f64())
        .bind(cfg.batch_size)
        .execute(pool)
        .instrument_db(kafkaman_core::db_poll_span!(
            "DELETE",
            table.qualified_name(),
            "purge outbox rows",
        ))
        .await?;

    Ok(PurgeStats {
        deleted: result.rows_affected(),
    })
}
