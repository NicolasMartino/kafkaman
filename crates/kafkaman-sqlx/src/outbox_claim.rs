//! Taking rows out of an outbox for publication.

use kafkaman_core::InstrumentDb;
use std::time::Duration;

use kafkaman_core::{ClaimedOutboxRow, OutboxStatus};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::outbox_mark::row_from_pg;
use crate::{OutboxTable, Result};

/// Claim up to `limit` publishable rows in a single statement.
///
/// One statement rather than a select-then-update-per-row loop: at the default
/// `batch_limit` of 100 that loop cost 101 round trips per relay cycle, and held
/// the claiming transaction open across all of them.
///
/// All rows in one call share a `claim_id`. That is sound because the id
/// identifies a claim *generation*, not a row: `mark_published` and
/// `mark_publish_failed` match on `(message_id, claim_id)`, so a row reclaimed
/// by another worker after a lease expiry still rejects the original worker's
/// late mark.
// Stays in the debug tier: runs on `poll_interval` and claims nothing on an idle service.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn claim_batch(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    worker_id: &str,
    lease_for: Duration,
    limit: i64,
) -> Result<Vec<ClaimedOutboxRow>> {
    collapse_stale_pending_rows(tx, table, limit).await?;

    // The lease expiry is computed from the database clock (`now()`) rather than
    // the application host clock, so lease ownership is immune to clock skew
    // between the worker and PostgreSQL.
    let sql = format!(
        "WITH candidates AS (
             SELECT candidate.message_id FROM {name} candidate
             WHERE (
                    candidate.status = {pending}
                    AND candidate.next_attempt_at <= now()
                    AND (
                        candidate.entity_key IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM {name} inflight
                            WHERE inflight.entity_key = candidate.entity_key
                              AND inflight.status = {publishing}
                        )
                    )
                 )
                OR (candidate.status = {publishing} AND candidate.claim_expires_at <= now())
             ORDER BY candidate.created_at
             FOR UPDATE SKIP LOCKED
             LIMIT $1
         )
         UPDATE {name} AS target
         SET status = {publishing},
             attempts = target.attempts + 1,
             claim_id = $2,
             claimed_by = $3,
             claim_expires_at = now() + make_interval(secs => $4)
         FROM candidates
         WHERE target.message_id = candidates.message_id
         RETURNING target.*",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );

    let claim_id = Uuid::new_v4();
    let rows = sqlx::query(&sql)
        .bind(limit)
        .bind(claim_id)
        .bind(worker_id)
        .bind(lease_for.as_secs_f64())
        .fetch_all(&mut **tx)
        .instrument_db(kafkaman_core::db_poll_span!(
            "UPDATE",
            table.qualified_name(),
            "claim outbox batch",
        ))
        .await?;

    let mut claimed = Vec::with_capacity(rows.len());
    for row in rows {
        claimed.push(ClaimedOutboxRow {
            row: row_from_pg(row)?,
            claim_id,
        });
    }

    // `UPDATE ... FROM` does not honour the CTE's ORDER BY, but per-entity
    // ordering is what the relay depends on, so restore it here. Publishing is
    // sequential in `relay_once`, so claim order is publish order.
    claimed.sort_by(|a, b| {
        a.row
            .created_at
            .cmp(&b.row.created_at)
            .then_with(|| a.row.message_id.cmp(&b.row.message_id))
    });

    Ok(claimed)
}

/// Supersede every `Pending` outbox row that a newer `Pending` row for the same
/// entity has overtaken, leaving at most one claimable row per entity.
///
/// `enqueue` already supersedes on the write path, but it can only supersede rows
/// that are `Pending` *at that moment*. A row that is `Publishing` when the next
/// state is enqueued survives, and `mark_publish_failed` then returns it to
/// `Pending` — so a transient publish failure leaves two `Pending` rows for one
/// entity, older and newer.
///
/// That is not merely wasteful, it is the corruption the offset ordinal exists to
/// prevent. The newer row carries `next_attempt_at = now()` from insert while the
/// retried older row carries `now() + retry_after`, so the newer state publishes
/// *first*, at the lower offset, and the older state lands above it. Every
/// consumer's convergence guard then sees stale state carrying the newest ordinal
/// and applies it — correctly, by its own rules, and permanently. See the
/// entity-first propagation decision, point 9; this is that failure mode reached
/// through retry rather than through replay.
///
/// Ordering is `(created_at, message_id)`, matching how [`claim_batch`] sorts what
/// it hands the relay, so "newer" means the same thing in both places.
///
/// This is one statement per relay cycle, not per row, and it must be separate
/// from the claim below: a data-modifying CTE would not see its own writes.
// Stays in the debug tier: part of the same empty poll as `claim_batch`.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn collapse_stale_pending_rows(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    limit: i64,
) -> Result<()> {
    sqlx::query(&collapse_stale_pending_rows_sql_impl(table))
        .bind(limit)
        .execute(&mut **tx)
        .instrument_db(kafkaman_core::db_poll_span!(
            "UPDATE",
            table.qualified_name(),
            "collapse stale pending outbox rows",
        ))
        .await?;
    Ok(())
}

/// The statement [`collapse_stale_pending_rows`] runs.
///
/// Exposed so its query plan can be measured against the claim it precedes; see
/// `tests/outbox_claim_cost.rs`. The shape below was chosen from those
/// measurements, not from reading the SQL.
///
/// It drives from the same bounded window the claim is about to take — `Pending`,
/// due, ordered by `created_at`, `LIMIT $1` — and looks *backwards* for older
/// siblings of each driver. The obvious formulation instead scans every `Pending`
/// row and looks forward with an `EXISTS`, which the planner executes as a hash
/// semi join over two full table scans: at a 200k backlog that measured 252ms and
/// spilled 2202 temp blocks, against 71ms and no spill for this one, for identical
/// results.
///
/// Driving from the claim window is also why it is *correct* to find nothing when
/// an overtaken pair sits outside that window. A row that will not be claimed this
/// cycle cannot be published out of order this cycle, and the pair is collapsed on
/// whichever cycle it does enter the window.
#[cfg(feature = "internal-hooks")]
#[doc(hidden)]
pub fn collapse_stale_pending_rows_sql(table: &OutboxTable) -> String {
    collapse_stale_pending_rows_sql_impl(table)
}

fn collapse_stale_pending_rows_sql_impl(table: &OutboxTable) -> String {
    format!(
        "UPDATE {name} AS stale
         SET status = {superseded},
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL,
             last_error = 'superseded by newer pending state for the same entity'
         FROM (
             SELECT entity_key, created_at, message_id
             FROM {name}
             WHERE status = {pending}
               AND next_attempt_at <= now()
               AND entity_key IS NOT NULL
             ORDER BY created_at
             LIMIT $1
         ) AS newer
         WHERE stale.entity_key = newer.entity_key
           AND stale.status = {pending}
           AND (stale.created_at, stale.message_id)
               < (newer.created_at, newer.message_id)",
        name = table.qualified_name(),
        superseded = OutboxStatus::Superseded.sql_literal(),
        pending = OutboxStatus::Pending.sql_literal(),
    )
}
