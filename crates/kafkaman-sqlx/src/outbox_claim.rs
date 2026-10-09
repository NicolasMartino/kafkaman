//! Taking rows out of an outbox for publication.

use kafkaman_core::InstrumentDb;
use std::time::Duration;

use kafkaman_core::{ClaimedOutboxRow, OutboxStatus};
use sqlx::{Postgres, Row, Transaction};
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
             SELECT candidate.message_id, candidate.entity_key, candidate.enqueue_order
             FROM {name} candidate
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
                 AND NOT EXISTS (
                   SELECT 1 FROM {name} newer
                   WHERE newer.entity_key = candidate.entity_key
                     AND newer.status = {pending}
                     AND newer.enqueue_order > candidate.enqueue_order
               )
             ) OR (candidate.status = {publishing} AND candidate.claim_expires_at <= now())
             ORDER BY candidate.enqueue_order
             FOR UPDATE SKIP LOCKED
             LIMIT $1
         ), superseded AS (
             UPDATE {name} AS stale
             SET status = {superseded}, claim_id = NULL, claimed_by = NULL,
                 claim_expires_at = NULL,
                 last_error = 'superseded by newer pending state for the same entity'
             FROM candidates
             WHERE stale.entity_key = candidates.entity_key
               AND stale.status = {pending}
               AND stale.enqueue_order < candidates.enqueue_order
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
        superseded = OutboxStatus::Superseded.sql_literal(),
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
        let enqueue_order: i64 = row.try_get("enqueue_order")?;
        claimed.push((
            enqueue_order,
            ClaimedOutboxRow {
                row: row_from_pg(row)?,
                claim_id,
            },
        ));
    }

    // UPDATE ... FROM does not preserve the candidate ordering. Restore INSERT
    // order for the relay's sequential publishes; keep the storage ordinal
    // internal rather than changing the public OutboxRow API.
    claimed.sort_unstable_by_key(|(enqueue_order, _)| *enqueue_order);
    Ok(claimed.into_iter().map(|(_, row)| row).collect())
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
/// Ordering is `enqueue_order`, allocated by INSERT under the entity lock,
/// matching [`claim_batch`]. Transaction start time can run in reverse of that
/// order, including when a fixed snapshot misses a preceding enqueue entirely.
///
/// This preliminary cleanup is bounded and separate from the claim. The claim
/// also supersedes its candidates' predecessors in its own statement snapshot:
/// a fixed-snapshot writer can commit another row between these statements.
/// Candidate selection already excludes older pending siblings, so that final
/// cleanup and the claim update touch disjoint rows without needing to see each
/// other's writes.
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
/// Exposed so its query plan can be measured; see `tests/outbox_claim_cost.rs`.
/// Drives from the claim's bounded window of due, newest pending rows, excluding
/// entities with an in-flight publication. Older siblings never take a window
/// slot, even with limit one, and a newer pending row that is not due also blocks
/// its predecessors. The entity/status/order index serves the sibling lookup.
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
             SELECT candidate.entity_key, candidate.enqueue_order
             FROM {name} candidate
             WHERE candidate.status = {pending} AND candidate.next_attempt_at <= now()
               AND candidate.entity_key IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM {name} inflight
                   WHERE inflight.entity_key = candidate.entity_key
                     AND inflight.status = {publishing}
               )
               AND NOT EXISTS (
                   SELECT 1 FROM {name} later
                   WHERE later.entity_key = candidate.entity_key
                     AND later.status = {pending}
                     AND later.enqueue_order > candidate.enqueue_order
               )
             ORDER BY candidate.enqueue_order LIMIT $1
         ) AS newer
         WHERE stale.entity_key = newer.entity_key
           AND stale.status = {pending}
           AND stale.enqueue_order < newer.enqueue_order",
        name = table.qualified_name(),
        superseded = OutboxStatus::Superseded.sql_literal(),
        pending = OutboxStatus::Pending.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    )
}
