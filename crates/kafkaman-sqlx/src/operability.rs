//! Read-only inspection of outbox and received tables: depth by status, and the
//! rows that have aged past an operator's threshold.
//!
//! # The two ages
//!
//! Every stuck row reports two durations, and conflating them is the mistake
//! this split exists to prevent.
//!
//! `age_ms` is time since `created_at`: how long this message has been waiting,
//! start to finish. It is the number that says how much delay a customer has
//! experienced.
//!
//! `stuck_for_ms` is time since the row became *late* — `claim_expires_at` for
//! an outbox row whose claimant died, `due_at` for a received row past its
//! backoff. It is the number `stuck_after` filters on, so it is the one that
//! says how long the fault has been going on.
//!
//! They differ by however long the row queued legitimately first, which on a
//! healthy backlog is most of it. Reporting only `age_ms` — as this did — makes
//! a thirty-second outage on an hour-old message look like an hour-long outage.
//!
//! Nothing here mutates. The one operational write that belongs with these — a
//! runtime DLQ redrive — lives in `replay.rs` beside the statement it reuses.
use std::time::Duration;

use kafkaman_core::{OutboxStatus, ReceiveStatus};
use serde::Serialize;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::retry_backoff::duration_to_time;
use crate::{Error, OutboxTable, ReceivedTable, Result};

/// One `(message type, status)` bucket of an outbox table, with the age of its
/// oldest row.
///
/// Timestamps carry the `rfc9557` adapter rather than `time`'s derived impl.
/// The workspace does not enable `time/serde-human-readable`, so a bare
/// `OffsetDateTime` serializes to a nine-integer array — unreadable to every
/// dashboard that consumes these summaries over HTTP.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OutboxStatusSummary {
    pub message_type: String,
    pub status: OutboxStatus,
    pub count: i64,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub oldest_created_at: Option<OffsetDateTime>,
    pub oldest_age_ms: Option<u64>,
    /// Set when this bucket holds queued work older than the configured
    /// `max_queue_age`. Always false for terminal statuses: a `Published` row
    /// waiting on retention is not a backlog.
    pub over_max_queue_age: bool,
}

/// One `(message type, status)` bucket of a received table. See
/// [`OutboxStatusSummary`] for the timestamp-format rationale.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReceivedStatusSummary {
    pub message_type: String,
    pub status: ReceiveStatus,
    pub count: i64,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub oldest_created_at: Option<OffsetDateTime>,
    pub oldest_age_ms: Option<u64>,
    /// Set when this bucket holds queued work older than the configured
    /// `max_queue_age`. Always false for terminal statuses.
    pub over_max_queue_age: bool,
}

/// An outbox row whose publish claim expired without the claimant marking it
/// either published or failed — the signature of a worker that died mid-publish.
///
/// `age_ms` measures from `created_at` and `stuck_for_ms` from
/// `claim_expires_at` — how long the message has been undelivered, and how long
/// it has been stuck. `stuck_after` filters on the second. They differ by
/// however long the row queued legitimately before its claimant died.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OutboxStuckRow {
    pub message_type: String,
    pub message_id: Uuid,
    pub status: OutboxStatus,
    pub claimed_by: Option<String>,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub claim_expires_at: Option<OffsetDateTime>,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub created_at: OffsetDateTime,
    /// Time since `created_at`: how long this message has been undelivered.
    pub age_ms: u64,
    /// Time since `claim_expires_at`: how long it has been *stuck*.
    pub stuck_for_ms: u64,
}

/// A received row that has been due for dispatch longer than the caller's
/// threshold — a dispatcher that is down, wedged, or falling behind.
///
/// `due_at` is `next_attempt_at` when a retry is scheduled and `created_at`
/// otherwise. A row inside its backoff window is not late, it is waiting, which
/// is why the fault clock starts at `due_at` rather than at `created_at`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReceivedStuckRow {
    pub message_type: String,
    pub message_id: Uuid,
    pub status: ReceiveStatus,
    pub attempts: i32,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub next_attempt_at: Option<OffsetDateTime>,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub created_at: OffsetDateTime,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub due_at: OffsetDateTime,
    /// Time since `created_at`: how long this message has been unprocessed.
    pub age_ms: u64,
    /// Time since `due_at`: how long it has been *overdue*.
    pub stuck_for_ms: u64,
}

/// Per-status row counts and oldest-row age for one outbox table.
///
/// This is a full aggregate over the table with no time bound, which is what
/// makes it a correct depth reading and also what makes it expensive: it is
/// index-only at best and a sequential scan on an unvacuumed table. Callers
/// exposing it over HTTP should cache or rate-limit rather than serving it per
/// request.
///
/// `max_queue_age` only decides the `over_max_queue_age` flag; it never filters
/// rows, so the counts stay complete regardless of the threshold.
pub async fn outbox_status_summary(
    pool: &PgPool,
    table: &OutboxTable,
    now: OffsetDateTime,
    max_queue_age: Duration,
) -> Result<Vec<OutboxStatusSummary>> {
    let sql = format!(
        "SELECT status, count(*) AS row_count, min(created_at) AS oldest_created_at
         FROM {}
         GROUP BY status
         ORDER BY status",
        table.qualified_name()
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status: OutboxStatus = status.parse().map_err(Error::Core)?;
            let oldest_created_at: Option<OffsetDateTime> = row.try_get("oldest_created_at")?;
            let oldest_age_ms = oldest_created_at.map(|created_at| age_ms_since(now, created_at));
            Ok(OutboxStatusSummary {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                status,
                count: row.try_get("row_count")?,
                oldest_created_at,
                oldest_age_ms,
                over_max_queue_age: over_max_queue_age(
                    status.is_terminal(),
                    oldest_age_ms,
                    max_queue_age,
                ),
            })
        })
        .collect()
}

/// Per-status row counts and oldest-row age for one received table. Carries the
/// same cost caveat as [`outbox_status_summary`].
pub async fn received_status_summary(
    pool: &PgPool,
    table: &ReceivedTable,
    now: OffsetDateTime,
    max_queue_age: Duration,
) -> Result<Vec<ReceivedStatusSummary>> {
    let sql = format!(
        "SELECT status, count(*) AS row_count, min(created_at) AS oldest_created_at
         FROM {}
         GROUP BY status
         ORDER BY status",
        table.qualified_name()
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status: ReceiveStatus = status.parse().map_err(Error::Core)?;
            let oldest_created_at: Option<OffsetDateTime> = row.try_get("oldest_created_at")?;
            let oldest_age_ms = oldest_created_at.map(|created_at| age_ms_since(now, created_at));
            Ok(ReceivedStatusSummary {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                status,
                count: row.try_get("row_count")?,
                oldest_created_at,
                oldest_age_ms,
                over_max_queue_age: over_max_queue_age(
                    status.is_terminal(),
                    oldest_age_ms,
                    max_queue_age,
                ),
            })
        })
        .collect()
}

/// Outbox rows whose publish claim expired more than `stuck_after` ago.
///
/// An expired claim is not itself a fault — the relay reclaims them on the next
/// cycle. Staying expired past the threshold is the fault, so the cutoff is
/// applied to `claim_expires_at` rather than to `created_at`.
pub async fn outbox_stuck_rows(
    pool: &PgPool,
    table: &OutboxTable,
    now: OffsetDateTime,
    stuck_after: Duration,
    limit: i64,
) -> Result<Vec<OutboxStuckRow>> {
    let cutoff = now - duration_to_time(stuck_after);
    let sql = format!(
        "SELECT message_id, status, claimed_by, claim_expires_at, created_at
         FROM {name}
         WHERE status = {publishing}
           AND claim_expires_at IS NOT NULL
           AND claim_expires_at <= $1
         ORDER BY claim_expires_at, created_at, message_id
         LIMIT $2",
        name = table.qualified_name(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    let rows = sqlx::query(&sql)
        .bind(cutoff)
        .bind(limit.max(0))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status = status.parse().map_err(Error::Core)?;
            let created_at: OffsetDateTime = row.try_get("created_at")?;
            let claim_expires_at: Option<OffsetDateTime> = row.try_get("claim_expires_at")?;
            Ok(OutboxStuckRow {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                message_id: row.try_get("message_id")?,
                status,
                claimed_by: row.try_get("claimed_by")?,
                claim_expires_at,
                created_at,
                age_ms: age_ms_since(now, created_at),
                // The query selected this row on `claim_expires_at`, so this is
                // the number that answers the question the filter asked.
                stuck_for_ms: claim_expires_at
                    .map(|expired_at| age_ms_since(now, expired_at))
                    .unwrap_or(0),
            })
        })
        .collect()
}

/// Received rows that have been due for dispatch for more than `stuck_after`.
///
/// The predicate is split per status rather than written as
/// `COALESCE(next_attempt_at, created_at) <= $1`. Both forms select the same
/// rows, but only this one is servable by the received table's
/// `(status, next_attempt_at, created_at)` index — a `COALESCE` across two age
/// columns is the shape [`create_outbox_retention_index_sql`] records as
/// unservable by any index. `claim_received_row` splits it for the same reason,
/// and this query must stay cheap: it runs per message type per admin request.
///
/// `due_at` stays a projected `COALESCE` and still drives `ORDER BY`, because
/// "most overdue first" is the only useful order for this list. That costs a
/// top-N sort, but only over rows the indexed `WHERE` already matched — which is
/// the overdue set itself, not the table.
///
/// [`create_outbox_retention_index_sql`]: crate::create_outbox_retention_index_sql
pub async fn received_stuck_rows(
    pool: &PgPool,
    table: &ReceivedTable,
    now: OffsetDateTime,
    stuck_after: Duration,
    limit: i64,
) -> Result<Vec<ReceivedStuckRow>> {
    let cutoff = now - duration_to_time(stuck_after);
    let sql = format!(
        "SELECT message_id, status, attempts, next_attempt_at, created_at,
                COALESCE(next_attempt_at, created_at) AS due_at
         FROM {name}
         WHERE (
             status = {pending}
             AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
             AND created_at <= $1
         ) OR (
             status = {retryable}
             AND next_attempt_at <= $1
         )
         ORDER BY due_at, created_at, message_id
         LIMIT $2",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        retryable = ReceiveStatus::Retryable.sql_literal(),
    );
    let rows = sqlx::query(&sql)
        .bind(cutoff)
        .bind(limit.max(0))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status = status.parse().map_err(Error::Core)?;
            let created_at: OffsetDateTime = row.try_get("created_at")?;
            let due_at: OffsetDateTime = row.try_get("due_at")?;
            Ok(ReceivedStuckRow {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                message_id: row.try_get("message_id")?,
                status,
                attempts: row.try_get("attempts")?,
                next_attempt_at: row.try_get("next_attempt_at")?,
                created_at,
                due_at,
                age_ms: age_ms_since(now, created_at),
                // Selected on `due_at`, so this is the overdue clock. It used to
                // be reported as `age_ms`, which meant the same field name
                // measured from `created_at` on the outbox side and from
                // `due_at` here — two answers to one question on the same
                // operator screen.
                stuck_for_ms: age_ms_since(now, due_at),
            })
        })
        .collect()
}

/// Milliseconds elapsed from `since` to `now`, clamped at zero.
///
/// `now` is caller-supplied so inspection reads share one clock across every
/// table in a request. A caller passing a `now` behind the row's timestamp gets
/// zero rather than a wrapped `u64`.
fn age_ms_since(now: OffsetDateTime, since: OffsetDateTime) -> u64 {
    let delta = now - since;
    if delta.is_negative() {
        return 0;
    }
    u64::try_from(delta.whole_milliseconds()).unwrap_or(u64::MAX)
}

/// Whether a status bucket holds queued work that has aged past the threshold.
///
/// Terminal buckets never warn. A `Published` outbox row or a `Processed`
/// received row is retained history, and its age reflects the retention window,
/// not a backlog — flagging it would make the warning meaningless on any
/// deployment that keeps history.
fn over_max_queue_age(
    is_terminal: bool,
    oldest_age_ms: Option<u64>,
    max_queue_age: Duration,
) -> bool {
    if is_terminal {
        return false;
    }
    let threshold = u64::try_from(max_queue_age.as_millis()).unwrap_or(u64::MAX);
    oldest_age_ms.is_some_and(|age_ms| age_ms >= threshold)
}
