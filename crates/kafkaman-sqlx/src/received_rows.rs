//! Reading and writing rows of a received table.
//!
//! Every function here takes `&mut PgConnection` rather than one variant for a
//! pool and another for a transaction. Both callers can produce it — `&mut *tx`
//! from a transaction, `&mut *conn` from a pooled connection — and the two-form
//! version of this module was four functions that were two, differing only in
//! the executor and drifting the moment one was fixed and the other was not.

use kafkaman_core::{MarkOutcome, ReceiveStatus, ReceivedError, ReceivedRow};
use sqlx::PgConnection;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::dispatch_failure::FailureDisposition;
use crate::queries::received_row_from_pg;
use crate::retry_backoff::received_failure_schedule;
use crate::{ReceivedTable, Result};

/// Take the next due row for dispatch, locking it against other workers.
///
/// `FOR UPDATE SKIP LOCKED` is what lets several dispatchers share one table:
/// a row another worker holds is passed over rather than waited on.
pub(crate) async fn claim_received_row(
    conn: &mut PgConnection,
    table: &ReceivedTable,
    due_at: OffsetDateTime,
) -> Result<Option<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {name}
         WHERE (
             status = {pending}
             AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
         ) OR (
             status = {retryable}
             AND next_attempt_at <= $1
         )
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        retryable = ReceiveStatus::Retryable.sql_literal(),
    );
    let row = sqlx::query(&sql).bind(due_at).fetch_optional(conn).await?;
    row.map(received_row_from_pg).transpose()
}

pub(crate) async fn mark_received_processed(
    conn: &mut PgConnection,
    table: &ReceivedTable,
    message_id: Uuid,
    processed_at: OffsetDateTime,
) -> Result<MarkOutcome> {
    let sql = format!(
        "UPDATE {} SET status = {}, processed_at = $2 WHERE message_id = $1 AND status IN ({}, {})",
        table.qualified_name(),
        ReceiveStatus::Processed.sql_literal(),
        ReceiveStatus::Pending.sql_literal(),
        ReceiveStatus::Retryable.sql_literal(),
    );
    let result = sqlx::query(&sql)
        .bind(message_id)
        .bind(processed_at)
        .execute(&mut *conn)
        .await?;

    if result.rows_affected() == 1 {
        Ok(MarkOutcome::Updated)
    } else {
        mark_miss_outcome(conn, table, message_id).await
    }
}

/// Everything one failure needs recorded about it.
///
/// Bundled rather than passed as seven positional arguments, three of which are
/// integers or timestamps that would silently swap.
pub(crate) struct ReceivedFailureRecord {
    pub(crate) message_id: Uuid,
    pub(crate) disposition: FailureDisposition,
    pub(crate) message: String,
    pub(crate) occurred_at: OffsetDateTime,
    pub(crate) current_attempts: i32,
}

impl ReceivedFailureRecord {
    /// Everything but the disposition and the message is read off the row, so
    /// the two recording paths cannot disagree about which attempt count or
    /// message id a failure belongs to.
    pub(crate) fn of(
        row: &ReceivedRow,
        disposition: FailureDisposition,
        message: String,
        occurred_at: OffsetDateTime,
    ) -> Self {
        Self {
            message_id: row.message_id,
            disposition,
            message,
            occurred_at,
            current_attempts: row.attempts,
        }
    }
}

/// Append a failure to a row's audit trail and schedule or exhaust its retry.
pub(crate) async fn record_received_failure(
    conn: &mut PgConnection,
    table: &ReceivedTable,
    failure: ReceivedFailureRecord,
) -> Result<MarkOutcome> {
    let error = serde_json::to_value(ReceivedError::new(
        failure.disposition.kind,
        failure.message,
        failure.occurred_at,
    ))?;
    let schedule = received_failure_schedule(
        table,
        failure.current_attempts,
        failure.occurred_at,
        failure.disposition,
    );

    let result = sqlx::query(&record_received_failure_sql(table))
        .bind(failure.message_id)
        .bind(error)
        .bind(schedule.exhausted)
        .bind(schedule.next_attempt_at)
        .bind(schedule.errors_limit)
        .bind(failure.occurred_at)
        .bind(failure.disposition.kind.discriminant())
        .execute(&mut *conn)
        .await?;

    if result.rows_affected() == 1 {
        Ok(MarkOutcome::Updated)
    } else {
        mark_miss_outcome(conn, table, failure.message_id).await
    }
}

/// Why a mark matched no row: the claim was lost, or the row is gone entirely.
///
/// The distinction matters to the caller. A stale claim means another worker
/// owns the row and will finish it; a missing row means the work is not coming
/// back and nothing else will report that.
async fn mark_miss_outcome(
    conn: &mut PgConnection,
    table: &ReceivedTable,
    message_id: Uuid,
) -> Result<MarkOutcome> {
    let sql = format!(
        "SELECT message_id FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let exists = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(conn)
        .await?
        .is_some();

    Ok(if exists {
        MarkOutcome::StaleClaim
    } else {
        MarkOutcome::Missing
    })
}

/// Record a failure and bound the stored history in one statement.
///
/// The `errors` column keeps only the newest `$5` entries: an unbounded array
/// on a row that fails repeatedly grows the JSONB without limit, and every
/// subsequent append rewrites the whole value.
fn record_received_failure_sql(table: &ReceivedTable) -> String {
    format!(
        "UPDATE {name}
         SET status = CASE WHEN $3::bool THEN {failed} ELSE {retryable} END,
             attempts = attempts + 1,
             next_attempt_at = CASE WHEN $3::bool THEN NULL ELSE $4::timestamptz END,
             errors = COALESCE((
                 SELECT jsonb_agg(value ORDER BY ord)
                 FROM (
                     SELECT value, ord
                     FROM jsonb_array_elements(errors || jsonb_build_array($2::jsonb))
                         WITH ORDINALITY AS entries(value, ord)
                     ORDER BY ord DESC
                     LIMIT $5
                 ) kept
             ), '[]'::jsonb),
             last_failed_at = $6::timestamptz,
             last_failure_kind = $7::text
         WHERE message_id = $1
           AND status IN ({pending}, {retryable})",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        retryable = ReceiveStatus::Retryable.sql_literal(),
        failed = ReceiveStatus::Failed.sql_literal(),
    )
}
