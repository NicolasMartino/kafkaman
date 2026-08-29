use std::collections::BTreeMap;

use kafkaman_core::{
    IdempotencyKey, ReceiveStatus, ReceivedError, ReceivedFailureKind, ReceivedRow,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::schema_sql::sql_string_literal;
use crate::{Error, ReceivedTable, Result};

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn received_row(
    pool: &PgPool,
    table: &ReceivedTable,
    message_id: Uuid,
) -> Result<Option<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(pool)
        .await?;
    row.map(received_row_from_pg).transpose()
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn received_row_by_idempotency_key(
    pool: &PgPool,
    table: &ReceivedTable,
    idempotency_key: IdempotencyKey,
) -> Result<Option<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {} WHERE idempotency_key = $1",
        table.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(idempotency_key.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(received_row_from_pg).transpose()
}

/// Narrows a terminal-failure (DLQ) inspection or redrive to a subset of the
/// `Failed` rows. An empty filter matches every terminal row.
#[derive(Clone, Debug, Default)]
pub struct ReceivedFailureFilter {
    /// Only rows whose latest recorded failure occurred at or after this instant.
    pub occurred_after: Option<OffsetDateTime>,
    /// Only rows whose most recent failure was of this kind.
    pub kind: Option<ReceivedFailureKind>,
}

impl ReceivedFailureFilter {
    pub fn since(mut self, occurred_after: OffsetDateTime) -> Self {
        self.occurred_after = Some(occurred_after);
        self
    }

    pub fn kind(mut self, kind: ReceivedFailureKind) -> Self {
        self.kind = Some(kind);
        self
    }
}

fn received_failed_where_sql(filter: &ReceivedFailureFilter) -> Result<String> {
    let mut sql = format!("status = {}", ReceiveStatus::Failed.sql_literal());
    if let Some(occurred_after) = filter.occurred_after {
        let formatted = occurred_after
            .format(&Rfc3339)
            .map_err(|err| Error::InvalidReceivedFilter(err.to_string()))?;
        sql.push_str(" AND ");
        sql.push_str(latest_failure_time_sql());
        sql.push_str(" >= ");
        sql.push_str(&sql_string_literal(&formatted));
        sql.push_str("::timestamptz");
    }
    if let Some(kind) = filter.kind {
        sql.push_str(&latest_failure_kind_clause(kind));
    }
    Ok(sql)
}

/// List terminal `Failed` (DLQ) receive rows for triage, oldest failure first.
/// A row reaches `Failed` only after its retry budget is exhausted, so these are
/// the rows an operator inspects before redriving them with [`Replay::received`](crate::Replay::received).
/// `filter` narrows by failure time and/or most-recent failure kind; `limit`
/// bounds the page size (clamped to non-negative). Each row carries its
/// preserved attempts and bounded error history intact.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn received_failed_rows(
    pool: &PgPool,
    table: &ReceivedTable,
    filter: &ReceivedFailureFilter,
    limit: i64,
) -> Result<Vec<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {name}
         WHERE {where_sql}
         ORDER BY {failure_order}
         LIMIT $1",
        name = table.qualified_name(),
        where_sql = received_failed_where_sql(filter)?,
        failure_order = received_failure_order_sql(),
    );
    let rows = sqlx::query(&sql).bind(limit.max(0)).fetch_all(pool).await?;
    rows.into_iter().map(received_row_from_pg).collect()
}

/// Count terminal `Failed` (DLQ) receive rows matching `filter` (an empty filter
/// counts the whole terminal backlog).
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn received_failed_count(
    pool: &PgPool,
    table: &ReceivedTable,
    filter: &ReceivedFailureFilter,
) -> Result<i64> {
    let sql = format!(
        "SELECT count(*) FROM {name} WHERE {where_sql}",
        name = table.qualified_name(),
        where_sql = received_failed_where_sql(filter)?,
    );
    Ok(sqlx::query_scalar::<_, i64>(&sql).fetch_one(pool).await?)
}

/// The column carrying the time of the most recent recorded failure.
///
/// This is deliberately a real `timestamptz` column rather than a cast over the
/// `errors` audit JSON. Casting bound DLQ triage to the JSON serialization
/// format — an RFC 9557 annotated timestamp, or `time`'s default component
/// array, cannot be cast at all — and could not be indexed without a functional
/// index over a JSON traversal.
pub(crate) fn latest_failure_time_sql() -> &'static str {
    "last_failed_at"
}

/// Total ordering for DLQ inspection and bounded redrive: oldest failure first,
/// ties broken by row age and finally by `message_id`.
///
/// `last_failed_at` alone is not a total order — a batch of rows failed by the
/// same dispatch pass shares an instant — and `message_id` is a random UUID, so
/// without `created_at` the tiebreak is arbitrary. That matters beyond
/// presentation: a redrive bounded by `max_rows` would otherwise select an
/// unpredictable subset of a tied group.
pub(crate) fn received_failure_order_sql() -> &'static str {
    "last_failed_at, created_at, message_id"
}

pub(crate) fn received_row_from_pg(row: PgRow) -> Result<ReceivedRow> {
    let status: String = row.try_get("status")?;
    let headers: serde_json::Value = row.try_get("headers")?;
    let headers: BTreeMap<String, String> = serde_json::from_value(headers)?;
    let errors: serde_json::Value = row.try_get("errors")?;
    let errors: Vec<ReceivedError> = serde_json::from_value(errors)?;
    let idempotency_key: String = row.try_get("idempotency_key")?;
    let idempotency_key = IdempotencyKey::from_hex(&idempotency_key).map_err(Error::Core)?;

    Ok(ReceivedRow {
        message_id: row.try_get("message_id")?,
        idempotency_key,
        idempotency_source: row.try_get("idempotency_source")?,
        status: status.parse::<ReceiveStatus>().map_err(Error::Core)?,
        attempts: row.try_get("attempts")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        errors,
        source_topic: row.try_get("source_topic")?,
        source_partition: row.try_get("source_partition")?,
        source_offset: row.try_get("source_offset")?,
        key: row.try_get("key")?,
        entity_key: row.try_get("entity_key")?,
        message_type: row.try_get("message_type")?,
        message_version: row.try_get("message_version")?,
        headers,
        payload: row.try_get("payload")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        trace: kafkaman_core::TraceContext::from_parts(
            row.try_get("traceparent")?,
            row.try_get("tracestate")?,
        ),
        occurred_at: row.try_get("occurred_at")?,
        created_at: row.try_get("created_at")?,
        processed_at: row.try_get("processed_at")?,
    })
}

/// SQL predicate fragment (` AND ...`) selecting received rows whose most recent
/// stored failure is of `kind`, read from the `last_failure_kind` column that
/// failure accounting maintains alongside the `errors` audit trail.
pub(crate) fn latest_failure_kind_clause(kind: ReceivedFailureKind) -> String {
    format!(
        " AND last_failure_kind = {}",
        sql_string_literal(kind.discriminant())
    )
}
