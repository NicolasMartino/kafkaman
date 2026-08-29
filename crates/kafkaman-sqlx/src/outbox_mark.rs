//! Recording what happened to a claimed outbox row, and reading one back.

use kafkaman_core::InstrumentDb;
use std::collections::BTreeMap;
use std::time::Duration;

use kafkaman_core::{IdempotencyKey, MarkOutcome, OutboxRow, OutboxStatus};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tracing::Instrument;
use uuid::Uuid;

use crate::{Error, OutboxTable, Result};

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn mark_published(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
) -> Result<MarkOutcome> {
    let sql = format!(
        "UPDATE {name}
         SET status = {published},
             published_at = now(),
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE message_id = $1 AND status = {publishing} AND claim_id = $2",
        name = table.qualified_name(),
        published = OutboxStatus::Published.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    // The span is built here rather than inside `mark_with_sql`, so a second
    // caller with different SQL cannot inherit this one's summary silently.
    mark_with_sql(
        pool,
        table,
        &sql,
        message_id,
        claim_id,
        kafkaman_core::db_span!("UPDATE", table.qualified_name(), "mark outbox published"),
    )
    .await
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn mark_publish_failed(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
    error: &str,
    retry_after: Duration,
) -> Result<MarkOutcome> {
    // Schedule the retry from the database clock (now() + interval) so retry
    // eligibility shares the same clock as claim eligibility.
    let sql = format!(
        "UPDATE {name}
         SET status = {pending},
             last_error = $3,
             next_attempt_at = now() + make_interval(secs => $4),
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE message_id = $1 AND status = {publishing} AND claim_id = $2",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    let result = sqlx::query(&sql)
        .bind(message_id)
        .bind(claim_id)
        .bind(error)
        .bind(retry_after.as_secs_f64())
        .execute(pool)
        .instrument_db(kafkaman_core::db_span!(
            "UPDATE",
            table.qualified_name(),
            "mark outbox publish failed",
        ))
        .await?;

    if result.rows_affected() == 1 {
        Ok(MarkOutcome::Updated)
    } else {
        mark_miss_outcome(pool, table, message_id).await
    }
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn mark_with_sql(
    pool: &PgPool,
    table: &OutboxTable,
    sql: &str,
    message_id: Uuid,
    claim_id: Uuid,
    span: tracing::Span,
) -> Result<MarkOutcome> {
    let result = sqlx::query(sql)
        .bind(message_id)
        .bind(claim_id)
        .execute(pool)
        .instrument(span)
        .await?;

    if result.rows_affected() == 1 {
        Ok(MarkOutcome::Updated)
    } else {
        mark_miss_outcome(pool, table, message_id).await
    }
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn mark_miss_outcome(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
) -> Result<MarkOutcome> {
    let sql = format!(
        "SELECT message_id FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let exists = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(pool)
        .instrument_db(kafkaman_core::db_span!(
            "SELECT",
            table.qualified_name(),
            "classify outbox mark miss",
        ))
        .await?
        .is_some();
    Ok(if exists {
        MarkOutcome::StaleClaim
    } else {
        MarkOutcome::Missing
    })
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn outbox_row(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
) -> Result<Option<OutboxRow>> {
    let sql = format!(
        "SELECT * FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(pool)
        .instrument_db(kafkaman_core::db_span!(
            "SELECT",
            table.qualified_name(),
            "read outbox row",
        ))
        .await?;
    row.map(row_from_pg).transpose()
}

pub(crate) fn row_from_pg(row: PgRow) -> Result<OutboxRow> {
    let status: String = row.try_get("status")?;
    let headers: serde_json::Value = row.try_get("headers")?;
    let headers: BTreeMap<String, String> = serde_json::from_value(headers)?;
    let idempotency_key: Option<String> = row.try_get("idempotency_key")?;
    let idempotency_key = idempotency_key
        .as_deref()
        .map(IdempotencyKey::from_hex)
        .transpose()
        .map_err(Error::Core)?;

    Ok(OutboxRow {
        message_id: row.try_get("message_id")?,
        idempotency_key,
        idempotency_source: row.try_get("idempotency_source")?,
        status: status.parse::<OutboxStatus>().map_err(Error::Core)?,
        attempts: row.try_get("attempts")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        last_error: row.try_get("last_error")?,
        claim_id: row.try_get("claim_id")?,
        claimed_by: row.try_get("claimed_by")?,
        claim_expires_at: row.try_get("claim_expires_at")?,
        topic: row.try_get("topic")?,
        partition_key: row.try_get("partition_key")?,
        entity_key: row.try_get("entity_key")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        trace: kafkaman_core::TraceContext::from_parts(
            row.try_get("traceparent")?,
            row.try_get("tracestate")?,
        ),
        headers,
        payload: row.try_get("payload")?,
        occurred_at: row.try_get("occurred_at")?,
        created_at: row.try_get("created_at")?,
        published_at: row.try_get("published_at")?,
    })
}
