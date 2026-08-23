use kafkaman_core::{Envelope, KafkaMessage, ReceiveStatus, ReceivedIngestFailureKind};
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::schema_sql::received_ingest_failures_table_name;
use crate::{
    Error, ReceivedIngestFailure, ReceivedIngestFailureRow, ReceivedInsertOutcome, ReceivedTable,
    ResolvedConfig, Result,
};

pub async fn insert_received<P>(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
    source_partition: i32,
    source_offset: i64,
    key: Option<&[u8]>,
) -> Result<bool>
where
    P: KafkaMessage + Serialize,
{
    Ok(matches!(
        insert_received_with_outcome(tx, cfg, evt, source_partition, source_offset, key).await?,
        ReceivedInsertOutcome::Inserted
    ))
}

pub async fn insert_received_with_outcome<P>(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
    source_partition: i32,
    source_offset: i64,
    key: Option<&[u8]>,
) -> Result<ReceivedInsertOutcome>
where
    P: KafkaMessage + Serialize,
{
    let table = ReceivedTable::for_message::<P>(cfg)?;
    if let Some(reserved) = kafkaman_core::reserved_header(&evt.headers) {
        return Err(Error::ReservedHeader(reserved.to_owned()));
    }
    let idempotency_key = evt
        .idempotency_key
        .as_ref()
        .ok_or(Error::MissingIdempotencyKey)?;
    let idempotency_key_hex = idempotency_key.key.to_string();
    let idempotency_source = idempotency_key
        .source
        .as_ref()
        .map(|source| source.value().clone());
    let headers = serde_json::to_value(&evt.headers)?;
    let payload = serde_json::to_value(&evt.payload)?;
    // Resolve the convergence identity here, where the payload is still typed,
    // and persist it as a column. Recovering it later from a `kafkaman-` header
    // would be fragile: ingest deliberately strips that namespace from user
    // headers, so a header-sourced entity key does not survive a broker round
    // trip.
    let entity_key = evt.payload.entity_key();

    let sql = format!(
        "INSERT INTO {name} (
            message_id, idempotency_key, idempotency_source, status, attempts, next_attempt_at, errors,
            source_topic, source_partition, source_offset, key, entity_key, message_type, message_version,
            headers, payload, correlation_id, causation_id, occurred_at
        ) VALUES (
            $1, $2, $3, {pending}, 0, NULL, '[]'::jsonb,
            $4, $5, $6, $7, $8, $9, 1,
            $10, $11, $12, $13, $14
        ) ON CONFLICT DO NOTHING",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
    );

    let result = sqlx::query(&sql)
        .bind(evt.message_id)
        .bind(idempotency_key_hex.as_str())
        .bind(idempotency_source)
        .bind(table.descriptor.topic.clone())
        .bind(source_partition)
        .bind(source_offset)
        .bind(key)
        .bind(entity_key)
        .bind(table.descriptor.message_type.as_str())
        .bind(headers)
        .bind(payload)
        .bind(evt.correlation_id)
        .bind(evt.causation_id)
        .bind(evt.occurred_at)
        .execute(&mut **tx)
        .await?;

    if result.rows_affected() == 1 {
        return Ok(ReceivedInsertOutcome::Inserted);
    }

    received_insert_conflict_outcome(tx, &table, evt.message_id, &idempotency_key_hex).await
}

async fn received_insert_conflict_outcome(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    message_id: Uuid,
    idempotency_key: &str,
) -> Result<ReceivedInsertOutcome> {
    let sql = format!(
        "SELECT message_id, idempotency_key
         FROM {}
         WHERE message_id = $1 OR idempotency_key = $2",
        table.qualified_name()
    );
    let rows = sqlx::query(&sql)
        .bind(message_id)
        .bind(idempotency_key)
        .fetch_all(&mut **tx)
        .await?;

    let mut saw_message_id = false;
    for row in rows {
        let row_message_id: Uuid = row.try_get("message_id")?;
        let row_idempotency_key: String = row.try_get("idempotency_key")?;
        if row_idempotency_key == idempotency_key {
            return Ok(ReceivedInsertOutcome::DuplicateIdempotencyKey);
        }
        saw_message_id |= row_message_id == message_id;
    }

    if saw_message_id {
        Ok(ReceivedInsertOutcome::MessageIdConflict)
    } else {
        Ok(ReceivedInsertOutcome::DuplicateIdempotencyKey)
    }
}

pub async fn insert_received_ingest_failure(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    failure: &ReceivedIngestFailure,
) -> Result<bool> {
    let table = received_ingest_failures_table_name(cfg)?;
    let sql = format!(
        "INSERT INTO {table} (
            source_topic, source_partition, source_offset, key, headers, payload,
            message_type, expected_topic, failure_kind, error
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING"
    );
    let result = sqlx::query(&sql)
        .bind(failure.source_topic.as_str())
        .bind(failure.source_partition)
        .bind(failure.source_offset)
        .bind(failure.key.as_deref())
        .bind(&failure.headers)
        .bind(failure.payload.as_deref())
        .bind(failure.message_type.as_str())
        .bind(failure.expected_topic.as_str())
        .bind(failure.kind.discriminant())
        .bind(failure.error.as_str())
        .execute(&mut **tx)
        .await?;

    Ok(result.rows_affected() == 1)
}

pub async fn received_ingest_failure_by_source(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    source_topic: &str,
    source_partition: i32,
    source_offset: i64,
) -> Result<Option<ReceivedIngestFailureRow>> {
    let table = received_ingest_failures_table_name(cfg)?;
    let sql = format!(
        "SELECT *
         FROM {table}
         WHERE source_topic = $1
           AND source_partition = $2
           AND source_offset = $3"
    );
    let row = sqlx::query(&sql)
        .bind(source_topic)
        .bind(source_partition)
        .bind(source_offset)
        .fetch_optional(pool)
        .await?;

    row.map(received_ingest_failure_row_from_pg).transpose()
}

fn received_ingest_failure_row_from_pg(row: PgRow) -> Result<ReceivedIngestFailureRow> {
    let kind: String = row.try_get("failure_kind")?;
    let kind = ReceivedIngestFailureKind::from_discriminant(&kind)
        .ok_or_else(|| Error::InvalidIngestFailureKind(kind.clone()))?;

    Ok(ReceivedIngestFailureRow {
        failure: ReceivedIngestFailure {
            source_topic: row.try_get("source_topic")?,
            source_partition: row.try_get("source_partition")?,
            source_offset: row.try_get("source_offset")?,
            key: row.try_get("key")?,
            headers: row.try_get("headers")?,
            payload: row.try_get("payload")?,
            message_type: row.try_get("message_type")?,
            expected_topic: row.try_get("expected_topic")?,
            kind,
            error: row.try_get("error")?,
        },
        created_at: row.try_get("created_at")?,
    })
}
