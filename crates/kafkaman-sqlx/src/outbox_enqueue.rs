use kafkaman_core::{Envelope, KafkaMessage, OutboxStatus};
use serde::Serialize;
use sqlx::{Connection, PgConnection, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::lock_keys::outbox_entity_lock_key;
use crate::{Error, OutboxTable, ResolvedConfig, Result};

pub async fn enqueue<P>(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
) -> Result<()>
where
    P: KafkaMessage + Serialize,
{
    enqueue_on_connection(tx, cfg, evt).await
}

/// Enqueue on a connection that may or may not already be inside a transaction.
///
/// This is the form a dispatch handler needs, since it is handed a
/// `&mut PgConnection` borrowed from the dispatch transaction.
///
/// The per-entity supersede sequence is wrapped in a nested transaction
/// ([`Connection::begin`], which emits `BEGIN` at depth 0 and `SAVEPOINT`
/// inside an open transaction). That is load-bearing rather than tidiness: the
/// entity lock is `pg_advisory_xact_lock`, which is released at the end of the
/// *statement* when the connection is in autocommit. Without an enclosing
/// transaction the lock would therefore be gone before the supersede ran, and
/// two concurrent writers for one entity could both observe no pending row and
/// both insert — exactly the interleaving the offset ordinal depends on being
/// impossible (entity-first propagation decision, point 8).
pub async fn enqueue_on_connection<P>(
    conn: &mut PgConnection,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
) -> Result<()>
where
    P: KafkaMessage + Serialize,
{
    let table = OutboxTable::for_message::<P>(cfg)?;
    if let Some(reserved) = kafkaman_core::reserved_header(&evt.headers) {
        return Err(Error::ReservedHeader(reserved.to_owned()));
    }
    let partition_key = evt.payload.partition_key();
    let entity_key = evt.payload.entity_key();
    let mut headers = evt.headers.clone();
    // Carried for foreign consumers that cannot deserialize the typed payload.
    // kafkaman's own ingest does not rely on it: it resolves the entity key from
    // the payload and stores it as a column, because this header is inside the
    // reserved namespace that ingest strips from user headers.
    if partition_key.as_deref() != Some(entity_key.as_str()) {
        headers.insert("kafkaman-entity-key".to_owned(), entity_key.clone());
    }
    let headers = serde_json::to_value(&headers)?;
    let payload = serde_json::to_value(&evt.payload)?;
    let identity = evt.idempotency_key.as_ref();
    let idempotency_key = identity.map(|identity| identity.key.to_string());
    let idempotency_source = identity
        .and_then(|identity| identity.source.as_ref())
        .map(|source| source.value().clone());

    let insert = InsertOutboxRow {
        table: &table,
        message_id: evt.message_id,
        idempotency_key: idempotency_key.as_deref(),
        idempotency_source,
        entity_key: &entity_key,
        partition_key,
        correlation_id: evt.correlation_id,
        causation_id: evt.causation_id,
        headers,
        payload,
        occurred_at: evt.occurred_at,
    };

    let Some(_identity) = identity else {
        // No idempotency identity: record the rejection as a `Failed` audit row
        // and report the error. The row is written on the caller's connection,
        // so it survives exactly when the caller commits and vanishes when the
        // caller rolls back — the error-row symmetry contract.
        insert
            .execute(
                conn,
                OutboxStatus::Failed,
                Some("missing idempotency identity"),
            )
            .await?;
        return Err(Error::MissingIdempotencyKey);
    };

    let mut guard = conn.begin().await?;
    lock_outbox_entity(&mut guard, &table, &entity_key).await?;
    supersede_pending_outbox_rows(&mut guard, &table, &entity_key).await?;
    insert
        .execute(&mut *guard, OutboxStatus::Pending, None)
        .await?;
    guard.commit().await?;
    Ok(())
}

/// The bound parameters of one outbox insert, so the statement is written once
/// for both the accepted and the rejected-audit path.
struct InsertOutboxRow<'a> {
    table: &'a OutboxTable,
    message_id: Uuid,
    idempotency_key: Option<&'a str>,
    idempotency_source: Option<serde_json::Value>,
    entity_key: &'a str,
    partition_key: Option<String>,
    correlation_id: Uuid,
    causation_id: Option<Uuid>,
    headers: serde_json::Value,
    payload: serde_json::Value,
    occurred_at: OffsetDateTime,
}

impl InsertOutboxRow<'_> {
    async fn execute<'c, E>(
        self,
        executor: E,
        status: OutboxStatus,
        last_error: Option<&str>,
    ) -> Result<()>
    where
        E: sqlx::Executor<'c, Database = Postgres>,
    {
        let sql = format!(
            "INSERT INTO {name} (
                message_id, idempotency_key, idempotency_source, status, attempts, next_attempt_at,
                last_error, topic, partition_key, entity_key, correlation_id, causation_id, headers,
                payload, occurred_at
            ) VALUES ($1, $2, $3, {status}, 0, now(), $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            name = self.table.qualified_name(),
            status = status.sql_literal(),
        );

        sqlx::query(&sql)
            .bind(self.message_id)
            .bind(self.idempotency_key)
            .bind(self.idempotency_source)
            .bind(last_error)
            .bind(self.table.descriptor.topic.clone())
            .bind(self.partition_key)
            .bind(self.entity_key)
            .bind(self.correlation_id)
            .bind(self.causation_id)
            .bind(self.headers)
            .bind(self.payload)
            .bind(self.occurred_at)
            .execute(executor)
            .await?;
        Ok(())
    }
}

async fn lock_outbox_entity(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    entity_key: &str,
) -> Result<()> {
    let key = outbox_entity_lock_key(table, entity_key);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn supersede_pending_outbox_rows(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    entity_key: &str,
) -> Result<()> {
    let sql = format!(
        "UPDATE {name}
         SET status = {superseded},
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE entity_key = $1 AND status = {pending}",
        name = table.qualified_name(),
        superseded = OutboxStatus::Superseded.sql_literal(),
        pending = OutboxStatus::Pending.sql_literal(),
    );
    sqlx::query(&sql)
        .bind(entity_key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
