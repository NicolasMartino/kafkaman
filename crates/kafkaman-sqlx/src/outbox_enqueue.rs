use kafkaman_core::InstrumentDb;
use kafkaman_core::{Envelope, KafkaMessage, OutboxStatus, TraceContext};
use serde::Serialize;
use sqlx::{Connection, PgConnection, Postgres, Transaction};
use time::OffsetDateTime;
use tracing::Instrument;
use uuid::Uuid;

use crate::catch_panic::catch_application_panic;
use crate::lock_keys::outbox_entity_lock_key;
use crate::{Error, OutboxTable, ResolvedConfig, Result};

// Stays in the debug tier: see the module docs — promoting it would move the trace root onto a function name.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
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
    // The span is opened here rather than inside, because the trace context
    // persisted on the row is captured *from* it: the relay's publish span
    // becomes a child of this one, minutes or hours later, which is the causal
    // link the outbox pattern otherwise breaks.
    let span = tracing::info_span!(
        "kafkaman.enqueue",
        "otel.kind" = "producer",
        "otel.status_code" = tracing::field::Empty,
        "otel.status_description" = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        message_type = P::MESSAGE_TYPE,
        messaging.system = "kafka",
        messaging.destination.name = P::TOPIC,
        messaging.operation.name = "create",
    );
    // Captured from `span` by name rather than from whatever span is current
    // inside `enqueue_inner`. The comment above has always claimed this; now it
    // is true regardless of what nests in between.
    let trace = kafkaman_core::capture_trace_context_of(&span);
    let result = enqueue_inner(conn, cfg, evt, trace)
        .instrument(span.clone())
        .await;
    if let Err(err) = &result {
        kafkaman_core::record_exception(&span, err);
    }
    result
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn enqueue_inner<P>(
    conn: &mut PgConnection,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
    trace: Option<kafkaman_core::TraceContext>,
) -> Result<()>
where
    P: KafkaMessage + Serialize,
{
    let table = OutboxTable::for_message::<P>(cfg)?;
    if let Some(reserved) = kafkaman_core::reserved_header(&evt.headers) {
        return Err(Error::ReservedHeader(reserved.to_owned()));
    }
    // The application's own code, called by kafkaman. A panic in either would
    // otherwise unwind out of `enqueue` into whatever called it — commonly an
    // HTTP handler, whose task dies with no stored trace of why. The receive
    // side wraps the same two calls; see `catch_application_panic`.
    let message_type = table.descriptor.message_type.as_str();
    let partition_key = catch_application_panic(message_type, "resolve partition key", || {
        evt.payload.partition_key()
    })?;
    let entity_key = catch_application_panic(message_type, "resolve entity key", || {
        evt.payload.entity_key()
    })?;
    let mut headers = evt.headers.clone();
    // Carried for foreign consumers that cannot deserialize the typed payload.
    // kafkaman's own ingest does not rely on it: it resolves the entity key from
    // the payload and stores it as a column, because this header is inside the
    // reserved namespace that ingest strips from user headers.
    if partition_key.as_deref() != Some(entity_key.as_str()) {
        headers.insert("kafkaman-entity-key".to_owned(), entity_key.clone());
    }
    let headers = serde_json::to_value(&headers)?;
    // The application's `Serialize` impl, for the same reason.
    let payload = catch_application_panic(message_type, "serialize payload", || {
        serde_json::to_value(&evt.payload)
    })??;
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
        trace,
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

    let mut guard = conn
        .begin()
        .instrument_db(kafkaman_core::db_span!(
            "BEGIN",
            table.qualified_name(),
            "open outbox enqueue transaction",
        ))
        .await?;
    lock_outbox_entity(&mut guard, &table, &entity_key).await?;
    supersede_pending_outbox_rows(&mut guard, &table, &entity_key).await?;
    insert
        .execute(&mut *guard, OutboxStatus::Pending, None)
        .await?;
    guard
        .commit()
        .instrument_db(kafkaman_core::db_span!(
            "COMMIT",
            table.qualified_name(),
            "commit outbox enqueue transaction",
        ))
        .await?;
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
    trace: Option<TraceContext>,
    headers: serde_json::Value,
    payload: serde_json::Value,
    occurred_at: OffsetDateTime,
}

impl InsertOutboxRow<'_> {
    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
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
                last_error, topic, partition_key, entity_key, correlation_id, causation_id,
                traceparent, tracestate, headers, payload, occurred_at
            ) VALUES (
                $1, $2, $3, {status}, 0, now(), $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )",
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
            .bind(
                self.trace
                    .as_ref()
                    .map(|trace| trace.traceparent().to_owned()),
            )
            .bind(
                self.trace
                    .as_ref()
                    .and_then(|trace| trace.tracestate().map(ToOwned::to_owned)),
            )
            .bind(self.headers)
            .bind(self.payload)
            .bind(self.occurred_at)
            .execute(executor)
            .instrument_db(kafkaman_core::db_span!(
                "INSERT",
                self.table.qualified_name(),
                "insert outbox row",
            ))
            .await?;
        Ok(())
    }
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn lock_outbox_entity(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    entity_key: &str,
) -> Result<()> {
    let key = outbox_entity_lock_key(table, entity_key);
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .instrument_db(kafkaman_core::db_span!(
            "SELECT",
            table.qualified_name(),
            "lock outbox entity",
        ))
        .await?;
    Ok(())
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
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
        .instrument_db(kafkaman_core::db_span!(
            "UPDATE",
            table.qualified_name(),
            "supersede pending outbox rows",
        ))
        .await?;
    Ok(())
}
