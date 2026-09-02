//! The read-only operator surface: health, queue depth, stuck rows, and DLQ
//! inspection.
//!
//! Every route here is a `GET` and none of them change anything. The
//! destructive redrive route deliberately lives in [`crate::redrive`] instead,
//! so that mounting it is a decision rather than a side effect.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use kafkaman_core::{MessageDescriptor, ReceivedError};
use kafkaman_sqlx::{
    outbox_status_summary, outbox_stuck_rows, received_failed_count, received_failed_rows,
    received_ingest_failure_summary, received_status_summary, received_stuck_rows, service_tables,
    OutboxStatusSummary, OutboxStuckRow, OutboxTable, ReceivedFailureFilter,
    ReceivedIngestFailureSummary, ReceivedStatusSummary, ReceivedStuckRow, ReceivedTable,
    ResolvedConfig, ServiceTables,
};
use serde::Serialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::AdminError;
use crate::state::AdminState;

/// Read-only operator routes: health, queue depth, stuck rows, DLQ inspection.
///
/// Every route here is a `GET` and none of them change anything. The destructive
/// redrive route is deliberately *not* included — it lives in
/// [`redrive_router`](crate::redrive_router), so that a deployment wanting dashboards does not acquire
/// a way to re-run handlers by accident.
///
/// # Security
///
/// **This router has no authentication or authorization.** Read-only is not the
/// same as harmless: `/dlq` returns business keys and failure messages, which
/// can be as sensitive as the payloads deliberately omitted from the response.
/// Mount it on an internal listener, or behind your own auth middleware — never
/// on a public route table:
///
/// ```ignore
/// let admin = admin_router(state).layer(my_auth_layer());
/// let app = Router::new().nest("/internal/kafkaman", admin);
/// ```
///
/// # Cost
///
/// The summary routes run unbounded aggregates per message type. They are built
/// for an operator or a scrape interval measured in seconds, not for a
/// per-request health probe. `/health` is the cheap one.
pub fn admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/outbox", get(outbox_summary))
        .route("/received", get(received_summary))
        .route("/ingest-failures", get(ingest_failure_summary))
        .route("/stuck", get(stuck_rows))
        .route("/dlq", get(dlq_summary))
        .with_state(state)
}

/// Liveness: the process is up and serving. Touches no dependency on purpose,
/// so a database blip cannot cause an orchestrator to kill a healthy process.
// Stays in the debug tier: an orchestrator probes this forever and it touches nothing.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Readiness: the process can reach Postgres.
///
/// Deliberately narrower than "kafkaman is healthy" — it does not verify that
/// tables exist, that migrations are current, or that workers are running.
// Stays in the debug tier: an orchestrator probes this forever.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn ready(State(state): State<AdminState>) -> Response {
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
    {
        Ok(_) => Json(serde_json::json!({ "status": "ready" })).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "kafkaman readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "status": "unready" })),
            )
                .into_response()
        }
    }
}

/// Outbox depth per message type and status.
///
/// One query per registered message type, run in sequence. Sequential is
/// deliberate: these are unbounded aggregates, and issuing them concurrently
/// would take one pool connection per message type away from the relay and
/// dispatcher to serve an operator request. It also keeps the response order
/// stable, which matters for a JSON API.
///
/// `now` is sampled once for the whole request so every table's age is measured
/// against the same clock.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn outbox_summary(
    State(state): State<AdminState>,
) -> Result<Json<Vec<OutboxStatusSummary>>, AdminError> {
    let now = OffsetDateTime::now_utc();
    let tables = service_tables(&state.pool, &state.cfg).await?;
    ensure_service_tables(&state.cfg, &tables)?;
    let mut summaries = Vec::new();
    for descriptor in state.cfg.messages() {
        let policy = state
            .cfg
            .observability
            .policy_for(descriptor.message_type.as_str());
        let table = OutboxTable::new(state.cfg.schema.clone(), descriptor.clone())?;
        // A type this service only consumes has no outbox table, and querying
        // one that was never created fails the whole request rather than
        // omitting a row. See `ServiceTables`.
        let qualified_name = table.qualified_name();
        if !tables.contains(&qualified_name) {
            log_skipped_table("outbox", descriptor, &qualified_name);
            continue;
        }
        summaries
            .extend(outbox_status_summary(&state.pool, &table, now, policy.max_queue_age).await?);
    }
    Ok(Json(summaries))
}

/// Received-table depth per message type and status. Same cost and ordering
/// rationale as [`outbox_summary`].
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn received_summary(
    State(state): State<AdminState>,
) -> Result<Json<Vec<ReceivedStatusSummary>>, AdminError> {
    let now = OffsetDateTime::now_utc();
    let tables = service_tables(&state.pool, &state.cfg).await?;
    ensure_service_tables(&state.cfg, &tables)?;
    let mut summaries = Vec::new();
    for descriptor in state.cfg.messages() {
        let policy = state
            .cfg
            .observability
            .policy_for(descriptor.message_type.as_str());
        let table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
        // Symmetrically: a type this service only publishes has no received
        // table.
        let qualified_name = table.qualified_name();
        if !tables.contains(&qualified_name) {
            log_skipped_table("received", descriptor, &qualified_name);
            continue;
        }
        summaries
            .extend(received_status_summary(&state.pool, &table, now, policy.max_queue_age).await?);
    }
    Ok(Json(summaries))
}

/// Schema-wide quarantine depth for records that never became received rows.
///
/// This is intentionally a summary, not a row listing. Quarantine rows can carry
/// the raw payload and Kafka headers that failed to decode, and returning those
/// from the unauthenticated read-only router would turn a storage-growth
/// diagnostic into a payload-inspection endpoint.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn ingest_failure_summary(
    State(state): State<AdminState>,
) -> Result<Json<Vec<ReceivedIngestFailureSummary>>, AdminError> {
    let now = OffsetDateTime::now_utc();
    Ok(Json(
        received_ingest_failure_summary(&state.pool, &state.cfg, now).await?,
    ))
}

/// Rows that are overdue on either side of the ledger.
///
/// Both lists are truncated per message type, so an empty list means "nothing
/// stuck" but a full one does not mean "exactly this much stuck".
#[derive(Clone, Debug, Serialize)]
pub struct StuckResponse {
    pub outbox: Vec<OutboxStuckRow>,
    pub received: Vec<ReceivedStuckRow>,
    /// Set when any message type filled its per-type cap, so the operator knows
    /// the lists are truncated rather than complete.
    pub truncated: bool,
}

/// Expired outbox claims and overdue received rows, using each message type's
/// configured `stuck_after` threshold.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn stuck_rows(State(state): State<AdminState>) -> Result<Json<StuckResponse>, AdminError> {
    const LIMIT_PER_TYPE: i64 = 100;

    let now = OffsetDateTime::now_utc();
    let tables = service_tables(&state.pool, &state.cfg).await?;
    ensure_service_tables(&state.cfg, &tables)?;
    let mut truncated = false;
    let mut outbox = Vec::new();
    let mut received = Vec::new();
    for descriptor in state.cfg.messages() {
        let policy = state
            .cfg
            .observability
            .policy_for(descriptor.message_type.as_str());
        let outbox_table = OutboxTable::new(state.cfg.schema.clone(), descriptor.clone())?;
        let outbox_name = outbox_table.qualified_name();
        if tables.contains(&outbox_name) {
            let outbox_batch = outbox_stuck_rows(
                &state.pool,
                &outbox_table,
                now,
                policy.stuck_after,
                LIMIT_PER_TYPE,
            )
            .await?;
            truncated |= i64::try_from(outbox_batch.len()).unwrap_or(i64::MAX) >= LIMIT_PER_TYPE;
            outbox.extend(outbox_batch);
        } else {
            log_skipped_table("outbox", descriptor, &outbox_name);
        }

        let received_table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
        let received_name = received_table.qualified_name();
        if tables.contains(&received_name) {
            let received_batch = received_stuck_rows(
                &state.pool,
                &received_table,
                now,
                policy.stuck_after,
                LIMIT_PER_TYPE,
            )
            .await?;
            truncated |= i64::try_from(received_batch.len()).unwrap_or(i64::MAX) >= LIMIT_PER_TYPE;
            received.extend(received_batch);
        } else {
            log_skipped_table("received", descriptor, &received_name);
        }
    }
    Ok(Json(StuckResponse {
        outbox,
        received,
        truncated,
    }))
}

/// Dead-lettered rows for one message type.
///
/// `count` is the true total; `rows` is a capped sample of it. They will differ
/// on any DLQ larger than the cap, and that is the point — an operator needs the
/// real depth even when the listing is truncated.
#[derive(Clone, Debug, Serialize)]
pub struct DlqSummary {
    pub message_type: String,
    pub count: i64,
    pub rows: Vec<DlqRowSummary>,
}

/// A dead-lettered row, reduced to what triage needs.
///
/// # What this deliberately omits
///
/// The `payload` body and the `headers` map are never included. A durable row
/// carries the full user message; this projection does not, so mounting the DLQ
/// route cannot turn into a payload-exfiltration endpoint.
///
/// # What it still carries
///
/// `entity_key` is a caller-chosen business key and `latest_error.detail` is a
/// free-text string produced by your handler. Neither is payload, but both can
/// carry whatever the application put in them, up to and including personal
/// data. A panic detail is even less curated: it can include assertion dumps or
/// `Debug` output the application never meant to expose. Treat this response as
/// sensitive; it is sanitized of message bodies, not of everything.
#[derive(Clone, Debug, Serialize)]
pub struct DlqRowSummary {
    pub message_id: Uuid,
    pub entity_key: Option<String>,
    pub attempts: i32,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub created_at: OffsetDateTime,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub processed_at: Option<OffsetDateTime>,
    pub latest_error: Option<ReceivedError>,
    pub error_count: usize,
}

/// Dead-letter depth and a capped listing per message type.
///
/// Two queries per message type — an exact count plus a bounded page — because
/// the count must stay honest when the listing is truncated.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn dlq_summary(State(state): State<AdminState>) -> Result<Json<Vec<DlqSummary>>, AdminError> {
    const ROW_LIMIT_PER_TYPE: i64 = 50;

    let filter = ReceivedFailureFilter::default();
    let tables = service_tables(&state.pool, &state.cfg).await?;
    ensure_service_tables(&state.cfg, &tables)?;
    let mut summaries = Vec::new();
    for descriptor in state.cfg.messages() {
        let table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
        // Only what this service consumes has a dead-letter queue.
        let qualified_name = table.qualified_name();
        if !tables.contains(&qualified_name) {
            log_skipped_table("received", descriptor, &qualified_name);
            continue;
        }
        let count = received_failed_count(&state.pool, &table, &filter).await?;
        let rows = received_failed_rows(&state.pool, &table, &filter, ROW_LIMIT_PER_TYPE).await?;
        summaries.push(DlqSummary {
            message_type: descriptor.message_type.as_str().to_owned(),
            count,
            rows: rows
                .into_iter()
                .map(|row| {
                    let latest_error = row.errors.last().cloned();
                    DlqRowSummary {
                        message_id: row.message_id,
                        entity_key: row.entity_key,
                        attempts: row.attempts,
                        source_topic: row.source_topic,
                        source_partition: row.source_partition,
                        source_offset: row.source_offset,
                        correlation_id: row.correlation_id,
                        causation_id: row.causation_id,
                        created_at: row.created_at,
                        processed_at: row.processed_at,
                        latest_error,
                        error_count: row.errors.len(),
                    }
                })
                .collect(),
        });
    }
    Ok(Json(summaries))
}

/// Resolves a path segment to a registered descriptor.
///
/// An unregistered type is a 404 rather than a 500: the caller named something
/// this application does not carry, which is their error, not ours.
pub(crate) fn descriptor_for_message_type(
    cfg: &ResolvedConfig,
    message_type: &str,
) -> Option<MessageDescriptor> {
    cfg.messages()
        .iter()
        .find(|descriptor| descriptor.message_type.as_str() == message_type)
        .cloned()
}

/// Refuse to answer for a configuration whose schema cannot back it.
///
/// # The invariant, and why it is the right one
///
/// A message type is declared because this service does *something* with it, and
/// the migrations create exactly one table per side. So every declared type must
/// have at least one readable side here. A type with neither is not a service
/// that publishes what it does not consume — it is a schema that was never
/// migrated, or a database role that cannot read it.
///
/// Checking only "are there zero tables in total" caught the fully-unmigrated
/// service and nothing else. A schema missing three of five types answered `200`
/// with those three silently absent, which reads as *these queues are empty* —
/// the most dangerous wrong answer an operator summary can give. Per type, the
/// two cases separate cleanly: one side missing is the normal shape of a
/// service, both sides missing is a broken deployment.
fn ensure_service_tables(cfg: &ResolvedConfig, tables: &ServiceTables) -> Result<(), AdminError> {
    let mut unusable = Vec::new();
    for descriptor in cfg.messages() {
        let outbox = OutboxTable::for_descriptor(cfg, descriptor.clone())?;
        let received = ReceivedTable::for_descriptor(cfg, descriptor.clone())?;
        if !tables.contains(&outbox.qualified_name())
            && !tables.contains(&received.qualified_name())
        {
            unusable.push(descriptor.message_type.as_str().to_owned());
        }
    }
    if unusable.is_empty() {
        return Ok(());
    }
    Err(AdminError::UnusableServiceTables(unusable))
}

/// Note a message type this service has no table for on one side.
///
/// `debug!` rather than `info!`, and correctly so now that
/// [`ensure_service_tables`] rejects the case worth shouting about. What reaches
/// here is a type this service only publishes or only consumes, which is the
/// normal shape of every real service and would be noise at every request.
fn log_skipped_table(kind: &'static str, descriptor: &MessageDescriptor, qualified_name: &str) {
    tracing::debug!(
        message_type = descriptor.message_type.as_str(),
        table = qualified_name,
        kind,
        "kafkaman admin route skipped the side of a message type this service does not have"
    );
}
