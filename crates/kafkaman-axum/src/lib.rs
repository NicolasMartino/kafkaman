#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
//! Axum integration for kafkaman: request correlation and operator routes.
//!
//! Nothing here is required to use kafkaman. It exists so applications do not
//! each rewrite the same health check and the same queue-depth query.
//!
//! Supervision is not here. Running the HTTP server as one more loop beside the
//! relay, the ingester, and the dispatcher is [`kafkaman::axum::serve`], which
//! needs the runtime this crate deliberately knows nothing about.
//!
//! [`kafkaman::axum::serve`]: https://docs.rs/kafkaman
//!
//! # Security
//!
//! Nothing here is authenticated. [`admin_router`] reads queue metadata and
//! failure details, and [`redrive_router`] re-enqueues dead-lettered messages.
//! They are separate functions so that mounting the destructive one is a
//! decision rather than a side effect — see their documentation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{MatchedPath, Path, State};
use axum::http::header::HeaderName;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use kafkaman_core::{MessageDescriptor, ReceivedError, ReceivedFailureKind};
use kafkaman_sqlx::{
    outbox_status_summary, outbox_stuck_rows, received_failed_count, received_failed_rows,
    received_ingest_failure_summary, received_status_summary, received_stuck_rows,
    redrive_received, service_table_access, service_tables, OutboxStatusSummary, OutboxStuckRow,
    OutboxTable, ReceivedFailureFilter, ReceivedIngestFailureSummary, ReceivedStatusSummary,
    ReceivedStuckRow, ReceivedTable, Replay, ResolvedConfig, ServiceTables, TableAccess,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::OffsetDateTime;
use tower::{Layer, Service};
use tracing::Instrument;
use uuid::Uuid;

/// Request/response header carrying the correlation id across a service hop.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// The `http.route` reported for a request that matched no route.
///
/// Bounded on purpose: see the comment in [`CorrelationService::call`].
const UNMATCHED_ROUTE: &str = "<unmatched>";

/// Longest client-supplied correlation id accepted before one is generated
/// instead.
///
/// A correlation id is echoed into a response header and into every log line
/// for the request, so its length is attacker-controlled log volume. 128 is
/// comfortably above any real id — a UUID is 36 — and far below a useful
/// amplification factor.
const MAX_CORRELATION_ID_LEN: usize = 128;

/// A request's correlation id, stored in request extensions by
/// [`CorrelationLayer`].
///
/// Either echoed from the inbound [`CORRELATION_ID_HEADER`] or freshly
/// generated. Values that survive from the client are constrained by
/// [`CorrelationId::is_acceptable`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationId(String);

impl CorrelationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether a client-supplied value may be adopted as-is.
    ///
    /// Restricted to printable, non-space ASCII within 128 bytes.
    /// `HeaderValue::from_str` already rejects
    /// control characters, so this is not about header splitting — it is about
    /// keeping unbounded or unprintable client input out of logs, and keeping
    /// the value greppable once it lands there.
    pub fn is_acceptable(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= MAX_CORRELATION_ID_LEN
            && value.bytes().all(|byte| byte.is_ascii_graphic())
    }
}

/// Ensures every request has a correlation id, in its extensions, its span, and
/// its response.
///
/// Adopts the inbound [`CORRELATION_ID_HEADER`] when it is acceptable, and
/// generates a UUID otherwise, so a downstream service always has something to
/// join on.
#[derive(Clone, Debug, Default)]
pub struct CorrelationLayer;

impl CorrelationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for CorrelationLayer {
    type Service = CorrelationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CorrelationService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct CorrelationService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for CorrelationService<S>
where
    S: Service<Request<Body>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let correlation_id = request
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| CorrelationId::is_acceptable(value))
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        request
            .extensions_mut()
            .insert(CorrelationId::new(correlation_id.clone()));

        // `http.route` and the exported span name are what a backend groups
        // transactions by, so both stay bounded. A request that matched no route
        // has no template to report, and reporting its raw path instead would
        // mint one transaction group per URL a scanner invents.
        let route = request
            .extensions()
            .get::<MatchedPath>()
            .map_or(UNMATCHED_ROUTE, MatchedPath::as_str);
        // The raw path is still worth having; it just belongs in a field nothing
        // groups by. Borrowed from `request` rather than cloned: the span macro
        // copies every value in as it builds the span, which is before the call
        // below moves the request.
        let otel_name = format!("{} {route}", request.method());
        let span = tracing::info_span!(
            "http.request",
            "otel.name" = otel_name.as_str(),
            "otel.kind" = "server",
            "otel.status_code" = tracing::field::Empty,
            "otel.status_description" = tracing::field::Empty,
            "http.request.method" = %request.method(),
            "http.route" = route,
            "url.path" = request.uri().path(),
            "http.response.status_code" = tracing::field::Empty,
            correlation_id = %correlation_id,
        );
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.instrument(span.clone()).await?;
            let status = response.status();
            span.record("http.response.status_code", i64::from(status.as_u16()));
            // 5xx only. A 4xx is the server correctly refusing a bad request, and
            // marking those failed makes an APM error rate track client mistakes
            // rather than service health.
            if status.is_server_error() {
                kafkaman_core::record_error(&span, &format_args!("HTTP {}", status.as_u16()));
            }
            if let Ok(value) = HeaderValue::from_str(&correlation_id) {
                response
                    .headers_mut()
                    .insert(HeaderName::from_static(CORRELATION_ID_HEADER), value);
            }
            Ok(response)
        })
    }
}

/// Pool and resolved config the admin routes read from.
#[derive(Clone, Debug)]
pub struct AdminState {
    pub pool: PgPool,
    pub cfg: Arc<ResolvedConfig>,
}

impl AdminState {
    pub fn new(pool: PgPool, cfg: Arc<ResolvedConfig>) -> Self {
        Self { pool, cfg }
    }
}

/// Read-only operator routes: health, queue depth, stuck rows, DLQ inspection.
///
/// Every route here is a `GET` and none of them change anything. The destructive
/// redrive route is deliberately *not* included — it lives in
/// [`redrive_router`], so that a deployment wanting dashboards does not acquire
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

/// The destructive route, alone, so that mounting it is a decision.
///
/// `POST /dlq/{message_type}/redrive` moves dead-lettered rows back to `Pending`,
/// which re-runs their handlers against whatever side effects those handlers
/// have. With `clear_history` it also erases the attempts and error record that
/// explain why the rows were dead-lettered — the evidence an operator is usually
/// in the middle of reading.
///
/// # Security
///
/// **No authentication or authorization**, like [`admin_router`], and with more
/// to lose. Separating the two routers is what lets one auth policy cover reads
/// and a stricter one cover writes:
///
/// ```ignore
/// let admin = admin_router(state.clone())
///     .layer(my_read_auth_layer())
///     .merge(redrive_router(state).layer(my_write_auth_layer()));
/// ```
///
/// Merging them with no layer at all reproduces exactly the surface this split
/// exists to prevent, so do that only where the whole listener is already
/// private.
pub fn redrive_router(state: AdminState) -> Router {
    Router::new()
        .route("/dlq/{message_type}/redrive", post(redrive_dlq))
        .with_state(state)
}

/// Liveness: the process is up and serving. Touches no dependency on purpose,
/// so a database blip cannot cause an orchestrator to kill a healthy process.
// Stays in the debug tier: an orchestrator probes this forever and it touches nothing.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn health() -> Json<serde_json::Value> {
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

/// Largest number of rows one redrive request may re-enqueue.
///
/// Redrive moves rows back to `Pending`, so an unbounded request is a way to
/// hand the dispatcher an entire DLQ in one transaction. Operators who need more
/// issue more requests, which also gives them a natural place to stop.
pub const MAX_REDRIVE_ROWS: i64 = 10_000;

/// Body of a DLQ redrive request.
///
/// `deny_unknown_fields` is not pedantry on a destructive route: without it a
/// misspelled `clearHistory` is silently ignored and the caller believes they
/// asked for something they did not get.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedriveRequest {
    /// Required and bounded by [`MAX_REDRIVE_ROWS`]. There is no "all" value.
    pub max_rows: i64,
    /// Redrive only rows whose most recent failure was of this kind.
    ///
    /// Accepts either spelling: the RFC 9457 `type` URI that
    /// [`dlq_rows`](AdminState) prints in `latest_error.type`, or the bare
    /// discriminant stored in `last_failure_kind`. An operator's workflow is to
    /// read the DLQ and then redrive part of it, and a filter that refuses the
    /// value the inspection response just showed them is a filter that gets left
    /// off — which redrives the whole queue instead of one failure class.
    #[serde(default, deserialize_with = "deserialize_failure_kind")]
    pub failure_kind: Option<ReceivedFailureKind>,
    /// Reset attempts and drop recorded failures. Off by default, because the
    /// history is what triage reads.
    #[serde(default)]
    pub clear_history: bool,
}

/// Resolve a redrive filter's failure kind from either wire spelling.
///
/// Strict, unlike the audit-record reader in `kafkaman-core` that degrades an
/// unrecognized `type` to the default kind. That leniency is right for reading a
/// stored problem detail written by a newer binary — the row still has to load.
/// It would be wrong here: this value decides which rows a destructive request
/// touches, and quietly resolving a misspelling to `Handler` redrives a
/// different failure class than the operator named.
fn deserialize_failure_kind<'de, D>(
    deserializer: D,
) -> Result<Option<ReceivedFailureKind>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(raw) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    ReceivedFailureKind::from_problem_type(&raw)
        .map(Some)
        .ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown failure_kind {raw:?}; expected one of {}",
                ReceivedFailureKind::ALL
                    .iter()
                    .map(|kind| kind.discriminant())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
}

/// Result of a redrive: how many rows actually moved back to `Pending`.
#[derive(Clone, Debug, Serialize)]
pub struct RedriveResponse {
    pub message_type: String,
    pub redriven: u64,
}

/// Re-enqueue dead-lettered rows for one message type.
///
/// Destructive and unauthenticated — see [`redrive_router`]. Bounded by
/// [`MAX_REDRIVE_ROWS`]; targets terminal rows only; preserves failure history
/// unless `clear_history` is set.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn redrive_dlq(
    State(state): State<AdminState>,
    Path(message_type): Path<String>,
    Json(request): Json<RedriveRequest>,
) -> Result<Json<RedriveResponse>, AdminError> {
    if request.max_rows <= 0 || request.max_rows > MAX_REDRIVE_ROWS {
        return Err(AdminError::BadRequest(format!(
            "max_rows must be between 1 and {MAX_REDRIVE_ROWS}"
        )));
    }
    let descriptor = descriptor_for_message_type(&state.cfg, &message_type)
        .ok_or_else(|| AdminError::UnknownMessageType(message_type.clone()))?;
    // Registered is not the same as consumed. A type this service only publishes
    // has no received table, so redriving it would fail against a relation that
    // was never created — a 500 for what is really the caller pointing a real
    // message type at the wrong service.
    //
    // `UPDATE` as well as `SELECT`, because redrive writes: `redrive_received`
    // moves failed rows back to pending. A role holding only `SELECT` would pass
    // a read-shaped probe and then fail against the statement the probe had just
    // cleared, which is the 500 this check exists to replace.
    let table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
    let access = service_table_access(
        &state.pool,
        &table.schema,
        &table.table,
        &["SELECT", "UPDATE"],
    )
    .await?;
    if !access.is_ready() {
        return Err(AdminError::NotRedrivable {
            message_type,
            access,
        });
    }
    let mut replay =
        Replay::received_descriptor(Replay::RUNTIME_VERSION, descriptor).max_rows(request.max_rows);
    if let Some(kind) = request.failure_kind {
        replay = replay.failure_kind(kind);
    }
    if request.clear_history {
        replay = replay.clear_history();
    }
    let redriven = redrive_received(&state.pool, &state.cfg, &replay).await?;
    Ok(Json(RedriveResponse {
        message_type,
        redriven,
    }))
}

/// Resolves a path segment to a registered descriptor.
///
/// An unregistered type is a 404 rather than a 500: the caller named something
/// this application does not carry, which is their error, not ours.
fn descriptor_for_message_type(
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

/// Failure of an admin request, mapped to a status code by its
/// [`IntoResponse`] impl.
///
/// Only `BadRequest` and `UnknownMessageType` echo their message to the caller.
/// A `Sqlx` failure logs in full and returns an opaque body, so schema names and
/// SQL text never reach an unauthenticated client.
#[derive(Debug)]
#[non_exhaustive]
pub enum AdminError {
    BadRequest(String),
    /// Message types this service declares but has no readable table for on
    /// either side.
    ///
    /// Every declared type gets a table on the side this service is on, so
    /// neither side present means the schema cannot back the configuration —
    /// unmigrated, or unreadable by this role. Reported rather than skipped
    /// because the alternative is answering `200` with those types silently
    /// absent, which reads as "these queues are empty".
    UnusableServiceTables(Vec<String>),
    Sqlx(kafkaman_sqlx::Error),
    UnknownMessageType(String),
    /// Registered, but this route cannot redrive it here.
    ///
    /// Separate from [`Self::UnknownMessageType`] because the repair is
    /// different: the caller named a real message type. The [`TableAccess`] says
    /// which repair — a type this service only publishes, a schema that was
    /// never migrated, or a role without the privileges redrive needs — and it
    /// also decides the status, because only the first of those three is the
    /// caller's to fix. See the [`IntoResponse`] impl.
    NotRedrivable {
        message_type: String,
        access: TableAccess,
    },
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) => write!(f, "bad request: {message}"),
            Self::UnusableServiceTables(message_types) => write!(
                f,
                "no readable kafkaman table for message type(s) {}",
                message_types.join(", ")
            ),
            Self::Sqlx(err) => write!(f, "storage error: {err}"),
            Self::UnknownMessageType(message_type) => {
                write!(f, "message type `{message_type}` is not registered")
            }
            Self::NotRedrivable {
                message_type,
                access,
            } => write!(
                f,
                "message type `{message_type}` {}: {}",
                // Agrees with the response body, because these two strings are
                // read by the same person minutes apart — one in a log, one in a
                // client. Saying the table is absent in a log while the response
                // says it is unreadable makes them doubt both.
                if access.is_ready() || *access == TableAccess::Missing {
                    "has no dead-letter queue in this service"
                } else {
                    "has a dead-letter queue in this service, but it cannot be read"
                },
                access.repair()
            ),
        }
    }
}

impl std::error::Error for AdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx(err) => Some(err),
            _ => None,
        }
    }
}

impl From<kafkaman_sqlx::Error> for AdminError {
    fn from(value: kafkaman_sqlx::Error) -> Self {
        match value {
            kafkaman_sqlx::Error::InvalidReplay { .. } => Self::BadRequest(value.to_string()),
            other => Self::Sqlx(other),
        }
    }
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        match self {
            Self::BadRequest(message) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::UnknownMessageType(message_type) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("message type `{message_type}` is not registered")
                })),
            )
                .into_response(),
            // 503, not 500: the request is well-formed and the service is the
            // right one to ask; its schema is not ready to answer. That is a
            // retry-after-you-fix-the-deployment condition, and it is what a
            // readiness probe would report if it checked tables.
            Self::UnusableServiceTables(message_types) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "kafkaman has no readable table for some configured message types; \
                              run this service's migrations, or grant its database role access",
                    "message_types": message_types,
                })),
            )
                .into_response(),
            // The status separates the two things a refusal can mean, because
            // they page different people.
            //
            // `Missing` is a 404 and belongs to the caller: every declared type
            // gets a table on the side this service is on, so a *received* table
            // that is absent means this type is published here and consumed
            // somewhere else. There is no dead-letter queue at this address and
            // there never will be.
            //
            // `NotATable` and `NoPrivilege` are 503, for the reason
            // `UnusableServiceTables` above is: the request is well-formed and
            // correctly addressed, and the schema behind it is not ready to
            // answer. Reporting those as 404 tells an operator the queue does
            // not exist when it does and is full — the same wrong answer, aimed
            // at the same person, that `ensure_service_tables` exists to
            // prevent. A retry after the deployment is fixed then succeeds,
            // which is exactly what 503 promises and 404 denies.
            //
            // Not 403 for `NoPrivilege`. 403 says *this caller* may not do this,
            // and no caller credential is involved: the role that lacks `UPDATE`
            // is the service's own database role. Sending an operator to look at
            // the requester's authorization would point them away from the
            // grant that is actually missing.
            Self::NotRedrivable {
                message_type,
                access,
            } => {
                let (status, error) = match access {
                    // Unreachable: the route only builds this variant after
                    // `is_ready()` returned false. Grouped with `Missing` so a
                    // later edit that loosens the guard degrades to the
                    // caller-facing answer rather than claiming an outage.
                    TableAccess::Missing | TableAccess::Ready => (
                        StatusCode::NOT_FOUND,
                        format!(
                            "message type `{message_type}` has no dead-letter queue in this \
                             service"
                        ),
                    ),
                    // The queue exists. Saying it does not would send an
                    // operator looking for the wrong service while their rows
                    // sit here.
                    TableAccess::NotATable | TableAccess::NoPrivilege => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!(
                            "message type `{message_type}` has a dead-letter queue in this \
                             service, but it cannot be read"
                        ),
                    ),
                };
                (
                    status,
                    Json(serde_json::json!({
                        "error": error,
                        "repair": access.repair(),
                    })),
                )
                    .into_response()
            }
            Self::Sqlx(err) => {
                tracing::error!(error = %err, "kafkaman admin request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "internal error" })),
                )
                    .into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    use kafkaman_core::ReceivedFailureKind;
    use time::format_description::well_known::Rfc3339;
    use tower::ServiceExt;

    /// Runs `request` through the layer over a handler that echoes the
    /// correlation id it sees, so one call checks both the response header and
    /// the extension the handler was given.
    async fn correlation_roundtrip(request: Request<Body>) -> (String, String) {
        let mut service =
            CorrelationLayer::new().layer(tower::service_fn(|request: Request<Body>| async move {
                let correlation = request
                    .extensions()
                    .get::<CorrelationId>()
                    .expect("correlation id extension should be present")
                    .as_str()
                    .to_owned();
                Ok::<_, Infallible>(Response::new(Body::from(correlation)))
            }));
        let response = service.ready().await.unwrap().call(request).await.unwrap();
        let header = response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("response must carry a correlation id")
            .to_owned();
        let body = http_body_util_collect(response.into_body()).await;
        (header, body)
    }

    /// Drains a response body to a `String` without pulling in another crate for
    /// the two tests that need it.
    async fn http_body_util_collect(body: Body) -> String {
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("test bodies are small and finite");
        String::from_utf8(bytes.to_vec()).expect("test bodies are utf-8")
    }

    /// Captures the field values of every `http.request` span opened while it is
    /// installed, so the route assertions below read what a backend would.
    #[derive(Clone, Default)]
    struct RecordedRoutes(Arc<std::sync::Mutex<Vec<(String, String)>>>);

    impl<S> tracing_subscriber::Layer<S> for RecordedRoutes
    where
        S: tracing::Subscriber,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attrs.metadata().name() != "http.request" {
                return;
            }
            #[derive(Default)]
            struct Fields {
                route: String,
                name: String,
            }
            impl tracing::field::Visit for Fields {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    match field.name() {
                        "http.route" => self.route = value.to_owned(),
                        "otel.name" => self.name = value.to_owned(),
                        _ => {}
                    }
                }
                fn record_debug(
                    &mut self,
                    _field: &tracing::field::Field,
                    _value: &dyn std::fmt::Debug,
                ) {
                }
            }
            let mut fields = Fields::default();
            attrs.record(&mut fields);
            self.0
                .lock()
                .expect("no test panics while holding this")
                .push((fields.route, fields.name));
        }
    }

    /// Serializes every test that installs a subscriber.
    ///
    /// `tracing` keeps a **process-global** maximum level, recomputed as
    /// subscribers come and go, and a span callsite consults it before it
    /// consults any subscriber. So a test installing a filtered subscriber on
    /// its own thread can silently disable another test's span on a different
    /// one — which shows up as a span that simply never opened, with nothing
    /// pointing at the cause. `set_default` being thread-local is not enough;
    /// the level hint it adjusts is not.
    ///
    /// Async because the guarded scope intentionally drives a request while the
    /// subscriber is installed. A sync mutex guard across that await blocks an
    /// executor worker thread and trips clippy for the right reason.
    static SUBSCRIBER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn lock_subscriber() -> tokio::sync::MutexGuard<'static, ()> {
        ensure_permissive_global();
        SUBSCRIBER_LOCK.lock().await
    }

    /// Install a permissive subscriber globally, once, before any test opens a
    /// span.
    ///
    /// The mutex above is necessary and not sufficient, because the thing being
    /// shared is not the dispatcher. `tracing` caches each *callsite's* interest
    /// globally, and rebuilds that cache as thread-local defaults come and go —
    /// computing it against the **global** dispatcher, which with none installed
    /// is `NoSubscriber`, and which answers "never" for every callsite. That
    /// answer is then cached, so a thread-local subscriber on another thread is
    /// never consulted and its spans simply do not open.
    ///
    /// It shows up as a span that was never recorded, on a test that does not
    /// install anything itself, only when the suite runs multi-threaded —
    /// measured here at roughly one run in eight, and never under
    /// `--test-threads=1`. Installing a permissive global once means every
    /// rebuild computes a real answer instead of that one.
    ///
    /// The result is ignored: a second call is an error and is exactly what
    /// `Once` is preventing.
    fn ensure_permissive_global() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
        });
    }

    /// Drives one request through a real router with the layer applied, and
    /// returns the `(http.route, otel.name)` its span recorded.
    async fn recorded_route(uri: &str) -> (String, String) {
        let _serialized = lock_subscriber().await;
        use tracing_subscriber::layer::SubscriberExt as _;

        let recorded = RecordedRoutes::default();
        let app = axum::Router::new()
            .route(
                "/products/{product_id}",
                get(|| async { StatusCode::NO_CONTENT }),
            )
            .layer(CorrelationLayer::new());

        {
            let _guard = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(recorded.clone()),
            );
            let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let _ = app.oneshot(request).await.unwrap();
        }

        let mut seen = recorded
            .0
            .lock()
            .expect("no test panics while holding this")
            .clone();
        assert_eq!(seen.len(), 1, "one request should open one span");
        seen.remove(0)
    }

    /// Records the name of every span opened while it is installed.
    #[derive(Clone, Default)]
    struct RecordedSpans(Arc<std::sync::Mutex<Vec<String>>>);

    impl<S> tracing_subscriber::Layer<S> for RecordedSpans
    where
        S: tracing::Subscriber,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0
                .lock()
                .expect("no test panics while holding this")
                .push(attrs.metadata().name().to_owned());
        }
    }

    /// The `kafkaman::internal` tier is split by level, and its poll half is
    /// reachable by its own target.
    ///
    /// Two things are pinned here:
    ///
    /// 1. A function that runs on a timer ([`health`], which an orchestrator
    ///    probes forever and which touches nothing) stays out of the default
    ///    filter. This is the whole cost argument for the tier being visible at
    ///    all — an idle service that exports its own scheduler exports nothing
    ///    else worth reading.
    /// 2. The directive `examples/README.md` documents still reaches the poll
    ///    half — and still does not require turning on `debug` globally, which
    ///    would drown it in `sqlx` and `rdkafka` output.
    ///
    /// The other half of the split — that a *message-path* function **is** in
    /// the default filter, without which this test passes just as well against a
    /// tier reverted to debug wholesale — is pinned by
    /// `running_service_run_is_visible_under_the_default_filter` in
    /// `kafkaman::axum`. It moved there with the supervision code: every
    /// promoted function left in this crate needs a database, so the assertion
    /// belongs where the one that does not now lives.
    #[tokio::test]
    async fn the_internal_span_tier_is_split_by_level_and_reachable_by_target() {
        use tracing_subscriber::layer::SubscriberExt as _;

        // Per-layer filtering, because that is what `kafkaman_otel::init` does:
        // it gives each layer its own `EnvFilter` rather than putting one on the
        // registry. A directive that works globally and not per layer would be a
        // directive that works in this test and not in a real service.
        async fn spans_opened_under(directive: &str) -> Vec<String> {
            use tracing_subscriber::Layer as _;

            let _serialized = lock_subscriber().await;
            let recorded = RecordedSpans::default();
            {
                let _guard = tracing::subscriber::set_default(
                    tracing_subscriber::registry().with(
                        recorded
                            .clone()
                            .with_filter(tracing_subscriber::EnvFilter::new(directive)),
                    ),
                );
                let _ = health().await;
            }
            let names = recorded
                .0
                .lock()
                .expect("no test panics while holding this")
                .clone();
            names
        }

        let default_filter = spans_opened_under("info").await;
        assert!(
            !default_filter.contains(&"health".to_owned()),
            "a function an orchestrator polls forever must stay out of the default \
             filter; opened: {default_filter:?}"
        );
        assert!(
            spans_opened_under("info,kafkaman::internal=debug")
                .await
                .contains(&"health".to_owned()),
            "the documented directive must reach the poll half too"
        );
    }

    /// A matched request reports the template, not the URL it was reached by.
    ///
    /// This also pins that `MatchedPath` is visible to a layer added with
    /// `Router::layer`, which is the only reason the template is available at
    /// all — the layer wraps each route, so routing has already happened.
    #[tokio::test]
    async fn a_matched_request_reports_its_route_template() {
        let (route, name) = recorded_route("/products/6f9619ff-8b86-d011-b42d-00cf4fc964ff").await;
        assert_eq!(route, "/products/{product_id}");
        assert_eq!(name, "GET /products/{product_id}");
    }

    /// An unmatched request reports a constant.
    ///
    /// `http.route` and the exported span name are what a backend groups
    /// transactions by. Falling back to the raw path would mint one transaction
    /// group per URL a scanner invents, which is unbounded cardinality from
    /// unauthenticated input. The path itself is still recorded, as `url.path`,
    /// which nothing groups by.
    #[tokio::test]
    async fn an_unmatched_request_reports_a_bounded_route() {
        for uri in ["/nope", "/totally/made/up/12345", "/products"] {
            let (route, name) = recorded_route(uri).await;
            assert_eq!(route, UNMATCHED_ROUTE, "{uri} should not become a route");
            assert_eq!(name, "GET <unmatched>", "{uri} should not become a group");
        }
    }

    #[tokio::test]
    async fn correlation_layer_preserves_existing_header_and_sets_extension() {
        let request = Request::builder()
            .uri("/")
            .header(CORRELATION_ID_HEADER, "request-123")
            .body(Body::empty())
            .unwrap();

        let (header, body) = correlation_roundtrip(request).await;
        assert_eq!(header, "request-123");
        assert_eq!(
            body, "request-123",
            "the extension the handler sees must match the header echoed back"
        );
    }

    #[tokio::test]
    async fn correlation_layer_generates_an_id_when_the_header_is_absent() {
        let request = Request::builder().uri("/").body(Body::empty()).unwrap();

        let (header, body) = correlation_roundtrip(request).await;
        assert_eq!(header, body);
        assert!(
            Uuid::parse_str(&header).is_ok(),
            "a generated correlation id should be a UUID, got {header:?}"
        );
    }

    #[tokio::test]
    async fn correlation_layer_replaces_blank_and_oversized_values() {
        // Whitespace-only carries no information, and an oversized value is
        // attacker-controlled log volume. Both are replaced, not propagated.
        for supplied in ["   ", "\t"] {
            let request = Request::builder()
                .uri("/")
                .header(CORRELATION_ID_HEADER, supplied)
                .body(Body::empty())
                .unwrap();
            let (header, _) = correlation_roundtrip(request).await;
            assert!(
                Uuid::parse_str(&header).is_ok(),
                "blank value {supplied:?} should be replaced, got {header:?}"
            );
        }

        let oversized = "a".repeat(MAX_CORRELATION_ID_LEN + 1);
        let request = Request::builder()
            .uri("/")
            .header(CORRELATION_ID_HEADER, &oversized)
            .body(Body::empty())
            .unwrap();
        let (header, _) = correlation_roundtrip(request).await;
        assert_ne!(header, oversized);
        assert!(Uuid::parse_str(&header).is_ok());

        // Exactly at the bound is still accepted.
        let at_bound = "a".repeat(MAX_CORRELATION_ID_LEN);
        let request = Request::builder()
            .uri("/")
            .header(CORRELATION_ID_HEADER, &at_bound)
            .body(Body::empty())
            .unwrap();
        let (header, _) = correlation_roundtrip(request).await;
        assert_eq!(header, at_bound);
    }

    #[test]
    fn correlation_id_rejects_unprintable_and_spaced_values() {
        assert!(CorrelationId::is_acceptable("abc-123"));
        assert!(!CorrelationId::is_acceptable(""));
        assert!(!CorrelationId::is_acceptable("has space"));
        assert!(!CorrelationId::is_acceptable("emoji-\u{1F600}"));
        assert!(!CorrelationId::is_acceptable(
            &"a".repeat(MAX_CORRELATION_ID_LEN + 1)
        ));
    }

    // ---- wire format -------------------------------------------------------
    //
    // The workspace does not enable `time/serde-human-readable`, so a bare
    // `OffsetDateTime` serializes as a nine-integer array. Every timestamp in
    // these responses therefore carries an explicit RFC 9557 adapter, and these
    // tests fail loudly if one is ever dropped.

    fn assert_rfc3339_string(value: &serde_json::Value, field: &str) {
        let raw = value
            .get(field)
            .unwrap_or_else(|| panic!("{field} should be present"));
        let text = raw
            .as_str()
            .unwrap_or_else(|| panic!("{field} must serialize as a timestamp string, got {raw}"));
        let base = text.split_once('[').map_or(text, |(base, _)| base);
        OffsetDateTime::parse(base, &Rfc3339)
            .unwrap_or_else(|err| panic!("{field} must parse as RFC 3339: {text:?} ({err})"));
    }

    #[test]
    fn dlq_row_summary_serializes_timestamps_as_strings() {
        let now = OffsetDateTime::now_utc();
        let row = DlqRowSummary {
            message_id: Uuid::nil(),
            entity_key: Some("entity-1".to_owned()),
            attempts: 3,
            source_topic: "orders".to_owned(),
            source_partition: 0,
            source_offset: 42,
            correlation_id: None,
            causation_id: None,
            created_at: now,
            processed_at: Some(now),
            latest_error: Some(ReceivedError::new(
                ReceivedFailureKind::Handler,
                "boom",
                now,
                Some(kafkaman_core::FailureStage::Handler),
            )),
            error_count: 1,
        };

        let value = serde_json::to_value(&row).expect("summary should serialize");
        assert_rfc3339_string(&value, "created_at");
        assert_rfc3339_string(&value, "processed_at");
        assert!(
            value.get("payload").is_none() && value.get("headers").is_none(),
            "the DLQ projection must never carry payload bodies or user headers"
        );
    }

    #[test]
    fn dlq_row_summary_omits_a_missing_processed_at_as_null() {
        let now = OffsetDateTime::now_utc();
        let row = DlqRowSummary {
            message_id: Uuid::nil(),
            entity_key: None,
            attempts: 0,
            source_topic: "orders".to_owned(),
            source_partition: 0,
            source_offset: 0,
            correlation_id: None,
            causation_id: None,
            created_at: now,
            processed_at: None,
            latest_error: None,
            error_count: 0,
        };

        let value = serde_json::to_value(&row).expect("summary should serialize");
        assert!(value["processed_at"].is_null());
        assert_rfc3339_string(&value, "created_at");
    }

    #[test]
    fn stuck_response_serializes_every_timestamp_as_a_string() {
        let now = OffsetDateTime::now_utc();
        let response = StuckResponse {
            outbox: vec![kafkaman_sqlx::OutboxStuckRow {
                message_type: "order_created".to_owned(),
                message_id: Uuid::nil(),
                status: kafkaman_core::OutboxStatus::Publishing,
                claimed_by: Some("worker-1".to_owned()),
                claim_expires_at: Some(now),
                created_at: now,
                age_ms: 1_000,
                stuck_for_ms: 250,
            }],
            received: vec![ReceivedStuckRow {
                message_type: "order_created".to_owned(),
                message_id: Uuid::nil(),
                status: kafkaman_core::ReceiveStatus::Retryable,
                attempts: 2,
                next_attempt_at: Some(now),
                created_at: now,
                due_at: now,
                age_ms: 2_000,
                stuck_for_ms: 500,
            }],
            truncated: false,
        };

        let value = serde_json::to_value(&response).expect("response should serialize");
        assert_rfc3339_string(&value["outbox"][0], "created_at");
        assert_rfc3339_string(&value["outbox"][0], "claim_expires_at");
        assert_rfc3339_string(&value["received"][0], "created_at");
        assert_rfc3339_string(&value["received"][0], "due_at");
        assert_rfc3339_string(&value["received"][0], "next_attempt_at");

        // Both durations reach the wire, and separately. `age_ms` alone told an
        // operator how long a message had existed while looking like it told
        // them how long the fault had lasted.
        assert_eq!(value["outbox"][0]["age_ms"], serde_json::json!(1_000));
        assert_eq!(value["outbox"][0]["stuck_for_ms"], serde_json::json!(250));
        assert_eq!(value["received"][0]["age_ms"], serde_json::json!(2_000));
        assert_eq!(value["received"][0]["stuck_for_ms"], serde_json::json!(500));
    }

    #[test]
    fn status_summaries_serialize_timestamps_and_the_queue_age_flag() {
        let now = OffsetDateTime::now_utc();
        let outbox = kafkaman_sqlx::OutboxStatusSummary {
            message_type: "order_created".to_owned(),
            status: kafkaman_core::OutboxStatus::Pending,
            count: 7,
            oldest_created_at: Some(now),
            oldest_age_ms: Some(5),
            over_max_queue_age: true,
        };
        let value = serde_json::to_value(&outbox).expect("summary should serialize");
        assert_rfc3339_string(&value, "oldest_created_at");
        assert_eq!(value["over_max_queue_age"], serde_json::json!(true));

        let empty = kafkaman_sqlx::ReceivedStatusSummary {
            message_type: "order_created".to_owned(),
            status: kafkaman_core::ReceiveStatus::Processed,
            count: 0,
            oldest_created_at: None,
            oldest_age_ms: None,
            over_max_queue_age: false,
        };
        let value = serde_json::to_value(&empty).expect("summary should serialize");
        assert!(value["oldest_created_at"].is_null());
    }

    // ---- request contract --------------------------------------------------

    #[test]
    fn redrive_request_rejects_unknown_fields() {
        // A misspelled field on a destructive route must fail loudly rather than
        // quietly doing something other than what the caller asked for.
        let typo = serde_json::json!({ "max_rows": 10, "clearHistory": true });
        assert!(serde_json::from_value::<RedriveRequest>(typo).is_err());

        let ok = serde_json::json!({ "max_rows": 10, "clear_history": true });
        let parsed: RedriveRequest = serde_json::from_value(ok).expect("valid body should parse");
        assert_eq!(parsed.max_rows, 10);
        assert!(parsed.clear_history);
        assert!(parsed.failure_kind.is_none());
    }

    #[test]
    fn redrive_accepts_the_failure_kind_dlq_inspection_prints() {
        // The two halves of one operator workflow: read the DLQ, redrive part of
        // it. Inspection renders the failure as an RFC 9457 `type` URI, so a
        // request body that only accepted the bare discriminant would reject the
        // exact string the operator just copied — and the way out of that is to
        // drop the filter, which redrives the whole queue.
        let printed = ReceivedError::new(
            ReceivedFailureKind::InvalidPayload,
            "boom",
            OffsetDateTime::UNIX_EPOCH,
            Some(kafkaman_core::FailureStage::Handler),
        );
        let printed = serde_json::to_value(&printed).expect("a problem detail should serialize");
        let printed = printed["type"].as_str().expect("RFC 9457 names it `type`");

        for spelling in [printed, "InvalidPayload"] {
            let body = serde_json::json!({ "max_rows": 1, "failure_kind": spelling });
            let parsed: RedriveRequest =
                serde_json::from_value(body).expect("both spellings name one failure class");
            assert_eq!(
                parsed.failure_kind,
                Some(ReceivedFailureKind::InvalidPayload)
            );
        }
    }

    #[test]
    fn redrive_refuses_a_failure_kind_it_does_not_recognize() {
        // Strict where the audit reader is lenient, and deliberately so: this
        // value decides which rows a destructive request touches. Degrading an
        // unknown kind to the default would redrive a different failure class
        // than the operator named, and report success.
        let body = serde_json::json!({ "max_rows": 1, "failure_kind": "Handlr" });
        let err = serde_json::from_value::<RedriveRequest>(body)
            .expect_err("a misspelled failure class must not silently become another one");
        assert!(
            err.to_string().contains("Handler"),
            "the error should name the vocabulary, got {err}"
        );
    }

    #[test]
    fn redrive_request_defaults_clear_history_to_false() {
        let parsed: RedriveRequest =
            serde_json::from_value(serde_json::json!({ "max_rows": 1 })).unwrap();
        assert!(
            !parsed.clear_history,
            "history is what triage reads; dropping it must be explicit"
        );
    }

    #[test]
    fn admin_error_maps_to_the_right_status_and_hides_sql_detail() {
        let bad = AdminError::BadRequest("max_rows must be between 1 and 10000".to_owned())
            .into_response();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);

        let missing = AdminError::UnknownMessageType("nope".to_owned()).into_response();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let schema =
            AdminError::UnusableServiceTables(vec!["order_snapshot".to_owned()]).into_response();
        assert_eq!(schema.status(), StatusCode::SERVICE_UNAVAILABLE);

        let internal = AdminError::Sqlx(kafkaman_sqlx::Error::Handler(
            "schema kafkaman_x".to_owned(),
        ))
        .into_response();
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn a_refused_redrive_separates_the_wrong_service_from_a_broken_one() {
        let not_redrivable = |access| {
            AdminError::NotRedrivable {
                message_type: "order_snapshot".to_owned(),
                access,
            }
            .into_response()
            .status()
        };

        // No received table here at all: this service publishes the type and
        // something else consumes it. The caller is at the wrong address, and
        // no amount of waiting changes that.
        assert_eq!(not_redrivable(TableAccess::Missing), StatusCode::NOT_FOUND);

        // The table is there. A 404 would tell an operator the queue does not
        // exist while it sits full behind a missing grant, and would send them
        // to the caller's address instead of to the deployment.
        assert_eq!(
            not_redrivable(TableAccess::NoPrivilege),
            StatusCode::SERVICE_UNAVAILABLE,
            "a role without UPDATE is this deployment's fault, not the caller's"
        );
        assert_eq!(
            not_redrivable(TableAccess::NotATable),
            StatusCode::SERVICE_UNAVAILABLE,
            "a relation kafkaman did not create is a broken schema, not a 404"
        );

        // The body has to agree with the status. A 503 that says the queue does
        // not exist is worse than either half alone: it tells an operator to
        // wait *and* that there is nothing to wait for.
        let described = |access| {
            AdminError::NotRedrivable {
                message_type: "order_snapshot".to_owned(),
                access,
            }
            .to_string()
        };
        assert!(
            described(TableAccess::Missing).contains("has no dead-letter queue"),
            "the 404 case must say the queue is absent: {}",
            described(TableAccess::Missing)
        );
        assert!(
            described(TableAccess::NoPrivilege).contains("cannot be read"),
            "the 503 case must say the queue exists and is unreachable, not that \
             it is absent: {}",
            described(TableAccess::NoPrivilege)
        );

        // Whichever status, the body names the repair: the code alone cannot
        // distinguish the three, and only one of them is the caller's to fix.
        for access in [
            TableAccess::Missing,
            TableAccess::NoPrivilege,
            TableAccess::NotATable,
        ] {
            assert!(
                !access.repair().is_empty(),
                "{access:?} must tell an operator what to do about it"
            );
        }
    }

    #[tokio::test]
    async fn admin_error_body_does_not_leak_storage_detail() {
        let response = AdminError::Sqlx(kafkaman_sqlx::Error::Handler(
            "secret schema name".to_owned(),
        ))
        .into_response();
        let body = http_body_util_collect(response.into_body()).await;
        assert!(
            !body.contains("secret schema name"),
            "an unauthenticated caller must not see storage internals, got {body}"
        );
    }

    #[test]
    fn invalid_replay_is_a_client_error_not_a_server_error() {
        // `max_rows` is caller input, so a rejected replay is a 400.
        let err = AdminError::from(kafkaman_sqlx::Error::InvalidReplay {
            version: 0,
            message: "max_rows is required".to_owned(),
        });
        assert!(matches!(err, AdminError::BadRequest(_)));
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn health_is_ok_without_touching_a_database() {
        // No pool is involved: liveness must not fail because storage blipped.
        let Json(value) = health().await;
        assert_eq!(value["status"], "ok");
    }
}
