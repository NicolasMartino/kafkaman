#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
//! Axum integration for kafkaman: request correlation, operator routes, and
//! runtime supervision.
//!
//! Nothing here is required to use kafkaman. It exists so applications do not
//! each rewrite the same health check, the same queue-depth query, and the same
//! "shut the server down when a worker dies" plumbing.
//!
//! # Security
//!
//! Nothing here is authenticated. [`admin_router`] reads queue contents,
//! including stored payloads, and [`redrive_router`] re-enqueues dead-lettered
//! messages. They are separate functions so that mounting the destructive one is
//! a decision rather than a side effect — see their documentation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

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
    received_status_summary, received_stuck_rows, redrive_received, OutboxStatusSummary,
    OutboxStuckRow, OutboxTable, ReceivedFailureFilter, ReceivedStatusSummary, ReceivedStuckRow,
    ReceivedTable, Replay, ResolvedConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
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
/// same as harmless: `/dlq` returns stored payloads and failure messages, which
/// is exactly the data most likely to be sensitive. Mount it on an internal
/// listener, or behind your own auth middleware — never on a public route table:
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
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Readiness: the process can reach Postgres.
///
/// Deliberately narrower than "kafkaman is healthy" — it does not verify that
/// tables exist, that migrations are current, or that workers are running.
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
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn outbox_summary(
    State(state): State<AdminState>,
) -> Result<Json<Vec<OutboxStatusSummary>>, AdminError> {
    let now = OffsetDateTime::now_utc();
    let mut summaries = Vec::new();
    for descriptor in state.cfg.messages() {
        let policy = state
            .cfg
            .observability
            .policy_for(descriptor.message_type.as_str());
        let table = OutboxTable::new(state.cfg.schema.clone(), descriptor.clone())?;
        summaries
            .extend(outbox_status_summary(&state.pool, &table, now, policy.max_queue_age).await?);
    }
    Ok(Json(summaries))
}

/// Received-table depth per message type and status. Same cost and ordering
/// rationale as [`outbox_summary`].
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn received_summary(
    State(state): State<AdminState>,
) -> Result<Json<Vec<ReceivedStatusSummary>>, AdminError> {
    let now = OffsetDateTime::now_utc();
    let mut summaries = Vec::new();
    for descriptor in state.cfg.messages() {
        let policy = state
            .cfg
            .observability
            .policy_for(descriptor.message_type.as_str());
        let table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
        summaries
            .extend(received_status_summary(&state.pool, &table, now, policy.max_queue_age).await?);
    }
    Ok(Json(summaries))
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
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn stuck_rows(State(state): State<AdminState>) -> Result<Json<StuckResponse>, AdminError> {
    const LIMIT_PER_TYPE: i64 = 100;

    let now = OffsetDateTime::now_utc();
    let mut truncated = false;
    let mut outbox = Vec::new();
    let mut received = Vec::new();
    for descriptor in state.cfg.messages() {
        let policy = state
            .cfg
            .observability
            .policy_for(descriptor.message_type.as_str());
        let outbox_table = OutboxTable::new(state.cfg.schema.clone(), descriptor.clone())?;
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

        let received_table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
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
/// data. Treat this response as sensitive; it is sanitized of message bodies,
/// not of everything.
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
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn dlq_summary(State(state): State<AdminState>) -> Result<Json<Vec<DlqSummary>>, AdminError> {
    const ROW_LIMIT_PER_TYPE: i64 = 50;

    let filter = ReceivedFailureFilter::default();
    let mut summaries = Vec::new();
    for descriptor in state.cfg.messages() {
        let table = ReceivedTable::for_descriptor(&state.cfg, descriptor.clone())?;
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
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
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

/// Failure of an admin request, mapped to a status code by its
/// [`IntoResponse`] impl.
///
/// Only `BadRequest` and `UnknownMessageType` echo their message to the caller.
/// A `Sqlx` failure logs in full and returns an opaque body, so schema names and
/// SQL text never reach an unauthenticated client.
#[derive(Debug)]
pub enum AdminError {
    BadRequest(String),
    Sqlx(kafkaman_sqlx::Error),
    UnknownMessageType(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) => write!(f, "bad request: {message}"),
            Self::Sqlx(err) => write!(f, "storage error: {err}"),
            Self::UnknownMessageType(message_type) => {
                write!(f, "message type `{message_type}` is not registered")
            }
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

/// A named background task supervised alongside the HTTP server.
///
/// The name exists for the error message: "a task exited" is not actionable,
/// "the `relay:order_created` task exited" is.
#[derive(Debug)]
pub struct RuntimeTask {
    name: String,
    handle: tokio::task::JoinHandle<Result<(), String>>,
}

impl RuntimeTask {
    /// Spawns `future` immediately and adopts it under `name`.
    ///
    /// The error is flattened to a `String` at the spawn boundary so tasks with
    /// different error types can be supervised in one collection.
    pub fn spawn<F, E>(name: impl Into<String>, future: F) -> Self
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let handle = tokio::spawn(async move { future.await.map_err(|err| err.to_string()) });
        Self {
            name: name.into(),
            handle,
        }
    }
}

/// An Axum server awaiting [`RuntimeServer::with_runtime`].
#[derive(Debug)]
pub struct RuntimeServer {
    listener: TcpListener,
    app: Router,
}

/// Binds `app` to `listener`, to be supervised by
/// [`RuntimeServer::with_runtime`].
pub fn serve(listener: TcpListener, app: Router) -> RuntimeServer {
    RuntimeServer { listener, app }
}

/// How long [`RuntimeServer::with_runtime`] waits for background tasks to
/// finish their current cycle after shutdown is signalled.
///
/// Workers observe the cancellation token between cycles, so a healthy drain
/// takes about one poll interval. This bound exists for the unhealthy case — a
/// task blocked on a slow broker or a stuck query — where waiting forever turns
/// a graceful shutdown into a hang.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

impl RuntimeServer {
    /// Runs the server and `tasks` together, with a shared shutdown signal.
    ///
    /// Whichever stops first stops the rest: a cancelled `shutdown` ends the
    /// server, and a task that exits before shutdown is treated as a fault —
    /// the relay finishing "successfully" while the process keeps serving would
    /// mean silently dropping outbox delivery.
    ///
    /// # Draining
    ///
    /// After the server stops, this cancels `shutdown` and then **waits for the
    /// tasks to finish**, up to [`DEFAULT_DRAIN_TIMEOUT`]. That wait is the
    /// whole point: returning as soon as the listener closes would drop the
    /// runtime mid-cycle and cut in-flight publishes, which is exactly the
    /// failure an outbox exists to prevent. Use
    /// [`RuntimeServer::with_runtime_drain_timeout`] to change the bound.
    #[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
    pub async fn with_runtime(
        self,
        tasks: Vec<RuntimeTask>,
        shutdown: CancellationToken,
    ) -> Result<(), RuntimeError> {
        self.with_runtime_drain_timeout(tasks, shutdown, DEFAULT_DRAIN_TIMEOUT)
            .await
    }

    /// [`RuntimeServer::with_runtime`] with an explicit drain bound.
    ///
    /// A zero timeout skips the drain entirely.
    pub async fn with_runtime_drain_timeout(
        self,
        tasks: Vec<RuntimeTask>,
        shutdown: CancellationToken,
        drain_timeout: Duration,
    ) -> Result<(), RuntimeError> {
        let server = axum::serve(self.listener, self.app).with_graceful_shutdown({
            let shutdown = shutdown.clone();
            async move {
                shutdown.cancelled().await;
            }
        });

        if tasks.is_empty() {
            let result = server.await;
            // Cancel even with nothing to supervise: the caller's token may also
            // gate work this function never saw.
            shutdown.cancel();
            return result.map_err(RuntimeError::Server);
        }

        // Adopt every handle into one stream of completions. `handles` keeps the
        // abort handles so a drain timeout can stop stragglers instead of
        // leaking them past process shutdown.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut handles = Vec::with_capacity(tasks.len());
        for task in tasks {
            let tx = tx.clone();
            let name = task.name.clone();
            let handle = task.handle;
            let abort = handle.abort_handle();
            handles.push(abort);
            tokio::spawn(async move {
                let result = handle.await;
                let _ = tx.send((name, result));
            });
        }
        drop(tx);

        let outcome = tokio::select! {
            result = server => result.map_err(RuntimeError::Server),
            task = rx.recv() => Err(exited_before_shutdown(task)),
        };

        // Signal first, then wait. Workers check the token between cycles, so
        // this is what turns "stop accepting" into "finish what you started".
        shutdown.cancel();
        let drained = drain(&mut rx, drain_timeout).await;
        for handle in handles {
            handle.abort();
        }

        // A real failure outranks a drain timeout: the timeout is a symptom, the
        // original error is the cause.
        match (outcome, drained) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

/// What a supervised task reported: its name, and either its own result or the
/// join failure that replaced it.
type TaskCompletion = (String, Result<Result<(), String>, JoinError>);

/// Waits for the remaining supervised tasks to report in.
///
/// A task that returns an error during drain is reported; a task that is simply
/// slow trips [`RuntimeError::DrainTimeout`]. Both leave the process free to
/// exit, which is the requirement — the point of the bound is that shutdown
/// always terminates.
async fn drain(
    rx: &mut mpsc::UnboundedReceiver<TaskCompletion>,
    drain_timeout: Duration,
) -> Result<(), RuntimeError> {
    if drain_timeout.is_zero() {
        return Ok(());
    }
    let drain_all = async {
        let mut failure = None;
        while let Some((name, result)) = rx.recv().await {
            match result {
                // Cancelled tasks are the expected shape of a clean drain.
                Ok(Ok(())) => {}
                Ok(Err(message)) => {
                    failure.get_or_insert(RuntimeError::WorkerExited { name, message });
                }
                Err(err) if err.is_cancelled() => {}
                Err(err) => {
                    failure.get_or_insert(RuntimeError::WorkerJoin { name, source: err });
                }
            }
        }
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    };

    match timeout(drain_timeout, drain_all).await {
        Ok(result) => result,
        Err(_) => Err(RuntimeError::DrainTimeout {
            timeout: drain_timeout,
        }),
    }
}

/// Classifies a task completion that arrived while the server was still running.
fn exited_before_shutdown(task: Option<TaskCompletion>) -> RuntimeError {
    match task {
        Some((name, Ok(Ok(())))) => RuntimeError::WorkerExited {
            name,
            message: "completed before shutdown".to_owned(),
        },
        Some((name, Ok(Err(message)))) => RuntimeError::WorkerExited { name, message },
        Some((name, Err(err))) => RuntimeError::WorkerJoin { name, source: err },
        None => RuntimeError::WorkerExited {
            name: "runtime".to_owned(),
            message: "all runtime tasks completed before shutdown".to_owned(),
        },
    }
}

/// Why the supervised runtime stopped.
#[derive(Debug)]
pub enum RuntimeError {
    /// The HTTP server itself failed.
    Server(std::io::Error),
    /// A supervised task returned before shutdown was signalled. Includes clean
    /// exits: a worker loop that ends while the server is still up is a fault.
    WorkerExited { name: String, message: String },
    /// A supervised task panicked or was aborted.
    WorkerJoin { name: String, source: JoinError },
    /// Shutdown was signalled but tasks did not finish in time. In-flight work
    /// may have been cut short.
    DrainTimeout { timeout: Duration },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server(err) => write!(f, "server failed: {err}"),
            Self::WorkerExited { name, message } => {
                write!(f, "runtime task `{name}` exited before shutdown: {message}")
            }
            Self::WorkerJoin { name, source } => {
                write!(f, "runtime task `{name}` join failed: {source}")
            }
            Self::DrainTimeout { timeout } => write!(
                f,
                "runtime tasks did not finish within {timeout:?} of shutdown; \
                 in-flight work may have been interrupted"
            ),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Server(err) => Some(err),
            Self::WorkerJoin { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// Drives one request through a real router with the layer applied, and
    /// returns the `(http.route, otel.name)` its span recorded.
    async fn recorded_route(uri: &str) -> (String, String) {
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

    /// The `kafkaman::internal` tier is off by default and on by its own target.
    ///
    /// [`health`] stands in for all of it: it is the one annotated function in
    /// this crate that touches nothing, and the attribute is identical on every
    /// other one. What is being pinned is the directive `examples/README.md`
    /// documents — including that reaching the tier does **not** require turning
    /// on `debug` for everything, which would drown it in `sqlx` and `rdkafka`
    /// output.
    #[tokio::test]
    async fn the_internal_span_tier_is_gated_by_its_own_target() {
        use tracing_subscriber::layer::SubscriberExt as _;

        // Per-layer filtering, because that is what `kafkaman_otel::init` does:
        // it gives each layer its own `EnvFilter` rather than putting one on the
        // registry. A directive that works globally and not per layer would be a
        // directive that works in this test and not in a real service.
        async fn spans_opened_under(directive: &str) -> Vec<String> {
            use tracing_subscriber::Layer as _;

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

        assert!(
            !spans_opened_under("info")
                .await
                .contains(&"health".to_owned()),
            "the default filter must not open internal-tier spans"
        );
        assert!(
            spans_opened_under("info,kafkaman::internal=debug")
                .await
                .contains(&"health".to_owned()),
            "the documented directive must open them"
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

        let internal = AdminError::Sqlx(kafkaman_sqlx::Error::Handler(
            "schema kafkaman_x".to_owned(),
        ))
        .into_response();
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
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

    // ---- runtime supervision ----------------------------------------------

    async fn test_listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port should be available")
    }

    #[tokio::test]
    async fn with_runtime_waits_for_tasks_to_drain_before_returning() {
        // The point of the drain: a worker mid-cycle when shutdown fires must
        // reach its own completion, not be cut off with the listener.
        let finished = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();

        let task = {
            let finished = Arc::clone(&finished);
            let token = shutdown.clone();
            RuntimeTask::spawn("drainer", async move {
                token.cancelled().await;
                tokio::time::sleep(Duration::from_millis(150)).await;
                finished.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Infallible>(())
            })
        };

        let listener = test_listener().await;
        let server = serve(listener, admin_free_router());

        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });

        server
            .with_runtime(vec![task], shutdown)
            .await
            .expect("a clean shutdown should not be an error");

        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "with_runtime returned before the task finished its work"
        );
    }

    #[tokio::test]
    async fn with_runtime_fails_when_a_task_exits_early() {
        // A relay that "succeeds" while the server keeps serving is a silent
        // outage, so an early exit is an error even when it is `Ok(())`.
        let shutdown = CancellationToken::new();
        let task = RuntimeTask::spawn("relay", async { Ok::<(), Infallible>(()) });

        let listener = test_listener().await;
        let err = serve(listener, admin_free_router())
            .with_runtime(vec![task], shutdown.clone())
            .await
            .expect_err("an early task exit must fail the runtime");

        match err {
            RuntimeError::WorkerExited { name, .. } => assert_eq!(name, "relay"),
            other => panic!("expected WorkerExited, got {other:?}"),
        }
        assert!(
            shutdown.is_cancelled(),
            "a failing runtime must signal everything else to stop"
        );
    }

    #[tokio::test]
    async fn with_runtime_reports_a_failing_task_by_name() {
        let shutdown = CancellationToken::new();
        let task = RuntimeTask::spawn("dispatcher", async {
            Err::<(), _>("database is gone".to_owned())
        });

        let err = serve(test_listener().await, admin_free_router())
            .with_runtime(vec![task], shutdown)
            .await
            .expect_err("a task error must fail the runtime");

        let rendered = err.to_string();
        assert!(rendered.contains("dispatcher"), "got {rendered}");
        assert!(rendered.contains("database is gone"), "got {rendered}");
    }

    #[tokio::test]
    async fn with_runtime_bounds_the_drain_of_a_wedged_task() {
        // A task that ignores the shutdown signal must not hang the process.
        let shutdown = CancellationToken::new();
        let task = RuntimeTask::spawn("wedged", async {
            std::future::pending::<()>().await;
            Ok::<(), Infallible>(())
        });

        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });

        let err = serve(test_listener().await, admin_free_router())
            .with_runtime_drain_timeout(vec![task], shutdown, Duration::from_millis(100))
            .await
            .expect_err("a wedged task should trip the drain timeout");

        assert!(
            matches!(err, RuntimeError::DrainTimeout { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn with_runtime_cancels_the_token_even_with_no_tasks() {
        let shutdown = CancellationToken::new();
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });

        serve(test_listener().await, admin_free_router())
            .with_runtime(Vec::new(), shutdown.clone())
            .await
            .expect("a clean shutdown should not be an error");

        assert!(shutdown.is_cancelled());
    }

    /// A router with no state, for supervision tests that never issue a request.
    fn admin_free_router() -> Router {
        Router::new().route("/health", get(health))
    }
}
