//! The one destructive operator route, kept apart from the read-only ones.
//!
//! `POST /dlq/{message_type}/redrive` moves dead-lettered rows back to
//! `Pending`, which re-runs their handlers against whatever side effects those
//! handlers have. Separating it from [`crate::admin`] is what lets one auth
//! policy cover reads and a stricter one cover writes.

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use kafkaman_core::ReceivedFailureKind;
use kafkaman_sqlx::{redrive_received, service_table_access, ReceivedTable, Replay};
use serde::{Deserialize, Serialize};

use crate::admin::descriptor_for_message_type;
use crate::error::AdminError;
use crate::state::AdminState;

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
/// **No authentication or authorization**, like [`admin_router`](crate::admin_router), and with more
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
    /// [`dlq_summary`](crate::admin_router) prints in `latest_error.type`, or the bare
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
