//! What an admin request can fail with, and the status each failure earns.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use kafkaman_sqlx::TableAccess;

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
