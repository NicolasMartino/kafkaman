//! Span helpers shared by every crate that instruments kafkaman's durable path.
//!
//! These live here rather than in `kafkaman-sqlx` or the examples because three
//! crates and two example services were each carrying their own copy. A span
//! shape that four places re-derive is a span shape that drifts, and the
//! attributes below are a documented compatibility surface — see
//! `wiki/compatibility/m6-observability-operability-api.compat.md`.
//!
//! Nothing here touches an OpenTelemetry type. The `otel.*` fields are the
//! `tracing-opentelemetry` bridge's own vocabulary, read by whichever subscriber
//! the host installed, which keeps SDK ownership where the ownership decision
//! put it.

use std::fmt;

/// Longest error text recorded as a span's OpenTelemetry status description.
///
/// A status description is attacker-influenced in the same way a log line is: a
/// database error can quote the value that violated a constraint, and a broker
/// error can quote a payload fragment. The status is there to say *what went
/// wrong*, and the first 256 bytes say that. Everything past them is unbounded
/// data being copied into a backend that indexes it.
const MAX_STATUS_DESCRIPTION: usize = 256;

/// Mark `span` failed, with a bounded description of `error`.
///
/// Recorded through the `tracing-opentelemetry` bridge's `otel.status_code` and
/// `otel.status_description` fields, so the span must have declared both as
/// [`tracing::field::Empty`] when it was created.
pub fn record_error(span: &tracing::Span, error: &dyn fmt::Display) {
    let description = error.to_string();
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", truncate(&description));
}

/// The longest prefix of `text` that fits in [`MAX_STATUS_DESCRIPTION`] bytes
/// and ends on a character boundary.
fn truncate(text: &str) -> &str {
    if text.len() <= MAX_STATUS_DESCRIPTION {
        return text;
    }
    let mut end = MAX_STATUS_DESCRIPTION;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// A PostgreSQL client span for one persistence statement.
///
/// ```
/// # use kafkaman_core::db_span;
/// let table = "kafkaman_outbox_product_snapshot";
/// let span = db_span!("INSERT", table, "insert outbox row");
/// ```
///
/// The SQL this project runs contains generated table names, so the span keeps
/// low-cardinality shape in `db.query.summary` and the concrete target relation
/// in `db.collection.name`. Full statement text is never recorded, and neither
/// are bind values.
///
/// A macro rather than a function because the exported span name is
/// `concat!("db.query ", $summary)` — built at compile time from a string
/// literal. The function this replaced called `format!` on every statement, on
/// every path, whether or not any subscriber had the span enabled.
#[macro_export]
macro_rules! db_span {
    ($operation:expr, $collection:expr, $summary:literal $(,)?) => {
        $crate::__tracing::info_span!(
            "db.query",
            "otel.name" = concat!("db.query ", $summary),
            "otel.kind" = "client",
            "otel.status_code" = $crate::__tracing::field::Empty,
            "otel.status_description" = $crate::__tracing::field::Empty,
            "db.system.name" = "postgresql",
            "db.operation.name" = $operation,
            "db.collection.name" = %$collection,
            "db.query.summary" = $summary,
        )
    };
}

/// A PostgreSQL client span for a scheduler polling statement.
///
/// Identical to [`db_span!`] but `debug` rather than `info`. These statements
/// fire on every cycle whether or not they claim a row, so at the default filter
/// an idle service would export nothing but empty polls and a waterfall would be
/// mostly noise. Raise `RUST_LOG` to `debug` to inspect the polling itself.
#[macro_export]
macro_rules! db_poll_span {
    ($operation:expr, $collection:expr, $summary:literal $(,)?) => {
        $crate::__tracing::debug_span!(
            "db.query",
            "otel.name" = concat!("db.query ", $summary),
            "otel.kind" = "client",
            "otel.status_code" = $crate::__tracing::field::Empty,
            "otel.status_description" = $crate::__tracing::field::Empty,
            "db.system.name" = "postgresql",
            "db.operation.name" = $operation,
            "db.collection.name" = %$collection,
            "db.query.summary" = $summary,
        )
    };
}
