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
use std::future::Future;

use crate::text::truncate_on_char_boundary;

/// Longest error text recorded as a span's OpenTelemetry status description.
///
/// A status description is attacker-influenced in the same way a log line is: a
/// database error can quote the value that violated a constraint, and a broker
/// error can quote a payload fragment. The status is there to say *what went
/// wrong*, and the first 256 bytes say that. Everything past them is unbounded
/// data being copied into a backend that indexes it.
const MAX_STATUS_DESCRIPTION: usize = 256;

/// The `tracing` target every exception event is emitted on.
///
/// Named rather than left as a literal because it is the seam a host filters at,
/// and it has to be filtered somewhere. The event is deliberately *message-less*
/// — the only shape `tracing-opentelemetry` rewrites into an OpenTelemetry
/// `exception` — which means anything else consuming `tracing` events sees an
/// ERROR record with an empty body. That is right for the span layer, which
/// turns it into the exception an APM backend groups errors by, and right for a
/// stdout formatter, which renders the fields. It is wrong for an OTLP *log*
/// exporter, where it duplicates every error already reported as an error and
/// arrives as a blank line: measured on the example stack, 13 of 13 ERROR log
/// records were these.
///
/// `kafkaman-otel` filters on this in its one-call `init`. A host composing its
/// own registry should do the same on whatever layer bridges events to logs.
pub const TELEMETRY_TARGET: &str = "kafkaman::telemetry";

/// Mark `span` failed, with a bounded description of `error`.
///
/// Recorded through the `tracing-opentelemetry` bridge's `otel.status_code` and
/// `otel.status_description` fields, so the span must have declared both as
/// [`tracing::field::Empty`] when it was created.
///
/// Reports a *status* and nothing else. Use [`record_exception`] on the
/// `kafkaman.*` span that owns a failure; use this one on the `db.query` spans
/// beneath it, and anywhere the failure is not an error value — an HTTP 5xx, for
/// instance. One failure should reach a backend as one error, not as one per
/// span in the stack that saw it go past.
pub fn record_error(span: &tracing::Span, error: &dyn fmt::Display) {
    let description = error.to_string();
    span.record("otel.status_code", "ERROR");
    span.record(
        "otel.status_description",
        truncate_on_char_boundary(&description, MAX_STATUS_DESCRIPTION),
    );
}

/// Mark `span` failed *and* report the failure as an OpenTelemetry exception.
///
/// The exception is what an APM backend groups errors by, so this is what turns
/// a failed transaction into an entry in an error list. The type comes from the
/// error itself via [`ProblemType`](crate::ProblemType) rather than from an
/// argument, so a call site cannot label a failure as something it is not.
pub fn record_exception<E>(span: &tracing::Span, error: &E)
where
    E: crate::ProblemType + fmt::Display + ?Sized,
{
    record_exception_as(span, error.problem_type(), &error);
}

/// [`record_exception`] for a failure whose type cannot be asked for.
///
/// One caller: `Publisher::publish` returns a boxed `std::error::Error` so a
/// transport crate can report failures without this crate depending on it, and
/// widening that bound would break every implementor. The publish path passes
/// [`PUBLISH`](crate::problem::PUBLISH) explicitly instead.
pub fn record_exception_as(
    span: &tracing::Span,
    problem_type: &'static str,
    error: &dyn fmt::Display,
) {
    let description = error.to_string();
    let bounded = truncate_on_char_boundary(&description, MAX_STATUS_DESCRIPTION);
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", bounded);
    // The same value the event carries as `exception.type`. On the span it is
    // what a backend groups *transactions* by; on the event, what it groups
    // *errors* by. A span that has not declared the field ignores this, which is
    // the correct outcome for one that does not want the attribute.
    span.record("error.type", problem_type);

    // Three things about this event are load-bearing, and all three are
    // properties of `tracing-opentelemetry`'s bridge rather than choices:
    //
    // - It carries **no message**. The bridge rewrites an event to the
    //   `exception` the OpenTelemetry conventions describe only when the event
    //   is unnamed and has a field called `error`; a format string names the
    //   event and the rewrite silently stops happening.
    // - The `error` field is `%`, not `?` and not a `&dyn std::error::Error`.
    //   The bridge's `&dyn Error` path fills in `exception.message` but does not
    //   perform the rename, so it produces an event no backend recognizes. What
    //   that path additionally offers — a `source()` chain — is empty here
    //   anyway, because kafkaman's `#[error(transparent)]` variants forward
    //   `source()` past the error they wrap.
    // - The value is **pre-truncated**. The bridge also copies this field into
    //   the span's status description, which would otherwise reinstate the
    //   unbounded string `MAX_STATUS_DESCRIPTION` exists to prevent. Passing the
    //   bounded text to both makes the two writes agree whichever lands last.
    span.in_scope(|| {
        tracing::error!(
            target: TELEMETRY_TARGET,
            error = %bounded,
            exception.type = problem_type,
        );
    });
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

/// Instrument a fallible future with a span, and mark the span failed if it is.
///
/// Pairs with [`db_span!`] and [`db_poll_span!`]. Both declare `otel.status_code`
/// and `otel.status_description`, and until this existed nothing ever wrote
/// them: `.instrument(db_span!(..)).await?` propagates the error past the span
/// it just closed, so a statement that failed exported green underneath a parent
/// that was red about it. A waterfall then shows the failure at the phase and
/// gives no clue which statement caused it.
///
/// Reports a status and not an exception, deliberately. The failure is already
/// travelling up to a `kafkaman.*` span that will report it once; see
/// [`record_error`].
pub trait InstrumentDb: Future + Sized {
    /// Run `self` inside `span`, recording an error status if it fails.
    fn instrument_db(self, span: tracing::Span) -> impl Future<Output = Self::Output>;
}

impl<F, T, E> InstrumentDb for F
where
    F: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    fn instrument_db(self, span: tracing::Span) -> impl Future<Output = Self::Output> {
        use tracing::Instrument as _;
        async move {
            let result = self.instrument(span.clone()).await;
            if let Err(error) = &result {
                record_error(&span, error);
            }
            result
        }
    }
}
