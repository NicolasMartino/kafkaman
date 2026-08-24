//! W3C trace context, and the three things kafkaman does with it.
//!
//! # Why trace context has to be durable
//!
//! The outbox pattern separates enqueue from publish in time — that separation
//! is the entire point. A producer span opened at publish time is therefore
//! orphaned from the business transaction that created the row, and the trace an
//! operator most wants to see is exactly the one the pattern breaks. The same is
//! true of the receive side: ingest stores a row, and dispatch runs it later.
//!
//! So context travels the way `correlation_id` already does — captured when the
//! row is written, stored in a column beside it, restored when the row is acted
//! on. See `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`.
//!
//! # Why this is a verb API rather than a type API
//!
//! Nothing here exposes an OpenTelemetry type. [`TraceContext`] is two strings,
//! and the operations are [`capture`], [`set_parent`], and [`add_link`]. That
//! keeps the OpenTelemetry dependency optional without the public surface
//! changing shape when the feature is off, and it keeps every caller free of
//! `#[cfg]`.
//!
//! Parsing and formatting are done by hand against the W3C Recommendation rather
//! than through a propagator, because the propagators live in
//! `opentelemetry_sdk` and no crate under `crates/` may depend on the SDK. The
//! format is fixed, short, and versioned; implementing it is cheaper than
//! breaking the ownership boundary.

use serde::{Deserialize, Serialize};

/// The W3C trace context carried by one message.
///
/// Both fields are exactly what the corresponding HTTP or Kafka headers would
/// hold: `traceparent` is `version-trace_id-parent_id-flags`, and `tracestate`
/// is the vendor list, present only when an upstream set one.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TraceContext {
    traceparent: String,
    tracestate: Option<String>,
}

impl TraceContext {
    /// Rebuild context from stored columns or received headers.
    ///
    /// Returns `None` for anything that is not a well-formed `traceparent`,
    /// including `None` itself. **Absent or unparseable context is normal and is
    /// never an error**: a row enqueued outside any span has none, and a record
    /// from an uninstrumented producer has none. Message flow does not depend on
    /// it, so a malformed value is dropped rather than propagated or rejected.
    pub fn from_parts(traceparent: Option<String>, tracestate: Option<String>) -> Option<Self> {
        let traceparent = traceparent?;
        if !is_well_formed_traceparent(&traceparent) {
            return None;
        }
        Some(Self {
            traceparent,
            tracestate: tracestate.filter(|value| !value.is_empty()),
        })
    }

    /// The `traceparent` value, for a column or a header.
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// The `tracestate` value, when an upstream set one.
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
}

/// Whether a string is a `traceparent` this code can act on.
///
/// Checks the shape the W3C Recommendation fixes — `xx-<32 hex>-<16 hex>-<2
/// hex>` — and rejects the all-zero trace and span ids the specification defines
/// as invalid. Unknown *versions* are deliberately accepted as long as the first
/// four fields parse, which is what the Recommendation's forward-compatibility
/// rule requires: a future version appends fields, it does not rearrange these.
fn is_well_formed_traceparent(value: &str) -> bool {
    let mut fields = value.split('-');
    let (Some(version), Some(trace_id), Some(parent_id), Some(flags)) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return false;
    };

    is_hex(version, 2)
        && version != "ff"
        && is_hex(trace_id, 32)
        && trace_id.bytes().any(|byte| byte != b'0')
        && is_hex(parent_id, 16)
        && parent_id.bytes().any(|byte| byte != b'0')
        && is_hex(flags, 2)
}

fn is_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(feature = "traces")]
mod enabled {
    use std::str::FromStr;

    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use super::TraceContext;

    /// The trace context of the span this code is running in, if there is one.
    ///
    /// Returns `None` when no OpenTelemetry layer is installed, when the current
    /// span is not recording, or when the process is simply not inside a span —
    /// all of which are ordinary states, not failures.
    pub fn capture() -> Option<TraceContext> {
        span_context_of(&tracing::Span::current()).map(|context| TraceContext {
            traceparent: format!(
                "00-{}-{}-{:02x}",
                context.trace_id(),
                context.span_id(),
                context.trace_flags() & TraceFlags::SAMPLED
            ),
            tracestate: Some(context.trace_state().header()).filter(|state| !state.is_empty()),
        })
    }

    /// Make `span` a child of stored context.
    ///
    /// Used where a durable gap has interrupted the in-memory context: the relay
    /// publishing a row enqueued minutes ago, or a dispatcher handling a row
    /// ingested before the process started.
    pub fn set_parent(span: &tracing::Span, context: &TraceContext) {
        let Some(span_context) = to_span_context(context) else {
            return;
        };
        // The result reports "no OpenTelemetry layer is installed", which is an
        // ordinary state rather than a failure: an uninstrumented host runs the
        // same code paths and must not be told about it on every message.
        let _ =
            span.set_parent(opentelemetry::Context::new().with_remote_span_context(span_context));
    }

    /// Record that `span` was caused by, but is not part of, `context`.
    ///
    /// This is what a consumer does with a producer's context. A consumer polls
    /// a batch that may hold records from many unrelated traces, so parenting
    /// would attach unrelated work to whichever trace happened to be first;
    /// messaging semantic conventions prescribe a link.
    pub fn add_link(span: &tracing::Span, context: &TraceContext) {
        let Some(span_context) = to_span_context(context) else {
            return;
        };
        // Returns unit rather than a result, unlike `set_parent`.
        span.add_link(span_context);
    }

    /// The OpenTelemetry context of a `tracing` span, if it has a valid one.
    fn span_context_of(span: &tracing::Span) -> Option<SpanContext> {
        let context = span.context();
        let span_context = context.span().span_context().clone();
        span_context.is_valid().then_some(span_context)
    }

    /// Parse a stored `traceparent`/`tracestate` pair into a remote span context.
    ///
    /// `is_remote` is true by definition here: this context came from another
    /// process, or from this one before a durable gap that no in-memory state
    /// survived.
    fn to_span_context(context: &TraceContext) -> Option<SpanContext> {
        let mut fields = context.traceparent().split('-');
        let (_version, trace_id, span_id, flags) = (
            fields.next()?,
            fields.next()?,
            fields.next()?,
            fields.next()?,
        );

        Some(SpanContext::new(
            TraceId::from_hex(trace_id).ok()?,
            SpanId::from_hex(span_id).ok()?,
            TraceFlags::new(u8::from_str_radix(flags, 16).ok()?),
            true,
            context
                .tracestate()
                .and_then(|state| TraceState::from_str(state).ok())
                .unwrap_or_default(),
        ))
    }
}

/// No-op stand-ins used when the `traces` feature is off.
///
/// [`TraceContext::from_parts`] deliberately stays live in both twins: a build
/// without tracing still reads a stored `traceparent` and still forwards it onto
/// the wire, so turning the feature off stops this process from *producing*
/// trace context without breaking propagation for the services around it.
#[cfg(not(feature = "traces"))]
mod disabled {
    use super::TraceContext;

    pub fn capture() -> Option<TraceContext> {
        None
    }

    pub fn set_parent(_span: &tracing::Span, _context: &TraceContext) {}

    pub fn add_link(_span: &tracing::Span, _context: &TraceContext) {}
}

#[cfg(feature = "traces")]
pub use enabled::{add_link, capture as capture_trace_context, set_parent};

#[cfg(not(feature = "traces"))]
pub use disabled::{add_link, capture as capture_trace_context, set_parent};

#[cfg(test)]
mod tests {
    use super::*;

    /// The example from the W3C Recommendation.
    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn a_well_formed_traceparent_round_trips() {
        let context = TraceContext::from_parts(Some(VALID.to_owned()), Some("vendor=1".to_owned()))
            .expect("the specification's own example should parse");
        assert_eq!(context.traceparent(), VALID);
        assert_eq!(context.tracestate(), Some("vendor=1"));
    }

    #[test]
    fn absent_context_is_absent_rather_than_an_error() {
        assert_eq!(TraceContext::from_parts(None, None), None);
        assert_eq!(
            TraceContext::from_parts(None, Some("vendor=1".to_owned())),
            None,
            "tracestate without traceparent describes nothing"
        );
    }

    #[test]
    fn malformed_traceparents_are_dropped() {
        for value in [
            "",
            "not a traceparent",
            // Too few fields.
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            // Trace id one character short.
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            // Non-hex in the span id.
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bg-01",
            // The specification's invalid all-zero ids.
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            // `ff` is reserved as a forbidden version.
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert_eq!(
                TraceContext::from_parts(Some(value.to_owned()), None),
                None,
                "{value:?} should not be accepted as trace context"
            );
        }
    }

    #[test]
    fn a_future_version_with_extra_fields_is_still_usable() {
        // The Recommendation requires forward compatibility: a later version
        // appends fields, it does not rearrange the first four. Refusing them
        // would silently drop context from a newer upstream.
        let value = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra";
        assert!(TraceContext::from_parts(Some(value.to_owned()), None).is_some());
    }

    #[test]
    fn an_empty_tracestate_is_no_tracestate() {
        let context = TraceContext::from_parts(Some(VALID.to_owned()), Some(String::new()))
            .expect("traceparent is valid");
        assert_eq!(context.tracestate(), None);
    }
}
