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

use serde::{Deserialize, Deserializer, Serialize};

/// The W3C trace context carried by one message.
///
/// Both fields are exactly what the corresponding HTTP or Kafka headers would
/// hold: `traceparent` is `version-trace_id-parent_id-flags`, and `tracestate`
/// is the vendor list, present only when an upstream set one.
///
/// # The invariant, and why it is enforced at every door
///
/// A value of this type is well-formed. Every consumer relies on that:
/// [`set_parent`] and [`add_link`] hand the strings to a parser that has no
/// error path left to take, and the publisher writes them onto the wire as
/// standard headers that the next service in the chain will parse without
/// asking kafkaman's permission. A `TraceContext` holding a malformed
/// `traceparent` is therefore not a local inconvenience — it is kafkaman
/// injecting garbage into a protocol other systems trust.
///
/// So there is exactly one constructor, [`from_parts`](Self::from_parts), and
/// the other two ways a Rust type is normally built are closed: there is no
/// `Default` (an empty `traceparent` is not a trace context), and the
/// [`Deserialize`] implementation validates rather than assigning fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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
    ///
    /// `tracestate` is validated separately and independently: an unusable
    /// vendor list is dropped on its own, and the `traceparent` beside it
    /// survives. That asymmetry is the Recommendation's — a `tracestate` that
    /// cannot be parsed is discarded and the trace continues — and it is the
    /// useful behaviour besides, because the trace id is what correlates and the
    /// vendor list is what decorates.
    pub fn from_parts(traceparent: Option<String>, tracestate: Option<String>) -> Option<Self> {
        let traceparent = traceparent?;
        if !is_well_formed_traceparent(&traceparent) {
            return None;
        }
        Some(Self {
            traceparent,
            tracestate: tracestate.as_deref().and_then(sanitized_tracestate),
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
/// as invalid, along with the reserved `ff` version.
///
/// # Where the grammar is strict, and where it is not
///
/// Version `00` is a closed format: the Recommendation gives it exactly those
/// four fields, so trailing content means the value was produced by something
/// that is not speaking version 00 and its bytes cannot be trusted to mean what
/// they look like. Higher versions are the opposite case — the specification's
/// forward-compatibility rule says a later version *appends* fields rather than
/// rearranging these, so extra fields are expected and the first four still
/// parse. Refusing them would silently drop context from a newer upstream.
///
/// Hex is lowercase-only in both cases, because the grammar spells it
/// `HEXDIGLC` and because an uppercase id is a different string to every system
/// that compares trace ids as bytes — which is most of them.
fn is_well_formed_traceparent(value: &str) -> bool {
    let mut fields = value.split('-');
    let (Some(version), Some(trace_id), Some(parent_id), Some(flags)) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return false;
    };

    if !is_hex(version, 2) || version == "ff" {
        return false;
    }
    // Version 00 admits nothing after the flags. A later version may append, but
    // an empty appended field is malformed under any version — and *every*
    // appended field is checked, not just the first: `01-…-01-aa-` ends in one,
    // and a parser that stops after `aa` would call that well formed.
    let mut appended = 0;
    for field in fields {
        appended += 1;
        if version == "00" || field.is_empty() || appended > MAX_APPENDED_FIELDS {
            return false;
        }
    }

    is_hex(trace_id, 32)
        && trace_id.bytes().any(|byte| byte != b'0')
        && is_hex(parent_id, 16)
        && parent_id.bytes().any(|byte| byte != b'0')
        && is_hex(flags, 2)
}

/// Lowercase hex of exactly `len` digits — the Recommendation's `HEXDIGLC`.
fn is_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// How many fields a future version may append before the value is treated as
/// something other than a `traceparent`.
///
/// The Recommendation fixes no ceiling, so this is kafkaman's: the header is
/// stored in a column and republished on every hop, and an unbounded field count
/// makes an unbounded header. Twelve is far past anything a version bump would
/// plausibly need and far short of anything that costs a row.
const MAX_APPENDED_FIELDS: usize = 12;

/// The Recommendation's ceiling on `tracestate` list members.
const MAX_TRACESTATE_MEMBERS: usize = 32;

/// The largest `tracestate` value a single list member may carry, from the
/// grammar's `value = 0*255(chr) nblk-chr`.
const MAX_TRACESTATE_VALUE: usize = 256;

/// The longest `tracestate` kafkaman will parse at all.
///
/// The 32-member ceiling bounds what is *stored*; this bounds what is *read*.
/// Without it a single header could carry a hundred thousand list members, every
/// one of which has to be checked against every earlier key for uniqueness. Four
/// times the 512 characters the Recommendation asks implementations to
/// propagate, which is also room for all 32 members at any realistic size.
const MAX_TRACESTATE_LEN: usize = 2048;

/// A `tracestate` kafkaman is willing to carry, or `None`.
///
/// # Why this is validated at all
///
/// `tracestate` never reaches a span — [`set_parent`] and [`add_link`] use the
/// `traceparent` alone — so nothing inside this process would notice a malformed
/// one. It is forwarded, though: stored in a column at enqueue and written back
/// onto the wire as a standard header at publish. Skipping validation would make
/// kafkaman a laundering step that takes an unparseable header from one hop and
/// re-emits it as a well-formed-looking one to the next, with kafkaman's
/// signature on it. Whatever is downstream then has to deal with a header this
/// process chose to pass on.
///
/// # Why it normalizes instead of only accepting
///
/// Two rules in the Recommendation are prescriptive rather than diagnostic: the
/// optional whitespace around commas is not part of a member, and a list longer
/// than 32 members is truncated from the right rather than rejected. Honouring
/// them means rebuilding the value, which also makes what kafkaman stores
/// canonical — the same logical `tracestate` produces the same column bytes
/// regardless of how the upstream spaced it.
///
/// A member that breaks the *grammar* is a different case and takes the
/// Recommendation's other instruction: discard the whole list and let the trace
/// continue on the `traceparent`. A repeated key is treated the same way: it is
/// a list constraint rather than an ABNF production, but `a=1,a=2` parses
/// cleanly and leaves every reader downstream to pick one, so forwarding it
/// hands on an ambiguity rather than a value.
///
/// The two rules do not conflict where they meet. Truncation is what a *valid*
/// list gets for being longer than the ceiling; a list with a repeated key is
/// not valid at any length, which is why uniqueness is judged over the whole
/// value and not just the part that would be kept.
fn sanitized_tracestate(value: &str) -> Option<String> {
    if value.len() > MAX_TRACESTATE_LEN {
        return None;
    }

    let mut members = Vec::new();
    let mut keys: Vec<&str> = Vec::new();
    for member in value.split(',') {
        let member = member.trim_matches([' ', '\t']);
        // Empty list members are explicitly permitted and carry nothing, so they
        // are dropped rather than counted against the ceiling.
        if member.is_empty() {
            continue;
        }
        let (key, entry) = member.split_once('=')?;
        if !is_tracestate_key(key) || !is_tracestate_value(entry) {
            return None;
        }
        // A key identifies at most one list member, checked across the whole
        // list rather than the prefix that survives the ceiling. A repeat past
        // the thirty-second member still means the value arrived invalid, and
        // letting truncation drop it would re-emit that value as a list that
        // now looks correct — the laundering this function exists to prevent.
        if keys.contains(&key) {
            return None;
        }
        keys.push(key);
        if members.len() < MAX_TRACESTATE_MEMBERS {
            members.push(member);
        }
    }

    (!members.is_empty()).then(|| members.join(","))
}

/// The Recommendation's `key`, in both its simple and multi-tenant forms.
fn is_tracestate_key(key: &str) -> bool {
    fn is_key_byte(byte: u8) -> bool {
        matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'*' | b'/')
    }

    match key.split_once('@') {
        // `tenant@vendor`, where the tenant may start with a digit and the
        // vendor half is the shorter one. Splitting on the first `@` is what
        // makes a second one a syntax error rather than part of a name.
        Some((tenant, vendor)) => {
            (1..=241).contains(&tenant.len())
                && matches!(tenant.bytes().next(), Some(b'a'..=b'z' | b'0'..=b'9'))
                && tenant.bytes().all(is_key_byte)
                && (1..=14).contains(&vendor.len())
                && matches!(vendor.bytes().next(), Some(b'a'..=b'z'))
                && vendor.bytes().all(is_key_byte)
        }
        None => {
            (1..=256).contains(&key.len())
                && matches!(key.bytes().next(), Some(b'a'..=b'z'))
                && key.bytes().all(is_key_byte)
        }
    }
}

/// The Recommendation's `value`: printable ASCII without `,` or `=`.
///
/// Tab is excluded even though it is optional whitespace elsewhere in the
/// grammar. `chr = %x20-2B / %x2D-3C / %x3E-7E` starts at space, so the only
/// place a tab may legally appear is the OWS around a comma — which is *between*
/// members, not inside one, and which the caller has already trimmed. Accepting
/// it here would forward `vendor=a\tb` as though it were a value.
///
/// The grammar also forbids a trailing blank (`value = 0*255(chr) nblk-chr`),
/// and there is deliberately no check for that: a space in that position is the
/// same trimmed OWS, so a second check would be unreachable rather than
/// defensive.
fn is_tracestate_value(value: &str) -> bool {
    (1..=MAX_TRACESTATE_VALUE).contains(&value.len())
        && value
            .bytes()
            .all(|byte| matches!(byte, 0x20..=0x2b | 0x2d..=0x3c | 0x3e..=0x7e))
}

impl<'de> Deserialize<'de> for TraceContext {
    /// Deserialize through [`TraceContext::from_parts`], so the invariant holds
    /// however a value was built.
    ///
    /// This one *errors* on a malformed value where every other entry point
    /// drops it, and the difference is the caller. Header and column readers ask
    /// "is there context here?", where the honest answer is often no. Anything
    /// deserializing a bare `TraceContext` has instead asserted that it holds
    /// one, and the useful reply to a false assertion is to say so — the rows
    /// that carry context optionally are read through
    /// the `optional_trace_context` serde adapter, which drops exactly as a
    /// column read does.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawTraceContext::deserialize(deserializer)?;
        TraceContext::from_parts(Some(raw.traceparent), raw.tracestate).ok_or_else(|| {
            serde::de::Error::custom("traceparent is not a W3C trace context kafkaman can forward")
        })
    }
}

/// The wire shape, without the invariant. Field names match the derived
/// `Serialize` above, which is what makes the pair a round trip.
#[derive(Deserialize)]
struct RawTraceContext {
    traceparent: String,
    #[serde(default)]
    tracestate: Option<String>,
}

/// Serde support for a row's `Option<TraceContext>`.
///
/// Reads drop context that no longer parses instead of failing the row, for the
/// same reason `problem_type` degrades an unrecognized failure kind: a stored
/// row must stay readable by a binary whose vocabulary has moved on. The
/// `traceparent` grammar has already been tightened once — lowercase hex, closed
/// version `00` — and a row serialized before that must still load, minus a
/// trace link that no longer means anything.
pub(crate) mod optional_trace_context {
    use super::{RawTraceContext, TraceContext};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<TraceContext>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<TraceContext>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Option::<RawTraceContext>::deserialize(deserializer)?
            .and_then(|raw| TraceContext::from_parts(Some(raw.traceparent), raw.tracestate)))
    }
}

/// A scope in which log records are stamped with a span's trace ids.
///
/// Restores the previous context when dropped. Never held across an `await`:
/// the underlying guard is `!Send`, and a context that outlives its scope
/// attributes unrelated work to the wrong trace.
#[derive(Debug)]
pub struct TraceScope {
    #[cfg(feature = "traces")]
    _guard: Option<opentelemetry::ContextGuard>,
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

    /// Attach a `tracing` span's OpenTelemetry context for the current scope.
    ///
    /// # Why this is needed at all
    ///
    /// `opentelemetry-appender-tracing` stamps a log record with the trace and
    /// span ids of the *OpenTelemetry* context that is current when the event is
    /// emitted. It does not read the `tracing` span stack — the two are separate
    /// stacks, and `tracing-opentelemetry` bridges spans without attaching them
    /// to the OpenTelemetry one.
    ///
    /// So an event emitted inside a `tracing` span reaches the log signal with
    /// no trace context unless something attaches it, and a log record that
    /// cannot be pivoted into its trace has lost the property that made
    /// exporting it worthwhile. This is that something.
    pub fn attach(span: &tracing::Span) -> super::TraceScope {
        super::TraceScope {
            _guard: span_context_of(span).map(|span_context| {
                opentelemetry::Context::new()
                    .with_remote_span_context(span_context)
                    .attach()
            }),
        }
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

    pub fn attach(_span: &tracing::Span) -> super::TraceScope {
        super::TraceScope {}
    }
}

#[cfg(feature = "traces")]
pub use enabled::{add_link, attach, capture as capture_trace_context, set_parent};

#[cfg(not(feature = "traces"))]
pub use disabled::{add_link, attach, capture as capture_trace_context, set_parent};
