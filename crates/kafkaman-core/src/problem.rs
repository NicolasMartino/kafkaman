//! The permanent problem-type vocabulary, and the trait that reports it.
//!
//! Every failure kafkaman records on a span names itself with one of the URIs
//! below. They are the same identifiers
//! [`ReceivedFailureKind::problem_type`](crate::ReceivedFailureKind::problem_type)
//! has always written into stored problem details, extended to cover the error
//! enums so that a span attribute and a DLQ row speak one vocabulary.
//!
//! # Why a trait rather than a string at the call site
//!
//! [`record_exception`](crate::record_exception) could have taken the URI as an
//! argument. It does not, because the call sites that report failures are not
//! the places that know what a failure *is* — `record_failure_span` used to be
//! handed a pre-formatted `String` with the concrete error already erased one
//! frame up. A trait puts the classification next to the variant it describes,
//! where the compiler checks that every variant has one.
//!
//! # These URIs are permanent
//!
//! They are written into stored problem details, exported as `error.type` and
//! `exception.type`, and may be matched by anything reading a DLQ inspection
//! API or an APM error group. Adding a URI is routine; changing one is a
//! breaking change for every consumer that pinned it. New error variants pick
//! an existing URI unless none of them describes the failure.

/// No handler is registered for the message type.
pub const MISSING_HANDLER: &str = "urn:kafkaman:problem:missing-handler";
/// A record's payload, headers, or entity key could not be decoded.
pub const INVALID_PAYLOAD: &str = "urn:kafkaman:problem:invalid-payload";
/// The database or the broker failed. Transient, and worth retrying.
pub const INFRASTRUCTURE: &str = "urn:kafkaman:problem:infrastructure";
/// An application handler returned an error.
pub const HANDLER: &str = "urn:kafkaman:problem:handler";
/// An application handler unwound instead of returning.
///
/// Separate from [`HANDLER`] because a panic is a bug rather than a failure the
/// handler chose to report, even though both retry on the same budget. The
/// distinction is deliberately absent from
/// [`ReceivedFailureKind`](crate::ReceivedFailureKind), whose four URIs are
/// persisted and must not grow.
pub const HANDLER_PANICKED: &str = "urn:kafkaman:problem:handler-panicked";
/// Application code outside a handler unwound.
///
/// A `KafkaMessage` method or a `Serialize` impl, on either the enqueue or the
/// ingest path.
pub const APPLICATION_PANICKED: &str = "urn:kafkaman:problem:application-panicked";
/// kafkaman is misconfigured. It will not fix itself by retrying.
pub const CONFIGURATION: &str = "urn:kafkaman:problem:configuration";
/// A migration, changeset, or stored value violates the schema contract.
pub const SCHEMA: &str = "urn:kafkaman:problem:schema";
/// A message type, topic registration, or header is not routable as declared.
pub const MESSAGE_ROUTING: &str = "urn:kafkaman:problem:message-routing";
/// A cache row cannot be applied without breaking the invariant that backs it.
///
/// Terminal by construction: these are the failures
/// `handler_failure_disposition` refuses to retry, because a second attempt
/// computes the same answer.
pub const CACHE_INVARIANT: &str = "urn:kafkaman:problem:cache-invariant";
/// A message has no usable idempotency identity.
pub const IDEMPOTENCY: &str = "urn:kafkaman:problem:idempotency";
/// A Kafka topic is missing, or its configuration contradicts what it is for.
pub const TOPIC: &str = "urn:kafkaman:problem:topic";
/// A replay was refused because performing it would publish stale state.
pub const UNSAFE_REPLAY: &str = "urn:kafkaman:problem:unsafe-replay";
/// Publishing a claimed outbox row to the broker failed.
pub const PUBLISH: &str = "urn:kafkaman:problem:publish";
/// A loop stopped itself because failures stopped looking like isolated ones.
pub const BREAKER_TRIPPED: &str = "urn:kafkaman:problem:breaker-tripped";

/// A statement violated a constraint the schema enforces.
///
/// Unique, foreign-key, not-null, check, exclusion. Split out of
/// [`INFRASTRUCTURE`] because it is the opposite of an environment failure:
/// nothing is broken, the write was refused. It groups with the handler's own
/// logic in an operator's head and with a closed connection pool in none of it.
pub const CONSTRAINT: &str = "urn:kafkaman:problem:constraint";

/// The database refused a transaction because of concurrency, not breakage.
///
/// Deadlock detected, serialization failure. Named `contention` rather than
/// `serialization` on purpose: in a Rust codebase the latter reads as a serde
/// failure, and this is the opposite end of the system.
///
/// Worth its own group because it is *expected* under load and the retry is the
/// correct response. Charted next to connection failures it looks like an
/// outage; charted on its own it looks like what it is.
pub const CONTENTION: &str = "urn:kafkaman:problem:contention";

/// The statement itself could not run as written, by this role.
///
/// Undefined table or column, bad cast, numeric overflow, insufficient
/// privilege. A code or deployment fault rather than an environment one, and
/// deterministic until someone changes the schema, the query, or the grant.
pub const STATEMENT: &str = "urn:kafkaman:problem:statement";

/// Every URI above, in declaration order.
///
/// The list exists so a test can assert that no implementation invented a URI
/// outside it. Compiler exhaustiveness catches a variant nobody classified;
/// only this catches a variant classified with a typo.
pub const ALL_PROBLEM_TYPES: [&str; 18] = [
    MISSING_HANDLER,
    INVALID_PAYLOAD,
    INFRASTRUCTURE,
    HANDLER,
    HANDLER_PANICKED,
    APPLICATION_PANICKED,
    CONFIGURATION,
    SCHEMA,
    MESSAGE_ROUTING,
    CACHE_INVARIANT,
    IDEMPOTENCY,
    TOPIC,
    UNSAFE_REPLAY,
    PUBLISH,
    BREAKER_TRIPPED,
    CONSTRAINT,
    CONTENTION,
    STATEMENT,
];

/// A failure that can name its class to an APM backend.
///
/// Implemented by every kafkaman error enum, and by
/// [`ReceivedFailureKind`](crate::ReceivedFailureKind). Implementations are
/// hand-written `match` blocks rather than a derive, for the reason
/// `discriminant_enum!` gives: a URI is data beyond the variant's own name, and
/// it belongs next to the variant so that both are read together.
pub trait ProblemType {
    /// The permanent RFC 9457 `type` URI for this failure.
    ///
    /// Must be one of the constants in this module.
    fn problem_type(&self) -> &'static str;

    /// The persisted class this failure stores as.
    ///
    /// Provided, and deliberately not overridable in practice: it is the same
    /// classification as [`Self::problem_type`] read at the resolution a row
    /// can hold. Deriving it here rather than at each recording site is what
    /// stops the two vocabularies from being computed independently and
    /// disagreeing — which is exactly what they did before, with a database
    /// error reading `infrastructure` in APM and `handler` in the dead-letter
    /// queue for one failure.
    fn failure_kind(&self) -> crate::ReceivedFailureKind {
        crate::ReceivedFailureKind::coarsening(self.problem_type())
    }
}

impl<T: ProblemType + ?Sized> ProblemType for &T {
    fn problem_type(&self) -> &'static str {
        (**self).problem_type()
    }
}
