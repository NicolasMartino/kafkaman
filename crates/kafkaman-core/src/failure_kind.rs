use crate::enum_macros::discriminant_enum;
use crate::problem::{self, ProblemType};

discriminant_enum! {
    /// Why a dispatch attempt failed.
    ///
    /// Two representations are stored, deliberately decoupled. The RFC 9457
    /// `type` URI from [`problem_type`](Self::problem_type) goes into the
    /// `errors` audit JSON; the bare variant name from
    /// [`discriminant`](Self::discriminant) goes into the `last_failure_kind`
    /// column that filters and CHECK constraints read. Keeping them separate
    /// lets the audit format evolve without rewriting migration history, which
    /// reads the discriminant.
    #[derive(Default)]
    pub enum ReceivedFailureKind {
        MissingHandler,
        InvalidPayload,
        Infrastructure,
        #[default]
        Handler,
    }
}

impl ReceivedFailureKind {
    /// Stable RFC 9457 `type` URI identifying this failure class.
    ///
    /// These are permanent identifiers: they are written into every stored
    /// problem detail and may be matched by consumers of a DLQ inspection API,
    /// so a variant's URI must not change once released.
    /// Only four of the URIs in [`crate::problem`] appear here, and that is the
    /// point: these are persisted in stored problem details, so this set cannot
    /// grow without rewriting history. Finer distinctions an APM view wants —
    /// a panic against a returned error, say — are carried by the error's own
    /// [`ProblemType`] impl instead.
    pub const fn problem_type(self) -> &'static str {
        match self {
            Self::MissingHandler => problem::MISSING_HANDLER,
            Self::InvalidPayload => problem::INVALID_PAYLOAD,
            Self::Infrastructure => problem::INFRASTRUCTURE,
            Self::Handler => problem::HANDLER,
        }
    }

    /// Short human-readable RFC 9457 `title` summarizing this failure class.
    pub const fn title(self) -> &'static str {
        match self {
            Self::MissingHandler => "No handler registered for message type",
            Self::InvalidPayload => "Message payload could not be decoded",
            Self::Infrastructure => "Infrastructure failure during dispatch",
            Self::Handler => "Handler returned an error",
        }
    }

    /// The persisted class for any telemetry problem type.
    ///
    /// The single, total, many-to-one relationship between the fifteen-value
    /// vocabulary an APM backend groups on and the four values a row stores.
    /// It lives here, next to both, because the previous arrangement had two
    /// independent hand-written classifiers over the same error enums and
    /// nothing checking that they agreed — which is how one failure came to read
    /// `infrastructure` in APM and `handler` in the dead-letter queue.
    ///
    /// **Not** [`from_problem_type`](Self::from_problem_type), which is the
    /// exact inverse over the four URIs that *are* kinds and is what reads
    /// stored rows back. This one is lossy on purpose: eleven URIs collapse onto
    /// `Infrastructure`, which is acceptable only because the fine value is on
    /// the span and in APM. The two agree on their overlap, which is tested.
    ///
    /// An unrecognised URI is `Infrastructure` rather than `Handler`: a class
    /// this version does not know cannot be attributed to application code.
    pub fn coarsening(problem_type: &str) -> Self {
        match problem_type {
            problem::MISSING_HANDLER => Self::MissingHandler,

            // The payload is unreadable however it was discovered.
            problem::INVALID_PAYLOAD => Self::InvalidPayload,

            // The two handler URIs are one stored kind. This is the refinement
            // the finer vocabulary exists for: a panic and a returned error
            // group separately in APM and dead-letter identically, because the
            // repair is the same — read the handler.
            problem::HANDLER | problem::HANDLER_PANICKED => Self::Handler,

            // Everything else is the environment, spelled out rather than left
            // to the catch-all so that adding a URI forces a decision here
            // rather than silently landing in this bucket.
            problem::INFRASTRUCTURE
            | problem::APPLICATION_PANICKED
            | problem::CONFIGURATION
            | problem::SCHEMA
            | problem::MESSAGE_ROUTING
            | problem::CACHE_INVARIANT
            | problem::IDEMPOTENCY
            | problem::TOPIC
            | problem::UNSAFE_REPLAY
            | problem::PUBLISH
            | problem::BREAKER_TRIPPED
            // The three SQL refinements coarsen here too. None of the four
            // stored kinds fits a constraint violation better: it is not the
            // handler's own error, not an undecodable payload, and not a
            // missing handler. The finer value is what an APM group is built
            // on; the row keeps the class it can hold.
            | problem::CONSTRAINT
            | problem::CONTENTION
            | problem::STATEMENT => Self::Infrastructure,

            _ => Self::Infrastructure,
        }
    }

    /// Recover a kind from a stored value.
    ///
    /// Accepts the RFC 9457 `type` URI written by [`Self::problem_type`] and,
    /// for rows written before the problem-detail format, the bare variant name.
    pub fn from_problem_type(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.problem_type() == value || kind.discriminant() == value)
    }
}

discriminant_enum! {
    /// Why a Kafka record could not become a received row at all.
    ///
    /// Distinct from [`ReceivedFailureKind`], which covers a row that *was*
    /// stored and then failed to dispatch. These describe records that never got
    /// that far and are quarantined in the ingest failure table instead.
    ///
    /// The discriminants persisted in that table's `failure_kind` column are the
    /// variant names above. They used to be a hand-written pair of `match`
    /// blocks in `kafkaman-sqlx`, one crate away from the variants they
    /// enumerated — so adding a variant compiled cleanly and failed at runtime,
    /// on the quarantine path, which is already the path nobody is watching.
    pub enum ReceivedIngestFailureKind {
        MissingPayload,
        MissingIdempotencyKey,
        InvalidPayload,
        InvalidHeader,
        UnexpectedTopic,
        MessageIdConflict,
    }
}

impl ReceivedIngestFailureKind {
    /// Recover a kind from a stored discriminant.
    pub fn from_discriminant(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.discriminant() == value)
    }
}

impl ProblemType for ReceivedFailureKind {
    fn problem_type(&self) -> &'static str {
        (*self).problem_type()
    }
}

impl ProblemType for ReceivedIngestFailureKind {
    /// Ingest quarantine classes reuse the dispatch vocabulary rather than
    /// growing one of their own: an operator asking "what is wrong with this
    /// message" wants the same answer whether it failed before or after it
    /// became a row.
    fn problem_type(&self) -> &'static str {
        match self {
            Self::MissingPayload | Self::InvalidPayload | Self::InvalidHeader => {
                problem::INVALID_PAYLOAD
            }
            Self::MissingIdempotencyKey | Self::MessageIdConflict => problem::IDEMPOTENCY,
            Self::UnexpectedTopic => problem::MESSAGE_ROUTING,
        }
    }
}

/// Which part of a dispatch produced a failure.
///
/// The *blame* axis, kept separate from [`ReceivedFailureKind`], which is the
/// taxonomy axis. They answer different questions — "whose code produced this"
/// against "what kind of failure was it" — and they were one field until a
/// database error returned by a handler was found reading `infrastructure` on
/// its span and `handler` in its row.
///
/// Three variants because a dispatch has three places it can fail, and they send
/// an operator to three different people:
///
/// - `Routing` — before any handler ran. No handler was registered for the
///   type, so nobody's handler is at fault; a deployment is.
/// - `Handler` — inside application code. Whatever the taxonomy says about the
///   error, this is the frame that raised it.
/// - `Bookkeeping` — kafkaman's own claim, savepoint, or commit. A library fault
///   or the database under it.
///
/// Not built with [`discriminant_enum!`](crate::enum_macros), whose contract is
/// that the variant name *is* the persisted string. Here it is not: the stored
/// and exported spelling is lowercase, because it was already exported as the
/// `kafkaman.failure.stage` span attribute before it was ever persisted, and a
/// trace and a row must not disagree about a stage's name.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailureStage {
    Routing,
    Handler,
    Bookkeeping,
}

impl FailureStage {
    /// Every variant, in declaration order.
    pub const ALL: [Self; 3] = [Self::Routing, Self::Handler, Self::Bookkeeping];

    /// The lowercase spelling used on spans and in stored problem details.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Routing => "routing",
            Self::Handler => "handler",
            Self::Bookkeeping => "bookkeeping",
        }
    }

    /// Recover a stage from its stored spelling.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|stage| stage.as_str() == value)
    }
}
