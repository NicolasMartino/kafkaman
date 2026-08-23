use crate::enum_macros::discriminant_enum;

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
    pub const fn problem_type(self) -> &'static str {
        match self {
            Self::MissingHandler => "urn:kafkaman:problem:missing-handler",
            Self::InvalidPayload => "urn:kafkaman:problem:invalid-payload",
            Self::Infrastructure => "urn:kafkaman:problem:infrastructure",
            Self::Handler => "urn:kafkaman:problem:handler",
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
