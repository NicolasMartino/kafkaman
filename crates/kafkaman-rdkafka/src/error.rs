use kafkaman_core::ReceivedIngestFailureKind;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("delivery failed: {0}")]
    Delivery(String),

    /// An `internal-hooks` observer returned an error.
    ///
    /// Named for what it is in this crate rather than for who uses it: the
    /// feature is `internal-hooks`, the seam is an observer, and the test
    /// vocabulary belongs in `kafkaman-test`. `TestHook` was the last of it left
    /// in a production crate's public error surface.
    #[cfg(feature = "internal-hooks")]
    #[error("ingest observer failed: {0}")]
    Observer(String),

    #[error("Kafka record had no payload")]
    MissingPayload,

    #[error("Kafka record must include kafkaman-idempotency-key")]
    MissingIdempotencyKey,

    #[error("invalid Kafka header `{name}`: {message}")]
    InvalidHeader { name: &'static str, message: String },

    #[error("Kafka record arrived on topic `{actual}` while `{expected}` was expected")]
    UnexpectedTopic {
        expected: &'static str,
        actual: String,
    },

    #[error(
        "consecutive ingest skip limit {limit} reached at partition {partition} offset {offset}"
    )]
    ConsecutiveSkipLimitExceeded {
        limit: usize,
        partition: i32,
        offset: i64,
    },
}

impl Error {
    /// The quarantine class for a failure that is a property of the record
    /// itself, or `None` when the failure is environmental and a redelivery
    /// could still succeed.
    ///
    /// This is the crate's whole poison-record policy, and deliberately one
    /// list rather than two. A separate "is it deterministic?" predicate
    /// alongside a separate "what kind is it?" mapping drifts the moment a
    /// variant is added to one and not the other — and the way that drift
    /// surfaces is a transient broker error filed permanently in the ingest
    /// failure table as `InvalidPayload`, which is a lie a human then has to
    /// debug.
    ///
    /// `Some` means: quarantine the record, acknowledge the offset, and move
    /// the partition on. Retrying cannot change the outcome, so stalling on it
    /// would block every later record behind one that can never be read.
    pub(crate) fn ingest_failure_kind(&self) -> Option<ReceivedIngestFailureKind> {
        match self {
            Error::MissingPayload => Some(ReceivedIngestFailureKind::MissingPayload),
            Error::MissingIdempotencyKey => Some(ReceivedIngestFailureKind::MissingIdempotencyKey),
            Error::Serde(_) => Some(ReceivedIngestFailureKind::InvalidPayload),
            Error::InvalidHeader { .. } => Some(ReceivedIngestFailureKind::InvalidHeader),
            Error::UnexpectedTopic { .. } => Some(ReceivedIngestFailureKind::UnexpectedTopic),
            _ => None,
        }
    }
}
