/// Every way a value in this crate can fail to be constructed.
///
/// All of these are validation failures on caller-supplied data, reported so the
/// caller can fix the input. The crate performs no I/O, so there is nothing
/// transient here and nothing worth retrying.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid SQL identifier `{value}`: {reason}")]
    InvalidIdentifier { value: String, reason: &'static str },

    #[error("invalid outbox status `{0}`")]
    InvalidOutboxStatus(String),

    #[error("invalid receive status `{0}`")]
    InvalidReceiveStatus(String),

    #[error("message descriptor is invalid: {0}")]
    InvalidMessageDescriptor(String),

    #[error("invalid idempotency namespace `{value}`: {reason}")]
    InvalidIdempotencyNamespace { value: String, reason: &'static str },

    #[error("invalid idempotency key `{value}`: {reason}")]
    InvalidIdempotencyKey { value: String, reason: &'static str },

    #[error("invalid idempotency source: {0}")]
    InvalidIdempotencySource(String),

    #[error("invalid relay config: {field} {reason}")]
    InvalidRelayConfig {
        field: &'static str,
        reason: &'static str,
    },

    #[error("invalid dispatcher config: {field} {reason}")]
    InvalidDispatcherConfig {
        field: &'static str,
        reason: &'static str,
    },

    #[error("invalid purge config: {field} {reason}")]
    InvalidPurgeConfig {
        field: &'static str,
        reason: &'static str,
    },

    #[error("invalid topic spec: {field} {reason}")]
    InvalidTopicSpec {
        field: &'static str,
        reason: &'static str,
    },

    #[error(
        "topic `{topic}` must be configured `cleanup.policy={expected}` but the broker reports \
         `{found}`: an entity snapshot topic that is not compacted cannot rebuild an entity from \
         the log"
    )]
    TopicPolicyMismatch {
        topic: String,
        expected: String,
        found: String,
    },

    #[error("topic `{topic}` does not exist")]
    TopicMissing { topic: String },

    #[error(
        "creating topic `{topic}` requires an explicit partition count: kafkaman will not guess \
         one, because changing it later means republishing every entity onto a new topic"
    )]
    TopicPartitionsUndeclared { topic: String },
}

impl crate::ProblemType for Error {
    /// Every variant here is a validation failure, so none of them is
    /// [`INFRASTRUCTURE`](crate::problem::INFRASTRUCTURE) — retrying any of
    /// them recomputes the same answer. The split that matters is *whose*
    /// input was wrong: a deployment's configuration, a stored value, a message
    /// type's declaration, or a broker topic.
    fn problem_type(&self) -> &'static str {
        use crate::problem;
        match self {
            Self::InvalidIdentifier { .. }
            | Self::InvalidRelayConfig { .. }
            | Self::InvalidDispatcherConfig { .. }
            | Self::InvalidPurgeConfig { .. }
            | Self::InvalidTopicSpec { .. } => problem::CONFIGURATION,

            Self::InvalidOutboxStatus(_) | Self::InvalidReceiveStatus(_) => problem::SCHEMA,

            Self::InvalidMessageDescriptor(_) => problem::MESSAGE_ROUTING,

            Self::InvalidIdempotencyNamespace { .. }
            | Self::InvalidIdempotencyKey { .. }
            | Self::InvalidIdempotencySource(_) => problem::IDEMPOTENCY,

            Self::TopicPolicyMismatch { .. }
            | Self::TopicMissing { .. }
            | Self::TopicPartitionsUndeclared { .. } => problem::TOPIC,
        }
    }
}
