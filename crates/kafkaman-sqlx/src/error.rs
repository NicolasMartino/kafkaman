use kafkaman_config::ConfigErrors;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Core(#[from] kafkaman_core::Error),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Config(#[from] kafkaman_config::ConfigError),

    #[error("invalid kafkaman config: {0}")]
    ConfigErrors(#[from] ConfigErrors),

    #[error("changeset versions must be unique and ordered: duplicate version {0}")]
    DuplicateChangesetVersion(i64),

    #[error("changeset versions must be declared in ascending order: {previous} before {next}")]
    DisorderedChangesetVersion { previous: i64, next: i64 },

    #[error(
        "changeset {version} checksum mismatch: history has {stored}, source declares {current}"
    )]
    ChecksumMismatch {
        version: i64,
        stored: String,
        current: String,
    },

    #[error("invalid replay changeset {version}: {message}")]
    InvalidReplay { version: i64, message: String },

    #[error(
        "outbox replay is unsafe for entity snapshots: republishing a stored row for message type \
         `{message_type}` would emit stale state at a fresh, higher Kafka offset, and every consumer \
         cache would treat that stale state as the newest. Repair by re-reading current entity state \
         and enqueueing it normally (state-sourced republish); use `Replay::received` to redrive \
         inbound rows"
    )]
    UnsafeOutboxReplay { message_type: String },

    #[error("invalid received ingest failure kind `{0}`")]
    InvalidIngestFailureKind(String),

    #[error("invalid received failure filter: {0}")]
    InvalidReceivedFilter(String),

    #[error("migration advisory lock is held by another migration; dry-run skipped")]
    MigrationLockBusy,

    #[error(
        "migrate needs at least 2 pooled connections (one holds the schema advisory lock while \
         the other applies the changesets) but the pool allows {max_connections}"
    )]
    MigrationPoolTooSmall { max_connections: u32 },

    #[error("message descriptor `{0}` is not configured")]
    UnknownMessageType(String),

    #[error(
        "message type `{message_type}` is already registered for topic `{registered}` and cannot \
         be re-registered for topic `{conflicting}`: one message type owns exactly one topic"
    )]
    ConflictingMessageType {
        message_type: String,
        registered: String,
        conflicting: String,
    },

    #[error("envelope header `{0}` is in the reserved `kafkaman-` namespace")]
    ReservedHeader(String),

    #[error("received message must include an idempotency key")]
    MissingIdempotencyKey,

    /// Application-owned code panicked while kafkaman was preparing a row, in
    /// either direction.
    ///
    /// Raised where kafkaman calls code it does not own outside a handler: a
    /// [`KafkaMessage`](kafkaman_core::KafkaMessage) method or a `Serialize`
    /// impl, on the enqueue path and on the ingest path alike.
    ///
    /// Separate from [`Self::HandlerPanicked`] because no handler has run and
    /// there is no row to retry yet. On ingest the incoming record is
    /// quarantined rather than treated as a transient database failure; on
    /// enqueue the error is returned to the caller, which is the application
    /// whose code panicked.
    #[error(
        "application code panicked preparing message `{message_type}` ({operation}): {message}"
    )]
    ApplicationPanicked {
        message_type: String,
        operation: &'static str,
        message: String,
    },

    #[error("entity key for message type `{message_type}` is not valid UTF-8")]
    InvalidEntityKey { message_type: String },

    #[error(
        "received row `{message_id}` for message type `{message_type}` has no resolvable entity key"
    )]
    MissingEntityKey {
        message_id: Uuid,
        message_type: String,
    },

    #[error(
        "cache row for entity `{entity_key}` was applied from {applied_topic}:{applied_partition} \
         but this record arrived on {incoming_topic}:{incoming_partition}; stored offsets are not \
         comparable across a topic or partition change and the cache must be re-bootstrapped"
    )]
    CacheOriginMismatch {
        entity_key: String,
        applied_topic: String,
        applied_partition: i32,
        incoming_topic: String,
        incoming_partition: i32,
    },

    #[error("no handler registered for message type `{0}`")]
    MissingHandler(String),

    #[error("handler failed: {0}")]
    Handler(String),

    /// A handler unwound instead of returning.
    ///
    /// Caught at the call boundary and classified as an ordinary handler
    /// failure, so the row retries on its normal budget and dead-letters rather
    /// than taking the dispatch loop — and with it the process — down. See
    /// `catch_panic`.
    #[error("handler panicked: {0}")]
    HandlerPanicked(String),

    #[error(
        "message type `{message_type}` is already declared as `{existing}` and cannot also be \
         declared as `{added}`: one message type gets one handler position, and \
         `handle_before` plus `handle` is the only legal pair"
    )]
    ConflictingRole {
        message_type: String,
        existing: &'static str,
        added: &'static str,
    },

    #[error(
        "generated changeset band {band} is claimed by both `{first}` and `{second}`: this is a \
         hash collision in kafkaman's changeset numbering, not a mistake in your roles — please \
         report it, and work around it by moving one of the two message types onto a \
         hand-written changelog"
    )]
    ChangesetBandCollision {
        band: i64,
        first: String,
        second: String,
    },
}

impl kafkaman_core::ProblemType for Error {
    /// `Core` delegates rather than collapsing to one URI. `#[error(transparent)]`
    /// already means the wrapped error's message is the only one a caller sees;
    /// its classification should travel the same way, or the wrapper would
    /// relabel an idempotency problem as something vaguer purely because it
    /// crossed a crate boundary.
    fn problem_type(&self) -> &'static str {
        use kafkaman_core::problem;
        match self {
            Self::Core(error) => error.problem_type(),

            Self::Sqlx(error) => sqlx_problem_type(error),
            Self::Serde(_) => problem::INVALID_PAYLOAD,

            Self::Config(_)
            | Self::ConfigErrors(_)
            | Self::InvalidReceivedFilter(_)
            | Self::MigrationPoolTooSmall { .. } => problem::CONFIGURATION,

            Self::DuplicateChangesetVersion(_)
            | Self::DisorderedChangesetVersion { .. }
            | Self::ChecksumMismatch { .. }
            | Self::InvalidReplay { .. }
            | Self::InvalidIngestFailureKind(_)
            | Self::MigrationLockBusy
            | Self::ChangesetBandCollision { .. } => problem::SCHEMA,

            Self::UnsafeOutboxReplay { .. } => problem::UNSAFE_REPLAY,

            Self::UnknownMessageType(_)
            | Self::ConflictingMessageType { .. }
            | Self::ReservedHeader(_)
            | Self::ConflictingRole { .. } => problem::MESSAGE_ROUTING,

            Self::MissingIdempotencyKey => problem::IDEMPOTENCY,

            Self::ApplicationPanicked { .. } => problem::APPLICATION_PANICKED,

            // Terminal by construction: `handler_failure_disposition` refuses to
            // retry these three, because a second attempt reaches the same
            // conclusion about the same stored row.
            Self::InvalidEntityKey { .. }
            | Self::MissingEntityKey { .. }
            | Self::CacheOriginMismatch { .. } => problem::CACHE_INVARIANT,

            Self::MissingHandler(_) => problem::MISSING_HANDLER,
            Self::Handler(_) => problem::HANDLER,
            Self::HandlerPanicked(_) => problem::HANDLER_PANICKED,
        }
    }
}

/// Classify a database error by what the database actually refused.
///
/// `Error::Sqlx` wraps every `sqlx::Error`, so a closed connection pool and a
/// unique-violation used to be one class — and the most common way a handler
/// fails in production is its own query, which meant the largest bucket in APM
/// was also the least informative. All of these still coarsen to
/// `Infrastructure` on the row, because none of the four stored kinds fits a
/// constraint violation better; the split is what an APM error group is built
/// on.
///
/// Postgres SQLSTATE classes, because kafkaman is Postgres-only. `kind()` is
/// asked first where it answers: it names the four constraint kinds portably
/// and in sqlx's own vocabulary, so that table is not duplicated here. It
/// returns `Other` for everything else, which is why the classes are still
/// needed.
fn sqlx_problem_type(error: &sqlx::Error) -> &'static str {
    use kafkaman_core::problem;

    let sqlx::Error::Database(database) = error else {
        // `PoolTimedOut`, `PoolClosed`, `Io`, `Tls`, `Protocol`,
        // `WorkerCrashed`: the connection or the driver, never the statement.
        return problem::INFRASTRUCTURE;
    };

    if !matches!(database.kind(), sqlx::error::ErrorKind::Other) {
        return problem::CONSTRAINT;
    }

    match database.code().as_deref().and_then(|code| code.get(..2)) {
        // 23 — integrity constraint violation. Reached for the ones `kind()`
        // does not name, such as an exclusion violation.
        Some("23") => problem::CONSTRAINT,
        // 40 — transaction rollback: deadlock detected, serialization failure.
        Some("40") => problem::CONTENTION,
        // 22 — data exception (bad cast, numeric overflow, division by zero).
        // 42 — syntax error or access rule violation, which is also where
        // `insufficient_privilege` lives: both mean this statement cannot run as
        // written, by this role.
        Some("22" | "42") => problem::STATEMENT,
        // 08 connection, 53 insufficient resources, 57 operator intervention,
        // 58 system error, and anything unrecognised. The environment.
        _ => problem::INFRASTRUCTURE,
    }
}
