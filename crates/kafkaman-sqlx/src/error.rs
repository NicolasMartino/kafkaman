use kafkaman_config::ConfigErrors;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
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
}
