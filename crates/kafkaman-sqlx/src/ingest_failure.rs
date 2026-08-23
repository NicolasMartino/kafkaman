use kafkaman_core::ReceivedIngestFailureKind;
use time::OffsetDateTime;

/// A Kafka record that could not become a received row, kept for triage.
///
/// Quarantining rather than stalling is the point: these failures are properties
/// of the record, so retrying can never change the outcome, and blocking the
/// partition on one unreadable record would stop every later record behind it.
#[derive(Clone, Debug)]
pub struct ReceivedIngestFailure {
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub key: Option<Vec<u8>>,
    pub headers: serde_json::Value,
    pub payload: Option<Vec<u8>>,
    pub message_type: String,
    pub expected_topic: String,
    pub kind: ReceivedIngestFailureKind,
    pub error: String,
}

/// A stored [`ReceivedIngestFailure`], with the timestamp the database assigned.
///
/// Composed rather than restated, so the two shapes cannot drift apart when a
/// column is added.
#[derive(Clone, Debug)]
pub struct ReceivedIngestFailureRow {
    pub failure: ReceivedIngestFailure,
    pub created_at: OffsetDateTime,
}

impl std::ops::Deref for ReceivedIngestFailureRow {
    type Target = ReceivedIngestFailure;

    fn deref(&self) -> &Self::Target {
        &self.failure
    }
}

/// What an insert into a received table did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceivedInsertOutcome {
    Inserted,
    /// The same idempotency key is already stored: an ordinary redelivery, and
    /// exactly what the dedupe index exists to absorb.
    DuplicateIdempotencyKey,
    /// The same `message_id` identifies a *different* logical message. Not a
    /// redelivery — a producer emitting colliding ids — so it is quarantined
    /// rather than silently dropped.
    MessageIdConflict,
}
