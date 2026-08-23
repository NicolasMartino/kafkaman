use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{IdempotencyKey, OutboxStatus, ReceiveStatus, ReceivedFailureKind};

/// One failure in a receive row's audit trail, shaped as an RFC 9457 problem
/// detail object.
///
/// This is persisted in the `errors` JSONB column as an audit trail only. No
/// SQL parses it: DLQ filtering and ordering read the `last_failed_at` and
/// `last_failure_kind` columns, which keeps this representation free to evolve
/// without breaking queries and lets it carry annotated RFC 9557 timestamps
/// that PostgreSQL could not cast.
///
/// RFC 9457's `status` member is omitted: it is defined as an HTTP status code
/// and has no meaning for a Kafka dispatch failure. `occurred_at` is an
/// extension member, which RFC 9457 permits.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedError {
    /// RFC 9457 `type`. Reads also accept the pre-problem-detail `kind` field.
    #[serde(rename = "type", alias = "kind", with = "crate::problem_type", default)]
    pub kind: ReceivedFailureKind,
    /// RFC 9457 `title`: human-readable summary of the failure class.
    #[serde(default)]
    pub title: String,
    /// RFC 9457 `detail`: explanation specific to this occurrence.
    #[serde(alias = "message")]
    pub detail: String,
    /// RFC 9457 extension member carrying an RFC 9557 timestamp.
    #[serde(with = "crate::rfc9557")]
    pub occurred_at: OffsetDateTime,
}

impl ReceivedError {
    /// Build a problem detail for `kind`, filling `title` from the failure class.
    pub fn new(
        kind: ReceivedFailureKind,
        detail: impl Into<String>,
        occurred_at: OffsetDateTime,
    ) -> Self {
        Self {
            kind,
            title: kind.title().to_owned(),
            detail: detail.into(),
            occurred_at,
        }
    }
}

/// A message that arrived, stored durably before anything acted on it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedRow {
    pub message_id: Uuid,
    pub idempotency_key: IdempotencyKey,
    pub idempotency_source: Option<serde_json::Value>,
    /// Convergence identity for the cache, resolved from the typed payload at
    /// ingest time. Stored as a real column rather than recovered from a header
    /// so cache application never depends on reserved metadata surviving a
    /// broker round trip. `None` only for rows written before this column
    /// existed.
    pub entity_key: Option<String>,
    pub status: ReceiveStatus,
    pub attempts: i32,
    pub next_attempt_at: Option<OffsetDateTime>,
    pub errors: Vec<ReceivedError>,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub key: Option<Vec<u8>>,
    pub message_type: String,
    pub message_version: i32,
    pub headers: BTreeMap<String, String>,
    pub payload: serde_json::Value,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub occurred_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub processed_at: Option<OffsetDateTime>,
}

/// Read-only message metadata handed to a dispatch handler alongside the
/// deserialized payload. Carries the identity, routing, and provenance fields
/// persisted on [`ReceivedRow`] so handlers can correlate, trace, and inspect
/// delivery state without re-querying the received table. `attempts` reflects
/// the count at claim time, i.e. the number of prior failed dispatches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedMeta {
    pub message_id: Uuid,
    pub idempotency_key: IdempotencyKey,
    pub idempotency_source: Option<serde_json::Value>,
    /// The entity this message is a snapshot of. See [`ReceivedRow::entity_key`].
    pub entity_key: Option<String>,
    pub message_type: String,
    pub message_version: i32,
    pub attempts: i32,
    pub headers: BTreeMap<String, String>,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub key: Option<Vec<u8>>,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub occurred_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
}

impl From<&ReceivedRow> for ReceivedMeta {
    fn from(row: &ReceivedRow) -> Self {
        Self {
            message_id: row.message_id,
            idempotency_key: row.idempotency_key,
            idempotency_source: row.idempotency_source.clone(),
            entity_key: row.entity_key.clone(),
            message_type: row.message_type.clone(),
            message_version: row.message_version,
            attempts: row.attempts,
            headers: row.headers.clone(),
            source_topic: row.source_topic.clone(),
            source_partition: row.source_partition,
            source_offset: row.source_offset,
            key: row.key.clone(),
            correlation_id: row.correlation_id,
            causation_id: row.causation_id,
            occurred_at: row.occurred_at,
            created_at: row.created_at,
        }
    }
}

/// A message waiting to reach the broker, written in the same transaction as
/// the business change that produced it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxRow {
    pub message_id: Uuid,
    pub idempotency_key: Option<IdempotencyKey>,
    pub idempotency_source: Option<serde_json::Value>,
    pub status: OutboxStatus,
    pub attempts: i32,
    pub next_attempt_at: OffsetDateTime,
    pub last_error: Option<String>,
    pub claim_id: Option<Uuid>,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<OffsetDateTime>,
    pub topic: String,
    pub partition_key: Option<String>,
    pub entity_key: Option<String>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub headers: BTreeMap<String, String>,
    pub payload: serde_json::Value,
    pub occurred_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub published_at: Option<OffsetDateTime>,
}

impl OutboxRow {
    /// The key this row is published under.
    ///
    /// It is the declared `partition_key`, falling back to `entity_key` when the
    /// type declares none. The fallback is load-bearing, not a convenience:
    /// Kafka routes keyless records round-robin, while the cache convergence
    /// guard compares offsets only within a single topic and partition. A
    /// keyless entity type would therefore scatter one entity's snapshots across
    /// partitions and could never converge.
    pub fn record_key(&self) -> Option<&str> {
        self.partition_key.as_deref().or(self.entity_key.as_deref())
    }
}

/// An outbox row a worker has taken, with the claim generation that authorizes
/// it to report the outcome.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimedOutboxRow {
    pub row: OutboxRow,
    pub claim_id: Uuid,
}

impl ClaimedOutboxRow {
    pub fn message_id(&self) -> Uuid {
        self.row.message_id
    }
}

/// Whether a mark took effect, and if not, why.
///
/// `StaleClaim` and `Missing` are distinct because they mean different things to
/// an operator: a stale claim means another worker owns the row and will finish
/// it, while a missing row means the work is not coming back and nothing else
/// will report that.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MarkOutcome {
    Updated,
    StaleClaim,
    Missing,
}

/// Where a published record landed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishAck {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// A record as a test publisher captured it, for assertions about what would
/// have gone on the wire.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishedRecord {
    pub topic: String,
    pub key: Option<String>,
    pub payload: serde_json::Value,
    pub headers: BTreeMap<String, String>,
    pub message_id: Uuid,
}
