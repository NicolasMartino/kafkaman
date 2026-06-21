use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    pub const MAX_LEN: usize = 63;

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }
}

impl fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for SqlIdentifier {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl FromStr for SqlIdentifier {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidIdentifier {
            value: value.to_owned(),
            reason: "must not be empty",
        });
    }

    if value.len() > SqlIdentifier::MAX_LEN {
        return Err(Error::InvalidIdentifier {
            value: value.to_owned(),
            reason: "must be at most 63 bytes",
        });
    }

    let mut chars = value.chars();
    let first = chars.next().expect("value is non-empty");
    if !first.is_ascii_lowercase() {
        return Err(Error::InvalidIdentifier {
            value: value.to_owned(),
            reason: "must start with a lowercase ASCII letter",
        });
    }

    if !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return Err(Error::InvalidIdentifier {
            value: value.to_owned(),
            reason: "may contain only lowercase ASCII letters, digits, and underscores",
        });
    }

    if RESERVED_WORDS.contains(&value) {
        return Err(Error::InvalidIdentifier {
            value: value.to_owned(),
            reason: "must not be a reserved SQL word",
        });
    }

    Ok(())
}

const RESERVED_WORDS: &[&str] = &[
    "all", "alter", "and", "as", "by", "case", "create", "delete", "drop", "from", "group",
    "insert", "into", "join", "limit", "not", "null", "or", "order", "select", "set", "table",
    "type", "update", "where",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageDescriptor {
    pub message_type: SqlIdentifier,
    pub topic: String,
}

impl MessageDescriptor {
    pub fn new(message_type: impl Into<String>, topic: impl Into<String>) -> Result<Self> {
        let topic = topic.into();
        if topic.trim().is_empty() {
            return Err(Error::InvalidMessageDescriptor(
                "topic must not be empty".to_owned(),
            ));
        }

        Ok(Self {
            message_type: SqlIdentifier::new(message_type)?,
            topic,
        })
    }
}

/// Header keys beginning with this prefix are reserved for kafkaman-managed
/// metadata (message id, correlation id, causation id, idempotency key) and may
/// not be set by callers, so user headers can never shadow or spoof them.
pub const RESERVED_HEADER_PREFIX: &str = "kafkaman-";

/// Returns the first envelope header key that intrudes on the reserved
/// `kafkaman-` namespace, if any. Comparison is ASCII case-insensitive, so
/// `Kafkaman-Message-Id` is rejected just like `kafkaman-message-id`.
pub fn reserved_header(headers: &BTreeMap<String, String>) -> Option<&str> {
    let prefix = RESERVED_HEADER_PREFIX.as_bytes();
    headers
        .keys()
        .find(|key| {
            let bytes = key.as_bytes();
            bytes.len() >= prefix.len() && bytes[..prefix.len()].eq_ignore_ascii_case(prefix)
        })
        .map(String::as_str)
}

pub trait KafkaMessage: Serialize {
    const MESSAGE_TYPE: &'static str;
    const TOPIC: &'static str;

    fn partition_key(&self) -> Option<String> {
        None
    }

    fn descriptor() -> Result<MessageDescriptor> {
        MessageDescriptor::new(Self::MESSAGE_TYPE, Self::TOPIC)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub message_id: Uuid,
    pub idempotency_key: Option<String>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub headers: BTreeMap<String, String>,
    pub payload: P,
    pub occurred_at: OffsetDateTime,
}

impl<P> Envelope<P> {
    pub fn new(payload: P) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            idempotency_key: None,
            correlation_id: Uuid::new_v4(),
            causation_id: None,
            headers: BTreeMap::new(),
            payload,
            occurred_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn with_message_id(mut self, message_id: Uuid) -> Self {
        self.message_id = message_id;
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OutboxStatus {
    Pending,
    Publishing,
    Published,
    Failed,
}

impl OutboxStatus {
    /// Every status value, in declaration order. SQL generators build CHECK
    /// constraints and `IN (...)` lists from this so the database can never
    /// drift from the Rust enum.
    pub const ALL: [OutboxStatus; 4] = [
        Self::Pending,
        Self::Publishing,
        Self::Published,
        Self::Failed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Publishing => "Publishing",
            Self::Published => "Published",
            Self::Failed => "Failed",
        }
    }

    /// The status rendered as a single-quoted SQL string literal, e.g.
    /// `'Pending'`. Status names are fixed ASCII identifiers, so this is safe to
    /// interpolate directly into generated SQL.
    pub fn sql_literal(self) -> String {
        format!("'{}'", self.as_str())
    }

    /// Comma-separated SQL literal list of every status, for use in `IN (...)`
    /// expressions and CHECK constraints.
    pub fn sql_literal_list() -> String {
        Self::ALL
            .iter()
            .map(|status| status.sql_literal())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for OutboxStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OutboxStatus {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "Pending" => Ok(Self::Pending),
            "Publishing" => Ok(Self::Publishing),
            "Published" => Ok(Self::Published),
            "Failed" => Ok(Self::Failed),
            other => Err(Error::InvalidOutboxStatus(other.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReceiveStatus {
    Pending,
    Processing,
    Processed,
    Retryable,
    Failed,
}

impl ReceiveStatus {
    /// Every receive status, in declaration order. SQL generators derive CHECK
    /// constraints from this so received tables cannot drift from the Rust enum.
    pub const ALL: [ReceiveStatus; 5] = [
        Self::Pending,
        Self::Processing,
        Self::Processed,
        Self::Retryable,
        Self::Failed,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Processing => "Processing",
            Self::Processed => "Processed",
            Self::Retryable => "Retryable",
            Self::Failed => "Failed",
        }
    }

    pub fn sql_literal(self) -> String {
        format!("'{}'", self.as_str())
    }

    pub fn sql_literal_list() -> String {
        Self::ALL
            .iter()
            .map(|status| status.sql_literal())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for ReceiveStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReceiveStatus {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "Pending" => Ok(Self::Pending),
            "Processing" => Ok(Self::Processing),
            "Processed" => Ok(Self::Processed),
            "Retryable" => Ok(Self::Retryable),
            "Failed" => Ok(Self::Failed),
            other => Err(Error::InvalidReceiveStatus(other.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReceivedFailureKind {
    MissingHandler,
    InvalidPayload,
    Infrastructure,
    #[default]
    Handler,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReceivedIngestFailureKind {
    MissingPayload,
    MissingIdempotencyKey,
    InvalidPayload,
    InvalidHeader,
    UnexpectedTopic,
    MessageIdConflict,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedError {
    #[serde(default)]
    pub kind: ReceivedFailureKind,
    pub message: String,
    pub occurred_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedRow {
    pub message_id: Uuid,
    pub idempotency_key: String,
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
    pub idempotency_key: String,
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
            idempotency_key: row.idempotency_key.clone(),
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxRow {
    pub message_id: Uuid,
    pub idempotency_key: Option<String>,
    pub status: OutboxStatus,
    pub attempts: i32,
    pub next_attempt_at: OffsetDateTime,
    pub last_error: Option<String>,
    pub claim_id: Option<Uuid>,
    pub claimed_by: Option<String>,
    pub claim_expires_at: Option<OffsetDateTime>,
    pub topic: String,
    pub partition_key: Option<String>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub headers: BTreeMap<String, String>,
    pub payload: serde_json::Value,
    pub occurred_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub published_at: Option<OffsetDateTime>,
}

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MarkOutcome {
    Updated,
    StaleClaim,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishAck {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishedRecord {
    pub topic: String,
    pub key: Option<String>,
    pub payload: serde_json::Value,
    pub headers: BTreeMap<String, String>,
    pub message_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct RelayConfig {
    pub worker_id: String,
    pub batch_limit: i64,
    pub lease_for: Duration,
    pub retry_after: Duration,
    pub poll_interval: Duration,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", Uuid::new_v4()),
            batch_limit: 100,
            lease_for: Duration::from_secs(30),
            retry_after: Duration::from_secs(1),
            poll_interval: Duration::from_millis(250),
        }
    }
}

impl RelayConfig {
    /// Reject configurations that would break relay correctness or spin the
    /// worker. A zero lease is the dangerous one: the claim would expire the
    /// instant it is taken, so another worker could reclaim and republish in a
    /// tight loop. `retry_after` may be zero (immediate retry is legitimate).
    pub fn validate(&self) -> Result<(), String> {
        if self.lease_for.is_zero() {
            return Err("lease_for must be greater than zero".to_owned());
        }
        if self.batch_limit <= 0 {
            return Err("batch_limit must be greater than zero".to_owned());
        }
        if self.poll_interval.is_zero() {
            return Err("poll_interval must be greater than zero".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelayStats {
    pub claimed: usize,
    pub published: usize,
    pub failed: usize,
    /// A mark was rejected because the row's claim had been lost to another
    /// worker (lease expired and reclaimed).
    pub stale: usize,
    /// A mark found no row at all (the outbox row was deleted/purged).
    pub missing: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_sql_identifier() {
        assert!(SqlIdentifier::new("order_created").is_ok());
        assert!(SqlIdentifier::new("OrderCreated").is_err());
        assert!(SqlIdentifier::new("type").is_err());
        assert!(SqlIdentifier::new("order-created").is_err());
    }

    #[test]
    fn parses_outbox_status() {
        assert_eq!(
            "Pending".parse::<OutboxStatus>().unwrap(),
            OutboxStatus::Pending
        );
        assert!("Nope".parse::<OutboxStatus>().is_err());
    }

    #[test]
    fn relay_config_rejects_unsafe_durations() {
        let cfg = RelayConfig::default();
        assert!(cfg.validate().is_ok());

        let zero_lease = RelayConfig {
            lease_for: Duration::from_secs(0),
            ..RelayConfig::default()
        };
        assert!(zero_lease.validate().is_err());

        let zero_poll = RelayConfig {
            poll_interval: Duration::from_secs(0),
            ..RelayConfig::default()
        };
        assert!(zero_poll.validate().is_err());
    }

    #[test]
    fn detects_reserved_header_namespace() {
        let mut headers = BTreeMap::new();
        headers.insert("x-trace-id".to_owned(), "abc".to_owned());
        assert!(reserved_header(&headers).is_none());

        headers.insert("Kafkaman-Message-Id".to_owned(), "spoof".to_owned());
        assert_eq!(reserved_header(&headers), Some("Kafkaman-Message-Id"));
    }

    #[test]
    fn status_sql_helpers_stay_aligned_with_enum() {
        for status in OutboxStatus::ALL {
            // The SQL literal, the display string, and the parser must all agree
            // on the same canonical name for every status.
            assert_eq!(status.sql_literal(), format!("'{status}'"));
            assert_eq!(status.as_str().parse::<OutboxStatus>().unwrap(), status);
        }
        assert_eq!(
            OutboxStatus::sql_literal_list(),
            "'Pending', 'Publishing', 'Published', 'Failed'"
        );
    }
}
