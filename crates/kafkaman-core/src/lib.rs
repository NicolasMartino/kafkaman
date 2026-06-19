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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OutboxStatus {
    Pending,
    Publishing,
    Published,
    Failed,
}

impl OutboxStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Publishing => "Publishing",
            Self::Published => "Published",
            Self::Failed => "Failed",
        }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboxRow {
    pub message_id: Uuid,
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelayStats {
    pub claimed: usize,
    pub published: usize,
    pub failed: usize,
    pub stale: usize,
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
}
