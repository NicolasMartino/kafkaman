use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
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

    #[error("invalid idempotency namespace `{value}`: {reason}")]
    InvalidIdempotencyNamespace { value: String, reason: &'static str },

    #[error("invalid idempotency key `{value}`: {reason}")]
    InvalidIdempotencyKey { value: String, reason: &'static str },

    #[error("invalid idempotency source: {0}")]
    InvalidIdempotencySource(String),
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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey([u8; 32]);

pub const LEGACY_STRING_IDEMPOTENCY_NAMESPACE: &str = "kafkaman:legacy-string:v1";

impl IdempotencyKey {
    pub const HEX_LEN: usize = 64;

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(value: &str) -> Result<Self> {
        if value.len() != Self::HEX_LEN {
            return Err(Error::InvalidIdempotencyKey {
                value: value.to_owned(),
                reason: "must be 64 hexadecimal characters",
            });
        }

        let mut bytes = [0_u8; 32];
        let raw = value.as_bytes();
        for index in 0..32 {
            let high =
                decode_hex_nibble(raw[index * 2]).ok_or_else(|| Error::InvalidIdempotencyKey {
                    value: value.to_owned(),
                    reason: "must contain only hexadecimal characters",
                })?;
            let low = decode_hex_nibble(raw[index * 2 + 1]).ok_or_else(|| {
                Error::InvalidIdempotencyKey {
                    value: value.to_owned(),
                    reason: "must contain only hexadecimal characters",
                }
            })?;
            bytes[index] = (high << 4) | low;
        }

        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(Self::HEX_LEN);
        for byte in self.0 {
            output.push(char::from_digit(u32::from(byte >> 4), 16).expect("nibble is < 16"));
            output.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("nibble is < 16"));
        }
        output
    }
}

impl PartialEq<&str> for IdempotencyKey {
    fn eq(&self, other: &&str) -> bool {
        IdempotencyIdentity::derive_legacy_string(*other)
            .map(|identity| identity.key == *self)
            .unwrap_or(false)
    }
}

impl PartialEq<IdempotencyKey> for &str {
    fn eq(&self, other: &IdempotencyKey) -> bool {
        other == self
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for IdempotencyKey {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::from_hex(value)
    }
}

impl Serialize for IdempotencyKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for IdempotencyKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdempotencySource(serde_json::Value);

impl IdempotencySource {
    pub fn new(source: impl Serialize) -> Result<Self> {
        let value = serde_json::to_value(source)
            .map_err(|err| Error::InvalidIdempotencySource(err.to_string()))?;
        Ok(Self(value))
    }

    pub fn value(&self) -> &serde_json::Value {
        &self.0
    }

    pub fn into_value(self) -> serde_json::Value {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyIdentity {
    pub key: IdempotencyKey,
    pub source: Option<IdempotencySource>,
}

impl IdempotencyIdentity {
    pub fn derive(namespace: impl AsRef<str>, source: impl Serialize) -> Result<Self> {
        let namespace = namespace.as_ref();
        if namespace.trim().is_empty() {
            return Err(Error::InvalidIdempotencyNamespace {
                value: namespace.to_owned(),
                reason: "must not be empty or whitespace",
            });
        }
        let source = IdempotencySource::new(source)?;
        let canonical_source = serde_json::to_vec(source.value())
            .map_err(|err| Error::InvalidIdempotencySource(err.to_string()))?;

        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0_u8]);
        hasher.update(canonical_source);
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&digest);

        Ok(Self {
            key: IdempotencyKey::from_bytes(bytes),
            source: Some(source),
        })
    }

    pub fn from_parts(key: IdempotencyKey, source: IdempotencySource) -> Self {
        Self {
            key,
            source: Some(source),
        }
    }

    pub fn from_key(key: IdempotencyKey) -> Self {
        Self { key, source: None }
    }

    pub fn derive_legacy_string(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.trim().is_empty() {
            return Err(Error::InvalidIdempotencySource(
                "legacy string source must not be empty or whitespace".to_owned(),
            ));
        }
        Self::derive(LEGACY_STRING_IDEMPOTENCY_NAMESPACE, value)
    }
}

pub trait IntoIdempotencyIdentity {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity>;
}

impl IntoIdempotencyIdentity for IdempotencyIdentity {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        Ok(self)
    }
}

impl IntoIdempotencyIdentity for &str {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        IdempotencyIdentity::derive_legacy_string(self)
    }
}

impl IntoIdempotencyIdentity for String {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        IdempotencyIdentity::derive_legacy_string(self)
    }
}

impl IntoIdempotencyIdentity for &String {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        IdempotencyIdentity::derive_legacy_string(self)
    }
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub message_id: Uuid,
    pub idempotency_key: Option<IdempotencyIdentity>,
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

    pub fn with_idempotency_key(mut self, idempotency_key: impl IntoIdempotencyIdentity) -> Self {
        self.idempotency_key = Some(
            idempotency_key
                .into_idempotency_identity()
                .expect("invalid idempotency identity"),
        );
        self
    }

    pub fn try_with_idempotency_key(
        mut self,
        idempotency_key: impl IntoIdempotencyIdentity,
    ) -> Result<Self> {
        self.idempotency_key = Some(idempotency_key.into_idempotency_identity()?);
        Ok(self)
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

    /// The canonical discriminant persisted in the `last_failure_kind` column
    /// and used in redrive/inspect filters and replay checksums.
    ///
    /// These strings are load-bearing for changeset checksums and must stay
    /// stable. They match the variant names serde writes, so a value stored by
    /// an older build still compares equal.
    pub const fn discriminant(self) -> &'static str {
        match self {
            Self::MissingHandler => "MissingHandler",
            Self::InvalidPayload => "InvalidPayload",
            Self::Infrastructure => "Infrastructure",
            Self::Handler => "Handler",
        }
    }

    /// Recover a kind from a stored discriminant.
    ///
    /// Accepts the RFC 9457 `type` URI written by [`Self::problem_type`] and,
    /// for rows written before the problem-detail format, the bare variant name.
    pub fn from_problem_type(value: &str) -> Option<Self> {
        match value {
            "urn:kafkaman:problem:missing-handler" | "MissingHandler" => Some(Self::MissingHandler),
            "urn:kafkaman:problem:invalid-payload" | "InvalidPayload" => Some(Self::InvalidPayload),
            "urn:kafkaman:problem:infrastructure" | "Infrastructure" => Some(Self::Infrastructure),
            "urn:kafkaman:problem:handler" | "Handler" => Some(Self::Handler),
            _ => None,
        }
    }
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

/// Serde support for RFC 9557 (IXDTF) timestamps.
///
/// RFC 9557 extends RFC 3339 with a bracketed annotation suffix. kafkaman
/// normalizes to UTC before formatting, so it always emits `[UTC]`. Parsing
/// tolerates a missing annotation, so plain RFC 3339 values — which RFC 9557
/// defines as valid IXDTF — and values from foreign producers both round-trip.
///
/// Note that PostgreSQL cannot cast an annotated timestamp to `timestamptz`.
/// This format is therefore only ever used inside audit JSON that SQL does not
/// parse; queries filter and order on real `timestamptz` columns instead.
pub mod rfc9557 {
    use serde::de::Error as _;
    use serde::ser::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};
    use time::format_description::well_known::Rfc3339;
    use time::{OffsetDateTime, UtcOffset};

    /// Render `value` as an RFC 9557 timestamp annotated `[UTC]`.
    pub fn render(value: OffsetDateTime) -> Result<String, time::error::Format> {
        let utc = value.to_offset(UtcOffset::UTC);
        Ok(format!("{}[UTC]", utc.format(&Rfc3339)?))
    }

    /// Parse an RFC 9557 timestamp, tolerating an absent annotation.
    pub fn parse(value: &str) -> Result<OffsetDateTime, time::error::Parse> {
        let base = value.split_once('[').map_or(value, |(base, _)| base);
        OffsetDateTime::parse(base, &Rfc3339)
    }

    pub fn serialize<S>(value: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&render(*value).map_err(S::Error::custom)?)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        parse(&raw).map_err(D::Error::custom)
    }
}

/// Serde support for [`ReceivedFailureKind`] as an RFC 9457 `type` URI.
mod problem_type {
    use super::ReceivedFailureKind;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(kind: &ReceivedFailureKind, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(kind.problem_type())
    }

    /// An unrecognized `type` degrades to the default kind rather than failing
    /// the read. A stored audit record must stay readable by an older binary
    /// that predates a newer failure class, for the same reason ingest tolerates
    /// unknown enum variants instead of quarantining them.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<ReceivedFailureKind, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(ReceivedFailureKind::from_problem_type(&raw).unwrap_or_default())
    }
}

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
    #[serde(rename = "type", alias = "kind", with = "problem_type", default)]
    pub kind: ReceivedFailureKind,
    /// RFC 9457 `title`: human-readable summary of the failure class.
    #[serde(default)]
    pub title: String,
    /// RFC 9457 `detail`: explanation specific to this occurrence.
    #[serde(alias = "message")]
    pub detail: String,
    /// RFC 9457 extension member carrying an RFC 9557 timestamp.
    #[serde(with = "rfc9557")]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceivedRow {
    pub message_id: Uuid,
    pub idempotency_key: IdempotencyKey,
    pub idempotency_source: Option<serde_json::Value>,
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
    fn renders_rfc9557_annotated_timestamps() {
        let value = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        assert_eq!(rfc9557::render(value).unwrap(), "2023-11-14T22:13:20Z[UTC]");
    }

    #[test]
    fn normalizes_offsets_to_utc_before_annotating() {
        // The annotation must never disagree with the offset it accompanies, so
        // a non-UTC input is converted rather than labelled `[UTC]` in place.
        let value = OffsetDateTime::from_unix_timestamp(1_700_000_000)
            .unwrap()
            .to_offset(time::UtcOffset::from_hms(2, 0, 0).unwrap());
        assert_eq!(rfc9557::render(value).unwrap(), "2023-11-14T22:13:20Z[UTC]");
    }

    #[test]
    fn parses_rfc9557_with_and_without_annotation() {
        let expected = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        // Annotated, as kafkaman writes it.
        assert_eq!(
            rfc9557::parse("2023-11-14T22:13:20Z[UTC]").unwrap(),
            expected
        );
        // Bare RFC 3339, which RFC 9557 defines as valid IXDTF.
        assert_eq!(rfc9557::parse("2023-11-14T22:13:20Z").unwrap(), expected);
        // A foreign producer's zone annotation, with a non-UTC offset.
        assert_eq!(
            rfc9557::parse("2023-11-15T00:13:20+02:00[Europe/Berlin]").unwrap(),
            expected
        );
    }

    #[test]
    fn received_error_serializes_as_an_rfc9457_problem_detail() {
        let occurred_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let error = ReceivedError::new(ReceivedFailureKind::Handler, "boom", occurred_at);
        let json = serde_json::to_value(&error).unwrap();

        assert_eq!(json["type"], "urn:kafkaman:problem:handler");
        assert_eq!(json["title"], "Handler returned an error");
        assert_eq!(json["detail"], "boom");
        assert_eq!(json["occurred_at"], "2023-11-14T22:13:20Z[UTC]");
        // RFC 9457's `status` is an HTTP status code and is deliberately absent.
        assert!(json.get("status").is_none());
    }

    #[test]
    fn received_error_reads_pre_problem_detail_rows() {
        // Rows written before the problem-detail format used `kind`/`message`
        // with a bare RFC 3339 timestamp. They must stay readable.
        let legacy = serde_json::json!({
            "kind": "InvalidPayload",
            "message": "could not decode",
            "occurred_at": "2023-11-14T22:13:20Z",
        });
        let error: ReceivedError = serde_json::from_value(legacy).unwrap();

        assert_eq!(error.kind, ReceivedFailureKind::InvalidPayload);
        assert_eq!(error.detail, "could not decode");
        assert_eq!(
            error.occurred_at,
            OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
        );
    }

    #[test]
    fn unknown_problem_type_degrades_to_the_default_kind() {
        // A newer build may write a failure class this binary does not know.
        // Reading the audit trail must not fail because of it.
        let forward = serde_json::json!({
            "type": "urn:kafkaman:problem:not-yet-invented",
            "title": "Something new",
            "detail": "boom",
            "occurred_at": "2023-11-14T22:13:20Z[UTC]",
        });
        let error: ReceivedError = serde_json::from_value(forward).unwrap();
        assert_eq!(error.kind, ReceivedFailureKind::default());
    }

    #[test]
    fn failure_kind_discriminants_are_stable() {
        // These strings are persisted in `last_failure_kind` and feed replay
        // checksums; changing one silently invalidates migration history.
        assert_eq!(
            ReceivedFailureKind::MissingHandler.discriminant(),
            "MissingHandler"
        );
        assert_eq!(
            ReceivedFailureKind::InvalidPayload.discriminant(),
            "InvalidPayload"
        );
        assert_eq!(
            ReceivedFailureKind::Infrastructure.discriminant(),
            "Infrastructure"
        );
        assert_eq!(ReceivedFailureKind::Handler.discriminant(), "Handler");
    }

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
    fn idempotency_identity_is_deterministic_and_retains_source() {
        let first = IdempotencyIdentity::derive(
            "OrderCreated:v1",
            serde_json::json!({ "order_id": "order-1" }),
        )
        .unwrap();
        let second = IdempotencyIdentity::derive(
            "OrderCreated:v1",
            serde_json::json!({ "order_id": "order-1" }),
        )
        .unwrap();
        let different = IdempotencyIdentity::derive(
            "OrderCreated:v1",
            serde_json::json!({ "order_id": "order-2" }),
        )
        .unwrap();

        assert_eq!(first.key, second.key);
        assert_ne!(first.key, different.key);
        assert_eq!(first.key.to_string().len(), IdempotencyKey::HEX_LEN);
        assert_eq!(
            first.source.as_ref().map(IdempotencySource::value),
            Some(&serde_json::json!({ "order_id": "order-1" }))
        );
    }

    #[test]
    fn idempotency_key_rejects_invalid_hex() {
        assert!(IdempotencyKey::from_hex("").is_err());
        assert!(IdempotencyKey::from_hex("not-a-digest").is_err());
        assert!(IdempotencyKey::from_hex(
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        )
        .is_err());
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
