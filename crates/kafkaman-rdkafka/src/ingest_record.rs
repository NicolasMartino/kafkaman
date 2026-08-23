use std::collections::BTreeMap;

use kafkaman_core::{
    Envelope, IdempotencyIdentity, IdempotencyKey, IdempotencySource, KafkaMessage,
    ReceivedIngestFailureKind, RESERVED_HEADER_PREFIX,
};
use kafkaman_sqlx::ReceivedIngestFailure;
use rdkafka::message::{Headers, Message};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{Error, Result};

pub(crate) fn ingest_failure_record<P, M>(
    message: &M,
    kind: ReceivedIngestFailureKind,
    error: String,
) -> ReceivedIngestFailure
where
    P: KafkaMessage,
    M: Message,
{
    ReceivedIngestFailure {
        source_topic: message.topic().to_owned(),
        source_partition: message.partition(),
        source_offset: message.offset(),
        key: message.key().map(Vec::from),
        headers: all_headers(message.headers()),
        payload: message.payload().map(Vec::from),
        message_type: P::MESSAGE_TYPE.to_owned(),
        expected_topic: P::TOPIC.to_owned(),
        kind,
        error,
    }
}

/// Every header on the record, quarantined verbatim for triage.
///
/// A list per key, because Kafka permits repeats and a quarantine record that
/// silently kept only the last one would misrepresent the thing it exists to
/// preserve. Values are lossy-decoded rather than rejected: this runs on the
/// path already handling a record known to be malformed.
///
/// Built as a JSON object directly instead of through `serde_json::to_value`,
/// so the signature carries no error case for a conversion that cannot fail.
/// Draining a `BTreeMap` also fixes key order regardless of whether anything in
/// the dependency graph enables serde_json's `preserve_order`.
fn all_headers<H: Headers>(headers: Option<&H>) -> serde_json::Value {
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if let Some(headers) = headers {
        for header in headers.iter() {
            let value = header
                .value
                .map(|value| String::from_utf8_lossy(value).into_owned())
                .unwrap_or_default();
            grouped
                .entry(header.key.to_owned())
                .or_default()
                .push(value);
        }
    }

    serde_json::Value::Object(
        grouped
            .into_iter()
            .map(|(key, values)| {
                let values = values.into_iter().map(serde_json::Value::String).collect();
                (key, serde_json::Value::Array(values))
            })
            .collect(),
    )
}

pub(crate) fn record_envelope<P, M>(message: &M) -> Result<(Envelope<P>, Option<Vec<u8>>)>
where
    P: KafkaMessage + DeserializeOwned,
    M: Message,
{
    if message.topic() != P::TOPIC {
        return Err(Error::UnexpectedTopic {
            expected: P::TOPIC,
            actual: message.topic().to_owned(),
        });
    }

    let payload = message.payload().ok_or(Error::MissingPayload)?;
    let payload = serde_json::from_slice::<P>(payload)?;
    let key = message.key().map(Vec::from);
    let headers = RecordHeaders::of(message);

    let mut envelope = Envelope::new(payload);
    envelope.headers = headers.user_headers();

    let idempotency_key = headers
        .get("kafkaman-idempotency-key")
        .ok_or(Error::MissingIdempotencyKey)?;
    let idempotency_key =
        IdempotencyKey::from_hex(idempotency_key).map_err(|err| Error::InvalidHeader {
            name: "kafkaman-idempotency-key",
            message: err.to_string(),
        })?;
    // Prefer the producer's source over an opaque digest, so the received row's
    // `idempotency_source` column survives the broker hop. An unparseable source
    // is not fatal: the digest is what dedupe compares, and the source is
    // explanatory metadata.
    envelope.idempotency_key = Some(
        match headers
            .get("kafkaman-idempotency-source")
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .and_then(|value| IdempotencySource::new(value).ok())
        {
            Some(source) => IdempotencyIdentity::from_parts(idempotency_key, source),
            None => IdempotencyIdentity::from_key(idempotency_key),
        },
    );
    // Event time comes from the producer when it is on the wire. Falling back to
    // `Envelope::new`'s `now()` would silently record consumer arrival time.
    if let Some(occurred_at) = headers.get("kafkaman-occurred-at") {
        envelope.occurred_at =
            kafkaman_core::rfc9557::parse(occurred_at).map_err(|err| Error::InvalidHeader {
                name: "kafkaman-occurred-at",
                message: err.to_string(),
            })?;
    }
    if let Some(message_id) = headers.get("kafkaman-message-id") {
        envelope.message_id = parse_uuid_header("kafkaman-message-id", message_id)?;
    }
    if let Some(correlation_id) = headers.get("kafkaman-correlation-id") {
        envelope.correlation_id = parse_uuid_header("kafkaman-correlation-id", correlation_id)?;
    }
    if let Some(causation_id) = headers.get("kafkaman-causation-id") {
        envelope.causation_id = Some(parse_uuid_header("kafkaman-causation-id", causation_id)?);
    }

    Ok((envelope, key))
}

/// A record's headers, decoded and split by namespace in one pass.
///
/// Envelope construction reads five reserved headers. Scanning the raw header
/// list once per lookup made that five passes over every record on the ingest
/// hot path, so this decodes once and indexes.
///
/// The two maps are kept apart rather than filtered on demand because they are
/// treated differently in three ways that must not be mixed up. Reserved keys are
/// matched ASCII case-insensitively and stored lowercased, so a producer writing
/// `Kafkaman-Message-Id` cannot slip a reserved key past the strip by changing
/// its case. User keys keep the case the producer wrote, because they are opaque
/// application data that is persisted and republished verbatim. And Kafka permits
/// repeated keys, which the two namespaces resolve in opposite directions — see
/// [`RecordHeaders::of`].
struct RecordHeaders {
    /// Producer-set headers, keys exactly as written.
    user: BTreeMap<String, String>,
    /// Reserved `kafkaman-` headers, keys lowercased for lookup.
    reserved: BTreeMap<String, String>,
}

impl RecordHeaders {
    fn of<M: Message>(message: &M) -> Self {
        let mut user = BTreeMap::new();
        let mut reserved = BTreeMap::new();

        if let Some(headers) = message.headers() {
            for header in headers.iter() {
                // A valueless header is indistinguishable from an absent one
                // here, and neither carries anything worth persisting.
                let Some(value) = header.value else {
                    continue;
                };
                let value = String::from_utf8_lossy(value).into_owned();
                let lowercased = header.key.to_ascii_lowercase();

                // The two namespaces resolve duplicates in opposite directions,
                // and both directions are load-bearing rather than incidental.
                //
                // Reserved: first occurrence wins, so a producer cannot append a
                // second `kafkaman-message-id` to override the one the relay
                // wrote.
                //
                // User: last occurrence wins, because that is what a `BTreeMap`
                // built with `insert` does, and it is what these headers have
                // always been persisted as. Nothing about kafkaman prefers
                // either end of a duplicate run — but silently swapping which
                // copy is stored changes what lands in a received row's
                // `headers` column, so it stays as it was.
                if lowercased.starts_with(RESERVED_HEADER_PREFIX) {
                    reserved.entry(lowercased).or_insert(value);
                } else {
                    user.insert(header.key.to_owned(), value);
                }
            }
        }

        Self { user, reserved }
    }

    /// A reserved `kafkaman-` header's value, if the record carries it.
    fn get(&self, name: &str) -> Option<&str> {
        debug_assert!(
            name.starts_with(RESERVED_HEADER_PREFIX) && name == name.to_ascii_lowercase(),
            "reserved lookups use the lowercase form of a `kafkaman-` key"
        );
        self.reserved.get(name).map(String::as_str)
    }

    /// The headers the producer set, with the whole reserved namespace removed.
    ///
    /// Stripping rather than trusting is what stops a foreign producer from
    /// spoofing kafkaman metadata by setting `kafkaman-message-id` itself.
    fn user_headers(&self) -> BTreeMap<String, String> {
        self.user.clone()
    }
}

fn parse_uuid_header(name: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|err| Error::InvalidHeader {
        name,
        message: err.to_string(),
    })
}
