use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Error, Result, SqlIdentifier, TopicSpec};

/// What a message type is called and where it is published.
///
/// One message type owns exactly one topic. The type name is an
/// [`SqlIdentifier`] because it becomes part of three table names
/// (`outbox_*`, `received_*`, `cache_*`), while the topic is a free string that
/// only ever reaches Kafka.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageDescriptor {
    pub message_type: SqlIdentifier,
    pub topic: String,
    /// What the topic must look like on the broker.
    ///
    /// Defaulted rather than declared per message type. Proposal 12 narrowed the
    /// purview so that every in-purview type is a compact entity snapshot, which
    /// makes compaction a property of the model — asking each contract to
    /// restate it would invite one of them to declare a topic the rest of
    /// kafkaman cannot honour.
    ///
    /// `#[serde(default)]` so descriptors serialized before this field existed
    /// still deserialize, onto the same default they would have had.
    #[serde(default)]
    pub topic_spec: TopicSpec,
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
            topic_spec: TopicSpec::default(),
        })
    }

    /// Override the topic configuration this type requires.
    ///
    /// The only override with a legitimate use today is partitioning; the
    /// cleanup policy is fixed by the model.
    pub fn with_topic_spec(mut self, topic_spec: TopicSpec) -> Self {
        self.topic_spec = topic_spec;
        self
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

/// A type that can be published through kafkaman.
///
/// Every message is a full-state snapshot of one entity, which is why
/// [`entity_key`](Self::entity_key) is required rather than optional: it is the
/// convergence identity a consumer's cache keys on, and a type without one could
/// never converge.
pub trait KafkaMessage: Serialize {
    const MESSAGE_TYPE: &'static str;
    const TOPIC: &'static str;

    /// The Kafka record key, when the type co-locates by something other than
    /// its entity — a region, a tenant, a shard.
    ///
    /// Defaulting to `None` is safe because the publisher falls back to the
    /// entity key; see [`OutboxRow::record_key`](crate::OutboxRow::record_key)
    /// for why that fallback is load-bearing rather than a convenience.
    fn partition_key(&self) -> Option<String> {
        None
    }

    /// The entity this message is a snapshot of.
    fn entity_key(&self) -> String;

    fn descriptor() -> Result<MessageDescriptor> {
        MessageDescriptor::new(Self::MESSAGE_TYPE, Self::TOPIC)
    }
}
