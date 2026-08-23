use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, PublishAck};
use kafkaman_worker::{BoxError, Publisher};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::{Error, Result};

#[derive(Clone)]
pub struct RdkafkaPublisher {
    producer: FutureProducer,
}

impl std::fmt::Debug for RdkafkaPublisher {
    /// `FutureProducer` is an opaque librdkafka handle with no `Debug`, so the
    /// struct is reported by name only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdkafkaPublisher").finish_non_exhaustive()
    }
}

impl RdkafkaPublisher {
    pub fn new(producer: FutureProducer) -> Self {
        Self { producer }
    }

    pub fn from_brokers(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // An outbox relay republishes on every uncertain outcome, so the
            // producer must deduplicate on the broker side; without idempotence
            // a retried send after a lost ack writes the snapshot twice at two
            // offsets. `acks=all` is its prerequisite and also stops a snapshot
            // from being acknowledged before it is replicated.
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self::new(producer))
    }

    pub async fn publish_row(&self, row: &ClaimedOutboxRow) -> Result<PublishAck> {
        let payload = serde_json::to_vec(&row.row.payload)?;
        let managed = managed_headers(row)?;

        let mut headers = OwnedHeaders::new();
        // User headers first, kafkaman's second: a user header cannot occupy a
        // reserved key (enqueue rejects that outright), so the order is only
        // about keeping the managed block contiguous and easy to read on the
        // wire.
        for (key, value) in &row.row.headers {
            headers = headers.insert(Header {
                key: key.as_str(),
                value: Some(value.as_str()),
            });
        }
        for (key, value) in &managed {
            headers = headers.insert(Header {
                key,
                value: Some(value.as_str()),
            });
        }

        let mut record = FutureRecord::to(&row.row.topic)
            .payload(payload.as_slice())
            .headers(headers);
        // Fall back to the entity key when a type declares no partition key.
        // Kafka routes keyless records round-robin, but the cache convergence
        // guard compares offsets only within one topic and partition — so a
        // keyless entity type would scatter its snapshots across partitions and
        // could never converge.
        if let Some(key) = row.row.record_key() {
            record = record.key(key);
        }

        match self.producer.send(record, Timeout::Never).await {
            Ok((partition, offset)) => Ok(PublishAck {
                topic: row.row.topic.clone(),
                partition,
                offset,
            }),
            Err((error, _message)) => Err(Error::Delivery(error.to_string())),
        }
    }
}

/// The `kafkaman-` headers this row publishes, owned so every value outlives the
/// borrow `OwnedHeaders` takes.
fn managed_headers(row: &ClaimedOutboxRow) -> Result<Vec<(&'static str, String)>> {
    let mut managed = vec![
        ("kafkaman-message-id", row.row.message_id.to_string()),
        (
            "kafkaman-correlation-id",
            row.row.correlation_id.to_string(),
        ),
    ];

    // The occurrence time is the producer's, not the consumer's. Without it on
    // the wire the consumer can only stamp its own arrival time, so event time
    // is lost for every message that crosses a broker.
    let occurred_at = kafkaman_core::rfc9557::render(row.row.occurred_at).map_err(|err| {
        Error::InvalidHeader {
            name: "kafkaman-occurred-at",
            message: err.to_string(),
        }
    })?;
    managed.push(("kafkaman-occurred-at", occurred_at));

    if let Some(key) = row.row.idempotency_key {
        managed.push(("kafkaman-idempotency-key", key.to_string()));
    }
    // The digest alone is opaque. Carrying the source keeps the received row's
    // `idempotency_source` column populated across a broker hop, which is what
    // makes a stored digest explicable during triage.
    if let Some(source) = &row.row.idempotency_source {
        managed.push(("kafkaman-idempotency-source", source.to_string()));
    }
    if let Some(id) = row.row.causation_id {
        managed.push(("kafkaman-causation-id", id.to_string()));
    }

    Ok(managed)
}

#[async_trait]
impl Publisher for RdkafkaPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        self.publish_row(row)
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }
}
