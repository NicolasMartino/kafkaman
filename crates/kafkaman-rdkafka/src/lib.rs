use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, PublishAck};
use kafkaman_worker::{BoxError, Publisher};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

pub use kafkaman_core;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("delivery failed: {0}")]
    Delivery(String),
}

#[derive(Clone)]
pub struct RdkafkaPublisher {
    producer: FutureProducer,
}

impl RdkafkaPublisher {
    pub fn new(producer: FutureProducer) -> Self {
        Self { producer }
    }

    pub fn from_brokers(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self::new(producer))
    }

    pub async fn publish_row(&self, row: &ClaimedOutboxRow) -> Result<PublishAck> {
        let payload = serde_json::to_vec(&row.row.payload)?;
        let mut headers = OwnedHeaders::new();

        for (key, value) in &row.row.headers {
            headers = headers.insert(Header {
                key: key.as_str(),
                value: Some(value.as_str()),
            });
        }

        let message_id = row.row.message_id.to_string();
        let correlation_id = row.row.correlation_id.to_string();
        headers = headers
            .insert(Header {
                key: "kafkaman-message-id",
                value: Some(message_id.as_str()),
            })
            .insert(Header {
                key: "kafkaman-correlation-id",
                value: Some(correlation_id.as_str()),
            });

        if let Some(idempotency_key) = row.row.idempotency_key.as_deref() {
            headers = headers.insert(Header {
                key: "kafkaman-idempotency-key",
                value: Some(idempotency_key),
            });
        }

        let causation_id;
        if let Some(id) = row.row.causation_id {
            causation_id = id.to_string();
            headers = headers.insert(Header {
                key: "kafkaman-causation-id",
                value: Some(causation_id.as_str()),
            });
        }

        let mut record = FutureRecord::to(&row.row.topic)
            .payload(payload.as_slice())
            .headers(headers);
        if let Some(key) = row.row.partition_key.as_deref() {
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

#[async_trait]
impl Publisher for RdkafkaPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        self.publish_row(row)
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }
}
