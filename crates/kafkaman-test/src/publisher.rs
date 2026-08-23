use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, PublishAck, PublishedRecord};
use kafkaman_worker::{BoxError, Publisher};

/// A publisher that records what it was asked to publish instead of sending it.
///
/// Offsets are the position in the captured list, which makes them monotonic per
/// harness — enough for the cache convergence guard, which only ever compares
/// offsets within one topic and partition.
#[derive(Clone, Debug, Default)]
pub struct CapturingPublisher {
    records: Arc<Mutex<Vec<PublishedRecord>>>,
}

impl CapturingPublisher {
    pub fn records(&self) -> Vec<PublishedRecord> {
        self.records
            .lock()
            .expect("capturing publisher mutex poisoned")
            .clone()
    }

    pub fn records_on(&self, topic: &str) -> Vec<PublishedRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.topic == topic)
            .collect()
    }
}

#[async_trait]
impl Publisher for CapturingPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        let offset = {
            let mut records = self
                .records
                .lock()
                .expect("capturing publisher mutex poisoned");
            let offset = records.len() as i64;
            records.push(PublishedRecord {
                topic: row.row.topic.clone(),
                // Mirror the real publisher's key rule exactly, or a harness
                // assertion about routing would not reflect production.
                key: row.row.record_key().map(ToOwned::to_owned),
                payload: row.row.payload.clone(),
                headers: row.row.headers.clone(),
                message_id: row.row.message_id,
            });
            offset
        };

        Ok(PublishAck {
            topic: row.row.topic.clone(),
            partition: 0,
            offset,
        })
    }
}

/// Where a [`Harness`](crate::Harness) publishes.
#[derive(Debug)]
pub enum HarnessPublisher {
    Capturing(CapturingPublisher),
    #[cfg(feature = "redpanda")]
    Redpanda(kafkaman_rdkafka::RdkafkaPublisher),
}

#[async_trait]
impl Publisher for HarnessPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        match self {
            HarnessPublisher::Capturing(publisher) => publisher.publish(row).await,
            #[cfg(feature = "redpanda")]
            HarnessPublisher::Redpanda(publisher) => publisher.publish(row).await,
        }
    }
}
