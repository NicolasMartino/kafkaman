//! Opt-in, prod-representative full-loop test.
//!
//! Enabled with `--features redpanda`. It starts a real Redpanda broker as a
//! testcontainer, publishes through [`kafkaman_rdkafka::RdkafkaPublisher`] via
//! the worker, then consumes the record straight off the broker and asserts the
//! payload, partition key, and `kafkaman-*` metadata headers survived the trip.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

// Test cases, split by theme; the helpers they share follow.
mod ingest_dedup;
mod ingest_failures;
mod publish_and_consume;
mod run_ingester;

use std::collections::HashMap;
use std::time::Duration;

use durable_send_tests::{
    idem_hex, idem_key, recreate_effect_table, start_redpanda_harness, TestResult,
};
use kafkaman_core::{Envelope, KafkaMessage, ReceivedIngestFailureKind};
use kafkaman_rdkafka::{Error as RdkafkaError, RdkafkaConsumer};
use kafkaman_sqlx::{
    dispatch_once, enqueue_on_connection, received_ingest_failure_by_source, CacheTable,
    MessageRouter, ReceivedInsertOutcome,
};
use kafkaman_test::{EnvelopeTestExt, Error as HarnessError, RdkafkaConsumerTestExt};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn idempotency_headers(value: &str) -> OwnedHeaders {
    with_idempotency_header(OwnedHeaders::new(), value)
}

fn with_idempotency_header(headers: OwnedHeaders, value: &str) -> OwnedHeaders {
    let value = idem_hex(value);
    headers.insert(Header {
        key: "kafkaman-idempotency-key",
        value: Some(value.as_str()),
    })
}

fn redpanda_test_lock() -> &'static Mutex<()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct OrderCreated {
    order_id: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }

    fn entity_key(&self) -> String {
        self.order_id.clone()
    }
}

/// A type whose partition key is *not* its entity key: orders co-locate by
/// region for throughput, but converge per order. This is the shape F2 broke, and
/// the only shape that can detect the break.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct RegionalOrder {
    order_id: String,
    region: String,
}

impl KafkaMessage for RegionalOrder {
    const MESSAGE_TYPE: &'static str = "regional_order";
    const TOPIC: &'static str = "regional-orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.region.clone())
    }

    fn entity_key(&self) -> String {
        self.order_id.clone()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct OrderAccepted {
    order_id: String,
}

impl KafkaMessage for OrderAccepted {
    const MESSAGE_TYPE: &'static str = "order_accepted";
    const TOPIC: &'static str = "accepted-orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }

    fn entity_key(&self) -> String {
        self.order_id.clone()
    }
}

async fn publish_order_record(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    order_id: &str,
    headers: OwnedHeaders,
) -> TestResult {
    let payload = serde_json::to_vec(&OrderCreated {
        order_id: order_id.to_owned(),
    })?;
    let record = FutureRecord::to(topic)
        .payload(payload.as_slice())
        .key(key)
        .headers(headers);

    match producer.send(record, Timeout::Never).await {
        Ok(_) => Ok(()),
        Err((error, _message)) => Err(Box::new(error)),
    }
}

async fn publish_raw_record(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
    headers: OwnedHeaders,
) -> TestResult {
    let record = FutureRecord::to(topic)
        .payload(payload)
        .key(key)
        .headers(headers);

    match producer.send(record, Timeout::Never).await {
        Ok(_) => Ok(()),
        Err((error, _message)) => Err(Box::new(error)),
    }
}
