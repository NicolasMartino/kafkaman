//! Opt-in, prod-representative full-loop test.
//!
//! Enabled with `--features redpanda`. It starts a real Redpanda broker as a
//! testcontainer, publishes through [`kafkaman_rdkafka::RdkafkaPublisher`] via
//! the worker, then consumes the record straight off the broker and asserts the
//! payload, partition key, and `kafkaman-*` metadata headers survived the trip.
#![cfg(feature = "redpanda")]

use std::collections::HashMap;
use std::time::Duration;

use kafkaman_core::{Envelope, KafkaMessage};
use kafkaman_test::Harness;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use serde::{Deserialize, Serialize};
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

// Fixed host port so Redpanda can advertise a broker address the host-side
// client can actually dial. The full-loop test is single-instance, so a fixed
// port is acceptable and keeps the advertised-address handshake simple.
const KAFKA_PORT: u16 = 19092;

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
}

#[tokio::test]
async fn full_loop_publishes_to_redpanda_and_consumer_reads_it_back() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;

    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-redpanda".to_owned(),
    })
    .with_idempotency_key("idem-rp");
    let message_id = event.message_id;
    let mut event = event;
    event
        .headers
        .insert("x-user-header".to_owned(), "user-value".to_owned());

    harness.enqueue(&event).await?;
    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("kafkaman-test-{}", Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .map_err(|_| "timed out waiting for the published record on Redpanda")??;

    let payload = message.payload().ok_or("record had no payload")?;
    let consumed: OrderCreated = serde_json::from_slice(payload)?;
    assert_eq!(
        consumed,
        OrderCreated {
            order_id: "order-redpanda".to_owned()
        }
    );

    let key = message
        .key()
        .map(|k| String::from_utf8_lossy(k).into_owned());
    assert_eq!(key.as_deref(), Some("order-redpanda"));

    let mut headers = HashMap::new();
    if let Some(record_headers) = message.headers() {
        for header in record_headers.iter() {
            let value = header
                .value
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default();
            headers.insert(header.key.to_owned(), value);
        }
    }

    assert_eq!(
        headers.get("kafkaman-message-id").map(String::as_str),
        Some(message_id.to_string().as_str())
    );
    assert_eq!(
        headers.get("kafkaman-idempotency-key").map(String::as_str),
        Some("idem-rp")
    );
    assert_eq!(
        headers.get("x-user-header").map(String::as_str),
        Some("user-value")
    );

    Ok(())
}

async fn start_postgres(
) -> Result<(ContainerAsync<GenericImage>, String), Box<dyn std::error::Error + Send + Sync>> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "kafkaman_test")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(port).await?;
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/kafkaman_test");
    Ok((container, database_url))
}

async fn start_redpanda(
) -> Result<(ContainerAsync<GenericImage>, String), Box<dyn std::error::Error + Send + Sync>> {
    let advertised = format!("PLAINTEXT://127.0.0.1:{KAFKA_PORT}");
    let listener = format!("PLAINTEXT://0.0.0.0:{KAFKA_PORT}");
    let container = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.7")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
        .with_mapped_port(KAFKA_PORT, KAFKA_PORT.tcp())
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--kafka-addr",
            &listener,
            "--advertise-kafka-addr",
            &advertised,
        ])
        .start()
        .await?;

    Ok((container, format!("127.0.0.1:{KAFKA_PORT}")))
}
