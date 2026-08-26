//! Whether the ingest outcome counters partition consumed records exactly once.
//!
//! # The defect this exists to prevent, restated
//!
//! `kafkaman.kafka.ingest.records` counts each consumed record under exactly one
//! outcome, so that summing the series gives the number of records ingested and
//! splitting it by `outcome` splits that total without overlap. Offset commits
//! are not an outcome: a commit accompanies a classification rather than
//! replacing one, so counting commits alongside `inserted`/`duplicate`/`skipped`
//! doubled every record. That shipped, and it was caught by reading the code.
//!
//! `record_ingest_stats` keeps a `debug_assert` for the invariant, which is
//! useful locally and compiled out of exactly the builds an operator runs. The
//! schema decision therefore requires the invariant to be pinned by a test
//! against real ingest cycles, which is this one.
//!
//! # Why it needs a broker
//!
//! The instruments are recorded by `run_ingester`, not by `ingest_once`, and
//! there is no ingest cycle without something to consume from. A mock consumer
//! would test the mock: the classifications under test are produced by a real
//! record failing to deserialize and a real second copy colliding on its
//! idempotency key.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use durable_send_tests::{idem_hex, start_redpanda_harness};
use kafkaman_core::KafkaMessage;
use kafkaman_rdkafka::RdkafkaConsumer;
use observability_tests::{Collected, MetricPipeline, ProductSnapshot, TestResult};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// One insert, one duplicate of it, one record that cannot be deserialized.
///
/// Three records covering all three classifications, which is what makes the
/// sum meaningful: a test that only ingests successes cannot tell a partition
/// from a coincidence.
const RECORDS_PRODUCED: f64 = 3.0;

#[tokio::test]
async fn ingest_outcomes_partition_the_consumed_records() -> TestResult {
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;

    // Registered before anything is produced: the harness creates the received
    // table and teaches its config about the type on this call, and an ingester
    // started without it fails every cycle transiently rather than loudly.
    let _received = harness.received_table::<ProductSnapshot>().await?;

    // Installed before the ingester starts, because that is when its instruments
    // are built.
    let pipeline = MetricPipeline::install();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let payload = serde_json::to_vec(&ProductSnapshot {
        product_id: "disjoint-1".to_owned(),
        name: "a product".to_owned(),
    })?;
    // The same idempotency key twice: the second copy is a duplicate rather than
    // a second product.
    send(&producer, "disjoint-1", &payload, "idem-disjoint-1").await?;
    send(&producer, "disjoint-1", &payload, "idem-disjoint-1").await?;
    // Last, so the single skip cannot trip the consecutive-skip breaker.
    send(&producer, "disjoint-2", b"not json", "idem-disjoint-2").await?;

    let group_id = format!("kafkaman-observability-{}", Uuid::new_v4());
    let consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?;
    consumer.subscribe(&[ProductSnapshot::TOPIC])?;

    let shutdown = CancellationToken::new();
    let ingester = {
        let pool = harness.pool().clone();
        let cfg = harness.config();
        let shutdown = shutdown.clone();
        async move {
            consumer
                .run_ingester::<ProductSnapshot>(&pool, &cfg, Duration::from_millis(50), shutdown)
                .await
        }
    };
    let ingester = tokio::spawn(ingester);

    // Waited on by the metric under test rather than by a sleep: the loop is
    // done when it has classified all three records, and nothing else knows
    // when that is.
    let records = await_total(&pipeline, RECORDS_PRODUCED, Duration::from_secs(60)).await;
    shutdown.cancel();
    let loop_stats = ingester.await??;

    assert_eq!(
        total(&records),
        RECORDS_PRODUCED,
        "the outcome counters must sum to the records consumed, not to more"
    );
    assert_eq!(records.point_with(&[("outcome", "inserted")]).value, 1.0);
    assert_eq!(records.point_with(&[("outcome", "duplicate")]).value, 1.0);
    assert_eq!(records.point_with(&[("outcome", "skipped")]).value, 1.0);

    // The metric and the loop's own accounting must agree. If they ever diverge,
    // one of them is lying and the sum above cannot say which.
    assert_eq!(loop_stats.consumed as f64, RECORDS_PRODUCED);
    assert_eq!(
        loop_stats.transient_errors, 0,
        "a retried cycle means the ingester was fighting something other than \
         the records under test"
    );
    assert_eq!(
        (loop_stats.inserted + loop_stats.duplicates + loop_stats.skipped) as f64,
        total(&records)
    );

    // Commits are counted, but on their own instrument. This is the exact shape
    // of the original defect: three commits added to three classifications would
    // have made `ingest.records` report six.
    let commits = pipeline.metric("kafkaman.kafka.ingest.commits");
    assert_eq!(commits.unit, "{commit}");
    assert_eq!(
        commits
            .point_with(&[("message_type", ProductSnapshot::MESSAGE_TYPE)])
            .value,
        RECORDS_PRODUCED,
        "every classification is acknowledged, including the quarantined one"
    );
    assert!(
        !records.attribute_keys().contains("commit"),
        "commits must never appear as an ingest outcome"
    );

    // The identity attributes ride along on every Kafka-side series, and the
    // topic is the one that matters most here: without it, ingest volume and
    // publish volume cannot be compared on the single attribute they obviously
    // share, and an operator asking "is this topic backing up" has to map
    // message types to topics by hand.
    for outcome in ["inserted", "duplicate", "skipped"] {
        let attributes = &records.point_with(&[("outcome", outcome)]).attributes;
        assert_eq!(
            attributes.get("messaging.destination.name"),
            Some(&ProductSnapshot::TOPIC.to_owned()),
            "the {outcome} series should name the topic it consumed from"
        );
        assert_eq!(
            attributes.get("messaging.system"),
            Some(&"kafka".to_owned())
        );
        assert_eq!(
            attributes.get("message_type"),
            Some(&ProductSnapshot::MESSAGE_TYPE.to_owned())
        );
    }

    // Commits carry the same identity minus the outcome — they track consumer
    // progress rather than record classification, which is the whole reason they
    // are a separate instrument.
    let commit_point = commits.point_with(&[("message_type", ProductSnapshot::MESSAGE_TYPE)]);
    assert_eq!(
        commit_point.attributes.get("messaging.destination.name"),
        Some(&ProductSnapshot::TOPIC.to_owned())
    );
    assert!(
        !commit_point.attributes.contains_key("outcome"),
        "a commit is not an ingest outcome, and must not be labelled as one"
    );

    Ok(())
}

/// Every outcome bucket added together.
fn total(records: &Collected) -> f64 {
    records.points.iter().map(|point| point.value).sum()
}

/// Flush until the outcome counters add up to `expected`, or give up.
async fn await_total(pipeline: &MetricPipeline, expected: f64, within: Duration) -> Collected {
    let deadline = std::time::Instant::now() + within;
    loop {
        let collected = pipeline.collect();
        if let Some(records) = collected
            .into_iter()
            .find(|metric| metric.name == "kafkaman.kafka.ingest.records")
        {
            if total(&records) >= expected {
                return records;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ingest did not classify {expected} records within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn send(
    producer: &FutureProducer,
    key: &str,
    payload: &[u8],
    idempotency_key: &str,
) -> TestResult {
    let value = idem_hex(idempotency_key);
    let record = FutureRecord::to(ProductSnapshot::TOPIC)
        .payload(payload)
        .key(key)
        .headers(OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some(value.as_str()),
        }));

    match producer.send(record, Timeout::Never).await {
        Ok(_) => Ok(()),
        Err((error, _message)) => Err(Box::new(error)),
    }
}
