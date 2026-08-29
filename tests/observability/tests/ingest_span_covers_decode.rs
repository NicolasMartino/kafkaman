//! Whether the ingest span covers the record that never decoded.
//!
//! # The gap this exists to close
//!
//! A consumed record can fail before it is a message at all: the payload is not
//! the type the topic promised, a reserved header is malformed, the record was
//! produced by something that speaks a different schema. Those records are
//! quarantined and acknowledged, which is the right behaviour and also the
//! reason nobody watches the path — the partition keeps moving and the queue
//! looks healthy.
//!
//! The span for that record therefore has to exist, and for a while it did not.
//! `ingest_once` decoded first and opened `kafkaman.ingest` afterwards, from
//! inside whichever arm the decode had chosen. A record that decoded was traced;
//! a record that failed to decode produced no span, and neither did the decode
//! itself. So the single record an operator goes hunting for after an alert was
//! the one record with nothing to find, and the "no spans" answer was
//! indistinguishable from "nothing was consumed".
//!
//! # The second half of the same gap
//!
//! Having a span was necessary and not sufficient. The span reported **success**
//! for a record the ingester had refused, because quarantining is a handled
//! outcome and `ingest_once` returns `Ok` for it. So the poison record produced
//! no exception, no failed transaction, and nothing in an APM error list — the
//! operator was back to the one place that did record it, a table nobody
//! queries. This test therefore pins the status and the exception as well as the
//! span's existence: handled is not the same as fine.
//!
//! # Why this needs a broker
//!
//! `ingest_once` reads from a real consumer, and the property is about a record
//! arriving from a topic rather than about a function being called. There is no
//! poison record without something to poison.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use durable_send_tests::{idem_hex, start_redpanda_harness};
use kafkaman_core::KafkaMessage;
use kafkaman_rdkafka::RdkafkaConsumer;
use observability_tests::{ProductSnapshot, TestResult, TracePipeline};
use opentelemetry::trace::Status;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A producer's context, in the form a tracing SDK would have written it.
const PRODUCER_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[tokio::test]
async fn a_record_that_cannot_be_decoded_is_still_traced() -> TestResult {
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    // Registered before anything is produced: the harness creates the received
    // and quarantine tables on this call, and an ingester started without them
    // fails every cycle transiently rather than quarantining anything.
    let _received = harness.received_table::<ProductSnapshot>().await?;

    let pipeline = TracePipeline::install();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    // Well-formed metadata around a payload that is not a `ProductSnapshot`.
    // Everything the span needs — topic, partition, offset, the producer's trace
    // — is in the record's envelope of headers, and none of it depends on the
    // payload parsing.
    let idempotency_key = idem_hex("idem-poison");
    let record = FutureRecord::to(ProductSnapshot::TOPIC)
        .payload(b"not json".as_slice())
        .key("poison")
        .headers(
            OwnedHeaders::new()
                .insert(Header {
                    key: "kafkaman-idempotency-key",
                    value: Some(idempotency_key.as_str()),
                })
                .insert(Header {
                    key: "traceparent",
                    value: Some(PRODUCER_TRACEPARENT),
                }),
        );
    producer
        .send(record, Timeout::Never)
        .await
        .map_err(|(error, _message)| error)?;

    let consumer = RdkafkaConsumer::from_brokers(
        &brokers,
        &format!("kafkaman-ingest-span-{}", Uuid::new_v4()),
    )?;
    consumer.subscribe(&[ProductSnapshot::TOPIC])?;
    let shutdown = CancellationToken::new();
    let ingester = {
        let pool = harness.pool().clone();
        let cfg = harness.config();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            consumer
                .run_ingester::<ProductSnapshot>(&pool, &cfg, Duration::from_millis(50), shutdown)
                .await
        })
    };

    // Waited on by the span itself, which is both the artifact under test and
    // the only honest completion signal: the loop has no other way to say it has
    // finished with a record.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !pipeline.has_span("kafkaman.ingest") {
        assert!(
            std::time::Instant::now() < deadline,
            "a quarantined record produced no kafkaman.ingest span"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();
    let loop_stats = ingester.await??;
    assert_eq!(
        loop_stats.skipped, 1,
        "the record under test must have been quarantined, not stored"
    );

    let span = pipeline.span("kafkaman.ingest");
    let attribute = |key: &str| {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
    };
    assert_eq!(
        attribute("messaging.destination.name").as_deref(),
        Some(ProductSnapshot::TOPIC),
        "the span identifies the record by its position in the log, which is \
         readable whether or not the payload is"
    );
    assert_eq!(attribute("messaging.kafka.partition").as_deref(), Some("0"));
    assert_eq!(attribute("messaging.kafka.offset").as_deref(), Some("0"));

    // The producer link too. It comes from the headers rather than from the
    // decoded envelope, so a record that never became an envelope still appears
    // in the trace that produced it — which is where the operator is looking.
    let link = span
        .links
        .iter()
        .next()
        .expect("the producer's context was on the record and should have been linked");
    assert_eq!(
        link.span_context.trace_id().to_string(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );
    assert_ne!(
        span.span_context.trace_id(),
        link.span_context.trace_id(),
        "a consumer links rather than continues, poison record or not"
    );

    // The refusal itself. `ingest_once` returned `Ok` — the row is written and
    // the offset advanced — so nothing about the *result* says this record was
    // rejected. The span has to.
    match &span.status {
        Status::Error { description } => assert!(
            !description.is_empty(),
            "a refused record's span must say what was wrong with it"
        ),
        status => panic!(
            "a quarantined record left kafkaman.ingest reporting {status:?}; a \
             failure the ingester absorbs is still a failure an operator has to \
             be able to find"
        ),
    }
    assert_eq!(
        attribute("error.type").as_deref(),
        Some("urn:kafkaman:problem:invalid-payload"),
        "the span is classified by the quarantine kind, so the transaction \
         groups with the errors it caused"
    );

    // Exactly one, for the same reason every other failure path pins it: an APM
    // backend derives one error document per exception event, and a record that
    // is refused once should not appear in the error list twice.
    let exceptions: Vec<_> = span
        .events
        .iter()
        .filter(|event| event.name == "exception")
        .collect();
    assert_eq!(
        exceptions.len(),
        1,
        "expected one exception event on the ingest span, found {:?}",
        span.events
    );
    let exception_type = exceptions[0]
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "exception.type")
        .map(|kv| kv.value.to_string());
    assert_eq!(
        exception_type.as_deref(),
        Some("urn:kafkaman:problem:invalid-payload")
    );

    Ok(())
}
