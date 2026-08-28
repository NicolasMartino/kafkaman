//! The opt-in trace shape for backends that should show one distributed
//! waterfall across the Kafka hop.
//!
//! `trace_propagation` pins kafkaman's default OpenTelemetry messaging shape:
//! the consumer span starts a new trace and links to the producer. This file
//! pins the explicit exception. The example stack processes one record at a time
//! and sets `observability.defaults.kafka_trace_handoff = "parented"`, so Kibana
//! APM can render `POST /products -> relay.publish -> ingest -> dispatch` as one
//! trace sample timeline.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_test::kafkaman_config::KafkaTraceHandoff;
use observability_tests::{drive_kafka_round_trip, TestResult};
use uuid::Uuid;

#[tokio::test]
async fn parented_handoff_continues_the_producer_trace_across_kafka() -> TestResult {
    // Same drive as `trace_propagation`, one argument different. Everything below
    // is the consequence of that argument.
    let round_trip = drive_kafka_round_trip(
        KafkaTraceHandoff::Parented,
        &format!("kafkaman-parented-handoff-{}", Uuid::new_v4()),
    )
    .await?;
    let pipeline = &round_trip.pipeline;
    let caller_trace_id = round_trip.caller_trace_id;

    let enqueue = pipeline.span("kafkaman.enqueue");
    assert_eq!(
        enqueue.span_context.trace_id(),
        caller_trace_id,
        "enqueue belongs to the caller's trace"
    );

    let publish = pipeline.span("kafkaman.relay.publish");
    assert_eq!(
        publish.span_context.trace_id(),
        caller_trace_id,
        "publish restores the context stored on the outbox row"
    );
    assert_eq!(
        publish.parent_span_id,
        enqueue.span_context.span_id(),
        "publish descends from enqueue"
    );

    let ingest_span = pipeline.span("kafkaman.ingest");
    assert_eq!(
        ingest_span.span_context.trace_id(),
        caller_trace_id,
        "parented handoff keeps the consumer in the producer trace"
    );
    assert_eq!(
        ingest_span.parent_span_id,
        publish.span_context.span_id(),
        "the propagated producer context becomes the ingest parent"
    );
    // Parented and linked are alternatives, not a belt-and-braces pair. Adding
    // the link as well would make the two modes indistinguishable to a backend
    // that renders links beside parentage, and would draw the broker hop twice.
    assert!(
        ingest_span.links.is_empty(),
        "parented handoff should replace the producer link, not add to it, but \
         the ingest span carried {:?}",
        ingest_span.links
    );

    let dispatch_span = pipeline.span("kafkaman.dispatch");
    assert_eq!(
        dispatch_span.span_context.trace_id(),
        caller_trace_id,
        "dispatch continues the same consolidated trace"
    );
    assert_eq!(
        dispatch_span.parent_span_id,
        ingest_span.span_context.span_id(),
        "dispatch still descends from the stored ingest context"
    );

    Ok(())
}
