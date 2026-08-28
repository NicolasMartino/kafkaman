//! Whether trace context survives the two gaps kafkaman puts in the middle of
//! it, and whether the broker hop is a link rather than a continuation.
//!
//! # What is actually hard here
//!
//! Neither gap is a network hop. The outbox pattern separates enqueue from
//! publish *in time* — different task, possibly different process, after a
//! crash, after a retry — and ingest and dispatch are separated the same way. No
//! in-memory context survives either. So the trace an operator most wants,
//! "this request wrote this row and that became this Kafka record which service
//! B handled", is exactly the one the pattern breaks.
//!
//! The fix is that context is durable: captured when a row is written, stored in
//! a column beside it, restored when the row is acted on. This test drives one
//! message through both gaps and a real broker, and asserts the shape that
//! `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`
//! specifies:
//!
//! ```text
//! test.request                 (stands in for the caller's HTTP span)
//! └── kafkaman.enqueue         — writes traceparent into the row
//!       ⋮ durable gap
//!     kafkaman.relay.publish   — child of enqueue, restored from the row
//!       ⋮ Kafka
//!     kafkaman.ingest          — LINKS to relay.publish, new trace
//!     └── kafkaman.dispatch    — child of ingest, restored from the row
//! ```
//!
//! The consumer links rather than parents because it polls a batch that may hold
//! records from many unrelated traces; parenting would attach all of them to
//! whichever trace happened to be first.
//!
//! So the answer is **two** traces joined by a link, not one that runs end to
//! end, and this test asserts the trace ids differ as deliberately as it asserts
//! the link exists. The distinction is the difference between a dashboard query
//! that works and one that silently stops at the broker.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_test::kafkaman_config::KafkaTraceHandoff;
use observability_tests::{drive_kafka_round_trip, TestResult};
use uuid::Uuid;

#[tokio::test]
async fn one_message_produces_two_linked_traces_across_both_durable_gaps() -> TestResult {
    // The drive is shared with `trace_parented_handoff`, and the handoff mode is
    // the only thing the two pass differently. That is the point: if the drive
    // ever stops exercising the real ingest path, both tests notice.
    let round_trip = drive_kafka_round_trip(
        KafkaTraceHandoff::Linked,
        &format!("kafkaman-trace-propagation-{}", Uuid::new_v4()),
    )
    .await?;
    let pipeline = &round_trip.pipeline;
    let caller_trace_id = round_trip.caller_trace_id;

    // --- The shape ---

    let enqueue = pipeline.span("kafkaman.enqueue");
    assert_eq!(
        enqueue.span_context.trace_id(),
        caller_trace_id,
        "enqueue belongs to the caller's trace, not one of its own"
    );

    let publish = pipeline.span("kafkaman.relay.publish");
    assert_eq!(
        publish.span_context.trace_id(),
        caller_trace_id,
        "the publish happened in a different task, minutes of wall clock later in \
         a real deployment, and still belongs to the transaction that caused it"
    );
    assert_eq!(
        publish.parent_span_id,
        enqueue.span_context.span_id(),
        "restored from the row's stored context, so publish descends from enqueue"
    );

    let ingest_span = pipeline.span("kafkaman.ingest");
    assert_ne!(
        ingest_span.span_context.trace_id(),
        caller_trace_id,
        "a consumer starts its own trace; a batch may hold records from many"
    );
    let link = ingest_span
        .links
        .iter()
        .find(|link| link.span_context.span_id() == publish.span_context.span_id())
        .expect("the ingest span should link to the publish that produced the record");
    assert_eq!(
        link.span_context.trace_id(),
        caller_trace_id,
        "the link points back into the producing trace"
    );
    assert_eq!(
        ingest_span.parent_span_id,
        opentelemetry::trace::SpanId::INVALID,
        "linked, not parented — the distinction messaging semconv draws"
    );

    let dispatch_span = pipeline.span("kafkaman.dispatch");
    assert_eq!(
        dispatch_span.span_context.trace_id(),
        ingest_span.span_context.trace_id(),
        "dispatch continues the consumer's trace"
    );
    assert_eq!(
        dispatch_span.parent_span_id,
        ingest_span.span_context.span_id(),
        "restored from the received row, across the receive side's durable gap"
    );

    Ok(())
}
