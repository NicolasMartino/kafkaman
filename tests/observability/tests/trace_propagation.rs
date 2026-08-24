//! Whether one trace survives the two gaps kafkaman puts in the middle of it.
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
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use durable_send_tests::start_redpanda_harness;
use kafkaman_core::{KafkaMessage, RelayConfig};
use kafkaman_rdkafka::{RdkafkaConsumer, RdkafkaPublisher};
use kafkaman_sqlx::{dispatch_once, MessageRouter};
use observability_tests::{ProductSnapshot, TestResult, TracePipeline};
use opentelemetry::trace::TraceContextExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn one_message_produces_one_connected_trace_across_both_durable_gaps() -> TestResult {
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let outbox_table = harness.outbox_table::<ProductSnapshot>().await?;
    let received_table = harness.received_table::<ProductSnapshot>().await?;

    let pipeline = TracePipeline::install();

    // Stands in for the span a host would already have open — an HTTP handler,
    // a job runner. The point of the enqueue span is that it descends from
    // whatever the caller was doing, so the trace starts before kafkaman.
    let caller = tracing::info_span!("test.request");
    let caller_trace_id = {
        let _entered = caller.enter();
        let envelope = ProductSnapshot::envelope("traced-product", "a traced product")
            .try_with_idempotency_key("traced-product")?;
        harness.enqueue(&envelope).await?;
        trace_id_of(&caller)
    };

    // Publish through a real broker, from a loop, exactly as a deployment would.
    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let shutdown = CancellationToken::new();
    let relay = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        outbox_table,
        RelayConfig {
            poll_interval: Duration::from_millis(50),
            ..RelayConfig::default()
        },
        shutdown.clone(),
    ));
    await_span(&pipeline, "kafkaman.relay.publish", Duration::from_secs(30)).await;
    shutdown.cancel();
    relay.await??;

    // Consume it back, which is where the Kafka half of the propagation lands.
    let consumer = RdkafkaConsumer::from_brokers(
        &brokers,
        &format!("kafkaman-trace-propagation-{}", Uuid::new_v4()),
    )?;
    consumer.subscribe(&[ProductSnapshot::TOPIC])?;
    let ingest_shutdown = CancellationToken::new();
    let ingester = {
        let pool = harness.pool().clone();
        let cfg = harness.config();
        let token = ingest_shutdown.clone();
        tokio::spawn(async move {
            consumer
                .run_ingester::<ProductSnapshot>(&pool, &cfg, Duration::from_millis(50), token)
                .await
        })
    };
    // Stopped once its span exists, rather than after an interval someone
    // guessed at — the span is the thing under test and also the completion
    // signal.
    await_span(&pipeline, "kafkaman.ingest", Duration::from_secs(30)).await;
    ingest_shutdown.cancel();
    let ingest_stats = ingester.await??;
    assert_eq!(ingest_stats.consumed, 1, "one record was published");

    // Dispatch it, which crosses the second durable gap.
    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let dispatch = dispatch_once(
        harness.pool(),
        &received_table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(dispatch.processed, 1, "the handler ran");

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

/// The trace id a `tracing` span belongs to.
fn trace_id_of(span: &tracing::Span) -> opentelemetry::trace::TraceId {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    span.context().span().span_context().trace_id()
}

/// Poll until a span with `name` has been recorded.
///
/// The relay runs on its own schedule; waiting for the artifact it produces is
/// the only thing this test can honestly wait on.
async fn await_span(pipeline: &TracePipeline, name: &str, within: Duration) {
    let deadline = std::time::Instant::now() + within;
    while !pipeline.has_span(name) {
        assert!(
            std::time::Instant::now() < deadline,
            "no {name} span was recorded within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
