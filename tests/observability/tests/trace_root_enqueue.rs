//! What happens when a tracer is installed but the caller has no span.
//!
//! This is a background job, a scheduled task, a CLI — anything that enqueues
//! outside a request. The naive expectation is that the row stores no trace
//! context, because there is no ambient span to capture. That is wrong, and the
//! reason is worth pinning: `enqueue` opens `kafkaman.enqueue` itself, so
//! whenever a tracer is installed there *is* a span, and it is a root.
//!
//! The behavior that follows is better than the expectation. The row carries the
//! root's context, the eventual publish descends from it, and an operator gets a
//! two-span trace showing exactly how long the row waited — rather than two
//! orphans with no way to tell they concern the same message.
//!
//! # Why its own binary
//!
//! It installs a tracer and asserts on span identity, and the global tracer
//! provider and subscriber are process-wide.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_test::Harness;
use observability_tests::{
    postgres_for_suite, relay_until_published, TestResult, TracePipeline, SUITE,
};
use opentelemetry::trace::SpanId;

#[tokio::test]
async fn an_enqueue_with_no_caller_span_still_anchors_the_trace() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    // At the default filter, deliberately. The `kafkaman::internal` tier puts a
    // function span on `enqueue`, which calls the function that opens
    // `kafkaman.enqueue` — so with the tier on the phase span has a parent, and
    // the root-ness this test is about is a property of the default filter.
    let pipeline = TracePipeline::install_at_default_filter();

    // No span is opened here: this is the background-job case.
    relay_until_published(&harness, "root-enqueue").await?;

    let enqueue = pipeline.span("kafkaman.enqueue");
    assert_eq!(
        enqueue.parent_span_id,
        SpanId::INVALID,
        "with no caller span, kafkaman's own enqueue span is the root of the trace"
    );

    let publish = pipeline.span("kafkaman.relay.publish");
    assert_eq!(
        publish.parent_span_id,
        enqueue.span_context.span_id(),
        "the publish still descends from the enqueue, which is the whole point of \
         storing the context rather than opening a fresh span at publish time"
    );
    assert_eq!(
        publish.span_context.trace_id(),
        enqueue.span_context.trace_id(),
        "one message, one trace"
    );

    // Not a real assertion about timing, but a statement of what the trace is
    // for: the gap between these two spans is the outbox latency.
    assert!(
        publish.start_time >= enqueue.end_time - Duration::from_secs(1),
        "publish should begin at or after the enqueue that caused it"
    );

    Ok(())
}
