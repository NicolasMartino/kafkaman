//! How failed receive-side work appears to APM.
//!
//! A handler failure is still a handled durable-message outcome: the row is
//! retried or parked, and `dispatch_once` returns `Ok(DispatchStats)`. For APM,
//! though, the consumer transaction must carry the failed outcome too, otherwise
//! the service overview has no failed transaction to split or chart.
//!
//! # Why its own binary
//!
//! It installs a tracer subscriber, which is process-wide. Each observability
//! trace concern lives in one integration test target for that reason.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_sqlx::{dispatch_once, Error, MessageRouter};
use kafkaman_test::Harness;
use observability_tests::{postgres_for_suite, ProductSnapshot, TestResult, TracePipeline, SUITE};
use opentelemetry::trace::{SpanKind, Status};
use opentelemetry_sdk::trace::SpanData;
use time::OffsetDateTime;

#[tokio::test]
async fn a_recorded_handler_failure_marks_the_dispatch_transaction() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let envelope = ProductSnapshot::envelope("apm-failure", "a product")
        .try_with_idempotency_key("apm-failure")?;
    assert!(
        harness
            .insert_received::<ProductSnapshot>(&envelope, 0, 0, None)
            .await?,
        "the received row should be inserted"
    );

    let pipeline = TracePipeline::install_at_default_filter();
    let router = MessageRouter::new().handler::<ProductSnapshot>(|_conn, _meta, _msg| {
        Box::pin(async move { Err(Error::Handler("boom".to_owned())) })
    });

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let dispatch = pipeline.span("kafkaman.dispatch");
    assert_eq!(dispatch.span_kind, SpanKind::Consumer);
    assert_error_status(&dispatch, "handler failed: boom");
    assert_failure_attrs(
        &dispatch,
        "Handler",
        "urn:kafkaman:problem:handler",
        "handler",
    );

    let handler = pipeline.span("kafkaman.handler");
    assert_eq!(handler.span_kind, SpanKind::Internal);
    assert_error_status(&handler, "handler failed: boom");
    assert_failure_attrs(
        &handler,
        "Handler",
        "urn:kafkaman:problem:handler",
        "handler",
    );
    assert_attr(&handler, "handler.position", "after");

    // The failure is reported as an error exactly once, on the narrower of the
    // two spans that record it. An APM backend derives one error per exception
    // event, so reporting it on both would put two errors in an operator's list
    // for a single failed message and give the error rate a factor of two.
    assert_eq!(
        exception_events(&handler),
        1,
        "the handler span owns the failure and reports it"
    );
    assert_eq!(
        exception_events(&dispatch),
        0,
        "the enclosing transaction carries the status, not a second error"
    );

    // Which attempt this was, and whether it was the last one. Neither value
    // reached any span before: a trace of a failure could say what went wrong
    // but not whether the message had just been dead-lettered, which is the
    // difference between an incident and a retry doing its job.
    assert_attr(&dispatch, "kafkaman.retry.attempt", "1");
    assert_attr(&dispatch, "kafkaman.retry.exhausted", "false");

    // Which transaction the failure record landed in. The atomic path and the
    // fallback that abandons the claim transaction used to emit byte-identical
    // spans, so an operator could not tell whether the row's attempt count and
    // its claim could still disagree after a crash.
    assert_attr(&dispatch, "kafkaman.failure.recorded_via", "savepoint");

    assert_the_dead_lettering_attempt_says_so(&pipeline, &harness, &table, &router).await?;

    Ok(())
}

/// The attempt that spends the last of the budget reports it.
///
/// `exhausted` is the whole value of putting the retry pair on the span. Nine
/// failures that will be retried and one that dead-letters are the same message,
/// the same error, and the same failure kind; only this separates the alert from
/// the noise. Asserted on the flip rather than on the flag's presence, because a
/// field that is always `false` would satisfy the earlier assertion and tell an
/// operator nothing.
async fn assert_the_dead_lettering_attempt_says_so(
    pipeline: &TracePipeline,
    harness: &Harness,
    table: &kafkaman_sqlx::ReceivedTable,
    router: &MessageRouter,
) -> TestResult {
    // A budget of one, so the first failure is also the last. Driving the
    // default ten would mean ten dispatches separated by real exponential
    // backoff, which is a slow test of the retry schedule rather than a fast
    // one of what the span says.
    let mut last_chance = table.clone();
    last_chance.retry.max_attempts = 1;

    let envelope = ProductSnapshot::envelope("apm-exhausted", "a doomed product")
        .try_with_idempotency_key("apm-exhausted")?;
    assert!(
        harness
            .insert_received::<ProductSnapshot>(&envelope, 0, 1, None)
            .await?,
        "the second received row should be inserted"
    );

    let stats = dispatch_once(
        harness.pool(),
        &last_chance,
        router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(stats.failed, 1, "the row should have failed again");

    // `TracePipeline::span` is an exactly-one assertion and there are two
    // `kafkaman.dispatch` spans by now, so pick the one belonging to this row.
    let message_id = envelope.message_id.to_string();
    let dispatch = pipeline
        .finished()
        .into_iter()
        .find(|span| {
            span.name == "kafkaman.dispatch"
                && span.attributes.iter().any(|attribute| {
                    attribute.key.as_str() == "messaging.message.id"
                        && attribute.value.to_string() == message_id
                })
        })
        .expect("a dispatch span for the exhausted row");

    assert_attr(&dispatch, "kafkaman.retry.attempt", "1");
    assert_attr(&dispatch, "kafkaman.retry.exhausted", "true");

    Ok(())
}

fn exception_events(span: &SpanData) -> usize {
    span.events
        .iter()
        .filter(|event| event.name == "exception")
        .count()
}

fn assert_error_status(span: &SpanData, expected: &str) {
    match &span.status {
        Status::Error { description } => assert!(
            description.contains(expected),
            "expected {expected:?} in {} status description {description:?}",
            span.name
        ),
        status => panic!("expected {} to be ERROR, got {status:?}", span.name),
    }
}

fn assert_failure_attrs(
    span: &SpanData,
    expected_kind: &str,
    expected_type: &str,
    expected_stage: &str,
) {
    assert_attr(span, "error.type", expected_type);
    assert_attr(span, "kafkaman.failure.kind", expected_kind);
    assert_attr(span, "kafkaman.failure.type", expected_type);
    assert_attr(span, "kafkaman.failure.stage", expected_stage);
}

fn assert_attr(span: &SpanData, key: &str, expected: &str) {
    let value = span
        .attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| attribute.value.to_string());
    assert_eq!(
        value.as_deref(),
        Some(expected),
        "expected {key}={expected:?} on {} among {:?}",
        span.name,
        span.attributes
    );
}
