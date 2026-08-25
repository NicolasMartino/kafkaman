//! Whether sampled success events are emitted at the configured rate, and
//! whether they land where an operator can pivot from them into a trace.
//!
//! # The two halves of `sample_success`
//!
//! `LifecycleSampler` decides *which* successes get an event: deterministically
//! every n-th, not a die roll per message, so a low rate cannot go a long
//! stretch emitting nothing — which is precisely when an operator turned
//! sampling on to see something. That half is unit-tested in `kafkaman-core`.
//!
//! This is the other half: that the events a real relay loop emits reach the
//! OpenTelemetry log signal, at the rate configured, carrying the ids of the
//! span they were emitted in. The correlation is the whole reason the events are
//! worth emitting rather than counting — a counter already says how many
//! messages published; a log record that pivots into the trace says what
//! happened to one of them.
//!
//! # Why its own binary
//!
//! It installs a logger provider and a subscriber, both process-wide, and counts
//! records across the whole export — anything else emitting into the same
//! provider would be indistinguishable from the events under test.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_core::{LifecycleEmission, RelayConfig};
use kafkaman_test::Harness;
use observability_tests::{
    postgres_for_suite, ProductSnapshot, SignallingPublisher, TestResult, SUITE,
};
use opentelemetry::logs::AnyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;

/// The message the relay logs for a sampled success.
const SUCCESS_EVENT: &str = "outbox message published";

/// Six messages at a rate of one in two. Even, so the expected count is not a
/// rounding argument, and more than one batch's worth of sampling state.
const SAMPLED_MESSAGES: usize = 6;
const SAMPLE_RATE: f64 = 0.5;

#[tokio::test]
async fn sampled_success_events_are_emitted_at_the_configured_rate_and_carry_their_trace(
) -> TestResult {
    let logs = InMemoryLogExporter::default();
    let logger = SdkLoggerProvider::builder()
        .with_simple_exporter(logs.clone())
        .build();
    // A tracer as well, because the property under test is not "an event was
    // logged" but "an event was logged inside a span and says so".
    let tracer = SdkTracerProvider::builder()
        .with_simple_exporter(InMemorySpanExporter::default())
        .build();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("kafkaman")))
            .with(OpenTelemetryTracingBridge::new(&logger)),
    )
    .expect("no other subscriber should be installed in this binary");

    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;

    publish_all(
        &harness,
        "sampled",
        SAMPLED_MESSAGES,
        LifecycleEmission::new(true, SAMPLE_RATE),
    )
    .await?;

    let emitted = success_events(&logs);
    assert_eq!(
        emitted.len(),
        SAMPLED_MESSAGES / 2,
        "one event in every two successes, exactly, rather than approximately"
    );
    for record in &emitted {
        let context = record
            .trace_context()
            .expect("a success event must carry the ids of the span it was emitted in");
        assert!(
            context.trace_id != opentelemetry::trace::TraceId::INVALID,
            "an event stamped with an invalid trace id pivots nowhere"
        );
    }

    // The default is silent, and this is the assertion that keeps it that way: a
    // healthy relay publishes continuously, so per-message success logging on by
    // default would cost more than the messages.
    let before = emitted.len();
    publish_all(&harness, "silent", 4, LifecycleEmission::default()).await?;
    assert_eq!(
        success_events(&logs).len(),
        before,
        "the default policy emits no per-message success events at all"
    );

    Ok(())
}

/// Every exported log record whose body is the relay's success event.
fn success_events(logs: &InMemoryLogExporter) -> Vec<opentelemetry_sdk::logs::SdkLogRecord> {
    logs.get_emitted_logs()
        .expect("emitted logs should be readable")
        .into_iter()
        .map(|log| log.record)
        .filter(|record| {
            matches!(record.body(), Some(AnyValue::String(body)) if body.as_str() == SUCCESS_EVENT)
        })
        .collect()
}

/// Enqueue `count` distinct products and run a relay loop until all are
/// published.
///
/// Distinct entity keys on purpose: enqueue supersedes pending rows for the same
/// entity, so `count` messages under one key would be one published row and the
/// sampling assertion would be measuring the wrong thing.
async fn publish_all(
    harness: &Harness,
    prefix: &str,
    count: usize,
    lifecycle: LifecycleEmission,
) -> TestResult {
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    for index in 0..count {
        let key = format!("{prefix}-{index}");
        let envelope =
            ProductSnapshot::envelope(&key, "a product").try_with_idempotency_key(key.as_str())?;
        harness.enqueue(&envelope).await?;
    }

    let (publisher, mut published) = SignallingPublisher::new();
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        table,
        RelayConfig {
            poll_interval: Duration::from_millis(25),
            lifecycle,
            ..RelayConfig::default()
        },
        shutdown.clone(),
    ));

    for _ in 0..count {
        published
            .recv()
            .await
            .expect("the relay should publish every enqueued row");
    }
    // The success events are emitted after the cycle's stats are recorded, so
    // the loop is stopped by cancelling and joining rather than at the moment
    // the last publish is acknowledged.
    shutdown.cancel();
    worker.await??;
    Ok(())
}
