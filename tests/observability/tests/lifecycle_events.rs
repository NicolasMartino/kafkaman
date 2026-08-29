//! Whether sampled success events are emitted at the configured rate, and
//! whether they land where an operator can pivot from them into a trace.
//!
//! # The two halves of `sample_success`
//!
//! `LifecycleSampler` decides *which* successes get an event: deterministically
//! and in proportion — after `N` successes exactly `⌊N × rate⌋` events have been
//! emitted — rather than a die roll per message. So a low rate cannot go a
//! long stretch emitting nothing, which is precisely when an operator turned
//! sampling on to see something, and a rate that is not a reciprocal is not
//! quietly rounded to one. That half is unit-tested in `kafkaman-core`.
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

use kafkaman_core::{DispatcherConfig, LifecycleEmission, RelayConfig};
use kafkaman_sqlx::MessageRouter;
use kafkaman_test::Harness;
use observability_tests::{
    next, postgres_for_suite, ProductSnapshot, SignallingPublisher, TestResult, SUITE,
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

/// The message the dispatcher logs for a sampled success.
const DISPATCH_EVENT: &str = "received message processed";

/// Six messages at a rate of one in two. Even, so the expected count is not a
/// rounding argument, and more than one batch's worth of sampling state.
const SAMPLED_MESSAGES: usize = 6;
const SAMPLE_RATE: f64 = 0.5;

/// The receive side samples every success instead, because what it proves is
/// correlation rather than rate — the rate is settled on the send side above.
const DISPATCHED_MESSAGES: usize = 4;

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

    let emitted = events(&logs, SUCCESS_EVENT);
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
        events(&logs, SUCCESS_EVENT).len(),
        before,
        "the default policy emits no per-message success events at all"
    );

    // The receive side, which is the half this property is easiest to lose. Its
    // span closes inside `dispatch_once`, so a loop that sampled successes after
    // the call returned had nothing left to attach to and emitted events that
    // pivoted nowhere. The sampler is handed to `dispatch_once_sampled` instead,
    // and this is what says so.
    dispatch_all(&harness, DISPATCHED_MESSAGES).await?;
    let dispatched = events(&logs, DISPATCH_EVENT);
    assert_eq!(
        dispatched.len(),
        DISPATCHED_MESSAGES,
        "every dispatched row gets an event at a rate of one"
    );
    for record in &dispatched {
        let context = record
            .trace_context()
            .expect("a dispatch success event must carry the ids of its span");
        assert!(
            context.trace_id != opentelemetry::trace::TraceId::INVALID,
            "an event stamped with an invalid trace id pivots nowhere"
        );
    }

    Ok(())
}

/// Every exported log record with the given body.
fn events(logs: &InMemoryLogExporter, body: &str) -> Vec<opentelemetry_sdk::logs::SdkLogRecord> {
    logs.get_emitted_logs()
        .expect("emitted logs should be readable")
        .into_iter()
        .map(|log| log.record)
        .filter(|record| {
            matches!(record.body(), Some(AnyValue::String(logged)) if logged.as_str() == body)
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
        next(
            &mut published,
            "the relay should publish every enqueued row",
        )
        .await;
    }
    // The success events are emitted after the cycle's stats are recorded, so
    // the loop is stopped by cancelling and joining rather than at the moment
    // the last publish is acknowledged.
    shutdown.cancel();
    worker.await??;
    Ok(())
}

/// Insert `count` received rows and run a real dispatcher loop until all are
/// handled.
///
/// The loop rather than `dispatch_once`, for the same reason the send side uses
/// one: the sampler lives on the loop, and a single call proves nothing about
/// state carried across cycles.
async fn dispatch_all(harness: &Harness, count: usize) -> TestResult {
    let table = harness.received_table::<ProductSnapshot>().await?;
    for index in 0..count {
        let key = format!("dispatched-{index}");
        let envelope =
            ProductSnapshot::envelope(&key, "a product").try_with_idempotency_key(key.as_str())?;
        assert!(
            harness
                .insert_received::<ProductSnapshot>(&envelope, 0, index as i64, None)
                .await?,
            "each received row should be inserted"
        );
    }

    // The handler signals so the loop is stopped once the work is known to have
    // happened, rather than after an interval someone hoped was long enough.
    let (handled, mut processed) = tokio::sync::mpsc::unbounded_channel();
    let router = MessageRouter::new().handler::<ProductSnapshot>(move |_conn, _meta, _msg| {
        let handled = handled.clone();
        Box::pin(async move {
            let _ = handled.send(());
            Ok(())
        })
    });

    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(kafkaman_worker::run_dispatcher(
        harness.pool().clone(),
        table,
        router,
        DispatcherConfig {
            poll_interval: Duration::from_millis(25),
            lifecycle: LifecycleEmission::new(true, 1.0),
            ..Default::default()
        },
        shutdown.clone(),
    ));

    for _ in 0..count {
        next(
            &mut processed,
            "the dispatcher should handle every received row",
        )
        .await;
    }
    // Cancelled and joined rather than dropped: the handler signals before the
    // row is marked processed, and the event is emitted after the mark.
    shutdown.cancel();
    dispatcher.await??;
    Ok(())
}
