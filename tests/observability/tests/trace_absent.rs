//! Whether a message with no trace context is still an ordinary message.
//!
//! This is the rule the propagation design is most likely to break by accident:
//! **absent trace context is normal and never an error.** A row enqueued outside
//! any span has none. A record from an uninstrumented producer has none. A
//! process that never installs a tracer produces none at all. Message flow does
//! not depend on trace context for correctness, and a version of this library
//! that only works when something is tracing would be worse than one that never
//! traced.
//!
//! # The absence under test
//!
//! No tracer is installed here, which is the state of every host that has not
//! wired one — the majority of adopters on the day they add kafkaman. The
//! sibling case, a host that *has* a tracer but enqueues outside any caller
//! span, turned out to behave differently and is pinned separately in
//! `trace_root_enqueue`: kafkaman opens its own `kafkaman.enqueue` span, so
//! there is context to capture even when the caller had none.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_core::RelayConfig;
use kafkaman_sqlx::{dispatch_once, MessageRouter};
use kafkaman_test::Harness;
use observability_tests::{
    next, postgres_for_suite, ProductSnapshot, SignallingPublisher, TestResult, SUITE,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn an_untraced_process_publishes_and_dispatches_normally() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let outbox_table = harness.outbox_table::<ProductSnapshot>().await?;
    let received_table = harness.received_table::<ProductSnapshot>().await?;

    let envelope = ProductSnapshot::envelope("untraced", "an untraced product")
        .try_with_idempotency_key("untraced")?;
    harness.enqueue(&envelope).await?;

    let row = harness
        .outbox_row::<ProductSnapshot>(envelope.message_id)
        .await?;
    assert!(
        row.trace.is_none(),
        "no tracer is installed, so there is nothing to capture and the column \
         stays null rather than holding an invalid context"
    );

    // The send side proceeds: a relay loop claims, publishes, and marks.
    let (publisher, mut published) = SignallingPublisher::new();
    let shutdown = CancellationToken::new();
    let relay = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        outbox_table,
        RelayConfig {
            poll_interval: Duration::from_millis(25),
            ..RelayConfig::default()
        },
        shutdown.clone(),
    ));
    next(
        &mut published,
        "a row without trace context still publishes",
    )
    .await;
    shutdown.cancel();
    relay.await??;

    // And so does the receive side.
    let received = ProductSnapshot::envelope("untraced-receive", "an untraced product")
        .try_with_idempotency_key("untraced-receive")?;
    assert!(
        harness
            .insert_received::<ProductSnapshot>(&received, 0, 1, None)
            .await?,
        "the received row should be inserted"
    );

    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let dispatch = dispatch_once(
        harness.pool(),
        &received_table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(
        dispatch.processed, 1,
        "a received row without trace context still dispatches"
    );

    Ok(())
}

/// A column holding something that is not a `traceparent` reads as no context.
///
/// Columns are written by a previous version, by a migration, or by hand during
/// an incident, and the grammar this parser enforces has already been tightened
/// once. So the question is not whether an unreadable value can be in the column
/// — it can — but what happens when a relay claims that row.
///
/// Two answers would be wrong, in opposite directions. Refusing to read the row
/// stops a message over a field with no business meaning. Forwarding the value
/// puts kafkaman's name on a header the next service will try to parse, which is
/// worse: the corruption spreads to systems that did nothing wrong. The right
/// answer is the third one — drop it, publish normally, and lose only the trace.
#[tokio::test]
async fn a_corrupt_stored_traceparent_neither_stops_the_row_nor_travels_with_it() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let outbox_table = harness.outbox_table::<ProductSnapshot>().await?;

    let envelope = ProductSnapshot::envelope("corrupt-trace", "a product")
        .try_with_idempotency_key("corrupt-trace")?;
    harness.enqueue(&envelope).await?;

    // Written past the constructor, which is the only way such a value can
    // exist — and exactly how it would arrive from an older binary.
    sqlx::query(&format!(
        "UPDATE {} SET traceparent = $1, tracestate = $2 WHERE message_id = $3",
        outbox_table.qualified_name()
    ))
    .bind("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01")
    .bind("not a tracestate")
    .bind(envelope.message_id)
    .execute(harness.pool())
    .await?;

    let row = harness
        .outbox_row::<ProductSnapshot>(envelope.message_id)
        .await?;
    assert!(
        row.trace.is_none(),
        "uppercase hex is not a W3C traceparent, and a row must not carry one"
    );

    let (publisher, mut published) = SignallingPublisher::new();
    let shutdown = CancellationToken::new();
    let relay = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        outbox_table,
        RelayConfig {
            poll_interval: Duration::from_millis(25),
            ..RelayConfig::default()
        },
        shutdown.clone(),
    ));
    next(
        &mut published,
        "a row with a corrupt context still publishes",
    )
    .await;
    shutdown.cancel();
    relay.await??;

    Ok(())
}
