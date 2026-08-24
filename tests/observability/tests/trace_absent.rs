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
    postgres_for_suite, ProductSnapshot, SignallingPublisher, TestResult, SUITE,
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
    published
        .recv()
        .await
        .expect("a row without trace context still publishes");
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
