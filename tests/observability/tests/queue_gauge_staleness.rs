//! What the gauges do when the sampler stops refreshing.
//!
//! The intuitive design is a gap: stop observing once a snapshot is too old, so
//! a stalled sampler shows as a hole rather than a flat line at a number that
//! quietly stopped being true. **It does not work**, and this test is where that
//! was found out — an asynchronous gauge under cumulative temporality
//! republishes its last recorded value on every collection cycle whether or not
//! the callback observed anything, so observing nothing produces the flat line
//! regardless.
//!
//! The first half of this test pins that SDK behaviour, because it is the reason
//! the design is what it is and it is invisible from kafkaman's side of the API.
//! The second half pins the answer: `kafkaman.queue.sample_age` keeps climbing,
//! so a depth reading always arrives next to a statement of how old it is. That
//! is the series to alert on.
//!
//! # Why its own binary
//!
//! It stops a sampler and waits for the consequences, and the global meter
//! provider is process-wide — any other test in the process would keep its own
//! sampler refreshing and hide exactly what this one is looking for.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_test::Harness;
use kafkaman_worker::{run_queue_metrics, QueueMetricsConfig};
use observability_tests::{postgres_for_suite, MetricPipeline, ProductSnapshot, TestResult, SUITE};
use tokio_util::sync::CancellationToken;

/// How long the sampler is left stopped before the age is checked. Long enough
/// to be unambiguous against a refresh interval of 50ms.
const STALLED_FOR: Duration = Duration::from_millis(600);

#[tokio::test]
async fn a_stalled_sampler_keeps_its_values_and_reports_their_age() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;

    // Installed before the sampler starts: the gauges bind to whichever provider
    // exists when they are registered, which is the same ordering contract every
    // other kafkaman instrument follows.
    let pipeline = MetricPipeline::install();

    let envelope =
        ProductSnapshot::envelope("stale-check", "a product").try_with_idempotency_key("stale")?;
    harness.enqueue(&envelope).await?;

    let shutdown = CancellationToken::new();
    let sampler = tokio::spawn(run_queue_metrics(
        harness.pool().clone(),
        vec![harness.outbox_table::<ProductSnapshot>().await?],
        Vec::new(),
        QueueMetricsConfig {
            refresh_interval: Duration::from_millis(50),
            query_timeout: Duration::from_secs(5),
            ..QueueMetricsConfig::default()
        },
        shutdown.clone(),
    ));

    let depth = pipeline
        .await_metric("kafkaman.outbox.depth", Duration::from_secs(30))
        .await;
    assert_eq!(
        depth
            .point_with(&[("message_type", "product_snapshot"), ("status", "Pending")])
            .value,
        1.0,
        "the sampler should be reporting the enqueued row before it is stopped"
    );

    let fresh_age = pipeline.metric("kafkaman.queue.sample_age");
    assert_eq!(fresh_age.unit, "s");
    let fresh_seconds = fresh_age
        .points
        .first()
        .expect("a refreshed sampler reports a sample age")
        .value;

    // Stopping the loop is what a permanently failing refresh looks like from
    // the callback's side: the snapshot stays in memory and stops being renewed.
    shutdown.cancel();
    sampler.await??;
    tokio::time::sleep(STALLED_FOR).await;

    let after = pipeline.metric("kafkaman.outbox.depth");
    assert_eq!(
        after
            .point_with(&[("message_type", "product_snapshot"), ("status", "Pending")])
            .value,
        1.0,
        "the last value keeps being republished — which is exactly why the age \
         series exists rather than a gap"
    );

    let stale_age = pipeline
        .metric("kafkaman.queue.sample_age")
        .points
        .first()
        .expect("the sample age is still reported while the snapshot exists")
        .value;
    assert!(
        stale_age >= STALLED_FOR.as_secs_f64(),
        "the sample age should have grown past {STALLED_FOR:?} while the sampler \
         was stopped, but it read {stale_age}s (it was {fresh_seconds}s when fresh)"
    );

    Ok(())
}
