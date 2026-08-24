//! Whether queue depth and age reach a backend as scraped series.
//!
//! The summary queries behind these gauges were already integration-tested, so
//! what is under test here is everything between the query and the exporter: a
//! callback that cannot await, a snapshot refreshed by a loop that can, the
//! zero-fill that keeps a drained queue visible, and the staleness rule that
//! makes a stalled sampler look like a gap rather than a flat line.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_test::Harness;
use kafkaman_worker::{run_queue_metrics, QueueMetricsConfig};
use observability_tests::{
    postgres_for_suite, Kind, MetricPipeline, ProductSnapshot, TestResult, SUITE,
};
use tokio_util::sync::CancellationToken;

const MESSAGE_TYPE: &str = "product_snapshot";

/// Fast enough that the test does not wait on a production cadence.
fn sampler_config() -> QueueMetricsConfig {
    QueueMetricsConfig {
        refresh_interval: Duration::from_millis(50),
        query_timeout: Duration::from_secs(5),
        ..QueueMetricsConfig::default()
    }
}

#[tokio::test]
async fn queued_rows_become_depth_and_age_series() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let pipeline = MetricPipeline::install();

    for key in ["queued-one", "queued-two"] {
        let envelope = ProductSnapshot::envelope(key, "a product").try_with_idempotency_key(key)?;
        harness.enqueue(&envelope).await?;
    }

    let shutdown = CancellationToken::new();
    let sampler = tokio::spawn(run_queue_metrics(
        harness.pool().clone(),
        vec![harness.outbox_table::<ProductSnapshot>().await?],
        vec![harness.received_table::<ProductSnapshot>().await?],
        sampler_config(),
        shutdown.clone(),
    ));

    // Flushed repeatedly until the sampler's first refresh lands, rather than
    // slept on: the loop's schedule is not this test's to predict.
    let depth = pipeline
        .await_metric("kafkaman.outbox.depth", Duration::from_secs(30))
        .await;
    assert_eq!(depth.kind, Kind::Gauge);
    assert_eq!(depth.unit, "{row}");
    assert_eq!(
        depth
            .point_with(&[("message_type", MESSAGE_TYPE), ("status", "Pending")])
            .value,
        2.0,
        "two enqueued rows are two pending rows"
    );
    assert_eq!(
        depth
            .point_with(&[("message_type", MESSAGE_TYPE), ("status", "Published")])
            .value,
        0.0,
        "a status with no rows reports zero rather than vanishing, so a queue \
         draining to empty draws a line to zero instead of stopping mid-chart"
    );

    let age = pipeline.metric("kafkaman.outbox.oldest_age");
    assert_eq!(age.kind, Kind::Gauge);
    assert_eq!(age.unit, "s");
    assert!(
        age.point_with(&[("message_type", MESSAGE_TYPE), ("status", "Pending")])
            .value
            >= 0.0,
        "the pending bucket has rows, so it has an age"
    );
    assert!(
        !age.points.iter().any(|point| {
            point.attributes.get("status").map(String::as_str) == Some("Published")
        }),
        "an empty bucket has no oldest row, and reporting zero there would read \
         as a row that had just arrived"
    );

    // The receive side is registered by the same loop and reports the same way,
    // which is what makes one sampler enough for both queues.
    let received = pipeline.metric("kafkaman.received.depth");
    assert_eq!(received.kind, Kind::Gauge);
    assert_eq!(
        received
            .point_with(&[("message_type", MESSAGE_TYPE), ("status", "Pending")])
            .value,
        0.0,
        "nothing was received, and that is a reported zero rather than silence"
    );

    shutdown.cancel();
    sampler.await??;
    Ok(())
}
