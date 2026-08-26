//! Whether the queue gauges survive a host that starts sampling before it
//! installs its meter provider.
//!
//! # Why this is a different question from `provider_ordering`
//!
//! That binary asks whether kafkaman's *counters* honour a provider installed
//! after a loop has run. They do, because each loop builds its instruments when
//! it starts, so a later run rebinds.
//!
//! The gauges cannot work that way, and the reason is an API limitation rather
//! than a choice: OpenTelemetry 0.32 offers no way to unregister an observable
//! gauge, so a per-loop registration would leave the previous loop's callback in
//! the SDK's pipeline forever and every restart would add another. So they are
//! registered exactly once per process, which means they bind to whichever
//! provider is installed at that moment — permanently, for the life of the
//! process, with no error and no warning.
//!
//! The consequence is the operator-facing rule this test exists to pin: **install
//! the meter provider before the first sampler starts.** A host that gets the
//! order wrong sees every other kafkaman series arrive normally and the queue
//! series silently missing, which reads as "the sampler is not running" rather
//! than as "the sampler is reporting into a provider nobody is listening to".
//!
//! # Why its own binary
//!
//! The property is about the order of two process-global installs, so it cannot
//! share a process with a test that installs them in the right order.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_test::Harness;
use kafkaman_worker::{run_queue_metrics, QueueMetricsConfig};
use observability_tests::{postgres_for_suite, MetricPipeline, ProductSnapshot, TestResult, SUITE};
use tokio_util::sync::CancellationToken;

fn sampler_config() -> QueueMetricsConfig {
    QueueMetricsConfig {
        refresh_interval: Duration::from_millis(50),
        query_timeout: Duration::from_secs(5),
        ..QueueMetricsConfig::default()
    }
}

#[tokio::test]
async fn gauges_bind_to_the_provider_installed_when_the_first_sampler_starts() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let outbox = vec![harness.outbox_table::<ProductSnapshot>().await?];
    let received = vec![harness.received_table::<ProductSnapshot>().await?];

    let envelope = ProductSnapshot::envelope("gauge-ordering", "a product")
        .try_with_idempotency_key("gauge-ordering")?;
    harness.enqueue(&envelope).await?;

    // 1. The host starts sampling before its telemetry pipeline exists. The
    //    gauges register here, against the no-op provider.
    let first = CancellationToken::new();
    let sampler = tokio::spawn(run_queue_metrics(
        harness.pool().clone(),
        outbox.clone(),
        received.clone(),
        sampler_config(),
        first.clone(),
    ));
    // Long enough for several refreshes at a 50ms interval. There is nothing to
    // wait *on*: the whole point is that these observations reach no exporter,
    // so no exporter can signal that they happened.
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.cancel();
    sampler.await??;

    // 2. The host finishes wiring its pipeline, and starts a second sampler —
    //    which is allowed, because the first has stopped.
    let pipeline = MetricPipeline::install();
    let second = CancellationToken::new();
    let sampler = tokio::spawn(run_queue_metrics(
        harness.pool().clone(),
        outbox,
        received,
        sampler_config(),
        second.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Nothing arrives, and that is the contract rather than a defect. The
    //    gauges were registered in step 1 and cannot be re-registered; the
    //    second sampler refreshes a snapshot that the no-op provider's callbacks
    //    are the only ones reading.
    let collected = pipeline.collected_names();
    second.cancel();
    sampler.await??;
    // Every kafkaman name, not a list of the five gauges: nothing else runs in
    // this binary, so anything at all arriving would mean the gauges rebound.
    let kafkaman: Vec<&String> = collected
        .iter()
        .filter(|name| name.starts_with("kafkaman."))
        .collect();
    assert!(
        kafkaman.is_empty(),
        "queue gauges registered before the provider existed cannot rebind to it; \
         if this now passes, the registration became per-provider and the ordering \
         rule documented on `run_queue_metrics` should be relaxed. Collected: \
         {kafkaman:?}"
    );

    Ok(())
}
