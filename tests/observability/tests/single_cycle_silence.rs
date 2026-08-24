//! Whether the public single-cycle helper records anything.
//!
//! `relay_once` does the work of one relay cycle and is public precisely so that
//! tests and operators can drive it by hand. It deliberately records no
//! scheduler counters: a process that calls it a hundred times while
//! investigating something must not thereby report a hundred relay cycles into
//! the same series a deployment's dashboard reads.
//!
//! That separation is one line away from being undone by accident every time an
//! instrument is added — the natural place to put a new recording is next to the
//! work, which is exactly here — so it is pinned rather than commented.
//!
//! # Why its own binary
//!
//! The assertion is that *nothing* was recorded, and the global meter provider
//! is process-wide. Any other test sharing this process would record into the
//! same provider and this test would fail for reasons that have nothing to do
//! with `relay_once`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_test::Harness;
use observability_tests::{postgres_for_suite, MetricPipeline, ProductSnapshot, TestResult, SUITE};

#[tokio::test]
async fn calling_relay_once_directly_records_nothing() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let pipeline = MetricPipeline::install();

    let envelope = ProductSnapshot::envelope("silent-cycle", "a product")
        .try_with_idempotency_key("silent-cycle")?;
    harness.enqueue(&envelope).await?;
    let stats = harness.relay_once::<ProductSnapshot>().await?;
    assert_eq!(stats.published, 1, "the helper still does the work");

    let collected = pipeline.collected_names();
    assert!(
        !collected.iter().any(|name| name.starts_with("kafkaman.")),
        "relay_once must not record, but the exporter saw {collected:?}"
    );

    Ok(())
}
