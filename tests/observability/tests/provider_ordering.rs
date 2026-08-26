//! Whether kafkaman's instruments honour a `MeterProvider` installed after a
//! run loop has already recorded.
//!
//! # What this pins, and why it is worth a container
//!
//! An OpenTelemetry instrument binds to whichever `MeterProvider` is installed
//! at the moment the instrument is *created*, and the API offers no way to
//! rebind it. An instrument built before the host installs its SDK is therefore
//! a no-op for the life of the process, silently — no error, no warning, no
//! metrics.
//!
//! That is not hypothetical. It reproduces in twenty lines against the raw API:
//! build a counter from `global::meter()`, install an `SdkMeterProvider`, record
//! on the counter, flush — and nothing is collected, while a counter built after
//! the install collects normally.
//!
//! What this test adds is whether *kafkaman* is exposed to it. That depends on
//! where kafkaman constructs its instruments, which is a property of the run
//! loops rather than of the API, and so needs the real loops and therefore a
//! real database.
//!
//! The ordering under test is the one a host actually produces: an application
//! that starts a relay before it finishes building its telemetry pipeline. That
//! is a plausible startup order, because the relay is the application's job and
//! telemetry is infrastructure.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_test::Harness;
use observability_tests::{
    postgres_for_suite, relay_until_published, MetricPipeline, TestResult, SUITE,
};

#[tokio::test]
async fn a_provider_installed_after_a_relay_has_run_still_collects() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;

    // 1. The host starts kafkaman first. Any instrument kafkaman builds here
    //    binds to the no-op provider, because no SDK exists yet.
    relay_until_published(&harness, "before-provider").await?;

    // 2. The host finishes wiring its telemetry pipeline.
    let pipeline = MetricPipeline::install();

    // 3. The relay runs again, under the installed pipeline.
    relay_until_published(&harness, "after-provider").await?;

    // 4. The second run's metrics must reach the exporter. Against instruments
    //    cached process-wide at first use, they do not: every counter is still
    //    bound to the no-op provider from step 1, and this assertion fails with
    //    an empty list.
    let collected = pipeline.collected_names();
    assert!(
        collected
            .iter()
            .any(|name| name == "kafkaman.scheduler.cycles"),
        "a relay cycle run after the provider was installed should be collected, \
         but the exporter only saw {collected:?}"
    );

    // 5. And only the second run's. The first row published in step 1 is gone,
    //    not buffered — an instrument bound to the no-op provider discards, it
    //    does not queue. Asserting the count rather than mere presence is what
    //    separates "the second run collected" from "both runs collected", and
    //    only one of those is what an instrument rebuilt per loop can do.
    let published = pipeline.metric("kafkaman.outbox.time_to_publish");
    let total: u64 = published.points.iter().map(|point| point.count).sum();
    assert_eq!(
        total, 1,
        "each run publishes exactly one row, and only the run after the install \
         is observable — a total of 2 would mean the pre-install observation had \
         been held somewhere, and a total of 0 that the rebind never happened"
    );

    Ok(())
}
