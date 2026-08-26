//! What a relay cycle actually reports: names, kinds, units, attributes, values.
//!
//! # Why this is worth a whole binary
//!
//! Instrument names, kinds, units and attribute keys are a compatibility surface
//! — a dashboard, an alert, and a recording rule are all built on them, and none
//! of those are in this repository to break loudly. Until this test existed the
//! only thing standing behind that surface was reading the code, which is how
//! the `ingest.records` double-count shipped in the first place.
//!
//! So the assertions here are deliberately literal. They restate the schedule in
//! `wiki/decisions/metric-instrument-and-attribute-schema.decision.md` rather
//! than deriving it from the code under test, because a test that asks the code
//! what it emits and then agrees with it proves nothing.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_test::Harness;
use observability_tests::{
    postgres_for_suite, relay_until_published, Kind, MetricPipeline, TestResult, SUITE,
};

/// The fixture's declared type and topic, restated so a change to either shows
/// up here as a failing attribute rather than as a silently renamed series.
const MESSAGE_TYPE: &str = "product_snapshot";
const TOPIC: &str = "products";

#[tokio::test]
async fn a_relay_cycle_reports_the_scheduled_instruments() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;

    // Installed first: instruments bind to whichever provider exists when the
    // loop starts, so a pipeline installed afterwards would see nothing.
    let pipeline = MetricPipeline::install();
    relay_until_published(&harness, "metrics-surface").await?;

    let cycles = pipeline.metric("kafkaman.scheduler.cycles");
    assert_eq!(cycles.kind, Kind::Sum);
    assert_eq!(cycles.unit, "{cycle}");
    assert!(
        cycles
            .point_with(&[("scheduler", "relay"), ("message_type", MESSAGE_TYPE)])
            .value
            >= 1.0,
        "the relay ran at least one cycle"
    );

    let rows = pipeline.metric("kafkaman.scheduler.rows");
    assert_eq!(rows.kind, Kind::Sum);
    assert_eq!(rows.unit, "{row}");
    assert_eq!(
        rows.point_with(&[
            ("scheduler", "relay"),
            ("message_type", MESSAGE_TYPE),
            ("status", "published"),
        ])
        .value,
        1.0,
        "exactly one row was enqueued, so exactly one was published"
    );

    let publish = pipeline.metric("kafkaman.relay.publish.duration");
    assert_eq!(publish.kind, Kind::Histogram);
    assert_eq!(publish.unit, "s");
    let published = publish.point_with(&[("topic", TOPIC), ("outcome", "published")]);
    assert_eq!(published.count, 1, "one publish, one observation");
    assert!(
        published.value >= 0.0,
        "a duration is never negative, however fast the fake publisher is"
    );
    assert_eq!(
        published.attributes.get("messaging.destination.name"),
        Some(&TOPIC.to_owned()),
        "the topic travels under the standard key as well as ours, so a backend \
         can group kafkaman with every other messaging library"
    );
    assert_eq!(
        published.attributes.get("messaging.system"),
        Some(&"kafka".to_owned())
    );

    let time_to_publish = pipeline.metric("kafkaman.outbox.time_to_publish");
    assert_eq!(time_to_publish.kind, Kind::Histogram);
    assert_eq!(time_to_publish.unit, "s");
    let latency = time_to_publish.point_with(&[("message_type", MESSAGE_TYPE)]);
    assert_eq!(latency.count, 1);
    assert!(
        latency.value >= 0.0,
        "occurred_at precedes the acknowledgement, so the latency is non-negative"
    );

    // And nothing else. The assertions above say every scheduled instrument is
    // present; this one says the schedule is the whole surface. An instrument
    // that appears without being scheduled is the same compatibility problem as
    // one that disappears — a dashboard is built on whatever it finds, and a
    // series added by accident is a series somebody depends on before anyone
    // notices it was never meant to exist.
    //
    // The list is the relay's own instruments and no others. `scheduler.errors`
    // is on it because the loop builds it whether or not anything goes wrong;
    // whether an instrument with no measurements reaches the exporter at all is
    // the SDK's business, and not something this test should assert either way.
    // What it does assert is that no *other* loop's series appear: no ingester
    // and no queue sampler ran here, so a `kafkaman.kafka.*` or
    // `kafkaman.queue.*` series would mean an instrument is being recorded from
    // somewhere that is not running.
    const RELAY_SCHEDULE: [&str; 5] = [
        "kafkaman.scheduler.cycles",
        "kafkaman.scheduler.rows",
        "kafkaman.scheduler.errors",
        "kafkaman.relay.publish.duration",
        "kafkaman.outbox.time_to_publish",
    ];
    let collected = pipeline.collected_names();
    let unscheduled: Vec<&String> = collected
        .iter()
        .filter(|name| !RELAY_SCHEDULE.contains(&name.as_str()))
        .collect();
    assert!(
        unscheduled.is_empty(),
        "a relay cycle reported instruments outside its schedule: {unscheduled:?}"
    );

    Ok(())
}
