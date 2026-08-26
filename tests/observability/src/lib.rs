//! Shared scaffolding for the observability integration tests.
//!
//! # Why this suite exists separately
//!
//! Every other suite asserts what kafkaman *does*. This one asserts what
//! kafkaman *reports* — which, until it existed, nothing did. The distinction
//! matters mechanically as well as conceptually: observing a metric requires an
//! installed `MeterProvider`, the global provider is process-wide, and an
//! integration test target is one process. That constraint shapes the whole
//! suite, so it gets its own binary per concern rather than sharing one with
//! tests that do not care.
//!
//! Assertion helpers legitimately panic, so the workspace's no-panic lints are
//! relaxed here, matching the durable-send suite.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, PublishAck, RelayConfig};
use kafkaman_test::Harness;
use kafkaman_worker::{BoxError, Publisher};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, Metric, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;

pub use durable_send_tests::{postgres_for_suite, ProductSnapshot, TestPostgres, TestResult};

/// The suite label carried by this suite's containers.
pub const SUITE: &str = "observability";

/// An installed SDK pipeline plus the exporter holding what it collected.
///
/// Returned together because the provider must outlive the assertions: dropping
/// it shuts the reader down, and a flush after that collects nothing.
#[derive(Debug)]
pub struct MetricPipeline {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl MetricPipeline {
    /// Build an SDK pipeline and install it as the global meter provider.
    ///
    /// # One provider per process
    ///
    /// `set_meter_provider` is process-global. A binary that installs twice
    /// replaces the first, and instruments already bound to the first keep
    /// reporting there — which is the very hazard this suite exists to pin. So
    /// each binary in this suite installs exactly once, and any test that needs
    /// a different pipeline needs a different binary.
    pub fn install() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        opentelemetry::global::set_meter_provider(provider.clone());
        Self { provider, exporter }
    }

    /// Flush the pipeline and return what the most recent export carried.
    ///
    /// Only the last export is read, deliberately. Every flush appends another
    /// `ResourceMetrics` to the exporter, and counters are cumulative, so
    /// folding them all together would report a counter that grew once as though
    /// it had grown once per flush. The last export is also the honest question:
    /// it is what a backend would have received just now.
    pub fn collect(&self) -> Vec<Collected> {
        self.provider
            .force_flush()
            .expect("force_flush should succeed");
        let exports = self
            .exporter
            .get_finished_metrics()
            .expect("finished metrics should be readable");
        let Some(latest) = exports.last() else {
            return Vec::new();
        };
        latest
            .scope_metrics()
            .flat_map(|scope| scope.metrics())
            .map(Collected::from_metric)
            .collect()
    }

    /// Flush and return every metric name collected.
    pub fn collected_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .collect()
            .into_iter()
            .map(|metric| metric.name)
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// One instrument by name, or a failure naming what was collected instead.
    ///
    /// Panics rather than returning an `Option` because every caller is an
    /// assertion: a missing instrument is the failure, and the list of what did
    /// arrive is the first thing needed to work out why.
    pub fn metric(&self, name: &str) -> Collected {
        let collected = self.collect();
        collected
            .iter()
            .find(|metric| metric.name == name)
            .unwrap_or_else(|| {
                let seen: Vec<&str> = collected.iter().map(|m| m.name.as_str()).collect();
                panic!("no instrument named {name}; the exporter saw {seen:?}")
            })
            .clone()
    }

    /// Flush until `name` appears, or give up.
    ///
    /// For instruments a background loop produces on its own schedule — the
    /// queue gauges refresh on an interval this test does not control — where
    /// the alternative is sleeping on a guess and hoping it was long enough.
    pub async fn await_metric(&self, name: &str, within: Duration) -> Collected {
        let deadline = Instant::now() + within;
        loop {
            if let Some(metric) = self
                .collect()
                .into_iter()
                .find(|metric| metric.name == name)
            {
                return metric;
            }
            assert!(
                Instant::now() < deadline,
                "instrument {name} did not appear within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// What kind of instrument produced a series.
///
/// Asserted alongside the name because a counter and a gauge with the same name
/// are different contracts to a backend, and a refactor that changes one without
/// the other is exactly the kind of silent break the schedule exists to prevent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Sum,
    Gauge,
    Histogram,
    ExponentialHistogram,
}

/// One collected instrument, flattened to what an assertion needs.
#[derive(Clone, Debug)]
pub struct Collected {
    pub name: String,
    pub unit: String,
    pub kind: Kind,
    pub points: Vec<CollectedPoint>,
}

/// One time series within an instrument.
#[derive(Clone, Debug)]
pub struct CollectedPoint {
    /// Attribute keys to values, stringified: tests compare against literals,
    /// and the typed value adds nothing an assertion can use.
    pub attributes: BTreeMap<String, String>,
    /// The counter or gauge value, or the histogram's sum of observations.
    pub value: f64,
    /// Observations behind this point: the histogram count, or 1 for a counter
    /// or gauge point.
    pub count: u64,
}

impl Collected {
    fn from_metric(metric: &Metric) -> Self {
        let (kind, points) = match metric.data() {
            AggregatedMetrics::F64(data) => flatten(data, |value: f64| value),
            AggregatedMetrics::U64(data) => flatten(data, |value: u64| value as f64),
            AggregatedMetrics::I64(data) => flatten(data, |value: i64| value as f64),
        };
        Self {
            name: metric.name().to_string(),
            unit: metric.unit().to_string(),
            kind,
            points,
        }
    }

    /// The one point carrying all of `attributes`, or a failure showing what was
    /// actually recorded.
    pub fn point_with(&self, attributes: &[(&str, &str)]) -> &CollectedPoint {
        let matching: Vec<&CollectedPoint> = self
            .points
            .iter()
            .filter(|point| {
                attributes.iter().all(|(key, value)| {
                    point.attributes.get(*key).map(String::as_str) == Some(*value)
                })
            })
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "expected exactly one {} point matching {attributes:?}, found {} among {:?}",
            self.name,
            matching.len(),
            self.points
        );
        matching[0]
    }

    /// Every attribute key this instrument emitted, across all of its points.
    pub fn attribute_keys(&self) -> BTreeSet<String> {
        self.points
            .iter()
            .flat_map(|point| point.attributes.keys().cloned())
            .collect()
    }
}

/// Reduce one typed `MetricData` to a kind and a flat list of points.
fn flatten<T: Copy>(
    data: &MetricData<T>,
    to_f64: impl Fn(T) -> f64,
) -> (Kind, Vec<CollectedPoint>) {
    match data {
        MetricData::Sum(sum) => (
            Kind::Sum,
            sum.data_points()
                .map(|point| CollectedPoint {
                    attributes: attributes(point.attributes()),
                    value: to_f64(point.value()),
                    count: 1,
                })
                .collect(),
        ),
        MetricData::Gauge(gauge) => (
            Kind::Gauge,
            gauge
                .data_points()
                .map(|point| CollectedPoint {
                    attributes: attributes(point.attributes()),
                    value: to_f64(point.value()),
                    count: 1,
                })
                .collect(),
        ),
        MetricData::Histogram(histogram) => (
            Kind::Histogram,
            histogram
                .data_points()
                .map(|point| CollectedPoint {
                    attributes: attributes(point.attributes()),
                    value: to_f64(point.sum()),
                    count: point.count(),
                })
                .collect(),
        ),
        MetricData::ExponentialHistogram(histogram) => (
            Kind::ExponentialHistogram,
            histogram
                .data_points()
                .map(|point| CollectedPoint {
                    attributes: attributes(point.attributes()),
                    value: to_f64(point.sum()),
                    count: point.count() as u64,
                })
                .collect(),
        ),
    }
}

fn attributes<'a>(pairs: impl Iterator<Item = &'a KeyValue>) -> BTreeMap<String, String> {
    pairs
        .map(|pair| (pair.key.as_str().to_string(), pair.value.to_string()))
        .collect()
}

/// A [`Publisher`] that acknowledges every row and announces that it did.
///
/// The signal is what makes the loop tests deterministic. Driving a run loop by
/// sleeping and hoping a cycle happened is the standard way these tests turn
/// flaky; waiting on a message the loop itself sent is not. This mirrors the
/// mpsc-signalled handler the dispatcher loop test already uses.
#[derive(Clone, Debug)]
pub struct SignallingPublisher {
    published: mpsc::UnboundedSender<()>,
}

impl SignallingPublisher {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<()>) {
        let (published, rx) = mpsc::unbounded_channel();
        (Self { published }, rx)
    }
}

#[async_trait]
impl Publisher for SignallingPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> Result<PublishAck, BoxError> {
        self.published
            .send(())
            .expect("publish receiver should still be alive");
        Ok(PublishAck {
            topic: row.row.topic.clone(),
            partition: 0,
            offset: 0,
        })
    }
}

/// Enqueue one row, run the real relay loop until it publishes, and stop.
///
/// Shared by every binary that needs metrics a *loop* produced. Driving
/// `relay_once` directly would be simpler and would prove nothing: instruments
/// are owned by the loop, and the public single-cycle helper deliberately
/// records no scheduler counters at all.
///
/// The publisher signals as it acknowledges, so the loop is cancelled once work
/// is known to have happened rather than after an interval someone hoped was
/// long enough.
pub async fn relay_until_published(harness: &Harness, key: &str) -> TestResult {
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let envelope = ProductSnapshot::envelope(key, "a product").try_with_idempotency_key(key)?;
    harness.enqueue(&envelope).await?;

    let (publisher, mut published) = SignallingPublisher::new();
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        table,
        RelayConfig {
            poll_interval: Duration::from_millis(25),
            ..RelayConfig::default()
        },
        shutdown.clone(),
    ));

    next(&mut published, "the relay should publish the enqueued row").await;
    shutdown.cancel();
    worker.await??;
    Ok(())
}

/// How long a bounded wait gives a loop before calling it stuck.
///
/// Generous, because these run against containers on a loaded machine and a
/// flaky suite teaches people to rerun rather than to read. Finite, because the
/// alternative is worse: an unbounded `recv().await` on a loop that stopped
/// publishing hangs until the whole test binary is killed, and what the operator
/// then sees is a timeout with no failing assertion and no indication which of
/// the waits it was.
pub const LOOP_TIMEOUT: Duration = Duration::from_secs(60);

/// Wait for the next signal from a loop, or fail saying which wait gave up.
///
/// Every channel wait in this suite goes through here. A regression that stops a
/// loop signalling should read as a named assertion failure, not as a suite that
/// never finishes.
pub async fn next<T>(channel: &mut mpsc::UnboundedReceiver<T>, expectation: &str) -> T {
    match tokio::time::timeout(LOOP_TIMEOUT, channel.recv()).await {
        Ok(Some(value)) => value,
        Ok(None) => panic!("{expectation}, but the channel closed first"),
        Err(_) => panic!("{expectation}, but nothing arrived within {LOOP_TIMEOUT:?}"),
    }
}

/// An installed tracer pipeline plus the exporter holding the spans it finished.
///
/// The trace counterpart of [`MetricPipeline`], and it carries one extra
/// responsibility: spans reach OpenTelemetry through the `tracing` subscriber,
/// so this installs a subscriber as well as a provider. A binary using this must
/// therefore not install its own.
#[derive(Debug)]
pub struct TracePipeline {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
}

impl TracePipeline {
    /// Build a tracer pipeline, install it globally, and bridge `tracing` into
    /// it.
    ///
    /// A simple exporter rather than a batch one: the test wants every span the
    /// moment it ends, and a batch processor would add a scheduling delay
    /// between the work finishing and the assertion being able to see it.
    pub fn install() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        opentelemetry::global::set_tracer_provider(provider.clone());
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("kafkaman"))),
        )
        .expect("no other subscriber should be installed in this binary");
        Self { provider, exporter }
    }

    /// Every span finished so far.
    pub fn finished(&self) -> Vec<SpanData> {
        self.provider
            .force_flush()
            .expect("force_flush should succeed");
        self.exporter
            .get_finished_spans()
            .expect("finished spans should be readable")
    }

    /// The one span with this name, or a failure listing what was recorded.
    ///
    /// Exactly one, deliberately: these tests drive one message through, so two
    /// spans of the same name means something ran twice and every id assertion
    /// after it would be picking arbitrarily between them.
    pub fn span(&self, name: &str) -> SpanData {
        let finished = self.finished();
        let mut matching: Vec<SpanData> = finished
            .iter()
            .filter(|span| span.name == name)
            .cloned()
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "expected exactly one {name} span, found {} among {:?}",
            matching.len(),
            finished.iter().map(|span| &span.name).collect::<Vec<_>>()
        );
        matching.remove(0)
    }

    /// Whether any span with this name has been recorded.
    pub fn has_span(&self, name: &str) -> bool {
        self.finished().iter().any(|span| span.name == name)
    }
}
