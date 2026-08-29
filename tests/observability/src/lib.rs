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
//! What it asserts is kafkaman's own telemetry — instrument and span names,
//! attributes, propagation, and the OTLP wire format — independent of whichever
//! example currently ships. The example *binaries'* telemetry lifecycle is a
//! different question with a different owner: `tests/example-telemetry`.
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

pub use durable_send_tests::{
    postgres_for_suite, ProductSnapshot, RegionalProduct, TestPostgres, TestResult,
};

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
        let (provider, exporter) = Self::provider();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("kafkaman"))),
        )
        .expect("no other subscriber should be installed in this binary");
        Self { provider, exporter }
    }

    /// The same pipeline, gated to the filter a default deployment runs.
    ///
    /// [`install`](Self::install) applies no filter at all, so it records the
    /// `kafkaman::internal` tier alongside the phase spans. That is deliberate
    /// for the trace-shape tests: the tier broke the durable trace context twice
    /// and those tests are its regression proof.
    ///
    /// It is wrong for a test asserting that a phase span is a *root*. Under the
    /// tier, `enqueue` carries a function span and calls `enqueue_on_connection`,
    /// which opens `kafkaman.enqueue` — so the phase span has a parent, quite
    /// correctly, and is the root only at the `info` a default deployment runs.
    pub fn install_at_default_filter() -> Self {
        use tracing_subscriber::Layer as _;

        let (provider, exporter) = Self::provider();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("kafkaman"))
                    // Per layer rather than registry-wide, matching how
                    // `kafkaman_otel::init` composes its own.
                    .with_filter(tracing_subscriber::EnvFilter::new("info")),
            ),
        )
        .expect("no other subscriber should be installed in this binary");
        Self { provider, exporter }
    }

    /// The provider and exporter both constructors share.
    fn provider() -> (SdkTracerProvider, InMemorySpanExporter) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        opentelemetry::global::set_tracer_provider(provider.clone());
        (provider, exporter)
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

/// One message driven through both durable gaps and a real broker.
///
/// The drive is shared by `trace_propagation` and `trace_parented_handoff`
/// because the *only* thing those two tests disagree about is the shape they
/// expect at the broker hop. Two copies of the drive would let one of them start
/// exercising a different path than the other while both still passed, which is
/// precisely the confusion the pair exists to prevent.
#[cfg(feature = "redpanda")]
#[derive(Debug)]
pub struct KafkaRoundTrip {
    /// The installed pipeline, holding every span the drive produced.
    ///
    /// Kept alive by the caller: dropping it shuts the provider down, and a
    /// flush after that collects nothing.
    pub pipeline: TracePipeline,
    /// The trace the caller's span opened, which `kafkaman.enqueue` must join.
    pub caller_trace_id: opentelemetry::trace::TraceId,
}

/// Enqueue one message, relay it through Redpanda, ingest it, and dispatch it.
///
/// `handoff` is the consumer-side policy under test. `consumer_group` must be
/// unique per run: a shared group splits the single partition and the loser idles
/// while reporting healthy.
///
/// Returns once every span exists. The containers are dropped before returning —
/// everything the assertions read is already in the pipeline's memory.
#[cfg(feature = "redpanda")]
pub async fn drive_kafka_round_trip(
    handoff: kafkaman_test::kafkaman_config::KafkaTraceHandoff,
    consumer_group: &str,
) -> TestResult<KafkaRoundTrip> {
    use durable_send_tests::start_redpanda_harness;
    use kafkaman_core::KafkaMessage;
    use kafkaman_rdkafka::{RdkafkaConsumer, RdkafkaPublisher};
    use kafkaman_sqlx::{dispatch_once, MessageRouter};
    use tracing::Instrument as _;

    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let outbox_table = harness.outbox_table::<ProductSnapshot>().await?;
    let received_table = harness.received_table::<ProductSnapshot>().await?;

    let pipeline = TracePipeline::install();

    // Stands in for the span a host would already have open — an HTTP handler,
    // a job runner. The point of the enqueue span is that it descends from
    // whatever the caller was doing, so the trace starts before kafkaman.
    let caller = tracing::info_span!("test.request");
    let envelope = ProductSnapshot::envelope("traced-product", "a traced product")
        .try_with_idempotency_key("traced-product")?;
    // `.instrument` rather than a held `enter()` guard. The guard is `!Send` and
    // an entered span left open across an `await` attributes whatever else runs
    // on the thread to the caller's trace — the exact failure `kafkaman_core`'s
    // `attach` documentation warns about, and a helper that models the wrong
    // pattern is a helper somebody copies.
    harness
        .enqueue(&envelope)
        .instrument(caller.clone())
        .await?;
    let caller_trace_id = trace_id_of(&caller);

    // Publish through a real broker, from a loop, exactly as a deployment would.
    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let shutdown = CancellationToken::new();
    let relay = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        outbox_table,
        RelayConfig {
            poll_interval: Duration::from_millis(50),
            ..RelayConfig::default()
        },
        shutdown.clone(),
    ));
    await_span(&pipeline, "kafkaman.relay.publish", Duration::from_secs(30)).await;
    shutdown.cancel();
    relay.await??;

    // Consume it back, which is where the Kafka half of the propagation lands.
    let consumer = RdkafkaConsumer::from_brokers(&brokers, consumer_group)?;
    consumer.subscribe(&[ProductSnapshot::TOPIC])?;

    let mut cfg = harness.config();
    cfg.observability.defaults.kafka_trace_handoff = handoff;

    let ingest_shutdown = CancellationToken::new();
    let ingester = {
        let pool = harness.pool().clone();
        let token = ingest_shutdown.clone();
        tokio::spawn(async move {
            consumer
                .run_ingester::<ProductSnapshot>(&pool, &cfg, Duration::from_millis(50), token)
                .await
        })
    };
    // Stopped once its span exists, rather than after an interval someone
    // guessed at — the span is the thing under test and also the completion
    // signal.
    await_span(&pipeline, "kafkaman.ingest", Duration::from_secs(30)).await;
    ingest_shutdown.cancel();
    let ingest_stats = ingester.await??;
    assert_eq!(ingest_stats.consumed, 1, "one record was published");

    // Dispatch it, which crosses the second durable gap.
    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let dispatch = dispatch_once(
        harness.pool(),
        &received_table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(dispatch.processed, 1, "the handler ran");

    // The whole `kafkaman::internal` tier must be live for this drive — both its
    // `info` half and its `debug` half — because that is what makes the
    // assertions downstream a regression test for it.
    //
    // It is live because `TracePipeline` installs no filter at all, which is a
    // stronger condition than a service ever runs under: at `RUST_LOG=info` only
    // the message-path half is on. Asserting against the wider setting is
    // deliberate, since the hazard is a function span nesting between a capture
    // and the phase span it means to capture, and a function that is `debug`
    // today can be promoted tomorrow.
    //
    // This fails loudly if the tier ever stops being exercised, rather than
    // letting the coverage evaporate silently. Twice during its introduction the
    // tier moved the stored trace context off the phase span and onto a private
    // function, so what these tests prove *while it is on* is the whole point.
    //
    // Detected structurally rather than by name: every deliberate span is
    // `kafkaman.*`, `db.query <summary>`, or `METHOD /route`, so a bare
    // identifier can only have come from `#[instrument]` on a function. That
    // survives the renames the compatibility note promises.
    assert!(
        pipeline
            .finished()
            .iter()
            .any(|span| !span.name.contains('.') && !span.name.contains(' ')),
        "no internal-tier span was recorded, so these assertions no longer cover \
         the interaction that broke the durable trace context twice. Recorded: {:?}",
        pipeline
            .finished()
            .iter()
            .map(|span| span.name.clone())
            .collect::<Vec<_>>()
    );

    Ok(KafkaRoundTrip {
        pipeline,
        caller_trace_id,
    })
}

/// The trace id a `tracing` span belongs to.
pub fn trace_id_of(span: &tracing::Span) -> opentelemetry::trace::TraceId {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    span.context().span().span_context().trace_id()
}

/// Poll until a span with `name` has been recorded.
///
/// The relay runs on its own schedule; waiting for the artifact it produces is
/// the only thing a caller can honestly wait on.
pub async fn await_span(pipeline: &TracePipeline, name: &str, within: Duration) {
    let deadline = Instant::now() + within;
    while !pipeline.has_span(name) {
        assert!(
            Instant::now() < deadline,
            "no {name} span was recorded within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
