//! Runs the example service *binaries* and captures the telemetry they export.
//!
//! # Which layer owns which proof
//!
//! Four suites touch this material, and they are deliberately not
//! interchangeable:
//!
//! - `tests/distributed-cache` — the example domain, over real HTTP. It calls
//!   `example_order::start` and `example_product::start_with`, which is the right
//!   shape for debugging domain failures and means it never executes `main.rs`.
//! - `tests/observability` — kafkaman's own telemetry semantics and its OTLP wire
//!   format, independent of whichever example currently ships.
//! - `crates/kafkaman-otel/tests` — the reusable host pipeline in isolation.
//! - **This suite** — the example binaries' telemetry lifecycle: environment
//!   discovery, `kafkaman_otel::init`, runtime construction, signal handling,
//!   drain, and provider shutdown.
//!
//! The gap this closes is narrow and was real: a crate can be correct while a
//! binary calls it in the wrong order or forgets to flush it, and none of the
//! other three execute the code where that would show. Telemetry does not belong
//! in `tests/distributed-cache` for the same reason it belongs here — that suite
//! runs both services in one process, and OpenTelemetry providers and
//! subscribers are process-global, so installing them there would test a
//! topology the examples do not run.
//!
//! # What makes the flush claim honest
//!
//! Every child runs with its metric interval and both batch schedule delays
//! pushed an hour out, so no scheduled export can fire inside the life of a
//! test. Anything captured was therefore forced out by `Telemetry::shutdown()`.
//! At the SDK defaults — 15s for metrics, 5s for spans, 1s for logs — a
//! Docker-backed run outlives all three, and these assertions would pass just as
//! happily against a binary that never flushes at all.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distributed_cache_tests::Cluster;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::resource::v1::Resource;
use otlp_capture::Capture;
use rustix::process::{kill_process, Pid, Signal};
use uuid::Uuid;

pub use distributed_cache_tests::{BoxError, TestResult};

/// The `service.name` each binary passes to `kafkaman_otel::init`.
///
/// Asserted rather than derived: if a binary changes what it calls itself, a
/// backend stops being able to tell the two services apart, and that is a
/// regression this gate exists to catch.
pub const ORDER_SERVICE: &str = "kafkaman-example-order";
pub const PRODUCT_SERVICE: &str = "kafkaman-example-product";

/// Where the built binaries are, passed in rather than discovered.
///
/// `CARGO_BIN_EXE_*` is only defined for integration tests of the package that
/// declares the binary, so it is unavailable from here and there is no
/// conditional to write. `just examples telemetry-test` builds both and sets
/// these.
const ORDER_BIN: &str = "EXAMPLE_ORDER_BIN";
const PRODUCT_BIN: &str = "EXAMPLE_PRODUCT_BIN";

/// An hour, in milliseconds, for every export schedule the SDK reads.
///
/// Longer than any run of this suite, which is the point: see the module note.
const EXPORT_INTERVAL_MS: &str = "3600000";

/// How long a binary has to boot, migrate, verify its topics, and serve
/// `/health`. Generous because the first cycle also pays for the consumer
/// group's first assignment.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a binary has to drain and flush after SIGINT before it is killed.
///
/// Matches the `stop_grace_period` the compose file gives the same binaries, and
/// for the same reason: shutdown is drain-then-flush, and three exporters get a
/// final export window.
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);

const POLL: Duration = Duration::from_millis(100);

/// The workspace root, from this package's manifest.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Resolve one of the binary-path variables, or explain how to set it.
///
/// Deliberately an error rather than a skip. A gate that quietly passes when it
/// did not run is worse than no gate.
fn binary_path(variable: &str) -> TestResult<PathBuf> {
    let raw = std::env::var(variable).map_err(|_| -> BoxError {
        format!(
            "{variable} must point at a built example binary. \
             Run this through `just examples telemetry-test`, which builds both \
             binaries and sets it."
        )
        .into()
    })?;
    let path = PathBuf::from(raw);
    if !path.is_file() {
        return Err(format!(
            "{variable} points at {}, which is not a file",
            path.display()
        )
        .into());
    }
    Ok(path)
}

/// Take a loopback port from the OS and hold it until the child is spawned.
///
/// Returned as a live listener on purpose. Binding, closing, and passing the
/// number along leaves a window in which anything else on the machine can take
/// it; holding it until the last moment before `spawn` makes that window as
/// small as this can make it. Hard-coding 3001/3002 is not an option — a
/// developer may have `just examples` running.
fn reserve_port() -> TestResult<(std::net::TcpListener, u16)> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok((listener, port))
}

/// A child's stdout and stderr, drained as they arrive.
///
/// Drained by threads rather than read at exit because a pipe nobody reads fills
/// at around 64KB and blocks the writer — and a service logging at `info` under
/// load reaches that. A harness that deadlocks the process it is testing would
/// look exactly like the shutdown bug this suite is written to find.
#[derive(Clone, Debug, Default)]
pub struct ServiceLog(Arc<Mutex<Vec<String>>>);

impl ServiceLog {
    fn follow(&self, stream: impl Read + Send + 'static, tag: &'static str) {
        let sink = Arc::clone(&self.0);
        std::thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                let Ok(line) = line else { return };
                // A poisoned sink means a reader thread panicked; losing log
                // lines is the right response to that, not a second panic.
                if let Ok(mut held) = sink.lock() {
                    held.push(format!("{tag} {line}"));
                }
            }
        });
    }

    /// The last `lines` lines, for a failure message.
    #[must_use]
    pub fn tail(&self, lines: usize) -> String {
        let Ok(held) = self.0.lock() else {
            return "<the log sink was poisoned>".to_owned();
        };
        let from = held.len().saturating_sub(lines);
        held[from..].join("\n")
    }
}

/// How to start one example binary.
struct Spec {
    /// The example's directory name under `examples/`, which is also its label.
    label: &'static str,
    service: &'static str,
    binary: &'static str,
    database_url: String,
    brokers: String,
    consumer_group: String,
    otlp_endpoint: String,
}

/// One example binary, running.
#[derive(Debug)]
pub struct Service {
    label: &'static str,
    service: &'static str,
    base_url: String,
    child: Option<Child>,
    log: ServiceLog,
}

impl Service {
    fn spawn(spec: &Spec) -> TestResult<Self> {
        let binary = binary_path(spec.binary)?;
        let (reservation, port) = reserve_port()?;

        // The binary's own directory, because `main.rs` calls
        // `Config::discover()`, which walks *up* from the current directory
        // looking for `kafkaman.toml`. Started anywhere else both binaries die
        // at boot on a config error that reads nothing like a telemetry failure.
        let directory = workspace_root().join("examples").join(spec.label);

        let mut command = Command::new(&binary);
        command
            .current_dir(&directory)
            .env("DATABASE_URL", &spec.database_url)
            .env("KAFKA_BROKERS", &spec.brokers)
            .env("BIND_ADDR", format!("127.0.0.1:{port}"))
            .env("KAFKA_CONSUMER_GROUP", &spec.consumer_group)
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", &spec.otlp_endpoint)
            // The signal-specific variables take precedence over the general one
            // in the OTLP specification, so a developer who exports one for
            // their own tooling would otherwise silently redirect that signal
            // away from this test's receiver — and the assertion for it would
            // fail with nothing pointing at the cause.
            .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
            .env("OTEL_METRIC_EXPORT_INTERVAL", EXPORT_INTERVAL_MS)
            .env("OTEL_BSP_SCHEDULE_DELAY", EXPORT_INTERVAL_MS)
            .env("OTEL_BLRP_SCHEDULE_DELAY", EXPORT_INTERVAL_MS)
            // The OTel log signal carries what the subscriber's filter admits,
            // and nothing else. Unset, the log assertions fail for filter
            // reasons that read as an export failure. Compose sets the same
            // default for the same reason.
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // The last possible moment: the child cannot bind a port this process
        // still holds.
        drop(reservation);
        let mut child = command.spawn().map_err(|err| -> BoxError {
            format!("spawning {}: {err}", binary.display()).into()
        })?;

        let log = ServiceLog::default();
        if let Some(stdout) = child.stdout.take() {
            log.follow(stdout, "out");
        }
        if let Some(stderr) = child.stderr.take() {
            log.follow(stderr, "err");
        }

        Ok(Self {
            label: spec.label,
            service: spec.service,
            base_url: format!("http://127.0.0.1:{port}"),
            child: Some(child),
            log,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The `service.name` this binary is expected to export under.
    #[must_use]
    pub fn service(&self) -> &'static str {
        self.service
    }

    #[must_use]
    pub fn log(&self) -> &ServiceLog {
        &self.log
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Whether the child has already exited.
    fn exited(&mut self) -> TestResult<Option<ExitStatus>> {
        match self.child.as_mut() {
            Some(child) => Ok(child.try_wait()?),
            None => Ok(None),
        }
    }

    /// Block until `/health` answers, or the child dies, or time runs out.
    async fn await_healthy(&mut self, http: &reqwest::Client) -> TestResult {
        let deadline = Instant::now() + HEALTH_TIMEOUT;
        let health = self.url("/health");

        while Instant::now() < deadline {
            if let Some(status) = self.exited()? {
                return Err(format!(
                    "{} exited with {status} before serving /health\n{}",
                    self.label,
                    self.log.tail(40)
                )
                .into());
            }
            if let Ok(response) = http.get(&health).send().await {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            tokio::time::sleep(POLL).await;
        }

        Err(format!(
            "{} did not serve /health within {HEALTH_TIMEOUT:?}\n{}",
            self.label,
            self.log.tail(40)
        )
        .into())
    }

    /// Ask the binary to stop the way an operator would.
    ///
    /// SIGINT, because that is all these binaries handle: both select on
    /// `tokio::signal::ctrl_c()`, and `examples/compose.yaml` sets
    /// `stop_signal: SIGINT` to match. `Child::kill` sends SIGKILL, which proves
    /// nothing about provider flushing — the whole point of this gate is the
    /// code that runs *after* the signal.
    fn interrupt(&self) -> TestResult {
        let Some(child) = self.child.as_ref() else {
            return Ok(());
        };
        let raw = i32::try_from(child.id())?;
        let pid = Pid::from_raw(raw)
            .ok_or_else(|| -> BoxError { format!("{raw} is not a usable pid").into() })?;
        kill_process(pid, Signal::INT)?;
        Ok(())
    }

    /// SIGINT, then wait for the process to leave on its own.
    ///
    /// A timeout here is a failure worth reporting rather than escalating past:
    /// a binary that will not drain within the window compose gives it is the
    /// defect, not the harness's problem to paper over. It is still killed, so
    /// nothing is left behind.
    async fn stop(&mut self) -> TestResult {
        self.interrupt()?;
        let deadline = Instant::now() + EXIT_TIMEOUT;

        while Instant::now() < deadline {
            if let Some(status) = self.exited()? {
                self.child = None;
                if !status.success() {
                    return Err(format!(
                        "{} exited with {status} after SIGINT; a clean shutdown is \
                         what makes its flush meaningful\n{}",
                        self.label,
                        self.log.tail(40)
                    )
                    .into());
                }
                return Ok(());
            }
            tokio::time::sleep(POLL).await;
        }

        Err(format!(
            "{} was still running {EXIT_TIMEOUT:?} after SIGINT\n{}",
            self.label,
            self.log.tail(40)
        )
        .into())
    }
}

impl Drop for Service {
    /// Nothing outlives the test.
    ///
    /// A `Child` is not killed when it drops, so without this an assertion panic
    /// leaves a service process holding a port and a consumer-group membership,
    /// and the next run fails for reasons that have nothing to do with the change
    /// under test. `stop` clears `child` on a clean exit, so this only fires on
    /// the paths that did not get one.
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Both example binaries, their infrastructure, and the endpoint they export to.
#[derive(Debug)]
pub struct Stack {
    pub order: Service,
    pub product: Service,
    pub http: reqwest::Client,
    capture: Capture,
    // Held, not used: dropping the cluster stops the containers underneath both
    // services.
    _cluster: Cluster,
}

impl Stack {
    /// Provision infrastructure, start both binaries, and wait for both to serve.
    pub async fn start() -> TestResult<Self> {
        let capture = Capture::start()?;
        let cluster = Cluster::start().await?;

        // Before either binary, mirroring the ordering `examples/compose.yaml`
        // enforces with `condition: service_completed_successfully`. Both
        // services verify their topics at boot and refuse to start on a missing
        // one, so this is a prerequisite rather than a convenience.
        cluster.provision_topics().await?;

        // Unique per run, so a repeated run against a reused server cannot
        // inherit converged state, and so two runs cannot share a consumer group
        // — a shared group splits the single partition and the loser idles while
        // still reporting healthy.
        let run = Uuid::new_v4().simple().to_string();
        let product_db = cluster.create_database(&format!("product_{run}")).await?;
        let order_db = cluster.create_database(&format!("order_{run}")).await?;

        let mut product = Service::spawn(&Spec {
            label: "product",
            service: PRODUCT_SERVICE,
            binary: PRODUCT_BIN,
            database_url: product_db,
            brokers: cluster.brokers().to_owned(),
            consumer_group: format!("product-service-{run}"),
            otlp_endpoint: capture.endpoint().to_owned(),
        })?;

        let mut order = Service::spawn(&Spec {
            label: "order",
            service: ORDER_SERVICE,
            binary: ORDER_BIN,
            database_url: order_db,
            brokers: cluster.brokers().to_owned(),
            consumer_group: format!("order-service-{run}"),
            otlp_endpoint: capture.endpoint().to_owned(),
        })?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        product.await_healthy(&http).await?;
        order.await_healthy(&http).await?;

        Ok(Self {
            order,
            product,
            http,
            capture,
            _cluster: cluster,
        })
    }

    /// Stop both binaries cleanly and collect everything they exported.
    ///
    /// The order matters and is the same as the in-process suite's: `order`
    /// first, because it holds a cache of what `product` publishes, and stopping
    /// the producer first would leave its ingester waiting on a broker
    /// connection that is going away.
    ///
    /// Capture happens strictly after both have exited. That is the assertion:
    /// with every export schedule pushed an hour out, anything that arrives
    /// arrived because shutdown forced it.
    pub async fn shutdown_and_capture(mut self, quiet_for: Duration) -> TestResult<Captured> {
        self.order.stop().await?;
        self.product.stop().await?;

        let requests = self.capture.drain(quiet_for);
        Captured::decode(&requests)
    }
}

/// Everything two binaries exported, decoded and grouped by signal.
#[derive(Debug, Default)]
pub struct Captured {
    /// Instrument name -> the services that exported it.
    pub metrics: HashMap<String, Vec<String>>,
    /// Span name -> the services that exported it.
    pub spans: HashMap<String, Vec<String>>,
    /// Decoded spans with identity, attributes, parent ids, and links.
    pub span_records: Vec<CapturedSpan>,
    /// Service name -> the log record bodies it exported.
    pub logs: HashMap<String, Vec<String>>,
    /// Every `/v1/...` path that was posted to.
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedSpan {
    pub service: String,
    pub name: String,
    pub kind: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub attributes: HashMap<String, String>,
    pub links: Vec<CapturedSpanLink>,
}

impl CapturedSpan {
    /// Whether this span's immediate parent is `parent`.
    ///
    /// Both halves matter. A matching `parent_span_id` in a *different* trace is
    /// what a broken handoff looks like — the id survives, the trace does not.
    ///
    /// Use this only where the *edge* is the subject. For "is this work part of
    /// that request", use [`Captured::is_descendant_of`]: the depth between two
    /// spans is a function of which functions happen to be instrumented, and
    /// pinning it turns every added span into a test failure.
    #[must_use]
    pub fn is_child_of(&self, parent: &CapturedSpan) -> bool {
        self.trace_id == parent.trace_id
            && self.parent_span_id.as_deref() == Some(parent.span_id.as_str())
    }
}

impl Captured {
    /// Whether `span` sits anywhere beneath `ancestor` in the same trace.
    ///
    /// Walks the recorded parent chain rather than comparing one edge. What a
    /// waterfall assertion means is "this work happened as part of that
    /// request", and that stays true when a span is added between the two —
    /// which the `kafkaman::internal` tier does by design, since promoting a
    /// function to the default filter inserts it into exactly these chains.
    ///
    /// Bounded by the number of recorded spans, so a malformed export that
    /// reports a cycle terminates instead of hanging the suite.
    #[must_use]
    pub fn is_descendant_of(&self, span: &CapturedSpan, ancestor: &CapturedSpan) -> bool {
        if span.trace_id != ancestor.trace_id || span.span_id == ancestor.span_id {
            return false;
        }
        let mut current = span;
        for _ in 0..self.span_records.len() {
            let Some(parent_id) = current.parent_span_id.as_deref() else {
                return false;
            };
            if parent_id == ancestor.span_id {
                return true;
            }
            let Some(parent) = self.span_records.iter().find(|candidate| {
                candidate.span_id == parent_id && candidate.trace_id == span.trace_id
            }) else {
                return false;
            };
            current = parent;
        }
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedSpanLink {
    pub trace_id: String,
    pub span_id: String,
    pub attributes: HashMap<String, String>,
}

impl Captured {
    fn decode(requests: &[otlp_capture::Captured]) -> TestResult<Self> {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
        use prost::Message as _;

        let mut decoded = Self::default();

        for request in requests {
            decoded.paths.push(request.path().to_owned());

            match request.path() {
                "/v1/metrics" => {
                    let export = ExportMetricsServiceRequest::decode(request.body())?;
                    for resource in &export.resource_metrics {
                        let service = service_of(resource.resource.as_ref());
                        for scope in &resource.scope_metrics {
                            for metric in &scope.metrics {
                                decoded
                                    .metrics
                                    .entry(metric.name.clone())
                                    .or_default()
                                    .push(service.clone());
                            }
                        }
                    }
                }
                "/v1/traces" => {
                    let export = ExportTraceServiceRequest::decode(request.body())?;
                    for resource in &export.resource_spans {
                        let service = service_of(resource.resource.as_ref());
                        for scope in &resource.scope_spans {
                            for span in &scope.spans {
                                decoded
                                    .spans
                                    .entry(span.name.clone())
                                    .or_default()
                                    .push(service.clone());
                                decoded.span_records.push(CapturedSpan {
                                    service: service.clone(),
                                    name: span.name.clone(),
                                    kind: span_kind(span.kind),
                                    trace_id: hex(&span.trace_id),
                                    span_id: hex(&span.span_id),
                                    parent_span_id: (!span.parent_span_id.is_empty())
                                        .then(|| hex(&span.parent_span_id)),
                                    attributes: attributes(&span.attributes),
                                    links: span
                                        .links
                                        .iter()
                                        .map(|link| CapturedSpanLink {
                                            trace_id: hex(&link.trace_id),
                                            span_id: hex(&link.span_id),
                                            attributes: attributes(&link.attributes),
                                        })
                                        .collect(),
                                });
                            }
                        }
                    }
                }
                "/v1/logs" => {
                    let export = ExportLogsServiceRequest::decode(request.body())?;
                    for resource in &export.resource_logs {
                        let service = service_of(resource.resource.as_ref());
                        for scope in &resource.scope_logs {
                            for record in &scope.log_records {
                                if let Some(AnyValueKind::StringValue(text)) =
                                    record.body.as_ref().and_then(|body| body.value.clone())
                                {
                                    decoded.logs.entry(service.clone()).or_default().push(text);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(decoded)
    }

    /// Whether `service` exported an instrument by this name.
    #[must_use]
    pub fn metric_from(&self, name: &str, service: &str) -> bool {
        self.metrics
            .get(name)
            .is_some_and(|services| services.iter().any(|found| found == service))
    }

    /// Whether `service` exported a span by this name.
    #[must_use]
    pub fn span_from(&self, name: &str, service: &str) -> bool {
        self.spans
            .get(name)
            .is_some_and(|services| services.iter().any(|found| found == service))
    }

    /// The first span from `service` with `name` and all supplied attributes.
    #[must_use]
    pub fn span_with_attrs(
        &self,
        name: &str,
        service: &str,
        attrs: &[(&str, &str)],
    ) -> Option<&CapturedSpan> {
        self.span_records.iter().find(|span| {
            span.name == name
                && span.service == service
                && attrs.iter().all(|(key, value)| {
                    span.attributes
                        .get(*key)
                        .is_some_and(|found| found == value)
                })
        })
    }

    /// Every `service.name` that appears anywhere in what was captured.
    #[must_use]
    pub fn services(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .metrics
            .values()
            .chain(self.spans.values())
            .flatten()
            .cloned()
            .chain(self.logs.keys().cloned())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// A one-line summary, for a failure message that has to explain what *did*
    /// arrive.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut instruments: Vec<&str> = self.metrics.keys().map(String::as_str).collect();
        instruments.sort_unstable();
        let mut spans: Vec<&str> = self.spans.keys().map(String::as_str).collect();
        spans.sort_unstable();
        format!(
            "services {:?}; {} requests {:?}; instruments {instruments:?}; spans {spans:?}; \
             log records {:?}",
            self.services(),
            self.paths.len(),
            unique(&self.paths),
            self.logs
                .iter()
                .map(|(service, records)| (service.as_str(), records.len()))
                .collect::<Vec<_>>()
        )
    }
}

/// The `service.name` on an OTLP resource, or the empty string when it declares
/// none — which is itself a failure the assertions should be able to report,
/// rather than something to filter out here.
fn service_of(resource: Option<&Resource>) -> String {
    resource
        .and_then(|resource| otlp_capture::service_name(Some(resource.attributes.as_slice())))
        .unwrap_or_default()
}

fn attributes(attributes: &[KeyValue]) -> HashMap<String, String> {
    attributes
        .iter()
        .filter_map(|attribute| {
            Some((
                attribute.key.clone(),
                value_to_string(attribute.value.as_ref()?)?,
            ))
        })
        .collect()
}

fn value_to_string(value: &opentelemetry_proto::tonic::common::v1::AnyValue) -> Option<String> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;

    match value.value.as_ref()? {
        AnyValueKind::StringValue(text) => Some(text.clone()),
        AnyValueKind::StringValueStrindex(value) => Some(value.to_string()),
        AnyValueKind::BoolValue(value) => Some(value.to_string()),
        AnyValueKind::IntValue(value) => Some(value.to_string()),
        AnyValueKind::DoubleValue(value) => Some(value.to_string()),
        AnyValueKind::BytesValue(value) => Some(hex(value)),
        AnyValueKind::ArrayValue(array) => Some(
            array
                .values
                .iter()
                .filter_map(value_to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        AnyValueKind::KvlistValue(list) => Some(
            list.values
                .iter()
                .filter_map(|entry| {
                    Some(format!(
                        "{}={}",
                        entry.key,
                        value_to_string(entry.value.as_ref()?)?
                    ))
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
    }
}

fn span_kind(kind: i32) -> String {
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;

    SpanKind::try_from(kind)
        .unwrap_or(SpanKind::Unspecified)
        .as_str_name()
        .to_owned()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn unique(values: &[String]) -> Vec<&str> {
    let mut seen: Vec<&str> = values.iter().map(String::as_str).collect();
    seen.sort_unstable();
    seen.dedup();
    seen
}
