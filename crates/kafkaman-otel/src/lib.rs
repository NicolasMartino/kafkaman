//! An opt-in OpenTelemetry pipeline for applications that host kafkaman.
//!
//! kafkaman itself never builds a provider. It emits through `tracing` and the
//! `opentelemetry` API and leaves the SDK to whoever hosts it, so that a host
//! which already exports its own telemetry composes rather than collides. This
//! crate is that host's side of the arrangement, packaged — nothing in kafkaman
//! depends on it, and nothing in it depends on kafkaman.
//!
//! # The 90% case
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let telemetry = kafkaman_otel::init("order-service")?;
//!
//! // ... build the runtime, serve, drain ...
//!
//! telemetry.shutdown()?;
//! # Ok(())
//! # }
//! ```
//!
//! Which signals are installed is decided by the environment, per the OTLP
//! specification. `OTEL_EXPORTER_OTLP_ENDPOINT` enables metrics, traces, and
//! logs; a signal-specific variable such as `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
//! enables only its own. With none of them set — or all of them blank — no
//! provider is installed at all and you get an ordinary `tracing_subscriber`
//! formatter, so a development run does not spend its life logging failed
//! exports at a backend that is not running.
//!
//! # When you already own your subscriber
//!
//! [`init`] installs a global subscriber, which is only acceptable when nothing
//! else wanted to. If you have your own — JSON output, an error reporter, a
//! filter you built — use [`builder`] instead. It installs the *providers*,
//! which is what the ordering contract below concerns, and hands the layers back
//! for you to compose:
//!
//! ```no_run
//! use tracing_subscriber::filter::FilterExt as _;
//! use tracing_subscriber::layer::SubscriberExt as _;
//! use tracing_subscriber::Layer as _;
//! use tracing_subscriber::util::SubscriberInitExt as _;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let telemetry = kafkaman_otel::builder("order-service")
//!     .service_version(env!("CARGO_PKG_VERSION"))
//!     .resource_attribute("deployment.environment", "staging")
//!     .without_logs()
//!     .build()?;
//!
//! let filter = || tracing_subscriber::EnvFilter::new("info");
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer().with_filter(filter()))
//!     .with(telemetry.trace_layer().map(|layer| {
//!         layer.with_filter(filter())
//!     }))
//!     // The log bridge gets one extra exclusion. kafkaman reports failures as
//!     // message-less events on `EXCEPTION_TARGET`, because that is the only
//!     // shape the span layer rewrites into an OpenTelemetry `exception` — so
//!     // exporting them as logs as well produces a blank ERROR record for every
//!     // error already reported as one. `init` does this for you.
//!     .with(telemetry.log_layer().map(|layer| {
//!         layer.with_filter(filter().and(tracing_subscriber::filter::filter_fn(
//!             |metadata| metadata.target() != kafkaman_otel::EXCEPTION_TARGET,
//!         )))
//!     }))
//!     .init();
//! # Ok(())
//! # }
//! ```
//!
//! # Bounding what leaves the process
//!
//! Two knobs, and they cut at different points.
//!
//! `RUST_LOG` decides which spans and events are *recorded at all*. [`init`]
//! gives each layer its own `EnvFilter` built from it, so a span below the
//! filter costs nothing anywhere. Giving each layer its own filter rather than
//! putting one on the registry is what lets a host bound exported telemetry
//! differently from stdout — a registry-wide filter bounds every layer after it
//! too, so this is about the freedom to differ, not about closing a leak.
//!
//! `OTEL_TRACES_SAMPLER` decides which recorded traces are *exported*. The SDK
//! reads it directly and this crate does not intercept it, so
//! `OTEL_TRACES_SAMPLER=parentbased_traceidratio` with
//! `OTEL_TRACES_SAMPLER_ARG=0.1` keeps a tenth of traces whole — head sampling,
//! so a sampled trace is complete rather than a tenth of every trace's spans.
//! Default is `parentbased_always_on`, which is right for an example and wrong
//! for anything with traffic.
//!
//! Neither knob is a retention policy. Whatever reaches your backend is kept for
//! as long as that backend is configured to keep it, and spans are the highest-
//! volume signal kafkaman produces.
//!
//! Going the other way, `RUST_LOG=info,kafkaman::internal=debug` adds a span per
//! kafkaman function beneath the phase spans. It has its own target so reaching
//! it does not also enable `sqlx` and `rdkafka` debug logging. Those span names
//! are function names and are not a stability surface.
//!
//! # Two contracts
//!
//! **Install before you build any kafkaman loop.** kafkaman creates its metric
//! instruments as each run loop starts, and the OpenTelemetry API binds an
//! instrument to whichever provider is installed at that moment, permanently —
//! there is no rebinding. A loop built before the `MeterProvider` is silent for
//! its whole life, with no error and no log line. Call [`init`] or
//! [`Builder::build`] first, and the window closes.
//!
//! **Shut down after the runtime drains.** A process that exits without flushing
//! loses the telemetry explaining why it exited, which is the telemetry worth
//! having. Drain the service, then call [`Telemetry::shutdown`], and make sure
//! every exit path reaches it — including the error paths, which is easier to get
//! wrong than it sounds.
//!
//! # Versioning
//!
//! This is a companion crate, not part of kafkaman's stability promise. Every
//! type below names an `opentelemetry` 0.32 type, and the OpenTelemetry Rust
//! crates release breaking versions in lockstep several times a year. Each of
//! those is a breaking release here.
//!
//! If that cadence does not suit you, do not take the dependency: copy this file
//! and own it. You lose the convenience and nothing else — there is no
//! capability here that kafkaman does not offer without it, and keeping that
//! true is a deliberate constraint on how this crate may grow.
//!
//! # Transport
//!
//! OTLP over HTTP/protobuf, plaintext. The exporter's HTTP client is built with
//! no TLS backend, so an `https://` endpoint fails at run time with nothing
//! failing at build time — which is worth knowing before you point this at a
//! managed collector.
//!
//! To get TLS, enable it on the exporter from your own manifest, on the same
//! `0.32` line this crate uses:
//!
//! ```toml
//! kafkaman-otel = "0.1"
//! opentelemetry-otlp = { version = "0.32", features = ["reqwest-rustls"] }
//! ```
//!
//! Cargo unifies features across the graph, so that reaches the exporter this
//! crate links, and no code here changes. It is done that way round on purpose:
//! the choice of crypto provider and root store is yours, and the common one
//! (`aws-lc-rs`) needs cmake and a C toolchain that most builds here should not
//! have to carry for a capability they do not use.
//!
//! gRPC is not offered — see
//! `wiki/decisions/kafkaman-otel-extraction.decision.md` option E.

// Tests assert; a library returns. Same opt-out every other crate here uses.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use std::borrow::Cow;
use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::filter::FilterExt as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer as _;

const OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const OTLP_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";
/// Read by the SDK itself, not by this crate — see [`Builder::metric_interval`].
const METRIC_EXPORT_INTERVAL: &str = "OTEL_METRIC_EXPORT_INTERVAL";

/// The `tracing` target kafkaman reports failures on, excluded from log export.
///
/// This is `kafkaman_core::TELEMETRY_TARGET`, spelled out rather than imported:
/// this crate deliberately does not depend on kafkaman, which is what lets a
/// host copy it and own it. `the_exception_target_matches_kafkaman_core` in
/// `observability-tests` — which does depend on both — is what stops the two
/// from drifting.
///
/// Events on this target carry no message, because that is the only shape
/// `tracing-opentelemetry` rewrites into an OpenTelemetry `exception`. That
/// makes them exactly right on the span layer and exactly wrong on the log
/// bridge, which exports each one as a log record with an empty body — one blank
/// ERROR line for every error already reported as an error. Measured on the
/// example stack before this exclusion: 13 of 13 ERROR log records were these.
pub const EXCEPTION_TARGET: &str = "kafkaman::telemetry";

/// The metric collection interval used when neither the caller nor the
/// environment picks one.
///
/// Chosen to sit well inside a typical container stop grace period, so a
/// shutdown flush has room to finish before the runtime is killed. The SDK's own
/// default is 60s, which does not.
const DEFAULT_METRIC_INTERVAL: Duration = Duration::from_secs(15);

/// What went wrong installing or tearing down the pipeline.
///
/// Typed rather than boxed so a host can decide policy per case: a failed
/// exporter build is usually fatal at boot, while a failed flush at shutdown
/// usually is not.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An OTLP exporter could not be constructed.
    #[error("building the OTLP {signal} exporter: {source}")]
    Exporter {
        /// `metrics`, `traces`, or `logs`.
        signal: &'static str,
        /// The underlying exporter build failure.
        #[source]
        source: opentelemetry_otlp::ExporterBuildError,
    },

    /// A global `tracing` subscriber was already installed.
    #[error("installing the global tracing subscriber: {0}")]
    Subscriber(#[from] tracing::subscriber::SetGlobalDefaultError),

    /// A provider failed to flush and shut down.
    #[error("shutting down the OpenTelemetry {signal} provider: {source}")]
    Shutdown {
        /// `metrics`, `traces`, or `logs`.
        signal: &'static str,
        /// The underlying SDK shutdown failure.
        #[source]
        source: opentelemetry_sdk::error::OTelSdkError,
    },
}

/// Install the providers and a default subscriber.
///
/// The one-call path. Equivalent to [`builder`] followed by
/// [`Builder::build`], plus a `tracing_subscriber` registry carrying a
/// formatter and whichever OpenTelemetry layers were enabled. Each layer gets
/// its own `EnvFilter` (from `RUST_LOG`, defaulting to `info`), so the same
/// filter bounds stdout, traces, and OTel logs — and a host composing its own
/// registry can give the telemetry layers a different one. See the crate docs on
/// bounding what leaves the process.
///
/// Call before constructing any kafkaman runtime loop; see the crate docs.
pub fn init(service_name: impl Into<Cow<'static, str>>) -> Result<Telemetry, Error> {
    let telemetry = builder(service_name).build()?;

    if let Err(err) = tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(env_filter()))
            .with(
                telemetry
                    .trace_layer()
                    .map(|layer| layer.with_filter(env_filter())),
            )
            .with(
                telemetry
                    .log_layer()
                    .map(|layer| layer.with_filter(log_export_filter())),
            ),
    ) {
        // `build` has already installed the providers and started their exporter
        // threads. Returning here without flushing would strand them for the life
        // of the process, with no handle left to reach them — so unwind first.
        // A shutdown failure is discarded rather than reported: the subscriber is
        // the failure worth returning, and there is no subscriber installed to
        // carry a second one anyway.
        drop(telemetry.shutdown());
        return Err(err.into());
    }

    if telemetry.is_enabled() {
        tracing::info!(
            service.name = %telemetry.service_name,
            metrics = telemetry.meter.is_some(),
            traces = telemetry.tracer.is_some(),
            logs = telemetry.logger.is_some(),
            "OpenTelemetry OTLP export enabled"
        );
    } else {
        tracing::info!("OpenTelemetry export disabled: no OTLP endpoint is configured");
    }

    Ok(telemetry)
}

/// Start configuring a pipeline for a host that owns its own subscriber.
///
/// See the crate docs for when to prefer this over [`init`].
pub fn builder(service_name: impl Into<Cow<'static, str>>) -> Builder {
    Builder {
        service_name: service_name.into(),
        service_version: None,
        attributes: Vec::new(),
        metric_interval: None,
        metrics: true,
        traces: true,
        logs: true,
    }
}

/// Configuration for the pipeline. Build one with [`builder`].
#[derive(Debug, Clone)]
pub struct Builder {
    service_name: Cow<'static, str>,
    service_version: Option<Cow<'static, str>>,
    attributes: Vec<KeyValue>,
    metric_interval: Option<Duration>,
    metrics: bool,
    traces: bool,
    logs: bool,
}

impl Builder {
    /// Set the `service.version` resource attribute.
    ///
    /// Usually `env!("CARGO_PKG_VERSION")`. Worth setting: it is what lets a
    /// dashboard attribute a latency change to a deploy.
    #[must_use]
    pub fn service_version(mut self, version: impl Into<Cow<'static, str>>) -> Self {
        self.service_version = Some(version.into());
        self
    }

    /// Add an arbitrary resource attribute, such as `deployment.environment`.
    #[must_use]
    pub fn resource_attribute(
        mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<opentelemetry::Value>,
    ) -> Self {
        self.attributes
            .push(KeyValue::new(key.into(), value.into()));
        self
    }

    /// How often metrics are collected and exported.
    ///
    /// Keep it comfortably below whatever will kill the process at shutdown; the
    /// final flush has to fit in that window.
    ///
    /// Unset, the OTLP specification's `OTEL_METRIC_EXPORT_INTERVAL` is honoured
    /// if the environment defines it, and 15s is used otherwise. Calling this
    /// overrides both — an explicit choice in code should not be silently
    /// retuned by a deployment.
    ///
    /// `Duration::ZERO` is not zero: the SDK ignores it and falls back to its own
    /// 60s default, which is longer than most stop grace periods. Pass a real
    /// interval or do not call this.
    #[must_use]
    pub fn metric_interval(mut self, interval: Duration) -> Self {
        self.metric_interval = Some(interval);
        self
    }

    /// Never install a `MeterProvider`, whatever the environment says.
    #[must_use]
    pub fn without_metrics(mut self) -> Self {
        self.metrics = false;
        self
    }

    /// Never install a `TracerProvider`, whatever the environment says.
    #[must_use]
    pub fn without_traces(mut self) -> Self {
        self.traces = false;
        self
    }

    /// Never install a `LoggerProvider`, whatever the environment says.
    ///
    /// The usual reason: log shipping is already someone else's job.
    #[must_use]
    pub fn without_logs(mut self) -> Self {
        self.logs = false;
        self
    }

    /// Install the providers and return the handle.
    ///
    /// Installs no `tracing` subscriber — take the layers off the returned
    /// [`Telemetry`] and compose them yourself. Use [`init`] if you have no
    /// subscriber of your own.
    ///
    /// A signal is installed when it was not turned off here *and* an endpoint
    /// is configured for it. With nothing configured this installs nothing and
    /// succeeds, which is the case that keeps a development run quiet.
    ///
    /// Installation is all-or-nothing. An `Err` means no global provider was
    /// installed, so a caller is free to report the failure and carry on
    /// unexported rather than having to treat it as fatal.
    ///
    /// Call it once per process. The providers it installs are global, and a
    /// second call replaces them while the first `Telemetry` still believes it
    /// owns them — the earlier providers then flush on whichever handle is shut
    /// down first. This is a property of the OpenTelemetry globals rather than of
    /// this crate, and there is nothing here that can detect it.
    pub fn build(self) -> Result<Telemetry, Error> {
        let metrics = self.metrics && endpoint_configured(OTLP_METRICS_ENDPOINT);
        let traces = self.traces && endpoint_configured(OTLP_TRACES_ENDPOINT);
        let logs = self.logs && endpoint_configured(OTLP_LOGS_ENDPOINT);

        let mut resource = Resource::builder().with_service_name(self.service_name.clone());
        if let Some(version) = &self.service_version {
            resource = resource.with_attribute(KeyValue::new(
                "service.version",
                version.clone().into_owned(),
            ));
        }
        if !self.attributes.is_empty() {
            resource = resource.with_attributes(self.attributes.clone());
        }
        let resource = resource.build();

        // Every fallible step runs before any global is installed.
        //
        // Installing as each provider is built would leave a live `MeterProvider`
        // — and the exporter thread behind it — running when the tracer's
        // exporter fails to build, because the `?` below returns no handle to
        // shut it down and the global cannot be taken back. Build first, install
        // second: the caller either gets a `Telemetry` that owns everything
        // installed, or an `Err` with nothing installed.
        let meter = if metrics {
            let exporter = MetricExporter::builder()
                .with_http()
                .build()
                .map_err(|source| Error::Exporter {
                    signal: "metrics",
                    source,
                })?;
            let mut reader = PeriodicReader::builder(exporter);
            // Precedence: an explicit `metric_interval` call, then the OTLP
            // specification's variable, then our default. Calling `with_interval`
            // unconditionally would override `OTEL_METRIC_EXPORT_INTERVAL` — which
            // the SDK reads for itself — leaving an operator who followed the
            // specification with no effect and no diagnostic.
            match self.metric_interval {
                Some(interval) => reader = reader.with_interval(interval),
                None if env_is_nonempty(METRIC_EXPORT_INTERVAL) => {}
                None => reader = reader.with_interval(DEFAULT_METRIC_INTERVAL),
            }
            Some(
                SdkMeterProvider::builder()
                    .with_reader(reader.build())
                    .with_resource(resource.clone())
                    .build(),
            )
        } else {
            None
        };

        let tracer = if traces {
            let exporter = SpanExporter::builder()
                .with_http()
                .build()
                .map_err(|source| Error::Exporter {
                    signal: "traces",
                    source,
                })?;
            Some(
                SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource.clone())
                    .build(),
            )
        } else {
            None
        };

        let logger = if logs {
            let exporter = LogExporter::builder()
                .with_http()
                .build()
                .map_err(|source| Error::Exporter {
                    signal: "logs",
                    source,
                })?;
            Some(
                SdkLoggerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource)
                    .build(),
            )
        } else {
            None
        };

        // Nothing below can fail. Installed globally because that is where
        // kafkaman looks: its instruments come from `global::meter`, per
        // wiki/decisions/telemetry-pipeline-ownership.decision.md item 2. The
        // logger provider needs no global — it reaches `tracing` through the
        // layer `log_layer` hands back.
        if let Some(provider) = &meter {
            opentelemetry::global::set_meter_provider(provider.clone());
        }
        if let Some(provider) = &tracer {
            opentelemetry::global::set_tracer_provider(provider.clone());
        }

        // No global text-map propagator is installed, and none is needed:
        // kafkaman formats and parses W3C `traceparent` itself rather than going
        // through a propagator, so that a build with tracing disabled still
        // forwards the context it was given.
        Ok(Telemetry {
            service_name: self.service_name,
            meter,
            tracer,
            logger,
        })
    }
}

/// The installed providers, held so shutdown can flush them.
///
/// Keep it alive for as long as the process is producing telemetry, and consume
/// it with [`shutdown`](Self::shutdown) after the runtime has drained.
#[derive(Debug)]
pub struct Telemetry {
    service_name: Cow<'static, str>,
    meter: Option<SdkMeterProvider>,
    tracer: Option<SdkTracerProvider>,
    logger: Option<SdkLoggerProvider>,
}

impl Telemetry {
    /// Whether any provider was installed.
    ///
    /// `false` means no OTLP endpoint was configured, so this is a no-op handle
    /// and [`shutdown`](Self::shutdown) will succeed trivially.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.meter.is_some() || self.tracer.is_some() || self.logger.is_some()
    }

    /// The `service.name` this pipeline reports.
    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// The span-export layer, if traces are installed.
    ///
    /// Compose it into your own registry. `None` when traces are off, and a
    /// `tracing_subscriber` registry accepts an `Option<Layer>` directly, so
    /// `.with(telemetry.trace_layer())` is correct either way.
    ///
    /// The instrumentation scope is the service name rather than `"kafkaman"`:
    /// this tracer covers the whole application, not only the library's spans,
    /// and naming it after the library would misattribute every one of them.
    pub fn trace_layer<S>(&self) -> Option<impl tracing_subscriber::Layer<S> + use<S>>
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        self.tracer.as_ref().map(|provider| {
            tracing_opentelemetry::layer().with_tracer(provider.tracer(self.service_name.clone()))
        })
    }

    /// The log-export layer, if logs are installed.
    ///
    /// Bridges `tracing` events to OpenTelemetry log records. `None` when logs
    /// are off.
    pub fn log_layer<S>(&self) -> Option<impl tracing_subscriber::Layer<S> + use<S>>
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        self.logger.as_ref().map(OpenTelemetryTracingBridge::new)
    }

    /// Flush and shut down every installed provider.
    ///
    /// Call after the runtime has drained. The order is load-bearing: the logger
    /// provider goes last, because a second or subsequent failure is reported
    /// through `tracing::error!`, and a logger already shut down would swallow
    /// exactly the diagnostics being reported.
    ///
    /// Returns the first failure and logs the rest rather than stopping at the
    /// first, so that a meter which fails to flush does not cost the trace and
    /// log flushes queued behind it.
    pub fn shutdown(self) -> Result<(), Error> {
        let mut first = None;
        if let Some(meter) = self.meter {
            remember(&mut first, "metrics", meter.shutdown());
        }
        if let Some(tracer) = self.tracer {
            remember(&mut first, "traces", tracer.shutdown());
        }
        if let Some(logger) = self.logger {
            remember(&mut first, "logs", logger.shutdown());
        }
        first.map_or(Ok(()), Err)
    }
}

/// Whether either the generic or the signal-specific endpoint variable is set.
fn endpoint_configured(signal_specific: &str) -> bool {
    env_is_nonempty(OTLP_ENDPOINT) || env_is_nonempty(signal_specific)
}

/// Blank counts as unset.
///
/// A container orchestrator has no way to pass "absent" through a `${VAR:-}`
/// default, so a variable that is declared but empty has to mean the same thing
/// as one that was never named — otherwise every stack that declares the
/// variable installs exporters pointed at nothing.
fn env_is_nonempty(name: &str) -> bool {
    std::env::var(name)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

/// `RUST_LOG`, minus the events that are already leaving as exceptions.
///
/// The same filter every other layer gets, and then [`EXCEPTION_TARGET`] removed
/// — see that constant for why. Dropping them here rather than at the callsite
/// is what keeps the other two consumers whole: the span layer still gets the
/// event it turns into an `exception`, and the stdout formatter still renders it
/// for whoever is watching a terminal.
///
/// A host building its own registry through [`builder`] composes its own
/// filters; the crate docs show this one.
fn log_export_filter<S>() -> impl tracing_subscriber::layer::Filter<S> {
    env_filter().and(tracing_subscriber::filter::filter_fn(|metadata| {
        metadata.target() != EXCEPTION_TARGET
    }))
}

/// Keep the first shutdown error, log any that follow.
fn remember(
    slot: &mut Option<Error>,
    signal: &'static str,
    result: opentelemetry_sdk::error::OTelSdkResult,
) {
    if let Err(source) = result {
        if slot.is_none() {
            *slot = Some(Error::Shutdown { signal, source });
        } else {
            tracing::error!(
                signal,
                error = %source,
                "additional OpenTelemetry shutdown failure"
            );
        }
    }
}

#[cfg(test)]
mod tests;
