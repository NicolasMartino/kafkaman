//! The OpenTelemetry pipeline both example services install.
//!
//! Shared rather than copied into each binary: the two services differ only in
//! the service name they pass to [`init`], and two copies of a provider
//! lifecycle are two copies to keep in step.
//!
//! kafkaman itself never builds a provider — it emits through `tracing` and the
//! `opentelemetry` API and leaves the SDK to whoever hosts it. This module is
//! that host, written out in full so an adopter can copy it rather than infer
//! it. See `wiki/decisions/telemetry-pipeline-ownership.decision.md`.
//!
//! # Transport
//!
//! Export is OTLP over **plaintext HTTP only**. The workspace pins
//! `opentelemetry-otlp` to `reqwest-blocking-client`, which resolves a `reqwest`
//! with no `rustls` or `native-tls` feature, and the runtime image in
//! `examples/Dockerfile` ships no `ca-certificates` to validate against. An
//! `https://` endpoint therefore fails at run time with nothing failing at
//! compile time. A deployment that needs TLS swaps in the exporter's
//! `reqwest-rustls-client` feature and adds a root store to the image.

use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
const OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const OTLP_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

/// How often the metrics reader collects and exports.
///
/// Kept well inside the container's `stop_grace_period` so a shutdown flush has
/// room to finish; see [`Telemetry::shutdown`].
const METRIC_EXPORT_INTERVAL: Duration = Duration::from_secs(15);

/// The installed providers, held so shutdown can flush them.
///
/// Every field is `None` when the matching signal has no endpoint configured,
/// which is the whole of the "no OTLP endpoint means no OTel at all" behaviour.
#[derive(Debug)]
pub struct Telemetry {
    meter: Option<SdkMeterProvider>,
    tracer: Option<SdkTracerProvider>,
    logger: Option<SdkLoggerProvider>,
}

impl Telemetry {
    /// Flush and shut down every installed provider.
    ///
    /// Call this *after* the service has drained: a process that exits without
    /// flushing loses the telemetry about why it exited, which is the telemetry
    /// worth having.
    ///
    /// The order is load-bearing. The logger provider goes last because
    /// `remember` reports a second and subsequent failure through
    /// `tracing::error!`, and a logger already shut down would swallow exactly
    /// the diagnostics being reported.
    ///
    /// Returns the first failure and logs the rest, rather than stopping at the
    /// first: a meter that fails to flush must not cost us the trace and log
    /// flushes queued behind it.
    pub fn shutdown(self) -> Result<(), BoxError> {
        let mut first_error = None;
        if let Some(meter) = self.meter {
            remember(
                &mut first_error,
                meter.shutdown(),
                "shutting down OpenTelemetry meter provider",
            );
        }
        if let Some(tracer) = self.tracer {
            remember(
                &mut first_error,
                tracer.shutdown(),
                "shutting down OpenTelemetry tracer provider",
            );
        }
        if let Some(logger) = self.logger {
            remember(
                &mut first_error,
                logger.shutdown(),
                "shutting down OpenTelemetry logger provider",
            );
        }
        first_error.map_or(Ok(()), Err)
    }
}

/// Install the tracing subscriber, and the OTel providers if an endpoint is set.
///
/// Call this before constructing any kafkaman loop. kafkaman's instruments are
/// created when a loop is built, and an instrument created before
/// `set_meter_provider` belongs to the no-op provider for the life of the
/// process.
///
/// `service_name` becomes the OTel `service.name` resource attribute, and also
/// the instrumentation scope for this application's spans. Naming the scope
/// after the service rather than after kafkaman keeps the attribution honest:
/// the tracer covers the example's own Axum handlers too, not only the library's
/// spans.
///
/// Safe to call from inside a Tokio runtime despite the exporter using a
/// blocking HTTP client. `opentelemetry_sdk` 0.32 runs `PeriodicReader` and both
/// batch processors on dedicated OS threads, so no export ever blocks a runtime
/// worker.
pub fn init(service_name: &'static str) -> Result<Telemetry, BoxError> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    // The generic `OTEL_EXPORTER_OTLP_ENDPOINT` enables all three signals; a
    // signal-specific variable enables just its own. That is what lets the plain
    // `just examples demo` path stay quiet: compose passes the generic variable
    // through as the empty string, blank counts as unset, and no provider is
    // installed to fail against a backend that is not running.
    let metrics_enabled = endpoint_configured(OTLP_METRICS_ENDPOINT);
    let traces_enabled = endpoint_configured(OTLP_TRACES_ENDPOINT);
    let logs_enabled = endpoint_configured(OTLP_LOGS_ENDPOINT);

    if !metrics_enabled && !traces_enabled && !logs_enabled {
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer()),
        )?;
        tracing::info!(
            "OpenTelemetry export disabled: no OTLP endpoint environment variable is set"
        );
        return Ok(Telemetry {
            meter: None,
            tracer: None,
            logger: None,
        });
    }

    let resource = Resource::builder().with_service_name(service_name).build();

    let meter = if metrics_enabled {
        let reader = PeriodicReader::builder(MetricExporter::builder().with_http().build()?)
            .with_interval(METRIC_EXPORT_INTERVAL)
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        Some(provider)
    } else {
        None
    };

    let tracer = if traces_enabled {
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(SpanExporter::builder().with_http().build()?)
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_tracer_provider(provider.clone());
        Some(provider)
    } else {
        None
    };

    let logger = if logs_enabled {
        Some(
            SdkLoggerProvider::builder()
                .with_batch_exporter(LogExporter::builder().with_http().build()?)
                .with_resource(resource)
                .build(),
        )
    } else {
        None
    };

    // No global text-map propagator is installed, and none is needed:
    // `kafkaman-core` formats and parses W3C `traceparent` itself rather than
    // going through a propagator, so that a `traces`-disabled build still
    // forwards context. See crates/kafkaman-core/src/trace.rs.
    let trace_layer = tracer
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer(service_name)));
    let log_layer = logger.as_ref().map(OpenTelemetryTracingBridge::new);

    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(trace_layer)
            .with(log_layer),
    )?;
    tracing::info!(
        service.name = service_name,
        metrics = metrics_enabled,
        traces = traces_enabled,
        logs = logs_enabled,
        "OpenTelemetry OTLP export enabled"
    );

    Ok(Telemetry {
        meter,
        tracer,
        logger,
    })
}

/// Whether either the generic or the signal-specific endpoint variable is set.
fn endpoint_configured(signal_specific: &str) -> bool {
    env_is_nonempty(OTLP_ENDPOINT) || env_is_nonempty(signal_specific)
}

/// Blank counts as unset.
///
/// Compose has no way to pass "absent" through `${VAR:-}`, so a variable that is
/// declared but empty has to mean the same thing as one that was never named, or
/// the plain example stack would install exporters pointed at nothing.
fn env_is_nonempty(name: &str) -> bool {
    std::env::var(name)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

/// Keep the first error, log any that follow.
fn remember<E>(slot: &mut Option<BoxError>, result: Result<(), E>, context: &'static str)
where
    E: std::fmt::Display,
{
    if let Err(err) = result {
        let message = format!("{context}: {err}");
        if slot.is_none() {
            *slot = Some(message.into());
        } else {
            tracing::error!(error = %err, context, "additional telemetry shutdown failure");
        }
    }
}
