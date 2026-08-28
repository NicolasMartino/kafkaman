//! Whether kafkaman's telemetry actually leaves the process, in all three
//! signals.
//!
//! # What the other binaries deliberately do not cover
//!
//! Every other test here reads an in-memory exporter, which proves kafkaman
//! records what it claims to record and stops at the process boundary. That
//! leaves the last hop unproven, and the last hop is the one an operator
//! depends on: an exporter that never posts looks exactly like a silent
//! instrument from inside the process.
//!
//! So this one runs a real relay loop against a real database, with a real OTLP
//! pipeline pointed at a socket, and asserts the bytes: a POST per signal,
//! protobuf, carrying kafkaman's own instrument names, span names, and log
//! lines. It needs no backend, because a socket is enough to prove the bytes
//! left.
//!
//! # Why it is here rather than in an example
//!
//! It was written against `apps/axum-outbox`, which made the proof hostage to
//! whichever example currently ships. The wire format is a property of kafkaman
//! and its exporters, not of any application, and the ownership decision already
//! names `tests/` as a place exporter dependencies may live.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use kafkaman_test::Harness;
use observability_tests::{postgres_for_suite, relay_until_published, TestResult, SUITE};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use otlp_capture::{kind_of, service_name, Capture, Captured, Kind};
use prost::Message as _;
use tracing_subscriber::layer::SubscriberExt;

/// Identifies the process in the export, so the assertions can tell kafkaman's
/// resource from anything else that might be listening.
const SERVICE_NAME: &str = "kafkaman-observability-suite";

#[tokio::test]
async fn kafkaman_telemetry_reaches_the_wire_as_metrics_traces_and_logs() -> TestResult {
    let capture = Capture::start()?;
    // Read by the exporters themselves rather than passed in: the general
    // variable has each signal's path appended to it, and restating that rule
    // here is a way to get it subtly wrong.
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", capture.endpoint());

    let resource = Resource::builder().with_service_name(SERVICE_NAME).build();
    let meter = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(MetricExporter::builder().with_http().build()?)
                .with_interval(Duration::from_secs(3600))
                .build(),
        )
        .with_resource(resource.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter.clone());

    let tracer = SdkTracerProvider::builder()
        .with_batch_exporter(SpanExporter::builder().with_http().build()?)
        .with_resource(resource.clone())
        .build();
    opentelemetry::global::set_tracer_provider(tracer.clone());

    let logger = SdkLoggerProvider::builder()
        .with_batch_exporter(LogExporter::builder().with_http().build()?)
        .with_resource(resource)
        .build();

    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("kafkaman")))
            .with(OpenTelemetryTracingBridge::new(&logger)),
    )
    .expect("no other subscriber should be installed in this binary");

    // Everything below this line is ordinary kafkaman: enqueue a row, run the
    // relay loop, let it publish. The telemetry is a side effect, which is the
    // point — nothing here is written to make the assertions pass.
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    relay_until_published(&harness, "otlp-wire").await?;

    // Each provider's shutdown performs a final export, so the collection below
    // waits on exports that have already been asked for.
    meter.shutdown()?;
    tracer.shutdown()?;
    logger.shutdown()?;

    // Every request, not the first three. The log signal batches on its own
    // schedule and this process emits plenty besides kafkaman's — a container
    // client is chatty — so the batch carrying the relay's line is rarely the
    // first one to arrive.
    let posted = capture.drain(Duration::from_secs(3));
    let paths: Vec<&str> = posted.iter().map(Captured::path).collect();
    for signal in ["/v1/metrics", "/v1/traces", "/v1/logs"] {
        assert!(
            paths.contains(&signal),
            "kafkaman should have exported {signal}, but only saw {paths:?}"
        );
    }

    // The transport claim first, and from the headers rather than from the body:
    // it is a statement about how the exporter was built, and the body is the
    // one place it cannot honestly be read.
    for request in &posted {
        assert!(
            request
                .content_type()
                .eq_ignore_ascii_case("application/x-protobuf"),
            "the suite builds `http-proto` only, so {} should be protobuf, not {:?}",
            request.path(),
            request.content_type()
        );
    }

    // One signal can span several exports, so each assertion below looks at
    // everything that arrived for it rather than at whichever batch came first.
    let decoded = |signal: &str| -> Vec<&[u8]> {
        posted
            .iter()
            .filter(|request| request.path() == signal)
            .map(Captured::body)
            .collect()
    };

    // --- Metrics ---

    let mut instruments: Vec<(String, Kind)> = Vec::new();
    for body in decoded("/v1/metrics") {
        let export = ExportMetricsServiceRequest::decode(body)
            .expect("an OTLP metrics export should be a decodable ExportMetricsServiceRequest");
        for resource in &export.resource_metrics {
            assert_eq!(
                service_name(resource.resource.as_ref().map(|r| r.attributes.as_slice())),
                Some(SERVICE_NAME.to_owned()),
                "an export must carry the resource the host configured, or a backend \
                 cannot tell which process it came from"
            );
            for scope in &resource.scope_metrics {
                for metric in &scope.metrics {
                    instruments.push((metric.name.clone(), kind_of(metric)));
                }
            }
        }
    }
    assert_eq!(
        instruments
            .iter()
            .find(|(name, _)| name == "kafkaman.scheduler.cycles")
            .map(|(_, kind)| *kind),
        Some(Kind::Sum),
        "a relay cycle's counter should be on the wire, and as a counter: the \
         instrument type is part of what a backend stores, and a byte search for \
         the name cannot tell a Sum from a Gauge that happens to be named one"
    );
    assert_eq!(
        instruments
            .iter()
            .find(|(name, _)| name == "kafkaman.outbox.time_to_publish")
            .map(|(_, kind)| *kind),
        Some(Kind::Histogram),
        "so should the histogram that only an outbox can measure"
    );

    // --- Traces ---

    let mut spans = Vec::new();
    for body in decoded("/v1/traces") {
        let export = ExportTraceServiceRequest::decode(body)
            .expect("an OTLP trace export should be a decodable ExportTraceServiceRequest");
        for resource in &export.resource_spans {
            assert_eq!(
                service_name(resource.resource.as_ref().map(|r| r.attributes.as_slice())),
                Some(SERVICE_NAME.to_owned())
            );
            for scope in &resource.scope_spans {
                spans.extend(scope.spans.iter().cloned());
            }
        }
    }
    let span_named = |name: &str| {
        spans
            .iter()
            .find(|span| span.name == name)
            .unwrap_or_else(|| panic!("the {name} span should be on the wire"))
    };
    let enqueue = span_named("kafkaman.enqueue");
    let publish = span_named("kafkaman.relay.publish");
    assert!(
        !enqueue.trace_id.iter().all(|byte| *byte == 0)
            && !enqueue.span_id.iter().all(|byte| *byte == 0),
        "an exported span with all-zero ids is a span no backend will accept"
    );
    // The durable gap, asserted on the wire rather than in memory. This is the
    // property the whole design exists for — publish happens in another task,
    // minutes later in a real deployment — and it is exactly what a byte search
    // for two span names cannot check: both names appear either way.
    assert_eq!(
        publish.trace_id, enqueue.trace_id,
        "publish must be exported into the trace the enqueue started"
    );
    assert_eq!(
        publish.parent_span_id, enqueue.span_id,
        "and as its child, restored from the row's stored context"
    );

    // --- Logs ---

    let mut bodies = Vec::new();
    for body in decoded("/v1/logs") {
        let export = ExportLogsServiceRequest::decode(body)
            .expect("an OTLP log export should be a decodable ExportLogsServiceRequest");
        for resource in &export.resource_logs {
            for scope in &resource.scope_logs {
                for record in &scope.log_records {
                    if let Some(AnyValueKind::StringValue(text)) =
                        record.body.as_ref().and_then(|body| body.value.clone())
                    {
                        bodies.push(text);
                    }
                }
            }
        }
    }
    assert!(
        bodies.iter().any(|text| text == "relay cycle complete"),
        "a kafkaman log line should reach the log signal as a record body — which \
         is what makes the log-to-trace pivot possible at all — but the exported \
         bodies were {bodies:?}"
    );

    Ok(())
}
