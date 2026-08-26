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

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
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
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use prost::Message as _;
use tracing_subscriber::layer::SubscriberExt;

/// Identifies the process in the export, so the assertions can tell kafkaman's
/// resource from anything else that might be listening.
const SERVICE_NAME: &str = "kafkaman-observability-suite";

#[tokio::test]
async fn kafkaman_telemetry_reaches_the_wire_as_metrics_traces_and_logs() -> TestResult {
    let (endpoint, requests) = otlp_endpoint();
    // Read by the exporters themselves rather than passed in: the general
    // variable has each signal's path appended to it, and restating that rule
    // here is a way to get it subtly wrong.
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint);

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
    let posted = drain(&requests, Duration::from_secs(3));
    let paths: Vec<&str> = posted.iter().map(|request| request.path.as_str()).collect();
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
            request.path,
            request.content_type()
        );
    }

    // One signal can span several exports, so each assertion below looks at
    // everything that arrived for it rather than at whichever batch came first.
    let decoded = |signal: &str| -> Vec<&[u8]> {
        posted
            .iter()
            .filter(|request| request.path == signal)
            .map(|request| request.body.as_slice())
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

/// The instrument type an OTLP metric carries, which is part of what a backend
/// stores and none of what its name says.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Sum,
    Gauge,
    Histogram,
    Other,
}

fn kind_of(metric: &Metric) -> Kind {
    match metric.data {
        Some(MetricData::Sum(_)) => Kind::Sum,
        Some(MetricData::Gauge(_)) => Kind::Gauge,
        Some(MetricData::Histogram(_)) => Kind::Histogram,
        _ => Kind::Other,
    }
}

/// The `service.name` a resource declares, if it declares one.
fn service_name(attributes: Option<&[KeyValue]>) -> Option<String> {
    attributes?.iter().find_map(|attribute| {
        if attribute.key != "service.name" {
            return None;
        }
        match attribute.value.as_ref()?.value.as_ref()? {
            AnyValueKind::StringValue(text) => Some(text.clone()),
            _ => None,
        }
    })
}

/// Bind a throwaway OTLP endpoint that answers `200` to everything.
///
/// One thread per connection: the three exporters are independent clients, and a
/// single-connection server would deadlock whichever two lost the race.
fn otlp_endpoint() -> (String, mpsc::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("binding a loopback port should work");
    let addr = listener
        .local_addr()
        .expect("a bound listener should have an address");
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                return;
            };
            let tx = tx.clone();
            std::thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("setting a read timeout should work");

                // Read until the declared body has arrived rather than trusting
                // one read to return the whole request; a POST is not obliged to
                // arrive in a single segment, and a test that usually passes is
                // worse than none.
                let mut request = Vec::new();
                let mut chunk = [0_u8; 8192];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&chunk[..read]),
                    }
                    if request_is_complete(&request) {
                        break;
                    }
                }

                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
                let _ = stream.flush();
                let _ = tx.send(request);
            });
        }
    });

    (format!("http://{addr}"), rx)
}

/// Whether the headers have arrived and the body is as long as they promised.
fn request_is_complete(request: &[u8]) -> bool {
    let Some(headers_end) = find_headers_end(request) else {
        return false;
    };
    let text = String::from_utf8_lossy(&request[..headers_end]);
    let content_length = text
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")?
                .trim()
                .parse::<usize>()
                .ok()
        })
        .unwrap_or(0);
    request.len() >= headers_end + 4 + content_length
}

/// One captured request, split into the parts the assertions ask about.
///
/// The body is kept as bytes rather than lossily decoded: it is protobuf, and
/// `from_utf8_lossy` replaces every byte it cannot read with U+FFFD — which is a
/// silent corruption of the exact thing under test. Searching that string for
/// instrument names happened to work because the names are ASCII inside
/// length-prefixed fields, and it could not distinguish a metric named
/// `kafkaman.scheduler.cycles` from a log line mentioning one.
struct Captured {
    path: String,
    headers: String,
    body: Vec<u8>,
}

impl Captured {
    fn content_type(&self) -> &str {
        self.headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-type")
                    .then(|| value.trim())
            })
            .unwrap_or_default()
    }
}

/// Gather every request that arrives.
///
/// Stops when nothing has arrived for `quiet_for`. Everything was flushed by the
/// shutdown above, so a quiet period means the exports are done rather than that
/// the test gave up early — which is why a few seconds is enough and why the
/// whole wait is added to the test's runtime exactly once.
fn drain(requests: &mpsc::Receiver<Vec<u8>>, quiet_for: Duration) -> Vec<Captured> {
    let mut posted = Vec::new();
    while let Ok(request) = requests.recv_timeout(quiet_for) {
        let Some(headers_end) = find_headers_end(&request) else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..headers_end]).into_owned();
        let path = headers
            .lines()
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        posted.push(Captured {
            path,
            body: request[headers_end + 4..].to_vec(),
            headers,
        });
    }
    posted
}

/// Where the header block ends, as a byte offset into the raw request.
fn find_headers_end(request: &[u8]) -> Option<usize> {
    request.windows(4).position(|window| window == b"\r\n\r\n")
}
