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
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
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
    let paths: Vec<&str> = posted.iter().map(|(path, _)| path.as_str()).collect();
    for signal in ["/v1/metrics", "/v1/traces", "/v1/logs"] {
        assert!(
            paths.contains(&signal),
            "kafkaman should have exported {signal}, but only saw {paths:?}"
        );
    }

    // One signal can span several exports, so the assertions look at everything
    // that arrived for it rather than at whichever batch came first.
    let body_for = |signal: &str| -> String {
        posted
            .iter()
            .filter(|(path, _)| path == signal)
            .map(|(_, body)| body.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    };

    let metrics = body_for("/v1/metrics");
    assert!(
        metrics
            .to_ascii_lowercase()
            .contains("application/x-protobuf"),
        "the suite builds `http-proto` only, so exports should be protobuf"
    );
    assert!(
        metrics.contains("kafkaman.scheduler.cycles"),
        "a relay cycle's counter should be on the wire"
    );
    assert!(
        metrics.contains("kafkaman.outbox.time_to_publish"),
        "so should the histogram that only an outbox can measure"
    );
    assert!(
        metrics.contains(SERVICE_NAME),
        "exports should carry the resource the host configured"
    );

    let traces = body_for("/v1/traces");
    for span in ["kafkaman.enqueue", "kafkaman.relay.publish"] {
        assert!(
            traces.contains(span),
            "the {span} span should be on the wire"
        );
    }

    let logs = body_for("/v1/logs");
    assert!(
        logs.contains("relay cycle complete"),
        "a kafkaman log line should reach the log signal, which is what makes the \
         log-to-trace pivot possible at all"
    );

    Ok(())
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
    let text = String::from_utf8_lossy(request);
    let Some(headers_end) = text.find("\r\n\r\n") else {
        return false;
    };
    let content_length = text[..headers_end]
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

/// Gather every request that arrives, as `(path, whole request)` pairs.
///
/// Stops when nothing has arrived for `quiet_for`. Everything was flushed by the
/// shutdown above, so a quiet period means the exports are done rather than that
/// the test gave up early — which is why a few seconds is enough and why the
/// whole wait is added to the test's runtime exactly once.
fn drain(requests: &mpsc::Receiver<Vec<u8>>, quiet_for: Duration) -> Vec<(String, String)> {
    let mut posted = Vec::new();
    while let Ok(request) = requests.recv_timeout(quiet_for) {
        let text = String::from_utf8_lossy(&request).into_owned();
        let path = text
            .lines()
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        posted.push((path, text));
    }
    posted
}
