//! A throwaway OTLP/HTTP endpoint that captures what an exporter posts to it.
//!
//! Every OTLP proof in this workspace needs the same thing: somewhere for a real
//! exporter to post to, and the bytes back afterwards to decode. This is that,
//! and it is deliberately not a web framework — an OTLP exporter speaks a narrow
//! enough dialect that `TcpListener` plus `content-length` framing covers it, and
//! a test server with no dependencies cannot fail for reasons that belong to a
//! dependency.
//!
//! It was written inside `tests/observability/tests/otlp_wire.rs` and lifted here
//! when a second suite needed it. Copying it instead is how two subtly different
//! capture servers appear, and then three.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//!
//! let capture = otlp_capture::Capture::start()?;
//! std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", capture.endpoint());
//!
//! // ... run something that exports, then shut its providers down ...
//!
//! for request in capture.drain(Duration::from_secs(3)) {
//!     println!("{} {} bytes", request.path(), request.body().len());
//! }
//! # Ok(())
//! # }
//! ```

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;

use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueKind;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::Metric;

/// How long a connection may sit idle before its thread gives up on it.
///
/// Bounds the thread, not the capture: an exporter that has finished posting
/// leaves its pooled connection open, and nothing should wait on it forever.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";

/// A bound OTLP endpoint and the requests posted to it.
#[derive(Debug)]
pub struct Capture {
    endpoint: String,
    requests: mpsc::Receiver<Vec<u8>>,
}

impl Capture {
    /// Bind a loopback port and start answering `200` to everything on it.
    ///
    /// One thread per connection: the three signal exporters are independent
    /// clients, and a single-connection server would deadlock whichever two lost
    /// the race.
    pub fn start() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let (tx, requests) = mpsc::channel();

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else {
                    return;
                };
                let tx = tx.clone();
                std::thread::spawn(move || serve(stream, &tx));
            }
        });

        Ok(Self {
            endpoint: format!("http://{addr}"),
            requests,
        })
    }

    /// The base URL to give `OTEL_EXPORTER_OTLP_ENDPOINT`.
    ///
    /// Without a signal path: the exporters append `/v1/metrics` and friends
    /// themselves, and restating that rule at a call site is a way to get it
    /// subtly wrong.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Gather every request that has arrived, and keep gathering until none has
    /// for `quiet_for`.
    ///
    /// Call it after the providers have been shut down, so a quiet period means
    /// the exports are finished rather than that the wait gave up early. The
    /// whole quiet period is added to the caller's runtime exactly once.
    #[must_use]
    pub fn drain(&self, quiet_for: Duration) -> Vec<Captured> {
        let mut posted = Vec::new();
        while let Ok(request) = self.requests.recv_timeout(quiet_for) {
            if let Some(captured) = Captured::parse(&request) {
                posted.push(captured);
            }
        }
        posted
    }
}

/// Serve one connection until the peer closes it or goes quiet.
///
/// The loop is the point. An OTLP exporter's HTTP client pools connections and
/// will send its next export down the one it already has, so a server that
/// answers a single request and drops the socket loses whatever was written into
/// the close — and OTLP POSTs are not retried. Answering one request per
/// connection was survivable when one process exported a handful of times; it is
/// the first thing to break when several processes export repeatedly.
fn serve(mut stream: TcpStream, tx: &mpsc::Sender<Vec<u8>>) {
    if stream.set_read_timeout(Some(READ_TIMEOUT)).is_err() {
        return;
    }

    let mut buffered: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; 8192];

    loop {
        // Drain what has already arrived before asking for more: one read can
        // carry the tail of one request and the head of the next, and a server
        // that handles only the first would stall holding the second.
        while let Some(len) = request_length(&buffered) {
            let request: Vec<u8> = buffered.drain(..len).collect();
            if stream.write_all(RESPONSE).is_err() || stream.flush().is_err() {
                return;
            }
            if tx.send(request).is_err() {
                return;
            }
        }

        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(read) => buffered.extend_from_slice(&chunk[..read]),
        }
    }
}

/// The full byte length of the request at the front of `buffer`, once all of it
/// has arrived.
///
/// `None` while the headers are incomplete or the body is still short of what
/// they promised. A POST is not obliged to arrive in one segment, and a test
/// that usually passes is worse than none.
fn request_length(buffer: &[u8]) -> Option<usize> {
    let headers_end = find_headers_end(buffer)?;
    let text = String::from_utf8_lossy(&buffer[..headers_end]);
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
    let total = headers_end + 4 + content_length;
    (buffer.len() >= total).then_some(total)
}

/// Where the header block ends, as a byte offset into the raw request.
fn find_headers_end(request: &[u8]) -> Option<usize> {
    request.windows(4).position(|window| window == b"\r\n\r\n")
}

/// One captured request, split into the parts assertions ask about.
///
/// The body is kept as bytes rather than lossily decoded: it is protobuf, and
/// `from_utf8_lossy` replaces every byte it cannot read with U+FFFD — which is a
/// silent corruption of the exact thing under test. Searching that string for
/// instrument names happens to work because the names are ASCII inside
/// length-prefixed fields, and it cannot distinguish a metric named
/// `kafkaman.scheduler.cycles` from a log line mentioning one.
#[derive(Clone, Debug)]
pub struct Captured {
    path: String,
    headers: String,
    body: Vec<u8>,
}

impl Captured {
    fn parse(request: &[u8]) -> Option<Self> {
        let headers_end = find_headers_end(request)?;
        let headers = String::from_utf8_lossy(&request[..headers_end]).into_owned();
        let path = headers
            .lines()
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        Some(Self {
            path,
            body: request[headers_end + 4..].to_vec(),
            headers,
        })
    }

    /// The request path, which is how the signal is told apart: `/v1/metrics`,
    /// `/v1/traces`, `/v1/logs`.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The protobuf body, undecoded.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The declared content type, read from the headers.
    ///
    /// A statement about how the exporter was built, and the body is the one
    /// place it cannot honestly be read.
    #[must_use]
    pub fn content_type(&self) -> &str {
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

/// The instrument type an OTLP metric carries, which is part of what a backend
/// stores and none of what its name says.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Sum,
    Gauge,
    Histogram,
    Other,
}

#[must_use]
pub fn kind_of(metric: &Metric) -> Kind {
    match metric.data {
        Some(MetricData::Sum(_)) => Kind::Sum,
        Some(MetricData::Gauge(_)) => Kind::Gauge,
        Some(MetricData::Histogram(_)) => Kind::Histogram,
        _ => Kind::Other,
    }
}

/// The `service.name` a resource declares, if it declares one.
#[must_use]
pub fn service_name(attributes: Option<&[KeyValue]>) -> Option<String> {
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
