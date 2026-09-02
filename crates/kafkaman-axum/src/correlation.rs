//! Giving every request a correlation id, and a bounded span to report it on.
//!
//! Nothing here touches a database or a kafkaman table, which is why it is the
//! one part of this crate an application can mount without an [`AdminState`].
//!
//! [`AdminState`]: crate::AdminState

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::header::HeaderName;
use axum::http::{HeaderValue, Request};
use axum::response::Response;
use tower::{Layer, Service};
use tracing::Instrument;
use uuid::Uuid;

/// Request/response header carrying the correlation id across a service hop.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// The `http.route` reported for a request that matched no route.
///
/// Bounded on purpose: see the comment in [`CorrelationService::call`].
pub(crate) const UNMATCHED_ROUTE: &str = "<unmatched>";

/// Longest client-supplied correlation id accepted before one is generated
/// instead.
///
/// A correlation id is echoed into a response header and into every log line
/// for the request, so its length is attacker-controlled log volume. 128 is
/// comfortably above any real id — a UUID is 36 — and far below a useful
/// amplification factor.
pub(crate) const MAX_CORRELATION_ID_LEN: usize = 128;

/// A request's correlation id, stored in request extensions by
/// [`CorrelationLayer`].
///
/// Either echoed from the inbound [`CORRELATION_ID_HEADER`] or freshly
/// generated. Values that survive from the client are constrained by
/// [`CorrelationId::is_acceptable`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationId(String);

impl CorrelationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether a client-supplied value may be adopted as-is.
    ///
    /// Restricted to printable, non-space ASCII within 128 bytes.
    /// `HeaderValue::from_str` already rejects
    /// control characters, so this is not about header splitting — it is about
    /// keeping unbounded or unprintable client input out of logs, and keeping
    /// the value greppable once it lands there.
    pub fn is_acceptable(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= MAX_CORRELATION_ID_LEN
            && value.bytes().all(|byte| byte.is_ascii_graphic())
    }
}

/// Ensures every request has a correlation id, in its extensions, its span, and
/// its response.
///
/// Adopts the inbound [`CORRELATION_ID_HEADER`] when it is acceptable, and
/// generates a UUID otherwise, so a downstream service always has something to
/// join on.
#[derive(Clone, Debug, Default)]
pub struct CorrelationLayer;

impl CorrelationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for CorrelationLayer {
    type Service = CorrelationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CorrelationService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct CorrelationService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for CorrelationService<S>
where
    S: Service<Request<Body>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let correlation_id = request
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| CorrelationId::is_acceptable(value))
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        request
            .extensions_mut()
            .insert(CorrelationId::new(correlation_id.clone()));

        // `http.route` and the exported span name are what a backend groups
        // transactions by, so both stay bounded. A request that matched no route
        // has no template to report, and reporting its raw path instead would
        // mint one transaction group per URL a scanner invents.
        let route = request
            .extensions()
            .get::<MatchedPath>()
            .map_or(UNMATCHED_ROUTE, MatchedPath::as_str);
        // The raw path is still worth having; it just belongs in a field nothing
        // groups by. Borrowed from `request` rather than cloned: the span macro
        // copies every value in as it builds the span, which is before the call
        // below moves the request.
        let otel_name = format!("{} {route}", request.method());
        let span = tracing::info_span!(
            "http.request",
            "otel.name" = otel_name.as_str(),
            "otel.kind" = "server",
            "otel.status_code" = tracing::field::Empty,
            "otel.status_description" = tracing::field::Empty,
            "http.request.method" = %request.method(),
            "http.route" = route,
            "url.path" = request.uri().path(),
            "http.response.status_code" = tracing::field::Empty,
            correlation_id = %correlation_id,
        );
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.instrument(span.clone()).await?;
            let status = response.status();
            span.record("http.response.status_code", i64::from(status.as_u16()));
            // 5xx only. A 4xx is the server correctly refusing a bad request, and
            // marking those failed makes an APM error rate track client mistakes
            // rather than service health.
            if status.is_server_error() {
                kafkaman_core::record_error(&span, &format_args!("HTTP {}", status.as_u16()));
            }
            if let Ok(value) = HeaderValue::from_str(&correlation_id) {
                response
                    .headers_mut()
                    .insert(HeaderName::from_static(CORRELATION_ID_HEADER), value);
            }
            Ok(response)
        })
    }
}
