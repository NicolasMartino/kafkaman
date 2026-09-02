//! Unit tests, split to mirror the module they cover.
//!
//! The helpers below are shared because the subscriber machinery is: `tracing`
//! keeps a process-global maximum level, so every test that installs a
//! subscriber has to serialize against every other one regardless of which
//! module it belongs to.

use super::admin::*;
use super::correlation::*;
use super::error::*;
use super::redrive::*;
use super::state::*;

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use kafkaman_core::{ReceivedError, ReceivedFailureKind};
use kafkaman_sqlx::{ReceivedStuckRow, TableAccess};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tower::{Layer, Service, ServiceExt};
use uuid::Uuid;

mod admin;
mod correlation;
mod error;
mod redrive;
mod wire_format;

/// Runs `request` through the layer over a handler that echoes the
/// correlation id it sees, so one call checks both the response header and
/// the extension the handler was given.
pub(super) async fn correlation_roundtrip(request: Request<Body>) -> (String, String) {
    let mut service =
        CorrelationLayer::new().layer(tower::service_fn(|request: Request<Body>| async move {
            let correlation = request
                .extensions()
                .get::<CorrelationId>()
                .expect("correlation id extension should be present")
                .as_str()
                .to_owned();
            Ok::<_, Infallible>(Response::new(Body::from(correlation)))
        }));
    let response = service.ready().await.unwrap().call(request).await.unwrap();
    let header = response
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .expect("response must carry a correlation id")
        .to_owned();
    let body = http_body_util_collect(response.into_body()).await;
    (header, body)
}

/// Drains a response body to a `String` without pulling in another crate for
/// the two tests that need it.
pub(super) async fn http_body_util_collect(body: Body) -> String {
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("test bodies are small and finite");
    String::from_utf8(bytes.to_vec()).expect("test bodies are utf-8")
}

/// Captures the field values of every `http.request` span opened while it is
/// installed, so the route assertions below read what a backend would.
#[derive(Clone, Default)]
pub(super) struct RecordedRoutes(Arc<std::sync::Mutex<Vec<(String, String)>>>);

impl<S> tracing_subscriber::Layer<S> for RecordedRoutes
where
    S: tracing::Subscriber,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if attrs.metadata().name() != "http.request" {
            return;
        }
        #[derive(Default)]
        struct Fields {
            route: String,
            name: String,
        }
        impl tracing::field::Visit for Fields {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "http.route" => self.route = value.to_owned(),
                    "otel.name" => self.name = value.to_owned(),
                    _ => {}
                }
            }
            fn record_debug(
                &mut self,
                _field: &tracing::field::Field,
                _value: &dyn std::fmt::Debug,
            ) {
            }
        }
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        self.0
            .lock()
            .expect("no test panics while holding this")
            .push((fields.route, fields.name));
    }
}

/// Serializes every test that installs a subscriber.
///
/// `tracing` keeps a **process-global** maximum level, recomputed as
/// subscribers come and go, and a span callsite consults it before it
/// consults any subscriber. So a test installing a filtered subscriber on
/// its own thread can silently disable another test's span on a different
/// one — which shows up as a span that simply never opened, with nothing
/// pointing at the cause. `set_default` being thread-local is not enough;
/// the level hint it adjusts is not.
///
/// Async because the guarded scope intentionally drives a request while the
/// subscriber is installed. A sync mutex guard across that await blocks an
/// executor worker thread and trips clippy for the right reason.
static SUBSCRIBER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) async fn lock_subscriber() -> tokio::sync::MutexGuard<'static, ()> {
    ensure_permissive_global();
    SUBSCRIBER_LOCK.lock().await
}

/// Install a permissive subscriber globally, once, before any test opens a
/// span.
///
/// The mutex above is necessary and not sufficient, because the thing being
/// shared is not the dispatcher. `tracing` caches each *callsite's* interest
/// globally, and rebuilds that cache as thread-local defaults come and go —
/// computing it against the **global** dispatcher, which with none installed
/// is `NoSubscriber`, and which answers "never" for every callsite. That
/// answer is then cached, so a thread-local subscriber on another thread is
/// never consulted and its spans simply do not open.
///
/// It shows up as a span that was never recorded, on a test that does not
/// install anything itself, only when the suite runs multi-threaded —
/// measured here at roughly one run in eight, and never under
/// `--test-threads=1`. Installing a permissive global once means every
/// rebuild computes a real answer instead of that one.
///
/// The result is ignored: a second call is an error and is exactly what
/// `Once` is preventing.
pub(super) fn ensure_permissive_global() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

/// Drives one request through a real router with the layer applied, and
/// returns the `(http.route, otel.name)` its span recorded.
pub(super) async fn recorded_route(uri: &str) -> (String, String) {
    let _serialized = lock_subscriber().await;
    use tracing_subscriber::layer::SubscriberExt as _;

    let recorded = RecordedRoutes::default();
    let app = axum::Router::new()
        .route(
            "/products/{product_id}",
            get(|| async { StatusCode::NO_CONTENT }),
        )
        .layer(CorrelationLayer::new());

    {
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(recorded.clone()));
        let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let _ = app.oneshot(request).await.unwrap();
    }

    let mut seen = recorded
        .0
        .lock()
        .expect("no test panics while holding this")
        .clone();
    assert_eq!(seen.len(), 1, "one request should open one span");
    seen.remove(0)
}

/// Records the name of every span opened while it is installed.
#[derive(Clone, Default)]
pub(super) struct RecordedSpans(Arc<std::sync::Mutex<Vec<String>>>);

impl<S> tracing_subscriber::Layer<S> for RecordedSpans
where
    S: tracing::Subscriber,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0
            .lock()
            .expect("no test panics while holding this")
            .push(attrs.metadata().name().to_owned());
    }
}
