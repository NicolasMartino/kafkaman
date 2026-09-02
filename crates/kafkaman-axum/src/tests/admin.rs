//! The admin handlers, and the span tier they are filtered by.

use super::*;

/// The `kafkaman::internal` tier is split by level, and its poll half is
/// reachable by its own target.
///
/// Two things are pinned here:
///
/// 1. A function that runs on a timer ([`health`], which an orchestrator
///    probes forever and which touches nothing) stays out of the default
///    filter. This is the whole cost argument for the tier being visible at
///    all — an idle service that exports its own scheduler exports nothing
///    else worth reading.
/// 2. The directive `examples/README.md` documents still reaches the poll
///    half — and still does not require turning on `debug` globally, which
///    would drown it in `sqlx` and `rdkafka` output.
///
/// The other half of the split — that a *message-path* function **is** in
/// the default filter, without which this test passes just as well against a
/// tier reverted to debug wholesale — is pinned by
/// `running_service_run_is_visible_under_the_default_filter` in
/// `kafkaman::axum`. It moved there with the supervision code: every
/// promoted function left in this crate needs a database, so the assertion
/// belongs where the one that does not now lives.
#[tokio::test]
async fn the_internal_span_tier_is_split_by_level_and_reachable_by_target() {
    use tracing_subscriber::layer::SubscriberExt as _;

    // Per-layer filtering, because that is what `kafkaman_otel::init` does:
    // it gives each layer its own `EnvFilter` rather than putting one on the
    // registry. A directive that works globally and not per layer would be a
    // directive that works in this test and not in a real service.
    async fn spans_opened_under(directive: &str) -> Vec<String> {
        use tracing_subscriber::Layer as _;

        let _serialized = lock_subscriber().await;
        let recorded = RecordedSpans::default();
        {
            let _guard = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(
                    recorded
                        .clone()
                        .with_filter(tracing_subscriber::EnvFilter::new(directive)),
                ),
            );
            let _ = health().await;
        }
        let names = recorded
            .0
            .lock()
            .expect("no test panics while holding this")
            .clone();
        names
    }

    let default_filter = spans_opened_under("info").await;
    assert!(
        !default_filter.contains(&"health".to_owned()),
        "a function an orchestrator polls forever must stay out of the default \
         filter; opened: {default_filter:?}"
    );
    assert!(
        spans_opened_under("info,kafkaman::internal=debug")
            .await
            .contains(&"health".to_owned()),
        "the documented directive must reach the poll half too"
    );
}

#[tokio::test]
async fn health_is_ok_without_touching_a_database() {
    // No pool is involved: liveness must not fail because storage blipped.
    let Json(value) = health().await;
    assert_eq!(value["status"], "ok");
}

/// A lazy pool, so a router can be built and routed against with no database.
///
/// `connect_lazy` opens nothing until a query runs, and every assertion below is
/// resolved by the router before a handler body is entered — which is the point:
/// the routing table is what is under test, not the handlers.
fn offline_state() -> AdminState {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://kafkaman:kafkaman@127.0.0.1:1/kafkaman")
        .expect("a lazy pool never contacts the server");
    AdminState::new(pool, Arc::new(kafkaman_sqlx::ResolvedConfig::default()))
}

/// The read-only router must not carry the destructive route.
///
/// The split between [`admin_router`](crate::admin_router) and
/// [`redrive_router`](crate::redrive_router) is the whole reason a deployment
/// can cover reads with one auth policy and writes with a stricter one. Nothing
/// about that is visible to the compiler: merging the redrive route into
/// `admin_router` would keep every other test in this crate green while handing
/// an unauthenticated reader a way to re-run handlers.
///
/// Asserted by routing rather than by reading the source, so it stays true
/// however the routers are assembled.
#[tokio::test]
async fn the_read_only_router_does_not_route_the_destructive_path() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    let redrive_request = || {
        Request::builder()
            .method("POST")
            .uri("/dlq/order_snapshot/redrive")
            .header("content-type", "application/json")
            // Deliberately not a valid `RedriveRequest`: extraction then fails
            // before the handler runs, so `redrive_router` answers without a
            // database while still proving the route exists.
            .body(Body::from("{}"))
            .unwrap()
    };

    let admin = admin_router(offline_state())
        .oneshot(redrive_request())
        .await
        .unwrap();
    assert_eq!(
        admin.status(),
        StatusCode::NOT_FOUND,
        "the read-only router must not know this path at all; anything other \
         than 404 means the destructive route has leaked into it"
    );

    // The other half, without which the assertion above passes just as well
    // against a redrive route that was deleted outright.
    let redrive = redrive_router(offline_state())
        .oneshot(redrive_request())
        .await
        .unwrap();
    assert_ne!(
        redrive.status(),
        StatusCode::NOT_FOUND,
        "the destructive router must still route it, or this test proves only \
         that the route is gone"
    );

    // And the read-only router really is serving, so a 404 above is about this
    // one path rather than an empty router.
    let health = admin_router(offline_state())
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
}
