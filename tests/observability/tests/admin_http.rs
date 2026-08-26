//! Every admin route, over a real socket, against a real database.
//!
//! # What a router-level test cannot tell you
//!
//! The existing coverage calls the handlers through an in-process `Router`.
//! That proves the handler logic and skips everything an operator actually
//! meets: status codes on the wire, JSON that a client can decode, path
//! parameters resolved by the router rather than by the test, and the
//! correlation header round trip — which exists precisely so a request can be
//! found again in the logs, and which no in-process call exercises.
//!
//! This is an observability test rather than an HTTP one. These routes are the
//! operator's answer to "how much is queued, how old is it, what is stuck, what
//! is dead, put it back" — the part of the surface that works with no telemetry
//! backend at all, and therefore the part that must not quietly break.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use kafkaman_axum::{
    admin_router, redrive_router, AdminState, CorrelationLayer, CORRELATION_ID_HEADER,
};
use kafkaman_test::Harness;
use observability_tests::{postgres_for_suite, ProductSnapshot, TestResult, SUITE};

#[tokio::test]
async fn every_admin_route_answers_over_http() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    // Registers the type's tables, which is what gives the summaries something
    // to report rather than an empty config.
    let _outbox = harness.outbox_table::<ProductSnapshot>().await?;
    let _received = harness.received_table::<ProductSnapshot>().await?;

    let envelope =
        ProductSnapshot::envelope("admin-http", "a product").try_with_idempotency_key("admin")?;
    harness.enqueue(&envelope).await?;

    let server = serve(&harness, Mounted::Everything).await?;
    let base = &server.base;
    let client = reqwest::Client::new();

    // Liveness and readiness answer without a body worth asserting on; what
    // matters is that they answer, and that ready is honest about the database.
    for route in ["health", "ready"] {
        let response = client.get(format!("{base}/{route}")).send().await?;
        assert_eq!(
            response.status(),
            200,
            "GET /{route} should succeed against a reachable database"
        );
    }

    let outbox = client.get(format!("{base}/outbox")).send().await?;
    // The content type, because a client decodes on it. A handler returning a
    // JSON-shaped body as `text/plain` passes every assertion below and fails
    // in the caller.
    assert_eq!(
        outbox
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json"),
        "the summaries are a JSON API and must say so on the wire"
    );
    let outbox: serde_json::Value = outbox.json().await?;
    let pending = outbox
        .as_array()
        .expect("the outbox summary is a list of buckets")
        .iter()
        .find(|bucket| bucket["status"] == "Pending")
        .expect("the row just enqueued is pending");
    assert_eq!(pending["count"], 1);
    assert_eq!(pending["message_type"], "product_snapshot");
    assert!(
        pending["oldest_age_ms"].is_number(),
        "a bucket with rows reports the age of its oldest, which is the number an \
         operator reads first"
    );

    let received: serde_json::Value = client
        .get(format!("{base}/received"))
        .send()
        .await?
        .json()
        .await?;
    assert!(
        received.is_array(),
        "the receive side answers even with nothing received"
    );

    let stuck: serde_json::Value = client
        .get(format!("{base}/stuck"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        stuck["truncated"], false,
        "nothing is stuck, so nothing is truncated"
    );
    assert!(stuck["outbox"].as_array().unwrap().is_empty());

    let dlq: serde_json::Value = client
        .get(format!("{base}/dlq"))
        .send()
        .await?
        .json()
        .await?;
    assert!(dlq.is_array() || dlq.is_object(), "the DLQ view answers");

    // The destructive one. Nothing is dead-lettered, so the honest answer is
    // zero rows redriven — which is also the assertion that the route resolves
    // its path parameter and decodes its body.
    let redrive = client
        .post(format!("{base}/dlq/product_snapshot/redrive"))
        .json(&serde_json::json!({ "max_rows": 10 }))
        .send()
        .await?;
    assert_eq!(redrive.status(), 200);
    let redrive: serde_json::Value = redrive.json().await?;
    assert_eq!(redrive["message_type"], "product_snapshot");
    assert_eq!(redrive["redriven"], 0);

    // A type this application does not carry is the caller's mistake, not a
    // server error. The body matters as much as the status: an operator reading
    // a bare 404 cannot tell a wrong message type from a wrong path.
    let unknown = client
        .post(format!("{base}/dlq/not_a_message_type/redrive"))
        .json(&serde_json::json!({ "max_rows": 10 }))
        .send()
        .await?;
    assert_eq!(unknown.status(), 404);
    let unknown: serde_json::Value = unknown.json().await?;
    assert!(
        unknown["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not_a_message_type"),
        "the error should name the type that was not registered, got: {unknown}"
    );

    // The failure filter accepts the spelling the DLQ view prints. This is a
    // wire-format compatibility check rather than a redrive test: the route
    // already answered above, and what is at stake here is whether the value an
    // operator copies out of `latest_error.type` is a value they can paste back
    // in.
    for spelling in ["urn:kafkaman:problem:handler", "Handler"] {
        let filtered = client
            .post(format!("{base}/dlq/product_snapshot/redrive"))
            .json(&serde_json::json!({ "max_rows": 10, "failure_kind": spelling }))
            .send()
            .await?;
        assert_eq!(
            filtered.status(),
            200,
            "{spelling:?} names a failure class the redrive route should accept"
        );
    }

    // An unrecognized one is refused rather than resolved to the default. On a
    // destructive route the difference is which rows move.
    let misspelled = client
        .post(format!("{base}/dlq/product_snapshot/redrive"))
        .json(&serde_json::json!({ "max_rows": 10, "failure_kind": "Handlr" }))
        .send()
        .await?;
    assert_eq!(
        misspelled.status(),
        422,
        "a misspelled failure class must not silently become another one"
    );

    // And the bound is enforced on the wire, not merely documented.
    let unbounded = client
        .post(format!("{base}/dlq/product_snapshot/redrive"))
        .json(&serde_json::json!({ "max_rows": 0 }))
        .send()
        .await?;
    assert_eq!(unbounded.status(), 400);
    let unbounded: serde_json::Value = unbounded.json().await?;
    assert!(
        unbounded["error"]
            .as_str()
            .unwrap_or_default()
            .contains("max_rows"),
        "the error should name the field that was out of range, got: {unbounded}"
    );

    Ok(())
}

/// The read-only router does not carry the destructive route.
///
/// This is the whole point of the split, and it is the kind of property that
/// decays silently: adding one `.route` line to `admin_router` would hand a
/// destructive endpoint to every deployment that mounted it for dashboards, and
/// nothing else in the suite would notice.
#[tokio::test]
async fn the_read_only_router_cannot_redrive() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let _outbox = harness.outbox_table::<ProductSnapshot>().await?;
    let _received = harness.received_table::<ProductSnapshot>().await?;

    let server = serve(&harness, Mounted::ReadOnly).await?;
    let base = &server.base;
    let client = reqwest::Client::new();

    let refused = client
        .post(format!("{base}/dlq/product_snapshot/redrive"))
        .json(&serde_json::json!({ "max_rows": 10 }))
        .send()
        .await?;
    assert_eq!(
        refused.status(),
        404,
        "the redrive route is absent from the read-only router, not merely unauthorized"
    );

    // The reads it does carry still answer, so this is a routing split rather
    // than a broken router.
    let dlq = client.get(format!("{base}/dlq")).send().await?;
    assert_eq!(dlq.status(), 200, "inspection stays on the read-only side");

    Ok(())
}

/// The correlation id a caller supplies comes back, and one is minted when they
/// supply none.
///
/// This is the whole point of the header: an operator holding an id from a
/// client can find the request in the logs, and a client holding none still gets
/// something to quote in a bug report.
#[tokio::test]
async fn the_correlation_header_makes_the_round_trip() -> TestResult {
    let postgres = postgres_for_suite(SUITE).await?;
    let harness = Harness::connect(postgres.url()).await?;
    let server = serve(&harness, Mounted::Everything).await?;
    let base = &server.base;
    let client = reqwest::Client::new();

    let supplied = "c0rrel4tion-from-the-caller";
    let response = client
        .get(format!("{base}/health"))
        .header(CORRELATION_ID_HEADER, supplied)
        .send()
        .await?;
    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(supplied),
        "a caller's id is echoed rather than replaced"
    );

    let minted = client.get(format!("{base}/health")).send().await?;
    let minted = minted
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .expect("a response should carry an id even when the request had none");
    assert!(
        !minted.is_empty() && minted != supplied,
        "the minted id should be its own, not an echo of someone else's"
    );

    Ok(())
}

/// Which routers a test server mounts.
#[derive(Clone, Copy, Debug)]
enum Mounted {
    /// `admin_router` alone, the way a deployment that only wants dashboards
    /// would mount it.
    ReadOnly,
    /// Both routers merged, the way a deployment that wants redrive too must
    /// opt into.
    Everything,
}

/// A running admin server on a loopback port.
///
/// Owns its task so the test can end it. Several of these run in one binary, and
/// a server left running holds pool connections against a container the next
/// test is also using — which is how a suite that passes one test at a time
/// starts timing out when run together.
struct AdminServer {
    base: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for AdminServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Bind an admin server to a loopback port and return it.
///
/// Port zero: several of these run in one binary and a fixed port would make
/// them fight over it.
async fn serve(harness: &Harness, mounted: Mounted) -> TestResult<AdminServer> {
    let state = AdminState::new(harness.pool().clone(), Arc::new(harness.config()));
    let app = match mounted {
        Mounted::ReadOnly => admin_router(state),
        Mounted::Everything => admin_router(state.clone()).merge(redrive_router(state)),
    }
    .layer(CorrelationLayer::new());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(AdminServer {
        base: format!("http://{addr}"),
        task,
    })
}
