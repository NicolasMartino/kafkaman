//! The example binaries install telemetry, export all three signals, and flush
//! on a clean shutdown.
//!
//! This is the only suite that runs `examples/order` and `examples/product` as
//! processes. `tests/distributed-cache` starts the same services as library
//! calls, which is right for debugging the domain and means it never reaches
//! `main.rs` — where `kafkaman_otel::init` is called, where the signal is
//! handled, and where `Telemetry::shutdown()` is sequenced after the drain.
//!
//! Run it through `just examples telemetry-test`, which builds both binaries and
//! points `EXAMPLE_ORDER_BIN` / `EXAMPLE_PRODUCT_BIN` at them.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

use distributed_cache_tests::{get, post, post_empty, TestResult};
use example_telemetry_tests::{
    BoxError, Captured, CapturedSpan, Stack, ORDER_SERVICE, PRODUCT_SERVICE,
};
use serde_json::json;

/// How long the two-hop round trip may take. The first hop also pays for the
/// consumer group's first assignment, which is why this matches the deadline
/// `examples/smoke.sh` and the distributed-cache suite both allow.
const CONVERGENCE: Duration = Duration::from_secs(90);

/// How long to keep listening after both processes have exited.
///
/// Everything was flushed by shutdown before the processes went away, so this is
/// draining a socket rather than waiting for a schedule.
const QUIET: Duration = Duration::from_secs(5);

/// Span names kafkaman emits. The gate asserts that *some* span from each
/// service arrives, not which: this is not a second span-schema suite, and
/// `tests/observability` already owns that question.
const KNOWN_SPANS: [&str; 4] = [
    "kafkaman.enqueue",
    "kafkaman.relay.publish",
    "kafkaman.dispatch",
    "kafkaman.ingest",
];

/// A counter every scheduler loop increments, so both services must report it.
const SCHEDULER_CYCLES: &str = "kafkaman.scheduler.cycles";

#[tokio::test]
async fn the_example_binaries_export_all_three_signals_and_flush_them_on_shutdown() -> TestResult {
    let stack = Stack::start().await?;

    // An ordinary two-service round trip, chosen because of what it makes the
    // runtime do rather than what it asserts: `product` enqueues and publishes a
    // snapshot, `order` ingests and dispatches it into its cache, then `order`
    // publishes an order and `product` ingests it, derives availability, and
    // republishes. Both services therefore run every loop that carries an
    // instrument.
    let product = post(
        &stack.http,
        &format!("{}/products", stack.product.base_url()),
        json!({"name": "widget", "price_cents": 1_250, "on_hand": 10}),
    )
    .await?;
    assert!(
        product.status.is_success(),
        "creating a product should succeed, got {}: {}",
        product.status,
        product.error_message()
    );
    let product_id = product.body["product_id"]
        .as_str()
        .ok_or("the product response should carry a product_id")?
        .to_owned();

    let first = await_cached_beyond(&stack, &product_id, -1).await?;

    let order = post(
        &stack.http,
        &format!("{}/orders", stack.order.base_url()),
        json!({"product_id": product_id, "quantity": 3}),
    )
    .await?;
    assert!(
        order.status.is_success(),
        "the order should be admitted from order's own cache, got {}: {}",
        order.status,
        order.error_message()
    );
    let order_id = order.body["order_id"]
        .as_str()
        .ok_or("the order response should carry an order_id")?
        .to_owned();

    let fulfilled = post_empty(
        &stack.http,
        &format!("{}/orders/{order_id}/fulfil", stack.order.base_url()),
    )
    .await?;
    assert!(
        fulfilled.status.is_success(),
        "fulfilling should succeed, got {}: {}",
        fulfilled.status,
        fulfilled.error_message()
    );

    // Waiting for the fulfilment to come back is what proves the second hop ran.
    // Without it the test could shut down while `product` had not yet ingested,
    // and half the instruments would be missing for a reason that is not a
    // telemetry defect.
    let after = await_cached_beyond(&stack, &product_id, first).await?;
    assert!(
        after > first,
        "the cache should have moved past offset {first}"
    );

    // Nothing is asserted before this point about telemetry, and nothing after
    // it about the domain. Both processes are gone by the time `captured` exists.
    let captured = stack.shutdown_and_capture(QUIET).await?;

    for signal in ["/v1/metrics", "/v1/traces", "/v1/logs"] {
        assert!(
            captured.paths.iter().any(|path| path == signal),
            "the binaries should have exported {signal}. Captured: {}",
            captured.summary()
        );
    }

    for service in [ORDER_SERVICE, PRODUCT_SERVICE] {
        assert!(
            captured.services().iter().any(|found| found == service),
            "{service} should appear as a service.name resource. Either that \
             binary stopped calling kafkaman_otel::init, or it is calling it with \
             a different name, or it never reached Telemetry::shutdown() — with \
             every export schedule pushed past this test's lifetime, a missing \
             flush and a missing pipeline look identical from here, and both are \
             regressions. Captured: {}",
            captured.summary()
        );

        assert!(
            captured.metric_from(SCHEDULER_CYCLES, service),
            "{service} should have exported {SCHEDULER_CYCLES} from a real \
             scheduler loop. Captured: {}",
            captured.summary()
        );

        assert!(
            KNOWN_SPANS
                .iter()
                .any(|span| captured.span_from(span, service)),
            "{service} should have exported at least one of {KNOWN_SPANS:?}. \
             Captured: {}",
            captured.summary()
        );

        assert!(
            captured
                .logs
                .get(service)
                .is_some_and(|records| !records.is_empty()),
            "{service} should have exported at least one log record, which is \
             what makes the log-to-trace pivot possible at all. Captured: {}",
            captured.summary()
        );
    }

    assert_waterfall_shape(&captured)?;

    Ok(())
}

fn assert_waterfall_shape(captured: &Captured) -> TestResult {
    let product_http = require_span(
        captured,
        "POST /products",
        PRODUCT_SERVICE,
        &[("http.request.method", "POST"), ("http.route", "/products")],
    )?;
    assert_span_kind(product_http, "SPAN_KIND_SERVER")?;
    let product_insert = require_child_span(
        captured,
        product_http,
        "db.query insert product",
        PRODUCT_SERVICE,
        &[
            ("db.collection.name", "products"),
            ("db.query.summary", "insert product"),
        ],
    )?;
    assert_span_kind(product_insert, "SPAN_KIND_CLIENT")?;

    let product_enqueue = require_child_span(
        captured,
        product_http,
        "kafkaman.enqueue",
        PRODUCT_SERVICE,
        &[("message_type", "product_snapshot")],
    )?;
    assert_span_kind(product_enqueue, "SPAN_KIND_PRODUCER")?;
    let outbox_insert = require_child_span(
        captured,
        product_enqueue,
        "db.query insert outbox row",
        PRODUCT_SERVICE,
        &[("db.query.summary", "insert outbox row")],
    )?;
    assert_span_kind(outbox_insert, "SPAN_KIND_CLIENT")?;

    let product_publish = require_child_span(
        captured,
        product_enqueue,
        "kafkaman.relay.publish",
        PRODUCT_SERVICE,
        &[("message_type", "product_snapshot")],
    )?;
    assert_span_kind(product_publish, "SPAN_KIND_PRODUCER")?;
    let mark_published = require_child_span(
        captured,
        product_publish,
        "db.query mark outbox published",
        PRODUCT_SERVICE,
        &[("db.query.summary", "mark outbox published")],
    )?;
    assert_span_kind(mark_published, "SPAN_KIND_CLIENT")?;

    let order_ingest = require_child_span(
        captured,
        product_publish,
        "kafkaman.ingest",
        ORDER_SERVICE,
        &[("message_type", "product_snapshot")],
    )?;
    assert_span_kind(order_ingest, "SPAN_KIND_CONSUMER")?;
    // Parented and linked are alternatives, not a belt-and-braces pair. An ingest
    // that both parented *and* linked would make the two handoff modes
    // indistinguishable on the wire, and would draw the broker hop twice in a
    // backend that renders links beside parentage.
    if !order_ingest.links.is_empty() {
        return Err(format!(
            "parented handoff should replace the producer link, not add to it, but \
             {} carried {} link(s): {:?}",
            order_ingest.name,
            order_ingest.links.len(),
            order_ingest.links
        )
        .into());
    }
    let order_dispatch = require_child_span(
        captured,
        order_ingest,
        "kafkaman.dispatch",
        ORDER_SERVICE,
        &[("message_type", "product_snapshot")],
    )?;
    assert_span_kind(order_dispatch, "SPAN_KIND_CONSUMER")?;
    let cache_upsert = require_child_span(
        captured,
        order_dispatch,
        "db.query apply received row to cache",
        ORDER_SERVICE,
        &[("db.query.summary", "apply received row to cache")],
    )?;
    assert_span_kind(cache_upsert, "SPAN_KIND_CLIENT")?;
    let mark_processed = require_child_span(
        captured,
        order_dispatch,
        "db.query mark received processed",
        ORDER_SERVICE,
        &[("db.query.summary", "mark received processed")],
    )?;
    assert_span_kind(mark_processed, "SPAN_KIND_CLIENT")?;

    assert_same_trace(
        &[
            ("product HTTP", product_http),
            ("product insert", product_insert),
            ("product enqueue", product_enqueue),
            ("outbox insert", outbox_insert),
            ("product relay publish", product_publish),
            ("order ingest", order_ingest),
            ("order dispatch", order_dispatch),
            ("order cache upsert", cache_upsert),
            ("order mark processed", mark_processed),
            ("product mark published", mark_published),
        ],
        "product-create to order-cache waterfall",
    )?;

    let order_http = require_span(
        captured,
        "POST /orders",
        ORDER_SERVICE,
        &[("http.request.method", "POST"), ("http.route", "/orders")],
    )?;
    assert_span_kind(order_http, "SPAN_KIND_SERVER")?;
    let read_cached = require_child_span(
        captured,
        order_http,
        "db.query read cached product",
        ORDER_SERVICE,
        &[("db.query.summary", "read cached product")],
    )?;
    assert_span_kind(read_cached, "SPAN_KIND_CLIENT")?;
    let order_enqueue = require_child_span(
        captured,
        order_http,
        "kafkaman.enqueue",
        ORDER_SERVICE,
        &[("message_type", "order_snapshot")],
    )?;
    assert_span_kind(order_enqueue, "SPAN_KIND_PRODUCER")?;

    // The order -> product hop, which is where an application handler actually
    // runs. `order` declares `ProductSnapshot` with `cache::<T>()` and has no
    // handler at all, so this is the only place in the demo where one exists.
    let product_dispatch = require_span(
        captured,
        "kafkaman.dispatch",
        PRODUCT_SERVICE,
        &[("message_type", "order_snapshot")],
    )?;
    let handler = require_child_span(
        captured,
        product_dispatch,
        "kafkaman.handler",
        PRODUCT_SERVICE,
        &[
            ("message_type", "order_snapshot"),
            ("handler.position", "after"),
        ],
    )?;
    assert_span_kind(handler, "SPAN_KIND_INTERNAL")?;

    assert_no_default_poll_spans(captured)?;
    assert_internal_tier_split(captured)?;

    Ok(())
}

/// Every function span that runs on a *poll* must stay off the wire, and the
/// ones on the message path must reach it.
///
/// The `kafkaman::internal` tier is no longer uniformly debug-level. It is split
/// by the rule in the span-depth decision: a function that runs once there is a
/// message to describe is `info` and belongs in the trace; a function that runs
/// on a timer whether or not there is work is `debug`, because an idle service
/// otherwise exports nothing but its own scheduler. This asserts both halves,
/// because only the pair is a tier — either one alone is satisfied by turning
/// the whole thing off.
fn assert_internal_tier_split(captured: &Captured) -> TestResult {
    // Function spans, found structurally rather than by listing names: every
    // span this project opens deliberately is `kafkaman.*`, `db.query
    // <summary>`, or `METHOD /route`, all of which carry a `.`, a `/`, or a
    // space. A bare identifier can only have come from `#[instrument]` on a
    // function, which survives the renames those spans are allowed to have.
    let function_spans = captured
        .span_records
        .iter()
        .filter(|span| {
            !span.name.contains('.') && !span.name.contains('/') && !span.name.contains(' ')
        })
        .collect::<Vec<_>>();

    // Named exactly, because these are the whole cost argument. Measured before
    // the promotion: an idle stack exported 2322 spans in two minutes, and
    // essentially all of it was these functions running on their intervals.
    // A rename here is a deliberate act and should fail this test.
    const POLL_TIER_FUNCTIONS: [&str; 16] = [
        "claim_batch",
        "collapse_stale_pending_rows",
        "claim_received_row",
        "relay_once",
        "dispatch_once",
        "dispatch_once_sampled",
        "dispatch_once_with_observer",
        "purge_outbox_once",
        "refresh",
        "collect",
        "observe",
        "outbox_status_summary",
        "received_status_summary",
        "health",
        "ready",
        // Not poll-shaped — excluded so `kafkaman.enqueue` stays a trace root
        // in a service with no caller span. See `trace_root_enqueue`.
        "enqueue",
    ];

    let leaked = function_spans
        .iter()
        .filter(|span| POLL_TIER_FUNCTIONS.contains(&span.name.as_str()))
        .map(|span| format!("{} {}", span.service, span.name))
        .collect::<Vec<_>>();
    if !leaked.is_empty() {
        return Err(format!(
            "these functions run on a timer whether or not there is work, so they \
             stay in the debug tier however deep the rest of it goes. Leaked: \
             {leaked:?}. Captured: {}",
            captured.summary()
        )
        .into());
    }

    // The other half. Without it this test passes just as well against a tier
    // that was reverted to debug wholesale, which is the regression the
    // promotion is most likely to suffer.
    if function_spans.is_empty() {
        return Err(format!(
            "no `kafkaman::internal` function span reached the wire at RUST_LOG=info. \
             The message-path half of the tier is supposed to be visible by default \
             — that is what makes the waterfall gapless. Captured: {}",
            captured.summary()
        )
        .into());
    }

    Ok(())
}

fn assert_span_kind(span: &CapturedSpan, expected: &str) -> TestResult {
    if span.kind == expected {
        Ok(())
    } else {
        Err(format!(
            "{} {} expected kind {expected}, got {}",
            span.service, span.name, span.kind
        )
        .into())
    }
}

fn assert_same_trace(spans: &[(&str, &CapturedSpan)], description: &str) -> TestResult {
    let Some((root_name, root)) = spans.first() else {
        return Ok(());
    };
    for (name, span) in spans.iter().skip(1) {
        if span.trace_id != root.trace_id {
            return Err(format!(
                "{description} should use one trace id, but {name} has {} and {root_name} has {}",
                span.trace_id, root.trace_id
            )
            .into());
        }
    }
    Ok(())
}

fn assert_no_default_poll_spans(captured: &Captured) -> TestResult {
    // Every `db.query` span the schedulers open on a cycle that claims nothing.
    // All of them are `debug_span!`, so at the examples' `RUST_LOG=info` none of
    // them may appear on the wire.
    const POLL_SUMMARIES: [&str; 7] = [
        "claim outbox batch",
        "collapse stale pending outbox rows",
        "claim received row",
        "open outbox claim transaction",
        "commit outbox claim transaction",
        "open received claim transaction",
        "commit empty received claim transaction",
    ];

    let poll_spans = captured
        .span_records
        .iter()
        .filter(|span| {
            span.attributes
                .get("db.query.summary")
                .is_some_and(|summary| POLL_SUMMARIES.contains(&summary.as_str()))
        })
        .collect::<Vec<_>>();

    if poll_spans.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "default info-level telemetry should not export recurring scheduler \
             poll spans {POLL_SUMMARIES:?}. Captured: {}. Poll spans: {:?}",
            captured.summary(),
            poll_spans
        )
        .into())
    }
}

fn require_span<'a>(
    captured: &'a Captured,
    name: &str,
    service: &str,
    attrs: &[(&str, &str)],
) -> TestResult<&'a CapturedSpan> {
    captured
        .span_with_attrs(name, service, attrs)
        .ok_or_else(|| {
            missing_span_error(
                captured,
                &format!("{service} span {name:?} with attributes {attrs:?}"),
            )
        })
}

/// The nearest matching span *beneath* `parent`, at any depth.
///
/// Depth deliberately unpinned. Which functions carry a span is a tuning
/// decision the span-depth decision reserves the right to change, and the
/// waterfall's subject is causality — that this query belongs to that request —
/// not the number of frames between them. Pinning the edge made every promotion
/// of the `kafkaman::internal` tier a failure in a test that has no opinion
/// about the tier.
fn require_child_span<'a>(
    captured: &'a Captured,
    parent: &CapturedSpan,
    name: &str,
    service: &str,
    attrs: &[(&str, &str)],
) -> TestResult<&'a CapturedSpan> {
    captured
        .span_records
        .iter()
        .find(|span| {
            span.name == name
                && span.service == service
                && has_attrs(span, attrs)
                && captured.is_descendant_of(span, parent)
        })
        .ok_or_else(|| {
            missing_span_error(
                captured,
                &format!(
                    "{service} child span {name:?} with attributes {attrs:?} under {} {}",
                    parent.name, parent.span_id
                ),
            )
        })
}

fn has_attrs(span: &CapturedSpan, attrs: &[(&str, &str)]) -> bool {
    attrs.iter().all(|(key, value)| {
        span.attributes
            .get(*key)
            .is_some_and(|found| found == value)
    })
}

fn missing_span_error(captured: &Captured, wanted: &str) -> BoxError {
    format!(
        "missing {wanted}. Captured: {}. Span details: {}",
        captured.summary(),
        captured_span_details(captured)
    )
    .into()
}

fn captured_span_details(captured: &Captured) -> String {
    let mut spans = captured
        .span_records
        .iter()
        .map(|span| {
            format!(
                "{} {} kind={} span={} parent={:?} attrs={:?} links={}",
                span.service,
                span.name,
                span.kind,
                span.span_id,
                span.parent_span_id,
                span.attributes,
                span.links.len()
            )
        })
        .collect::<Vec<_>>();
    spans.sort();
    spans.join(" | ")
}

/// Block until `order`'s cache holds a view of the product past `after`, and
/// return the offset it settled on.
///
/// `409` is a stage of convergence rather than a failure: it is what `order`
/// answers for a product it has not applied yet.
async fn await_cached_beyond(stack: &Stack, product_id: &str, after: i64) -> TestResult<i64> {
    let url = format!("{}/products/{product_id}", stack.order.base_url());
    let deadline = Instant::now() + CONVERGENCE;

    while Instant::now() < deadline {
        let response = get(&stack.http, &url).await?;
        if response.status.is_success() {
            if let Some(offset) = response.body["applied_offset"].as_i64() {
                if offset > after {
                    return Ok(offset);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!(
        "order's cache for {product_id} did not move past offset {after} within \
         {CONVERGENCE:?}\n--- order ---\n{}\n--- product ---\n{}",
        stack.order.log().tail(40),
        stack.product.log().tail(40)
    )
    .into())
}
