//! The two-service distributed-cache test: HTTP in, HTTP out, nothing else.
//!
//! No database handle, no `Harness`, no direct call into kafkaman. Both example
//! services are started exactly as their binaries start them, and every
//! assertion goes through their public HTTP surface. That is the only altitude
//! at which a wiring mistake is observable — a loop never spawned, a changeset
//! missing from a changelog, a topic that does not match — because every one of
//! those passes the entire rest of the workspace's test suite.
//!
//! # Why so few tests
//!
//! One PostgreSQL, one Redpanda, two services, six loops, and multi-hop
//! convergence between every assertion. Containers are owned per test, so the
//! cost is paid per test function; a lifecycle walked once in one fat test is
//! far cheaper than the same ground covered by six thin ones, and it is also the
//! only way to assert that *cancellation restores availability*, which needs the
//! three preceding steps to have happened.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

use distributed_cache_tests::{get, post, post_empty, Cluster, Services, TestResult};
use serde::Deserialize;
use uuid::Uuid;

/// How long a single convergence step may take.
///
/// Generous because the first hop also pays for the consumer group's first
/// assignment. (It no longer pays for topic auto-creation: the fixture
/// provisions the topics up front, because the services now verify them at boot
/// and would refuse to start otherwise.) A step that is genuinely broken fails
/// at the deadline; a step that is merely slow must not.
const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(90);
/// Gap between cache reads while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How long a value must hold before it counts as converged.
///
/// A two-hop round trip passes through an intermediate republish, so asserting
/// on the first changed value seen would sometimes pass by accident and
/// sometimes read a value that is about to be replaced.
const SETTLE_FOR: Duration = Duration::from_millis(750);

/// `order`'s view of a product it does not own, as served from its cache.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct CachedProductView {
    product_id: Uuid,
    name: String,
    price_cents: i64,
    status: String,
    available: i64,
    applied_partition: i32,
    applied_offset: i64,
}

#[derive(Clone, Debug, Deserialize)]
struct OrderView {
    order_id: Uuid,
    status: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ProductView {
    product_id: Uuid,
    on_hand: i64,
    available: i64,
}

#[tokio::test]
async fn the_order_service_converges_on_product_state_and_product_derives_availability_back(
) -> TestResult {
    let cluster = Cluster::start().await?;
    let services = Services::start(&cluster).await?;

    // 1. `product` creates a product. Nothing tells `order` about it except the
    //    topic.
    let created = post(
        &services.http,
        &services.product_url("/products"),
        serde_json::json!({ "name": "widget", "price_cents": 1_250, "on_hand": 10 }),
    )
    .await?;
    assert_eq!(created.status, 201, "{:?}", created.body);
    let product: ProductView = created.json()?;
    assert_eq!(product.on_hand, 10);
    assert_eq!(product.available, 10);
    let product_id = product.product_id;

    let converged = await_cache_beyond(&services, product_id, -1).await?;
    assert_eq!(converged.status, "Available");
    assert_eq!(converged.available, 10);
    assert_eq!(converged.name, "widget");
    assert_eq!(converged.price_cents, 1_250);

    // 2. `order` accepts an order from its cache alone, and availability does
    //    *not* move: a placed order reserves nothing. This is the step that
    //    proves the number is derived from fulfilled orders rather than from
    //    "an order happened".
    let placed = post(
        &services.http,
        &services.order_url("/orders"),
        serde_json::json!({ "product_id": product_id, "quantity": 3 }),
    )
    .await?;
    assert_eq!(placed.status, 201, "{:?}", placed.body);
    let order: OrderView = placed.json()?;
    assert_eq!(order.status, "Placed");
    let order_id = order.order_id;

    let after_placed = await_cache_beyond(&services, product_id, converged.applied_offset).await?;
    assert_eq!(
        after_placed.available, 10,
        "a placed order must not reserve stock"
    );

    // 3. One HTTP write on `order`, observable two hops later through one HTTP
    //    read on `order` — via `product`, which never saw the request.
    let fulfilled = post_empty(
        &services.http,
        &services.order_url(&format!("/orders/{order_id}/fulfil")),
    )
    .await?;
    assert_eq!(fulfilled.status, 200, "{:?}", fulfilled.body);
    assert_eq!(fulfilled.json::<OrderView>()?.status, "Fulfilled");

    let after_fulfilled =
        await_cache_beyond(&services, product_id, after_placed.applied_offset).await?;
    assert_eq!(after_fulfilled.available, 7);

    // 4. The step that justifies deriving rather than decrementing: cancelling
    //    gives the stock back, and there is no compensating logic anywhere in
    //    either service. The order simply stops matching `product`'s filter.
    let cancelled = post_empty(
        &services.http,
        &services.order_url(&format!("/orders/{order_id}/cancel")),
    )
    .await?;
    assert_eq!(cancelled.status, 200, "{:?}", cancelled.body);
    assert_eq!(cancelled.json::<OrderView>()?.status, "Cancelled");

    let after_cancelled =
        await_cache_beyond(&services, product_id, after_fulfilled.applied_offset).await?;
    assert_eq!(
        after_cancelled.available, 10,
        "cancellation must restore availability with no compensating write"
    );

    // 5. Rejected on availability alone. Checked here, while the product is
    //    still `Available`, because after the next step a rejection would prove
    //    nothing about the quantity comparison.
    let too_many = post(
        &services.http,
        &services.order_url("/orders"),
        serde_json::json!({ "product_id": product_id, "quantity": 999 }),
    )
    .await?;
    assert_eq!(too_many.status, 409, "{:?}", too_many.body);
    assert!(
        too_many.error_message().contains("available"),
        "{}",
        too_many.error_message()
    );

    // 6. Rejected on status alone, with availability untouched at 10. Both
    //    conditions matter, so the snapshot has to carry state and not just a
    //    counter.
    let discontinued = post_empty(
        &services.http,
        &services.product_url(&format!("/products/{product_id}/discontinue")),
    )
    .await?;
    assert_eq!(discontinued.status, 200, "{:?}", discontinued.body);

    let after_discontinued =
        await_cache_beyond(&services, product_id, after_cancelled.applied_offset).await?;
    assert_eq!(after_discontinued.status, "Discontinued");
    assert_eq!(
        after_discontinued.available, 10,
        "a discontinued product keeps its stock; it is the status that forbids the order"
    );

    let rejected = post(
        &services.http,
        &services.order_url("/orders"),
        serde_json::json!({ "product_id": product_id, "quantity": 1 }),
    )
    .await?;
    assert_eq!(rejected.status, 409, "{:?}", rejected.body);
    assert!(
        rejected.error_message().contains("not available"),
        "{}",
        rejected.error_message()
    );

    services.shutdown().await
}

#[tokio::test]
async fn an_order_for_an_unpropagated_product_is_refused_rather_than_fetched() -> TestResult {
    // The negative case for the whole design: `order` has no synchronous path to
    // `product`, so a product it has not received is a product it cannot sell.
    // Reported as 409 rather than 404, because waiting can make it succeed.
    let cluster = Cluster::start().await?;
    let services = Services::start(&cluster).await?;

    let response = post(
        &services.http,
        &services.order_url("/orders"),
        serde_json::json!({ "product_id": Uuid::new_v4(), "quantity": 1 }),
    )
    .await?;
    assert_eq!(response.status, 409, "{:?}", response.body);
    assert!(
        response.error_message().contains("propagated"),
        "{}",
        response.error_message()
    );

    let read = get(
        &services.http,
        &services.order_url(&format!("/products/{}", Uuid::new_v4())),
    )
    .await?;
    assert_eq!(read.status, 409, "{:?}", read.body);

    services.shutdown().await
}

/// The same lifecycle, with `product` hand-wired instead of built from roles.
///
/// The runtime builder's whole promise is that it derives what a service would
/// otherwise assemble by hand. That is only worth anything if the hand-assembled
/// version still works — and "still works" cannot be established by reading
/// `service_manual.rs`, because every mistake it could contain also compiles.
///
/// Deliberately shorter than the builder-path test above rather than a second
/// full walk. Containers are owned per test function, so a duplicate six-step
/// lifecycle would double the suite's wall clock to re-assert steps whose
/// behaviour lives entirely in code both paths share. What differs between the
/// paths is *assembly*: the changelog, the tables, the loops, and the dispatch
/// wiring. Booting, converging one hop out, and deriving one hop back exercises
/// all four; cancellation and rejection semantics do not touch any of them.
#[tokio::test]
async fn the_hand_wired_boot_path_produces_an_equivalent_runtime() -> TestResult {
    let cluster = Cluster::start().await?;
    let services = Services::start_with(&cluster, example_product::BootMode::Manual).await?;

    // Out: a product created on `product` reaches `order`'s cache.
    let created = post(
        &services.http,
        &services.product_url("/products"),
        serde_json::json!({ "name": "hand-wired widget", "price_cents": 800, "on_hand": 6 }),
    )
    .await?;
    assert_eq!(created.status, 201, "{:?}", created.body);
    let product_id = created.json::<ProductView>()?.product_id;

    let converged = await_cache_beyond(&services, product_id, -1).await?;
    assert_eq!(converged.name, "hand-wired widget");
    assert_eq!(converged.available, 6);

    // Back: an order fulfilled on `order` is derived and republished by
    // `product`'s hand-registered handler, in its own dispatch transaction.
    let placed = post(
        &services.http,
        &services.order_url("/orders"),
        serde_json::json!({ "product_id": product_id, "quantity": 2 }),
    )
    .await?;
    assert_eq!(placed.status, 201, "{:?}", placed.body);
    let order_id = placed.json::<OrderView>()?.order_id;

    let after_placed = await_cache_beyond(&services, product_id, converged.applied_offset).await?;
    assert_eq!(after_placed.available, 6, "a placed order reserves nothing");

    let fulfilled = post_empty(
        &services.http,
        &services.order_url(&format!("/orders/{order_id}/fulfil")),
    )
    .await?;
    assert_eq!(fulfilled.status, 200, "{:?}", fulfilled.body);

    let after_fulfilled =
        await_cache_beyond(&services, product_id, after_placed.applied_offset).await?;
    assert_eq!(
        after_fulfilled.available, 4,
        "the hand-wired dispatcher must derive availability exactly as the builder's does"
    );

    services.shutdown().await
}

/// Wait until `order`'s cache for `product_id` has moved past `after_offset` and
/// then held still.
///
/// Two conditions, and both are needed. The offset must advance, or a read taken
/// before the round trip even started would satisfy an assertion about the value
/// it is *already* at. And the value must then hold for [`SETTLE_FOR`], or an
/// intermediate republish part-way through a two-hop trip could be mistaken for
/// the final state.
async fn await_cache_beyond(
    services: &Services,
    product_id: Uuid,
    after_offset: i64,
) -> TestResult<CachedProductView> {
    let started = Instant::now();
    let mut candidate: Option<(CachedProductView, Instant)> = None;

    loop {
        let observed = read_cached_product(services, product_id).await?;
        match observed {
            Some(view) if view.applied_offset > after_offset => match &candidate {
                Some((previous, since)) if *previous == view => {
                    if since.elapsed() >= SETTLE_FOR {
                        return Ok(view);
                    }
                }
                _ => candidate = Some((view, Instant::now())),
            },
            _ => candidate = None,
        }

        if started.elapsed() > CONVERGENCE_DEADLINE {
            return Err(format!(
                "order's cache for {product_id} did not settle past offset {after_offset} \
                 within {CONVERGENCE_DEADLINE:?}; last observed: {candidate:?}"
            )
            .into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// `order`'s cached view, or `None` while nothing has been applied yet.
async fn read_cached_product(
    services: &Services,
    product_id: Uuid,
) -> TestResult<Option<CachedProductView>> {
    let response = get(
        &services.http,
        &services.order_url(&format!("/products/{product_id}")),
    )
    .await?;
    match response.status.as_u16() {
        200 => Ok(Some(response.json()?)),
        409 => Ok(None),
        other => Err(format!(
            "unexpected status {other} reading cached product: {:?}",
            response.body
        )
        .into()),
    }
}
