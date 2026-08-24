//! `product`'s own tests: the config file it ships, and the availability the
//! dispatch handler derives.
//!
//! This is where the handler's hard cases live, deliberately rather than in
//! `tests/distributed-cache`. Driving `dispatch_once` directly needs Postgres
//! but no broker, so a case like "two orders for one product, one of them
//! re-fulfilled" costs a fraction of a two-service round trip — and it is the
//! only case that can distinguish a correct exclusion from a missing one.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use example_contracts::{OrderSnapshot, OrderStatus, ProductSnapshot};
use example_product::{
    build_router, dispatch_router, ensure_business_schema, AppState,
    PRODUCT_SNAPSHOT_IDEMPOTENCY_NAMESPACE,
};
use kafkaman::config::Config;
use kafkaman::sqlx::{
    dispatch_once, insert_received, migrate, Changeset, MigrationContext, OutboxTable,
    ReceivedTable, ResolvedConfig, Role, RoleRegistry,
};
use kafkaman::{Envelope, IdempotencyIdentity, KafkaMessage};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

const CONTAINER_LABEL_PROJECT: &str = "com.kafkaman.project";
const CONTAINER_LABEL_PROJECT_VALUE: &str = "kafkaman";
const CONTAINER_LABEL_MANAGED_BY: &str = "com.kafkaman.managed-by";
const CONTAINER_LABEL_MANAGED_BY_VALUE: &str = "testcontainers";
const CONTAINER_LABEL_SUITE: &str = "com.kafkaman.test-suite";
const CONTAINER_LABEL_SUITE_VALUE: &str = "example-product";
const CONTAINER_LABEL_SERVICE: &str = "com.kafkaman.test-service";

/// The message types this service registers, in the order `service::start` does.
fn registered() -> Vec<kafkaman::MessageDescriptor> {
    vec![
        example_contracts::ProductSnapshot::descriptor().unwrap(),
        OrderSnapshot::descriptor().unwrap(),
    ]
}

#[test]
fn the_shipped_config_file_is_discoverable_and_complete() {
    // Cargo runs an integration test with the package root as the current
    // directory, so this is `Config::discover()` itself rather than a stand-in.
    let discovered = Config::discover()
        .expect("discovery must not fail")
        .expect("examples/product/kafkaman.toml must be discoverable from the package root");
    let cfg = ResolvedConfig::from_config(Some(&discovered), registered())
        .expect("the shipped config must fully resolve");
    assert_eq!(cfg.schema().as_str(), "kafkaman");
    assert_eq!(cfg.relay.worker_id, "product-1");

    // Asserted present rather than merely effective: `[topics]` defaults to
    // `verify`, so a deleted section would still resolve to the same value and
    // this test would not notice the example had stopped documenting the knob.
    assert!(
        discovered.contains("topics.mode"),
        "the shipped config must spell out the [topics] section"
    );
    assert_eq!(cfg.topics, kafkaman::TopicMode::Verify);
}

/// The roles `service::start` declares, restated so a test can migrate without
/// booting a broker.
fn generated_changelog() -> Vec<Box<dyn Changeset>> {
    let mut roles = RoleRegistry::new();
    roles
        .declare(
            ProductSnapshot::descriptor().expect("descriptor"),
            Role::Publish,
        )
        .expect("publish role");
    roles
        .declare(
            OrderSnapshot::descriptor().expect("descriptor"),
            Role::Handle,
        )
        .expect("handle role");
    roles.changelog().expect("changelog must build")
}

#[test]
fn the_declared_roles_cover_every_table_this_service_needs() {
    let changesets = generated_changelog();
    let mut names = changesets
        .iter()
        .map(|changeset| changeset.name())
        .collect::<Vec<_>>();
    names.sort_unstable();

    assert_eq!(
        names,
        vec![
            "create_cache_table",
            "create_outbox_table",
            "create_received_table",
            "init_schema",
        ],
        "inter-table order is by generated version and is deliberately arbitrary"
    );
}

#[tokio::test]
async fn products_can_be_listed_and_are_documented_in_openapi() {
    let postgres = postgres().await;
    let fixture = Fixture::start(&postgres).await;
    let app = fixture.app();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();

    for (product_id, name) in [(first, "widget"), (second, "gadget")] {
        let (status, body) = post(
            app.clone(),
            "/products",
            &format!(
                r#"{{"product_id":"{product_id}","name":"{name}","price_cents":1250,"on_hand":10}}"#
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    let (status, body) = get(app.clone(), "/products").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.as_array().expect("product list is an array");
    assert_eq!(rows.len(), 2);
    let ids = rows
        .iter()
        .map(|row| {
            row["product_id"]
                .as_str()
                .expect("product_id")
                .parse::<Uuid>()
                .expect("valid product_id")
        })
        .collect::<Vec<_>>();
    assert!(ids.contains(&first), "{body}");
    assert!(ids.contains(&second), "{body}");

    let (status, openapi) = get(app, "/api-docs/openapi.json").await;
    assert_eq!(status, StatusCode::OK, "{openapi}");
    assert!(
        openapi["paths"]["/products"].get("get").is_some(),
        "Swagger must document GET /products: {openapi}"
    );
}

#[tokio::test]
async fn availability_is_derived_from_fulfilled_orders_and_survives_replay() {
    let postgres = postgres().await;
    let fixture = Fixture::start(&postgres).await;
    let product_id = fixture.create_product(10).await;
    let order = Uuid::new_v4();

    // Placed reserves nothing.
    fixture
        .deliver(snapshot(order, product_id, 3, OrderStatus::Placed))
        .await;
    assert_eq!(fixture.available(product_id).await, 10);

    // Fulfilled counts. The handler sees its own cache row still holding
    // `Placed` — it runs *before* the upsert — which is why the query excludes
    // this entity and adds the incoming quantity back explicitly. Without that,
    // the order would be counted at both statuses and availability would land on
    // 7 for the wrong reason only when the quantities happened to match.
    fixture
        .deliver(snapshot(order, product_id, 3, OrderStatus::Fulfilled))
        .await;
    assert_eq!(fixture.available(product_id).await, 7);

    // Re-delivering the same fulfilled state changes nothing. Idempotence by
    // construction: the cache converges to one row per order, so a repeated
    // snapshot cannot be counted twice. A decrementing handler would need the
    // processed-mark to save it here.
    fixture
        .deliver(snapshot(order, product_id, 3, OrderStatus::Fulfilled))
        .await;
    assert_eq!(fixture.available(product_id).await, 7);

    // Cancelling restores the count with no compensating logic anywhere: the
    // order simply stops matching the filter.
    fixture
        .deliver(snapshot(order, product_id, 3, OrderStatus::Cancelled))
        .await;
    assert_eq!(fixture.available(product_id).await, 10);
}

#[tokio::test]
async fn two_orders_for_one_product_are_summed_and_neither_is_double_counted() {
    // The case that separates a correct `entity_key <> $3` exclusion from a
    // missing one. With a single order the two implementations agree; with a
    // second already-fulfilled order in the cache, dropping the exclusion counts
    // the re-fulfilled order at both its cached and its incoming status.
    let postgres = postgres().await;
    let fixture = Fixture::start(&postgres).await;
    let product_id = fixture.create_product(10).await;
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();

    fixture
        .deliver(snapshot(first, product_id, 4, OrderStatus::Fulfilled))
        .await;
    assert_eq!(fixture.available(product_id).await, 6);

    fixture
        .deliver(snapshot(second, product_id, 3, OrderStatus::Fulfilled))
        .await;
    assert_eq!(fixture.available(product_id).await, 3);

    // Re-fulfil the first: still 4 + 3, never 4 + 4 + 3.
    fixture
        .deliver(snapshot(first, product_id, 4, OrderStatus::Fulfilled))
        .await;
    assert_eq!(fixture.available(product_id).await, 3);

    // Availability never goes negative, even when fulfilled orders exceed stock
    // — which they can, because `order` admits against a cache that is by
    // definition slightly behind.
    let third = Uuid::new_v4();
    fixture
        .deliver(snapshot(third, product_id, 99, OrderStatus::Fulfilled))
        .await;
    assert_eq!(fixture.available(product_id).await, 0);
}

#[tokio::test]
async fn every_recompute_republishes_the_product_through_the_outbox() {
    // Consume-then-produce in one transaction. The republished snapshot is
    // state-sourced — read back from `products` after the update — rather than a
    // stored outbox row replayed, which under an offset ordinal would hand stale
    // state a newer offset at every consumer.
    let postgres = postgres().await;
    let fixture = Fixture::start(&postgres).await;
    let product_id = fixture.create_product(10).await;

    let before = fixture.outbox_rows(product_id).await;
    fixture
        .deliver(snapshot(
            Uuid::new_v4(),
            product_id,
            2,
            OrderStatus::Fulfilled,
        ))
        .await;
    let after = fixture.outbox_rows(product_id).await;
    assert_eq!(after.len(), before.len() + 1);

    let (payload, idempotency_key) = after.last().unwrap().clone();
    let version = fixture.version(product_id).await;
    assert_eq!(version, 2, "the recompute must bump the product's version");
    assert_eq!(payload["available"], 8);
    assert!(
        payload.get("on_hand").is_none(),
        "stock on hand is private to this service: {payload}"
    );
    // The idempotency key is derived from `(product_id, version)`, so a value
    // that returns to a previously published one still gets a fresh key and is
    // not dropped as a duplicate by the consumer's dedupe.
    let expected = IdempotencyIdentity::derive(
        PRODUCT_SNAPSHOT_IDEMPOTENCY_NAMESPACE,
        (product_id, version),
    )
    .expect("derive")
    .key
    .to_string();
    assert_eq!(idempotency_key, expected);
}

#[tokio::test]
async fn an_order_for_an_unknown_product_is_absorbed_rather_than_retried() {
    // Nothing here will become true by waiting, so failing the row would only
    // spend its attempt budget and park it in the DLQ.
    let postgres = postgres().await;
    let fixture = Fixture::start(&postgres).await;

    let stats = fixture
        .deliver(snapshot(
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            OrderStatus::Fulfilled,
        ))
        .await;
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 0);
}

fn snapshot(order_id: Uuid, product_id: Uuid, quantity: i64, status: OrderStatus) -> OrderSnapshot {
    OrderSnapshot {
        order_id,
        product_id,
        quantity,
        status,
    }
}

/// A migrated `product` database with its dispatch router, plus the bookkeeping
/// needed to feed it received rows.
struct Fixture {
    pool: PgPool,
    cfg: Arc<ResolvedConfig>,
    router: kafkaman::sqlx::MessageRouter,
    received: ReceivedTable,
    /// Source offsets have to increase, or the cache's convergence guard
    /// correctly ignores the record and the test would be asserting on the
    /// guard rather than on the handler.
    next_offset: std::sync::atomic::AtomicI64,
}

impl Fixture {
    async fn start(postgres: &TestPostgres) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(postgres.url())
            .await
            .expect("connect pool");

        let discovered = Config::discover()
            .expect("discovery")
            .expect("examples/product/kafkaman.toml");
        let cfg =
            ResolvedConfig::from_config(Some(&discovered), registered()).expect("resolve config");

        ensure_business_schema(&pool)
            .await
            .expect("business schema");
        migrate(
            &pool,
            &cfg,
            &MigrationContext::default(),
            &generated_changelog(),
        )
        .await
        .expect("migrate");

        let cfg = Arc::new(cfg);
        let received = ReceivedTable::for_message::<OrderSnapshot>(&cfg).expect("received table");
        let router = dispatch_router(Arc::clone(&cfg)).expect("router");
        Self {
            pool,
            cfg,
            router,
            received,
            next_offset: std::sync::atomic::AtomicI64::new(0),
        }
    }

    fn app(&self) -> axum::Router {
        build_router(AppState {
            pool: self.pool.clone(),
            cfg: Arc::clone(&self.cfg),
        })
    }

    async fn create_product(&self, on_hand: i64) -> Uuid {
        let product_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO products
                 (product_id, name, price_cents, status, on_hand, available, version)
             VALUES ($1, 'widget', 1000, 'Available', $2, $2, 1)",
        )
        .bind(product_id)
        .bind(on_hand)
        .execute(&self.pool)
        .await
        .expect("insert product");
        product_id
    }

    /// Write a received row the way the ingester would, then dispatch it.
    async fn deliver(&self, order: OrderSnapshot) -> kafkaman::sqlx::DispatchStats {
        let offset = self
            .next_offset
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // A fresh key per delivery, so redelivery of one *state* is what the
        // test controls rather than what the received table's dedupe hides.
        let identity =
            IdempotencyIdentity::derive("example-product:test", (order.order_id, offset))
                .expect("derive");
        let envelope = Envelope::new(order)
            .try_with_idempotency_key(identity)
            .expect("attach identity");

        let mut tx = self.pool.begin().await.expect("begin");
        insert_received(&mut tx, &self.cfg, &envelope, 0, offset, None)
            .await
            .expect("insert received");
        tx.commit().await.expect("commit");

        dispatch_once(
            &self.pool,
            &self.received,
            &self.router,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("dispatch")
    }

    async fn available(&self, product_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT available FROM products WHERE product_id = $1")
            .bind(product_id)
            .fetch_one(&self.pool)
            .await
            .expect("read available")
    }

    /// Every published snapshot for a product, oldest first.
    async fn outbox_rows(&self, product_id: Uuid) -> Vec<(serde_json::Value, String)> {
        let outbox = OutboxTable::for_message::<example_contracts::ProductSnapshot>(&self.cfg)
            .expect("outbox table")
            .qualified_name();
        sqlx::query_as(&format!(
            "SELECT payload, idempotency_key FROM {outbox}
              WHERE entity_key = $1
           ORDER BY created_at, message_id"
        ))
        .bind(product_id.to_string())
        .fetch_all(&self.pool)
        .await
        .expect("read outbox")
    }

    async fn version(&self, product_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT version FROM products WHERE product_id = $1")
            .bind(product_id)
            .fetch_one(&self.pool)
            .await
            .expect("read version")
    }
}

async fn post(app: axum::Router, uri: &str, body: &str) -> (StatusCode, serde_json::Value) {
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_owned()))
            .expect("request"),
    )
    .await
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    send(
        app,
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request"),
    )
    .await
}

async fn send(app: axum::Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
    let response = app.oneshot(request).await.expect("handler response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let parsed = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, parsed)
}

#[derive(Debug)]
struct TestPostgres {
    _container: ContainerAsync<GenericImage>,
    url: String,
}

impl TestPostgres {
    fn url(&self) -> &str {
        self.url.as_str()
    }
}

/// One owned PostgreSQL container per test; see the note in
/// `examples/order/tests/service.rs`.
async fn postgres() -> TestPostgres {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_label(CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE)
        .with_label(CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE)
        .with_label(CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE)
        .with_label(CONTAINER_LABEL_SERVICE, "postgres")
        .with_env_var("POSTGRES_DB", "product_service")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("host");
    let host_port = container.get_host_port_ipv4(port).await.expect("host port");
    TestPostgres {
        url: format!("postgres://postgres:postgres@{host}:{host_port}/product_service"),
        _container: container,
    }
}
