//! `order`'s own tests: the config file it ships, and the admission rules it
//! applies to its cache.
//!
//! These sit one tier below `tests/distributed-cache`. Nothing here involves a
//! broker or a second service — the cache is seeded directly, because at this
//! tier the question is "given converged state, does this service behave?", not
//! "does state converge?".
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use example_contracts::OrderSnapshot;
use example_contracts::{ProductSnapshot, ProductStatus};
use example_order::{
    build_router, ensure_business_schema, AppState, ORDER_SNAPSHOT_IDEMPOTENCY_NAMESPACE,
};
use kafkaman::config::Config;
use kafkaman::sqlx::{
    migrate, CacheTable, Changeset, MigrationContext, OutboxTable, ResolvedConfig, Role,
    RoleRegistry,
};
use kafkaman::KafkaMessage;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tower::ServiceExt;
use uuid::Uuid;

const CONTAINER_LABEL_PROJECT: &str = "com.kafkaman.project";
const CONTAINER_LABEL_PROJECT_VALUE: &str = "kafkaman";
const CONTAINER_LABEL_MANAGED_BY: &str = "com.kafkaman.managed-by";
const CONTAINER_LABEL_MANAGED_BY_VALUE: &str = "testcontainers";
const CONTAINER_LABEL_SUITE: &str = "com.kafkaman.test-suite";
const CONTAINER_LABEL_SUITE_VALUE: &str = "example-order";
const CONTAINER_LABEL_SERVICE: &str = "com.kafkaman.test-service";

/// The message types this service registers, in the order `service::start` does.
fn registered() -> Vec<kafkaman::MessageDescriptor> {
    vec![
        example_contracts::OrderSnapshot::descriptor().unwrap(),
        ProductSnapshot::descriptor().unwrap(),
    ]
}

#[test]
fn the_shipped_config_file_is_discoverable_and_complete() {
    // Cargo runs an integration test with the package root as the current
    // directory, which is exactly where the binary is meant to be run from — so
    // this exercises `Config::discover()` itself, not a stand-in.
    //
    // The previous example had no `kafkaman.toml` anywhere in the repository and
    // could not start at all. Nothing caught it, because its only test built a
    // config from a string and bypassed discovery. This is that regression test.
    let discovered = Config::discover()
        .expect("discovery must not fail")
        .expect("examples/order/kafkaman.toml must be discoverable from the package root");

    // Resolution is where a missing key, an unparseable duration, or a retry
    // policy naming an unregistered message type is rejected.
    let cfg = ResolvedConfig::from_config(Some(&discovered), registered())
        .expect("the shipped config must fully resolve");
    assert_eq!(cfg.schema().as_str(), "kafkaman");
    assert_eq!(cfg.relay.worker_id, "order-1");

    // `[topics]` defaults to `verify` when absent, so deleting the section
    // would leave this resolving and every other assertion here passing. The
    // key has to be asserted present, not merely effective — an example that
    // silently relies on a default is not showing the knob.
    assert!(
        discovered.contains("topics.mode"),
        "the shipped config must spell out the [topics] section"
    );
    assert_eq!(cfg.topics, kafkaman::TopicMode::Verify);
}

/// The roles `service::start` declares, restated so a test can migrate without
/// booting a broker.
///
/// A duplicate of two lines, and the duplication is the reason this test exists:
/// if the two ever disagree, the assertion below is what notices.
fn roles() -> RoleRegistry {
    let mut roles = RoleRegistry::new();
    roles
        .declare(
            OrderSnapshot::descriptor().expect("descriptor"),
            Role::Publish,
        )
        .expect("publish role");
    roles
        .declare(
            ProductSnapshot::descriptor().expect("descriptor"),
            Role::Cache,
        )
        .expect("cache role");
    roles
}

fn generated_changelog() -> Vec<Box<dyn Changeset>> {
    roles().changelog().expect("changelog must build")
}

#[test]
fn the_declared_roles_cover_every_table_this_service_needs() {
    // Ordering and version uniqueness are asserted inside `RoleRegistry`; what
    // this adds is that all three tables are present. A changelog missing the
    // cache table migrates, boots, publishes and ingests without complaint, and
    // only fails at the very last statement of `dispatch_once`.
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
async fn an_order_is_accepted_from_cache_and_enqueued_in_the_same_transaction() {
    let postgres = postgres().await;
    let (app, pool, cfg) = service(&postgres).await;

    let product_id = Uuid::new_v4();
    seed_cache(
        &pool,
        &cfg,
        snapshot(product_id, ProductStatus::Available, 10),
        0,
    )
    .await;

    let (status, body) = post(
        app.clone(),
        "/orders",
        &format!(r#"{{"product_id":"{product_id}","quantity":3}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let order_id = body["order_id"].as_str().expect("order_id").to_owned();
    assert_eq!(body["status"], "Placed");
    assert_eq!(body["version"], 1);

    let (status, body) = get(app.clone(), "/orders").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.as_array().expect("order list is an array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["order_id"], order_id);

    let (status, openapi) = get(app, "/api-docs/openapi.json").await;
    assert_eq!(status, StatusCode::OK, "{openapi}");
    assert!(
        openapi["paths"]["/orders"].get("get").is_some(),
        "Swagger must document GET /orders: {openapi}"
    );

    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders WHERE order_id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&pool)
        .await
        .expect("count orders");
    assert_eq!(orders, 1);

    // The snapshot is in the outbox because it was written by the same
    // transaction that inserted the order — not by a follow-up call that a crash
    // could land between.
    let outbox = OutboxTable::for_message::<example_contracts::OrderSnapshot>(&cfg)
        .expect("outbox table")
        .qualified_name();
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {outbox} WHERE payload->>'order_id' = $1"
    ))
    .bind(&order_id)
    .fetch_one(&pool)
    .await
    .expect("count outbox");
    assert_eq!(count, 1);

    // The entity key is the order id, which is what makes per-entity supersede
    // and the consumer's convergence guard line up on the same identity.
    let entity_key: String = sqlx::query_scalar(&format!(
        "SELECT entity_key FROM {outbox} WHERE payload->>'order_id' = $1"
    ))
    .bind(&order_id)
    .fetch_one(&pool)
    .await
    .expect("read entity key");
    assert_eq!(entity_key, order_id);

    // And the idempotency key is derived from `(order_id, version)`, so the same
    // state re-enqueued deduplicates while the next state does not.
    let key: String = sqlx::query_scalar(&format!(
        "SELECT idempotency_key FROM {outbox} WHERE payload->>'order_id' = $1"
    ))
    .bind(&order_id)
    .fetch_one(&pool)
    .await
    .expect("read idempotency key");
    let expected = kafkaman::IdempotencyIdentity::derive(
        ORDER_SNAPSHOT_IDEMPOTENCY_NAMESPACE,
        (Uuid::parse_str(&order_id).unwrap(), 1_i64),
    )
    .expect("derive")
    .key
    .to_string();
    assert_eq!(key, expected);
}

#[tokio::test]
async fn admission_is_decided_by_cached_state_and_never_by_a_call_to_product() {
    let postgres = postgres().await;
    let (app, pool, cfg) = service(&postgres).await;

    // Nothing cached: refused, and refused as a conflict rather than a 404,
    // because the product may simply not have propagated yet.
    let unknown = Uuid::new_v4();
    let (status, body) = post(
        app.clone(),
        "/orders",
        &format!(r#"{{"product_id":"{unknown}","quantity":1}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["error"].as_str().unwrap().contains("propagated"));

    // Discontinued with stock on hand: still unorderable. The snapshot has to
    // carry state, not only a counter, for this to be expressible at all.
    let discontinued = Uuid::new_v4();
    seed_cache(
        &pool,
        &cfg,
        snapshot(discontinued, ProductStatus::Discontinued, 10),
        1,
    )
    .await;
    let (status, body) = post(
        app.clone(),
        "/orders",
        &format!(r#"{{"product_id":"{discontinued}","quantity":1}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["error"].as_str().unwrap().contains("not available"));

    // Available but not enough of it.
    let scarce = Uuid::new_v4();
    seed_cache(
        &pool,
        &cfg,
        snapshot(scarce, ProductStatus::Available, 2),
        2,
    )
    .await;
    let (status, body) = post(
        app.clone(),
        "/orders",
        &format!(r#"{{"product_id":"{scarce}","quantity":3}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["error"].as_str().unwrap().contains("available"));

    // A quantity that is not a quantity is a client error, and is rejected
    // before any database work.
    let (status, body) = post(
        app.clone(),
        "/orders",
        &format!(r#"{{"product_id":"{scarce}","quantity":0}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // Reading a product this service does not own is served entirely locally.
    let (status, body) = get(app, &format!("/products/{scarce}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["available"], 2);
    assert_eq!(body["status"], "Available");
    assert_eq!(body["applied_offset"], 2);

    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders")
        .fetch_one(&pool)
        .await
        .expect("count orders");
    assert_eq!(orders, 0, "no rejected request may leave an order behind");
}

fn snapshot(product_id: Uuid, status: ProductStatus, available: i64) -> ProductSnapshot {
    ProductSnapshot {
        product_id,
        name: "widget".to_owned(),
        price_cents: 1_000,
        status,
        available,
    }
}

/// Write a cache row the way a dispatched message would.
///
/// Direct SQL rather than a full ingest+dispatch round trip: this tier is about
/// what the service does with converged state, and manufacturing that state
/// through a broker would test the broker.
async fn seed_cache(pool: &PgPool, cfg: &ResolvedConfig, snapshot: ProductSnapshot, offset: i64) {
    let cache = CacheTable::for_message::<ProductSnapshot>(cfg)
        .expect("cache table")
        .qualified_name();
    sqlx::query(&format!(
        "INSERT INTO {cache}
             (entity_key, payload, applied_topic, applied_partition, applied_offset)
         VALUES ($1, $2, $3, 0, $4)"
    ))
    .bind(snapshot.product_id.to_string())
    .bind(serde_json::to_value(&snapshot).expect("serialize snapshot"))
    .bind(ProductSnapshot::TOPIC)
    .bind(offset)
    .execute(pool)
    .await
    .expect("seed cache");
}

async fn service(postgres: &TestPostgres) -> (axum::Router, PgPool, ResolvedConfig) {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(postgres.url())
        .await
        .expect("connect pool");

    // The shipped file, not a literal: a config that only exists inside a test
    // is a config that can silently stop matching the one that ships.
    let discovered = Config::discover()
        .expect("discovery")
        .expect("examples/order/kafkaman.toml");
    let cfg = ResolvedConfig::from_config(Some(&discovered), registered()).expect("resolve config");

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

    let app = build_router(AppState::new(pool.clone(), Arc::new(cfg.clone())).expect("app state"));
    (app, pool, cfg)
}

async fn post(app: axum::Router, uri: &str, body: &str) -> (StatusCode, serde_json::Value) {
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap(),
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
            .unwrap(),
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

/// One owned PostgreSQL container per test.
///
/// Testcontainers stops containers through `ContainerAsync`'s `Drop`, and a
/// value in a `static` never drops at process exit — which leaks a container per
/// binary per run.
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
        .with_env_var("POSTGRES_DB", "order_service")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("host");
    let host_port = container.get_host_port_ipv4(port).await.expect("host port");
    TestPostgres {
        url: format!("postgres://postgres:postgres@{host}:{host_port}/order_service"),
        _container: container,
    }
}
