//! Integration test for the axum-outbox example: drives the real handler against
//! a testcontainer Postgres and asserts the business row and outbox event are
//! written in one transaction.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_outbox::{build_router, changelog, ensure_business_schema, AppState, OrderCreated};
use durable_send_tests::{postgres_for_suite, TestPostgres};
use kafkaman::config::Config;
use kafkaman::sqlx::{migrate, MigrationContext, OutboxTable, ResolvedConfig};
use kafkaman::KafkaMessage;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

/// A PostgreSQL container for this suite, with the example's business table
/// already created.
async fn postgres() -> TestPostgres {
    let postgres = postgres_for_suite("axum-outbox")
        .await
        .expect("start postgres");

    // One short-lived connection: the business table is the example's own, not
    // kafkaman's, so no changeset creates it.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(postgres.url())
        .await
        .expect("connect for business schema");
    ensure_business_schema(&pool)
        .await
        .expect("business schema");
    pool.close().await;

    postgres
}

/// A router wired to a freshly migrated schema of its own.
async fn app_with_schema(
    postgres: &TestPostgres,
    schema: &str,
) -> (axum::Router, sqlx::PgPool, ResolvedConfig) {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(postgres.url())
        .await
        .expect("connect pool");

    let config = Config::parse(&format!(
        r#"
        [database]
        schema = "{schema}"

        [relay]
        worker_id = "example-test"
        batch_limit = 100
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "250ms"
        "#
    ))
    .expect("parse config");
    let cfg = ResolvedConfig::from_config(
        Some(&config),
        [OrderCreated::descriptor().expect("valid message")],
    )
    .expect("resolve config");
    migrate(
        &pool,
        &cfg,
        &MigrationContext::default(),
        &changelog::changelog().expect("changelog"),
    )
    .await
    .expect("migrate");

    let app = build_router(AppState {
        pool: pool.clone(),
        cfg: Arc::new(cfg.clone()),
    });
    (app, pool, cfg)
}

async fn post_order(app: axum::Router, body: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/orders")
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .expect("handler response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let parsed = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, parsed)
}

#[tokio::test]
async fn create_order_persists_business_row_and_outbox_event() {
    let postgres = postgres().await;
    let (app, pool, cfg) = app_with_schema(&postgres, "kafkaman_ok").await;

    let (status, parsed) = post_order(app, r#"{"description":"a widget"}"#).await;
    assert_eq!(status, StatusCode::OK);
    let order_id = parsed["order_id"].as_str().expect("order_id field");

    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders WHERE order_id = $1::uuid")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("count orders");
    assert_eq!(orders, 1);

    let table = OutboxTable::for_message::<OrderCreated>(&cfg).expect("outbox table");
    let outbox: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {} WHERE payload->>'order_id' = $1",
        table.qualified_name()
    ))
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("count outbox");
    assert_eq!(outbox, 1);
}

#[tokio::test]
async fn a_repeated_order_id_is_reported_as_a_conflict_not_a_server_error() {
    // A retried POST is a client-visible, expected outcome. Returning 500 would
    // tell the caller to retry again, which can never succeed.
    let postgres = postgres().await;
    let (app, _pool, _cfg) = app_with_schema(&postgres, "kafkaman_dup").await;
    let order_id = uuid::Uuid::new_v4();
    let body = format!(r#"{{"order_id":"{order_id}","description":"a widget"}}"#);

    let (first, _) = post_order(app.clone(), &body).await;
    assert_eq!(first, StatusCode::OK);

    let (second, parsed) = post_order(app, &body).await;
    assert_eq!(second, StatusCode::CONFLICT);
    // The body must describe the problem without leaking database internals.
    let message = parsed["error"].as_str().unwrap_or_default();
    assert!(message.contains("already exists"), "{message}");
    assert!(
        !message.contains("duplicate key"),
        "leaked driver error: {message}"
    );
    assert!(
        !message.to_lowercase().contains("constraint"),
        "leaked schema: {message}"
    );
}

#[tokio::test]
async fn an_empty_description_is_rejected_before_any_database_work() {
    let postgres = postgres().await;
    let (app, pool, _cfg) = app_with_schema(&postgres, "kafkaman_bad").await;

    let (status, parsed) = post_order(app, r#"{"description":"   "}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(parsed["error"]
        .as_str()
        .unwrap_or_default()
        .contains("description"));

    // Nothing was written: validation runs before the transaction opens. Scoped
    // to this description because `orders` is shared across the binary's tests.
    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders WHERE description = $1")
        .bind("   ")
        .fetch_one(&pool)
        .await
        .expect("count orders");
    assert_eq!(orders, 0);
}
