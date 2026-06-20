//! Integration test for the axum-outbox example: drives the real handler against
//! a testcontainer Postgres and asserts the business row and outbox event are
//! written in one transaction.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_outbox::{build_router, changelog, ensure_business_schema, AppState, OrderCreated};
use kafkaman::config::Config;
use kafkaman::sqlx::{migrate, MigrationContext, OutboxTable, ResolvedConfig};
use kafkaman::KafkaMessage;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tower::ServiceExt;

#[tokio::test]
async fn create_order_persists_business_row_and_outbox_event() {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "kafkaman_example")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await
        .expect("start postgres");
    let host_port = container.get_host_port_ipv4(port).await.expect("host port");
    let database_url =
        format!("postgres://postgres:postgres@127.0.0.1:{host_port}/kafkaman_example");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect pool");
    ensure_business_schema(&pool)
        .await
        .expect("business schema");
    let config = Config::from_str(
        r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "example-test"
        batch_limit = 100
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "250ms"
        "#,
    )
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
        &changelog::changelog(),
    )
    .await
    .expect("migrate");

    let app = build_router(AppState {
        pool: pool.clone(),
        cfg: Arc::new(cfg.clone()),
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/orders")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"description":"a widget"}"#))
                .unwrap(),
        )
        .await
        .expect("handler response");
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    let order_id = parsed["order_id"].as_str().expect("order_id field");

    let orders: i64 = sqlx::query_scalar("SELECT count(*) FROM orders WHERE order_id = $1::uuid")
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("count orders");
    assert_eq!(orders, 1);

    let table = OutboxTable::for_message::<OrderCreated>(&cfg).expect("outbox table");
    let outbox: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {}", table.qualified_name()))
            .fetch_one(&pool)
            .await
            .expect("count outbox");
    assert_eq!(outbox, 1);
}
