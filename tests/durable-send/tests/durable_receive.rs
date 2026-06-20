use kafkaman_core::{Envelope, KafkaMessage, ReceiveStatus};
use kafkaman_sqlx::{dispatch_once, Changeset, CreateReceivedTable, MessageRouter};
use kafkaman_test::Harness;
use kafkaman_worker::BoxError;
use serde::{Deserialize, Serialize};
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::OffsetDateTime;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OrderCreated {
    order_id: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }
}

#[tokio::test]
async fn receive_table_uses_required_idempotency_key() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let cfg = harness.config();

    let idempotency_nullable: String = sqlx::query_scalar(
        "SELECT is_nullable
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = $2
           AND column_name = 'idempotency_key'",
    )
    .bind(cfg.schema.as_str())
    .bind(table.table.as_str())
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(idempotency_nullable, "NO");

    let unique_indexes: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_indexes
         WHERE schemaname = $1
           AND tablename = $2
           AND indexdef ILIKE '%UNIQUE%'
           AND indexdef ILIKE '%idempotency_key%'",
    )
    .bind(cfg.schema.as_str())
    .bind(table.table.as_str())
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(unique_indexes, 1);

    let changeset = CreateReceivedTable::new(99, OrderCreated::descriptor()?);
    assert_eq!(changeset.name(), "create_received_table");

    Ok(())
}

#[tokio::test]
async fn dispatch_once_commits_handler_effect_and_processed_status() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-456".to_owned(),
    })
    .with_idempotency_key("idem-456");
    assert!(
        harness
            .insert_received(&envelope, 1, 22, Some(b"order-456"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 0);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-456")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);
    assert_eq!(row.processed_at, Some(now));

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn dispatch_failure_rolls_back_effect_and_parks_retryable() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-789".to_owned(),
    })
    .with_idempotency_key("idem-789");
    assert!(
        harness
            .insert_received(&envelope, 2, 33, Some(b"order-789"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Err(kafkaman_sqlx::Error::Handler("boom".to_owned()))
        })
    });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-789")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.next_attempt_at, None);
    assert_eq!(row.errors.len(), 1);
    assert!(row.errors[0].message.contains("boom"));

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 0);

    let second = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(second.claimed, 0);
    assert_eq!(second.processed, 0);
    assert_eq!(second.failed, 0);

    Ok(())
}

#[tokio::test]
async fn duplicate_received_rows_converge_to_one_dispatch_effect() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    for offset in 100..105 {
        let envelope = Envelope::new(OrderCreated {
            order_id: "order-effective-once".to_owned(),
        })
        .with_idempotency_key("idem-effective-once");
        let inserted = harness
            .insert_received(&envelope, 0, offset, Some(b"order-effective-once"))
            .await?;
        assert_eq!(inserted, offset == 100);
    }

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let now = OffsetDateTime::now_utc();
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.processed, 1);
    let second = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(second.claimed, 0);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn receive_insert_deduplicates_by_idempotency_key() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;

    let first = Envelope::new(OrderCreated {
        order_id: "order-123".to_owned(),
    })
    .with_idempotency_key("idem-123");
    let duplicate = Envelope::new(OrderCreated {
        order_id: "order-123".to_owned(),
    })
    .with_idempotency_key("idem-123");

    assert!(
        harness
            .insert_received(&first, 0, 10, Some(b"order-123"))
            .await?
    );
    assert!(
        !harness
            .insert_received(&duplicate, 0, 11, Some(b"order-123"))
            .await?
    );

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-123")
        .await?;
    assert_eq!(row.message_id, first.message_id);
    assert_eq!(row.idempotency_key, "idem-123");
    assert_eq!(row.status, ReceiveStatus::Pending);
    assert_eq!(row.attempts, 0);
    assert_eq!(row.source_topic, "orders");
    assert_eq!(row.source_partition, 0);
    assert_eq!(row.source_offset, 10);
    assert_eq!(row.errors.len(), 0);

    Ok(())
}

async fn start_postgres() -> Result<(ContainerAsync<GenericImage>, String), BoxError> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await?;
    let host = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(port).await?;
    let database_url = format!("postgres://postgres:postgres@{host}:{host_port}/postgres");

    Ok((container, database_url))
}
