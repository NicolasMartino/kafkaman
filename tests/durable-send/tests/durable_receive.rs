use kafkaman_config::Config;
use kafkaman_core::{
    Envelope, IdempotencyIdentity, IdempotencyKey, KafkaMessage, OutboxStatus, ReceiveStatus,
    ReceivedError, ReceivedFailureKind, ReceivedMeta,
};
use kafkaman_sqlx::{
    changelog, dispatch_once, dispatch_once_with_hooks, enqueue_on_connection,
    insert_received_with_outcome, migrate, migrate_dry_run, received_failed_count,
    received_failed_rows, Changeset, CreateReceivedTable, DispatchTestHooks, InitSchema,
    MessageRouter, MigrationAction, MigrationContext, ReceivedFailureFilter, ReceivedInsertOutcome,
    Replay,
};
use kafkaman_test::Harness;
use kafkaman_worker::{run_dispatcher, BoxError};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::OffsetDateTime;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn idem_key(value: &str) -> IdempotencyKey {
    IdempotencyIdentity::derive_legacy_string(value)
        .expect("test idempotency source is valid")
        .key
}

fn receive_test_lock() -> &'static Mutex<()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn retry_test_config(schema: &str, max_attempts: u32, errors_limit: u32) -> Config {
    Config::from_str(&format!(
        r#"
        [database]
        schema = "{schema}"

        [relay]
        worker_id = "worker-retry-test"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "50ms"

        [retry.defaults]
        max_attempts = {max_attempts}
        initial_backoff = "2s"
        max_backoff = "5s"
        multiplier = 2.0
        errors_limit = {errors_limit}
        dlq = "table"
        "#
    ))
    .expect("retry test config is valid")
}

/// Build a stored `errors` JSONB array the way production failure accounting
/// does (serde-serialized `ReceivedError`), so it round-trips back through
/// `ReceivedRow` deserialization in assertions.
fn failure_errors_json(kind: ReceivedFailureKind, count: usize) -> serde_json::Value {
    failure_errors_json_at(kind, count, OffsetDateTime::now_utc())
}

fn failure_errors_json_at(
    kind: ReceivedFailureKind,
    count: usize,
    occurred_at: OffsetDateTime,
) -> serde_json::Value {
    let errors = (0..count)
        .map(|idx| ReceivedError::new(kind, format!("boom-{idx}"), occurred_at))
        .collect::<Vec<_>>();
    serde_json::to_value(errors).expect("serialize received errors")
}

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct OrderAccepted {
    order_id: String,
}

impl KafkaMessage for OrderAccepted {
    const MESSAGE_TYPE: &'static str = "order_accepted";
    const TOPIC: &'static str = "accepted-orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }
}

#[tokio::test]
async fn receive_table_uses_required_idempotency_key() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
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
    let _test_guard = receive_test_lock().lock().await;
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

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
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
async fn dispatch_exposes_message_metadata_to_handler() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let correlation_id = uuid::Uuid::new_v4();
    let mut envelope = Envelope::new(OrderCreated {
        order_id: "order-meta".to_owned(),
    })
    .with_idempotency_key("idem-meta")
    .with_correlation_id(correlation_id);
    envelope
        .headers
        .insert("tenant".to_owned(), "acme".to_owned());

    assert!(
        harness
            .insert_received(&envelope, 7, 99, Some(b"order-meta-key"))
            .await?
    );

    let captured: Arc<std::sync::Mutex<Option<ReceivedMeta>>> =
        Arc::new(std::sync::Mutex::new(None));
    let captured_for_handler = Arc::clone(&captured);
    let router = MessageRouter::new().handler::<OrderCreated>(move |_conn, meta, _msg| {
        let captured = Arc::clone(&captured_for_handler);
        Box::pin(async move {
            *captured.lock().expect("capture mutex poisoned") = Some(meta);
            Ok(())
        })
    });

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.processed, 1);

    let meta = captured
        .lock()
        .expect("capture mutex poisoned")
        .clone()
        .expect("handler should have observed metadata");
    assert_eq!(meta.message_id, envelope.message_id);
    assert_eq!(meta.idempotency_key, "idem-meta");
    assert_eq!(
        meta.idempotency_source,
        Some(serde_json::json!("idem-meta"))
    );
    assert_eq!(meta.message_type, "order_created");
    assert_eq!(meta.attempts, 0);
    assert_eq!(meta.correlation_id, Some(correlation_id));
    assert_eq!(meta.headers.get("tenant").map(String::as_str), Some("acme"));
    assert_eq!(meta.source_topic, "orders");
    assert_eq!(meta.source_partition, 7);
    assert_eq!(meta.source_offset, 99);
    assert_eq!(meta.key.as_deref(), Some(b"order-meta-key".as_slice()));

    Ok(())
}

#[tokio::test]
async fn dispatch_failure_rolls_back_effect_and_parks_retryable() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
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

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
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
    assert!(row.next_attempt_at.is_some_and(|retry_at| retry_at > now));
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Handler);
    assert!(row.errors[0].detail.contains("boom"));

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
async fn missing_handler_is_recorded_and_does_not_block_younger_rows() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let first = Envelope::new(OrderCreated {
        order_id: "order-missing-handler".to_owned(),
    })
    .with_idempotency_key("idem-missing-handler");
    let second = Envelope::new(OrderCreated {
        order_id: "order-after-missing-handler".to_owned(),
    })
    .with_idempotency_key("idem-after-missing-handler");

    assert!(
        harness
            .insert_received(&first, 4, 55, Some(b"order-missing-handler"))
            .await?
    );
    assert!(
        harness
            .insert_received(&second, 4, 56, Some(b"order-after-missing-handler"))
            .await?
    );

    let stats = dispatch_once(
        harness.pool(),
        &table,
        &MessageRouter::new(),
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-missing-handler")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::MissingHandler);
    assert!(row.errors[0].detail.contains("no handler registered"));

    let success_router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    let stats = dispatch_once(
        harness.pool(),
        &table,
        &success_router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 0);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-after-missing-handler")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn poisoned_handler_transaction_is_recorded_as_dispatch_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-poisoned-transaction".to_owned(),
    })
    .with_idempotency_key("idem-poisoned-transaction");
    assert!(
        harness
            .insert_received(&envelope, 5, 66, Some(b"order-poisoned-transaction"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, _msg| {
        Box::pin(async move {
            let err = sqlx::query("SELECT * FROM kafkaman_missing_table")
                .execute(conn)
                .await
                .expect_err("handler intentionally poisons its transaction");
            assert!(err.to_string().contains("kafkaman_missing_table"));
            Ok(())
        })
    });

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-poisoned-transaction")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Infrastructure);
    assert!(row.errors[0]
        .detail
        .contains("current transaction is aborted"));

    Ok(())
}

#[tokio::test]
async fn rollback_failure_still_records_infrastructure_dispatch_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-rollback-failure".to_owned(),
    })
    .with_idempotency_key("idem-rollback-failure");
    assert!(
        harness
            .insert_received(&envelope, 5, 67, Some(b"order-rollback-failure"))
            .await?
    );

    let handler_backend_pid = Arc::new(Mutex::new(None::<i32>));
    let handler_backend_pid_ready = Arc::new(Notify::new());
    let handler_backend_pid_for_handler = Arc::clone(&handler_backend_pid);
    let handler_backend_pid_ready_for_handler = Arc::clone(&handler_backend_pid_ready);
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, _msg| {
        let handler_backend_pid = Arc::clone(&handler_backend_pid_for_handler);
        let handler_backend_pid_ready = Arc::clone(&handler_backend_pid_ready_for_handler);
        Box::pin(async move {
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *conn)
                .await?;
            *handler_backend_pid.lock().await = Some(pid);
            handler_backend_pid_ready.notify_one();

            let err = sqlx::query("SELECT * FROM kafkaman_missing_table")
                .execute(conn)
                .await
                .expect_err("handler intentionally poisons its transaction");
            assert!(err.to_string().contains("kafkaman_missing_table"));
            Ok(())
        })
    });

    let pool_for_hook = harness.pool().clone();
    let handler_backend_pid_for_hook = Arc::clone(&handler_backend_pid);
    let handler_backend_pid_ready_for_hook = Arc::clone(&handler_backend_pid_ready);
    let hooks = DispatchTestHooks::new().before_failure_rollback(move |context| {
        let pool = pool_for_hook.clone();
        let handler_backend_pid = Arc::clone(&handler_backend_pid_for_hook);
        let handler_backend_pid_ready = Arc::clone(&handler_backend_pid_ready_for_hook);
        async move {
            assert_eq!(context.idempotency_key, "idem-rollback-failure");
            assert_eq!(context.kind, ReceivedFailureKind::Infrastructure);
            handler_backend_pid_ready.notified().await;
            let pid = (*handler_backend_pid.lock().await)
                .expect("handler backend pid must be recorded before rollback");
            let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&pool)
                .await?;
            assert!(terminated);
            Ok(())
        }
    });

    let stats = dispatch_once_with_hooks(
        harness.pool(),
        &table,
        &router,
        OffsetDateTime::now_utc(),
        &hooks,
    )
    .await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rollback-failure")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Infrastructure);
    assert!(row.errors[0]
        .detail
        .contains("current transaction is aborted"));

    Ok(())
}

#[tokio::test]
async fn corrupted_received_payload_is_recorded_as_invalid_payload_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-corrupted-payload".to_owned(),
    })
    .with_idempotency_key("idem-corrupted-payload");
    assert!(
        harness
            .insert_received(&envelope, 5, 67, Some(b"order-corrupted-payload"))
            .await?
    );

    sqlx::query(&format!(
        "UPDATE {table_name}
         SET payload = jsonb_build_object('order_id', 42)
         WHERE idempotency_key = $1"
    ))
    .bind(idem_key("idem-corrupted-payload").to_string())
    .execute(harness.pool())
    .await?;

    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move {
            panic!("corrupted payload must fail before the typed handler runs");
            #[allow(unreachable_code)]
            Ok(())
        })
    });

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-corrupted-payload")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::InvalidPayload);

    Ok(())
}

#[tokio::test]
async fn handler_sql_constraint_error_is_recorded_as_handler_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;
    sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
        .bind("order-business-constraint")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-business-constraint".to_owned(),
    })
    .with_idempotency_key("idem-business-constraint");
    assert!(
        harness
            .insert_received(&envelope, 5, 68, Some(b"order-business-constraint"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-business-constraint")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Handler);

    Ok(())
}

#[tokio::test]
async fn retryable_failure_schedules_backoff_and_due_dispatch() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let schema = format!("kafkaman_retry_{}", uuid::Uuid::new_v4().simple());
    let harness =
        Harness::connect_with_config(&database_url, retry_test_config(&schema, 3, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-retry-backoff".to_owned(),
    })
    .with_idempotency_key("idem-retry-backoff");
    assert!(
        harness
            .insert_received(&envelope, 8, 88, Some(b"order-retry-backoff"))
            .await?
    );

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_handler = Arc::clone(&attempts);
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let attempts = Arc::clone(&attempts_for_handler);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                return Err(kafkaman_sqlx::Error::Handler("retry-once".to_owned()));
            }
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.claimed, 1);
    assert_eq!(first.processed, 0);
    assert_eq!(first.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-retry-backoff")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.next_attempt_at, Some(now + time::Duration::seconds(2)));

    let early = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(1),
    )
    .await?;
    assert_eq!(early.claimed, 0);
    assert_eq!(early.processed, 0);
    assert_eq!(early.failed, 0);

    let due = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(2),
    )
    .await?;
    assert_eq!(due.claimed, 1);
    assert_eq!(due.processed, 1);
    assert_eq!(due.failed, 0);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-retry-backoff")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);
    assert_eq!(row.attempts, 1);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn max_attempts_moves_received_row_to_failed_with_bounded_errors() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let schema = format!("kafkaman_retry_{}", uuid::Uuid::new_v4().simple());
    let harness =
        Harness::connect_with_config(&database_url, retry_test_config(&schema, 2, 1)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-terminal-retry".to_owned(),
    })
    .with_idempotency_key("idem-terminal-retry");
    assert!(
        harness
            .insert_received(&envelope, 8, 89, Some(b"order-terminal-retry"))
            .await?
    );

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_handler = Arc::clone(&attempts);
    let router = MessageRouter::new().handler::<OrderCreated>(move |_conn, _meta, _msg| {
        let attempts = Arc::clone(&attempts_for_handler);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            Err(kafkaman_sqlx::Error::Handler(format!(
                "terminal-retry-{attempt}"
            )))
        })
    });

    let now = OffsetDateTime::from_unix_timestamp(1_700_010_000)?;
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-terminal-retry")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.next_attempt_at, Some(now + time::Duration::seconds(2)));
    assert_eq!(row.errors.len(), 1);
    assert!(row.errors[0].detail.contains("terminal-retry-1"));

    let second = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(2),
    )
    .await?;
    assert_eq!(second.claimed, 1);
    assert_eq!(second.processed, 0);
    assert_eq!(second.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-terminal-retry")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Failed);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.next_attempt_at, None);
    assert_eq!(row.errors.len(), 1);
    assert!(row.errors[0].detail.contains("terminal-retry-2"));

    let later = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(30),
    )
    .await?;
    assert_eq!(later.claimed, 0);
    assert_eq!(later.processed, 0);
    assert_eq!(later.failed, 0);

    Ok(())
}

#[tokio::test]
async fn failure_recording_holds_row_lock_until_retryable_commit() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-single-flight-failure".to_owned(),
    })
    .with_idempotency_key("idem-single-flight-failure");
    assert!(
        harness
            .insert_received(&envelope, 2, 34, Some(b"order-single-flight-failure"))
            .await?
    );

    let before_failure_record = Arc::new(Notify::new());
    let allow_failure_record = Arc::new(Notify::new());
    let before_failure_record_for_hook = Arc::clone(&before_failure_record);
    let allow_failure_record_for_hook = Arc::clone(&allow_failure_record);
    let hooks = DispatchTestHooks::new().before_record_failure(move |context| {
        let before_failure_record = Arc::clone(&before_failure_record_for_hook);
        let allow_failure_record = Arc::clone(&allow_failure_record_for_hook);
        async move {
            assert_eq!(context.idempotency_key, "idem-single-flight-failure");
            assert_eq!(context.kind, ReceivedFailureKind::Handler);
            assert!(context.message.contains("boom-single-flight"));
            before_failure_record.notify_one();
            allow_failure_record.notified().await;
            Ok(())
        }
    });

    let failing_router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Err(kafkaman_sqlx::Error::Handler(
                "boom-single-flight".to_owned(),
            ))
        })
    });

    let dispatch_pool = harness.pool().clone();
    let dispatch_table = table.clone();
    let now = OffsetDateTime::now_utc();
    let dispatch_task = tokio::spawn(async move {
        dispatch_once_with_hooks(
            &dispatch_pool,
            &dispatch_table,
            &failing_router,
            now,
            &hooks,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(10), before_failure_record.notified()).await?;

    let success_router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    let success_stats = dispatch_once(harness.pool(), &table, &success_router, now).await?;
    assert_eq!(success_stats.claimed, 0);
    assert_eq!(success_stats.processed, 0);
    assert_eq!(success_stats.failed, 0);

    allow_failure_record.notify_one();
    let failure_stats = tokio::time::timeout(Duration::from_secs(10), dispatch_task).await???;
    assert_eq!(failure_stats.claimed, 1);
    assert_eq!(failure_stats.processed, 0);
    assert_eq!(failure_stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-single-flight-failure")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Handler);
    assert!(row.processed_at.is_none());

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 0);

    Ok(())
}

#[tokio::test]
async fn crash_during_dispatch_rolls_back_and_can_be_redriven() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-crash-redrive".to_owned(),
    })
    .with_idempotency_key("idem-crash-redrive");
    assert!(
        harness
            .insert_received(&envelope, 3, 44, Some(b"order-crash-redrive"))
            .await?
    );

    let crash_router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            panic!("simulated crash during dispatch");
            #[allow(unreachable_code)]
            Ok(())
        })
    });
    let crash_pool = harness.pool().clone();
    let crash_table = table.clone();
    let crash_result = tokio::spawn(async move {
        dispatch_once(
            &crash_pool,
            &crash_table,
            &crash_router,
            OffsetDateTime::now_utc(),
        )
        .await
    })
    .await;
    assert!(crash_result
        .expect_err("crash dispatch task should panic")
        .is_panic());

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-crash-redrive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Pending);
    assert_eq!(row.attempts, 0);
    assert_eq!(row.errors.len(), 0);

    let handled_after_crash: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled_after_crash, 0);

    let success_router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    let stats = dispatch_once(
        harness.pool(),
        &table,
        &success_router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 0);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-crash-redrive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);
    assert_eq!(row.attempts, 0);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn received_failure_errors_keep_most_recent_twenty_entries() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let schema = format!("kafkaman_retry_{}", uuid::Uuid::new_v4().simple());
    let harness =
        Harness::connect_with_config(&database_url, retry_test_config(&schema, 30, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = format!("{}.{}", table.schema.quoted(), table.table.quoted());

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-error-ring".to_owned(),
    })
    .with_idempotency_key("idem-error-ring");
    assert!(
        harness
            .insert_received(&envelope, 3, 45, Some(b"order-error-ring"))
            .await?
    );

    for attempt in 0..25 {
        if attempt > 0 {
            let unpark_sql = format!(
                "UPDATE {table_name}
                 SET next_attempt_at = $2
                 WHERE idempotency_key = $1"
            );
            sqlx::query(&unpark_sql)
                .bind(idem_key("idem-error-ring").to_string())
                .bind(OffsetDateTime::now_utc())
                .execute(harness.pool())
                .await?;
        }

        let message = format!("boom-{attempt}");
        let router = MessageRouter::new().handler::<OrderCreated>(move |_conn, _meta, _msg| {
            let message = message.clone();
            Box::pin(async move { Err(kafkaman_sqlx::Error::Handler(message)) })
        });
        let stats =
            dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
        assert_eq!(stats.claimed, 1);
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.failed, 1);
    }

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-error-ring")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 25);
    assert_eq!(row.errors.len(), 20);
    assert!(row.errors[0].detail.contains("boom-5"));
    assert!(row.errors[19].detail.contains("boom-24"));

    Ok(())
}

#[tokio::test]
async fn duplicate_received_rows_converge_to_one_dispatch_effect() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
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

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
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
async fn random_redeliveries_converge_to_one_effect_per_idempotency_key() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut first_order_for_key = vec![None::<String>; 12];
    for offset in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let logical = if offset < first_order_for_key.len() as i64 {
            offset as usize
        } else {
            ((seed >> 32) % first_order_for_key.len() as u64) as usize
        };
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let duplicate_variant = seed % 31;
        let idempotency_key = format!("idem-property-{logical}");
        let order_id = format!("order-property-{logical}-{duplicate_variant}");
        let envelope = Envelope::new(OrderCreated {
            order_id: order_id.clone(),
        })
        .with_idempotency_key(idempotency_key);

        let inserted = harness
            .insert_received(&envelope, 4, offset, Some(order_id.as_bytes()))
            .await?;
        if inserted {
            first_order_for_key[logical] = Some(order_id);
        }
    }

    let expected_orders = first_order_for_key
        .iter()
        .filter_map(Clone::clone)
        .collect::<Vec<_>>();
    assert_eq!(expected_orders.len(), first_order_for_key.len());

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let mut processed = 0;
    loop {
        let stats =
            dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
        processed += stats.processed;
        if stats.claimed == 0 {
            break;
        }
    }
    assert_eq!(processed, expected_orders.len());

    for (logical, expected_order) in first_order_for_key.iter().enumerate() {
        let row = harness
            .received_row_by_idempotency_key::<OrderCreated>(&format!("idem-property-{logical}"))
            .await?;
        assert_eq!(row.status, ReceiveStatus::Processed);
        assert_eq!(row.payload["order_id"].as_str(), expected_order.as_deref());
    }

    let handled = sqlx::query_scalar::<_, String>(
        "SELECT string_agg(order_id, ',' ORDER BY order_id) FROM handled_orders",
    )
    .fetch_one(harness.pool())
    .await?;
    let mut expected_sorted = expected_orders;
    expected_sorted.sort();
    assert_eq!(handled, expected_sorted.join(","));

    Ok(())
}

/// The consume-then-produce recovery path: when a handler's enqueue is rejected,
/// nothing it wrote survives, the receive row is never acknowledged, and the
/// message is delivered again. This is what makes rolling back an invalid send
/// safe rather than lossy — the work is recovered by redelivery, not by the
/// audit row that the rollback discards.
#[tokio::test]
async fn handler_enqueue_failure_is_rolled_back_and_the_message_redelivered() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let received_table = harness.received_table::<OrderCreated>().await?;
    let outbox_table = harness.outbox_table::<OrderAccepted>().await?;
    let outbox_name = outbox_table.qualified_name();

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-redelivered".to_owned(),
    })
    .with_idempotency_key("idem-order-redelivered");
    assert!(
        harness
            .insert_received(&event, 9, 9, Some(b"order-redelivered"))
            .await?
    );

    // The handler writes a business row and then enqueues an envelope carrying
    // no idempotency identity, which `enqueue_on_connection` rejects.
    let cfg = harness.config();
    let failing_router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let cfg = cfg.clone();
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id.as_str())
                .execute(&mut *conn)
                .await?;
            let accepted = Envelope::new(OrderAccepted {
                order_id: msg.order_id,
            });
            enqueue_on_connection(conn, &cfg, &accepted).await?;
            Ok(())
        })
    });

    let first = dispatch_once(
        harness.pool(),
        &received_table,
        &failing_router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(first.processed, 0);
    assert_eq!(first.failed, 1);

    // The handler's business write and the invalid-send audit row are discarded
    // together by the handler savepoint rollback.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM handled_orders")
            .fetch_one(harness.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {outbox_name}"))
            .fetch_one(harness.pool())
            .await?,
        0
    );

    // The message is not acknowledged: the row still owes work rather than
    // having been consumed.
    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-redelivered")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert!(row.processed_at.is_none());
    // kafkaman owns the receive transaction, so unlike the send path it keeps its
    // own failure record even though the handler's work was rolled back.
    assert_eq!(row.errors.len(), 1);
    assert!(
        row.errors[0].detail.contains("idempotency"),
        "the recorded failure must be the rejected enqueue, not an incidental \
         error: {}",
        row.errors[0].detail
    );

    // And it is delivered again. A handler that supplies an identity completes
    // the same message on its next attempt, past the retry backoff.
    let cfg = harness.config();
    let recovering_router =
        MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
            let cfg = cfg.clone();
            Box::pin(async move {
                sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                    .bind(msg.order_id.as_str())
                    .execute(&mut *conn)
                    .await?;
                let accepted = Envelope::new(OrderAccepted {
                    order_id: msg.order_id,
                })
                .with_idempotency_key("accepted-order-redelivered");
                enqueue_on_connection(conn, &cfg, &accepted).await?;
                Ok(())
            })
        });

    let second = dispatch_once(
        harness.pool(),
        &received_table,
        &recovering_router,
        OffsetDateTime::now_utc() + Duration::from_secs(3_600),
    )
    .await?;
    assert_eq!(second.processed, 1);

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM handled_orders")
            .fetch_one(harness.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE status = {}",
            OutboxStatus::Pending.sql_literal()
        ))
        .fetch_one(harness.pool())
        .await?,
        1
    );

    Ok(())
}

#[tokio::test]
async fn handler_enqueues_outbox_atomically_with_receive_transaction() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let received_table = harness.received_table::<OrderCreated>().await?;
    let outbox_table = harness.outbox_table::<OrderAccepted>().await?;
    let outbox_name = outbox_table.qualified_name();

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let success = Envelope::new(OrderCreated {
        order_id: "order-consume-produce-ok".to_owned(),
    })
    .with_idempotency_key("idem-consume-produce-ok");
    assert!(
        harness
            .insert_received(&success, 8, 88, Some(b"order-consume-produce-ok"))
            .await?
    );

    let cfg = harness.config();
    let success_router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let cfg = cfg.clone();
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id.as_str())
                .execute(&mut *conn)
                .await?;
            let accepted = Envelope::new(OrderAccepted {
                order_id: msg.order_id,
            })
            .with_idempotency_key("accepted-consume-produce-ok");
            enqueue_on_connection(conn, &cfg, &accepted).await?;
            Ok(())
        })
    });

    let stats = dispatch_once(
        harness.pool(),
        &received_table,
        &success_router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(stats.processed, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE status = {}",
            OutboxStatus::Pending.sql_literal()
        ))
        .fetch_one(harness.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM handled_orders")
            .fetch_one(harness.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE idempotency_key = $1"
        ))
        .bind(idem_key("accepted-consume-produce-ok").to_string())
        .fetch_one(harness.pool())
        .await?,
        1
    );

    let duplicate = Envelope::new(OrderCreated {
        order_id: "order-consume-produce-ok-duplicate".to_owned(),
    })
    .with_idempotency_key("idem-consume-produce-ok");
    let cfg = harness.config();
    let mut duplicate_tx = harness.pool().begin().await?;
    let duplicate_outcome = insert_received_with_outcome(
        &mut duplicate_tx,
        &cfg,
        &duplicate,
        8,
        90,
        Some(b"order-consume-produce-ok-duplicate"),
    )
    .await?;
    duplicate_tx.commit().await?;
    assert_eq!(
        duplicate_outcome,
        ReceivedInsertOutcome::DuplicateIdempotencyKey
    );

    let duplicate_dispatch = dispatch_once(
        harness.pool(),
        &received_table,
        &success_router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(duplicate_dispatch.claimed, 0);
    assert_eq!(duplicate_dispatch.processed, 0);
    assert_eq!(duplicate_dispatch.failed, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM handled_orders WHERE order_id LIKE 'order-consume-produce-ok%'"
        )
        .fetch_one(harness.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE idempotency_key = $1"
        ))
        .bind(idem_key("accepted-consume-produce-ok").to_string())
        .fetch_one(harness.pool())
        .await?,
        1
    );

    let failure = Envelope::new(OrderCreated {
        order_id: "order-consume-produce-rollback".to_owned(),
    })
    .with_idempotency_key("idem-consume-produce-rollback");
    assert!(
        harness
            .insert_received(&failure, 8, 89, Some(b"order-consume-produce-rollback"))
            .await?
    );

    let cfg = harness.config();
    let failure_router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let cfg = cfg.clone();
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id.as_str())
                .execute(&mut *conn)
                .await?;
            let accepted = Envelope::new(OrderAccepted {
                order_id: msg.order_id,
            })
            .with_idempotency_key("accepted-consume-produce-rollback");
            enqueue_on_connection(conn, &cfg, &accepted).await?;
            Err(kafkaman_sqlx::Error::Handler(
                "rollback consume-produce".to_owned(),
            ))
        })
    });

    let stats = dispatch_once(
        harness.pool(),
        &received_table,
        &failure_router,
        OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE idempotency_key = $1"
        ))
        .bind(idem_key("accepted-consume-produce-rollback").to_string())
        .fetch_one(harness.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM handled_orders WHERE order_id = $1")
            .bind("order-consume-produce-rollback")
            .fetch_one(harness.pool())
            .await?,
        0
    );

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-consume-produce-rollback")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);

    Ok(())
}

#[tokio::test]
async fn dispatcher_loop_processes_due_rows_and_stops_on_cancellation() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-dispatcher-loop".to_owned(),
    })
    .with_idempotency_key("idem-dispatcher-loop");
    let second = Envelope::new(OrderCreated {
        order_id: "order-dispatcher-loop-2".to_owned(),
    })
    .with_idempotency_key("idem-dispatcher-loop-2");
    assert!(
        harness
            .insert_received(&envelope, 7, 77, Some(b"order-dispatcher-loop"))
            .await?
    );
    assert!(
        harness
            .insert_received(&second, 7, 78, Some(b"order-dispatcher-loop-2"))
            .await?
    );

    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let processed_tx = processed_tx.clone();
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            processed_tx
                .send(())
                .expect("processed receiver should still be alive");
            Ok(())
        })
    });

    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker = tokio::spawn(run_dispatcher(
        harness.pool().clone(),
        table.clone(),
        router,
        Duration::from_secs(60),
        worker_shutdown,
    ));

    tokio::time::timeout(Duration::from_secs(5), processed_rx.recv()).await?;
    tokio::time::timeout(Duration::from_secs(5), processed_rx.recv()).await?;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), worker).await???;

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-dispatcher-loop")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);
    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-dispatcher-loop-2")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 2);

    Ok(())
}

#[tokio::test]
async fn dispatcher_loop_finishes_in_flight_dispatch_before_shutdown() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let first = Envelope::new(OrderCreated {
        order_id: "order-dispatcher-shutdown-1".to_owned(),
    })
    .with_idempotency_key("idem-dispatcher-shutdown-1");
    let second = Envelope::new(OrderCreated {
        order_id: "order-dispatcher-shutdown-2".to_owned(),
    })
    .with_idempotency_key("idem-dispatcher-shutdown-2");
    assert!(
        harness
            .insert_received(&first, 7, 79, Some(b"order-dispatcher-shutdown-1"))
            .await?
    );
    assert!(
        harness
            .insert_received(&second, 7, 80, Some(b"order-dispatcher-shutdown-2"))
            .await?
    );

    let handler_started = Arc::new(Notify::new());
    let allow_finish = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_started_for_handler = Arc::clone(&handler_started);
    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let calls_for_handler = Arc::clone(&calls);
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let handler_started = Arc::clone(&handler_started_for_handler);
        let allow_finish = Arc::clone(&allow_finish_for_handler);
        let calls = Arc::clone(&calls_for_handler);
        Box::pin(async move {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                handler_started.notify_one();
                allow_finish.notified().await;
            }

            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker = tokio::spawn(run_dispatcher(
        harness.pool().clone(),
        table.clone(),
        router,
        Duration::from_millis(10),
        worker_shutdown,
    ));

    tokio::time::timeout(Duration::from_secs(5), handler_started.notified()).await?;
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!worker.is_finished());

    allow_finish.notify_one();
    tokio::time::timeout(Duration::from_secs(5), worker).await???;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let first_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-dispatcher-shutdown-1")
        .await?;
    assert_eq!(first_row.status, ReceiveStatus::Processed);
    let second_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-dispatcher-shutdown-2")
        .await?;
    assert_eq!(second_row.status, ReceiveStatus::Pending);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn replay_received_redrives_failed_rows_without_replaying_processed_rows() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let events = [
        "replay-received-a",
        "replay-received-b",
        "replay-received-c",
    ]
    .into_iter()
    .map(|order_id| {
        Envelope::new(OrderCreated {
            order_id: order_id.to_owned(),
        })
        .with_idempotency_key(format!("idem-{order_id}"))
    })
    .collect::<Vec<_>>();
    for (idx, event) in events.iter().enumerate() {
        assert!(
            harness
                .insert_received(
                    event,
                    6,
                    idx as i64,
                    Some(event.payload.order_id.as_bytes())
                )
                .await?
        );
    }

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    for _ in 0..events.len() {
        let stats =
            dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
        assert_eq!(stats.processed, 1);
    }

    let processed = ReceiveStatus::Processed.sql_literal();
    let pending = ReceiveStatus::Pending.sql_literal();
    let failed = ReceiveStatus::Failed.sql_literal();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {processed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );

    // Give the processed rows a failure history too, so the redrive below is
    // proven to skip them on status rather than on absence of failure metadata.
    // The legacy `message` key is deliberate: it exercises the compatibility
    // alias that keeps pre-problem-detail rows readable.
    sqlx::query(&format!(
        "UPDATE {table_name}
         SET attempts = 3,
             errors = jsonb_build_array(jsonb_build_object('message', 'old', 'occurred_at', now())),
             last_failed_at = now(),
             last_failure_kind = 'Handler',
             processed_at = now()
         WHERE status = {processed}"
    ))
    .execute(harness.pool())
    .await?;

    let failed_events = ["replay-failed-a", "replay-failed-b", "replay-failed-c"]
        .into_iter()
        .map(|order_id| {
            Envelope::new(OrderCreated {
                order_id: order_id.to_owned(),
            })
            .with_idempotency_key(format!("idem-{order_id}"))
        })
        .collect::<Vec<_>>();
    for (idx, event) in failed_events.iter().enumerate() {
        assert!(
            harness
                .insert_received(
                    event,
                    6,
                    100 + idx as i64,
                    Some(event.payload.order_id.as_bytes())
                )
                .await?
        );
    }
    // Match on stored digests: `idempotency_key` holds a SHA-256 hex digest, so
    // a prefix LIKE against the plaintext key would silently match no rows.
    let failed_keys = failed_events
        .iter()
        .map(|event| idem_key(&format!("idem-{}", event.payload.order_id)).to_string())
        .collect::<Vec<_>>();
    let seeded = sqlx::query(&format!(
        "UPDATE {table_name}
         SET status = {failed},
             attempts = 3,
             next_attempt_at = NULL,
             errors = jsonb_build_array(jsonb_build_object('message', 'old', 'occurred_at', now())),
             last_failed_at = now(),
             last_failure_kind = 'Handler'
         WHERE idempotency_key = ANY($1)"
    ))
    .bind(&failed_keys)
    .execute(harness.pool())
    .await?;
    assert_eq!(seeded.rows_affected(), failed_events.len() as u64);

    let cfg = harness.config();
    let skipped = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, OrderCreated::descriptor()?),
        Replay::received::<OrderCreated>(10_001)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(2)
            .contexts(&["prod"]),
    ];
    let skipped_report = migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default().with_context("staging"),
        &skipped,
    )
    .await?;
    assert_eq!(
        skipped_report.steps()[2].action,
        MigrationAction::SkippedContext
    );

    let replay = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, OrderCreated::descriptor()?),
        Replay::received::<OrderCreated>(10_002)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(2)
            .contexts(&["staging"]),
    ];
    let ctx = MigrationContext::default().with_context("staging");
    let dry_run = migrate_dry_run(harness.pool(), &cfg, &ctx, &replay).await?;
    let preview = dry_run.steps()[2].preview.as_deref().unwrap_or_default();
    assert!(preview.contains("received"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {processed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );

    let applied = migrate(harness.pool(), &cfg, &ctx, &replay).await?;
    assert_eq!(applied.steps()[2].action, MigrationAction::Applied);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {processed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {pending}"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name}
             WHERE status = {pending}
               AND attempts = 3
               AND jsonb_array_length(errors) = 1
               AND processed_at IS NULL"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );

    let rerun = migrate(harness.pool(), &cfg, &ctx, &replay).await?;
    assert_eq!(
        rerun.steps()[2].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {pending}"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        1
    );

    Ok(())
}

#[tokio::test]
async fn received_failed_rows_inspect_surface_lists_terminal_dlq_rows() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let schema = format!("kafkaman_dlq_{}", uuid::Uuid::new_v4().simple());
    let harness =
        Harness::connect_with_config(&database_url, retry_test_config(&schema, 1, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    // Three rows that exhaust their single attempt and land in terminal `Failed`,
    // plus one younger row left `Pending` to prove the inspect surface excludes
    // non-terminal rows.
    let failing = ["dlq-a", "dlq-b", "dlq-c"];
    for (idx, order_id) in failing.iter().enumerate() {
        let envelope = Envelope::new(OrderCreated {
            order_id: (*order_id).to_owned(),
        })
        .with_idempotency_key(format!("idem-{order_id}"));
        assert!(
            harness
                .insert_received(&envelope, 4, idx as i64, Some(order_id.as_bytes()))
                .await?
        );
    }
    let pending = Envelope::new(OrderCreated {
        order_id: "dlq-pending".to_owned(),
    })
    .with_idempotency_key("idem-dlq-pending");
    assert!(
        harness
            .insert_received(&pending, 4, 99, Some(b"dlq-pending"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move { Err(kafkaman_sqlx::Error::Handler("dlq-bound".to_owned())) })
    });

    let now = OffsetDateTime::from_unix_timestamp(1_700_020_000)?;
    for _ in 0..failing.len() {
        let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
        assert_eq!(stats.failed, 1);
    }

    // Count and list reflect exactly the three terminal rows; the pending row is
    // excluded.
    let all = ReceivedFailureFilter::default();
    assert_eq!(
        received_failed_count(harness.pool(), &table, &all).await?,
        3
    );

    let listed = received_failed_rows(harness.pool(), &table, &all, 10).await?;
    assert_eq!(listed.len(), 3);
    assert!(listed.iter().all(|row| row.status == ReceiveStatus::Failed));
    assert!(listed.iter().all(|row| row.attempts == 1));
    // Forensic error history is preserved on every terminal row.
    assert!(listed.iter().all(|row| !row.errors.is_empty()));
    // Oldest failure first. All three failed in the same dispatch pass and so
    // share a `last_failed_at`; the order below is guaranteed by the `created_at`
    // tiebreak, which resolves a tied group by row age rather than by random
    // message id.
    assert_eq!(
        listed
            .iter()
            .map(|row| row.idempotency_key)
            .collect::<Vec<_>>(),
        [
            idem_key("idem-dlq-a"),
            idem_key("idem-dlq-b"),
            idem_key("idem-dlq-c")
        ]
    );
    // The non-terminal pending row never appears in the DLQ inspect surface.
    assert!(listed
        .iter()
        .all(|row| row.idempotency_key != "idem-dlq-pending"));

    // `limit` bounds the page to the oldest terminal rows.
    let page = received_failed_rows(harness.pool(), &table, &all, 2).await?;
    assert_eq!(
        page.iter()
            .map(|row| row.idempotency_key)
            .collect::<Vec<_>>(),
        [idem_key("idem-dlq-a"), idem_key("idem-dlq-b")]
    );

    Ok(())
}

#[tokio::test]
async fn received_failed_filter_narrows_by_kind_and_since() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    // Seed three terminal rows with controlled business time and most-recent
    // failure time/kind. Business time is deliberately misleading: the old
    // business event can fail recently, and a newer business event can have an
    // older failure.
    let seed = [
        (
            "handler-old",
            1_700_100_000_i64,
            1_700_000_000_i64,
            ReceivedFailureKind::Handler,
        ),
        (
            "handler-new",
            1_700_000_000_i64,
            1_700_100_000_i64,
            ReceivedFailureKind::Handler,
        ),
        (
            "invalid-new",
            1_700_100_000_i64,
            1_700_100_000_i64,
            ReceivedFailureKind::InvalidPayload,
        ),
    ];
    for (idx, (label, _, _, _)) in seed.iter().enumerate() {
        let envelope = Envelope::new(OrderCreated {
            order_id: (*label).to_owned(),
        })
        .with_idempotency_key(format!("idem-{label}"));
        assert!(
            harness
                .insert_received(&envelope, 7, idx as i64, Some(label.as_bytes()))
                .await?
        );
    }
    for (label, business_epoch, failure_epoch, kind) in seed {
        let errors =
            failure_errors_json_at(kind, 1, OffsetDateTime::from_unix_timestamp(failure_epoch)?);
        sqlx::query(&format!(
            "UPDATE {table_name}
             SET status = 'Failed',
                 occurred_at = to_timestamp($1),
                 errors = $2::jsonb,
                 last_failed_at = to_timestamp($4),
                 last_failure_kind = $5
             WHERE idempotency_key = $3"
        ))
        .bind(business_epoch)
        .bind(errors)
        .bind(idem_key(&format!("idem-{label}")).to_string())
        .bind(failure_epoch)
        .bind(kind.discriminant())
        .execute(harness.pool())
        .await?;
    }

    let midpoint = OffsetDateTime::from_unix_timestamp(1_700_050_000)?;

    // Kind narrows to the two Handler rows, oldest first by created_at.
    let handlers = ReceivedFailureFilter::default().kind(ReceivedFailureKind::Handler);
    assert_eq!(
        received_failed_count(harness.pool(), &table, &handlers).await?,
        2
    );
    assert_eq!(
        received_failed_rows(harness.pool(), &table, &handlers, 10)
            .await?
            .iter()
            .map(|row| row.idempotency_key)
            .collect::<Vec<_>>(),
        ["idem-handler-old", "idem-handler-new"]
    );

    // Since narrows to the two newer rows regardless of kind.
    let recent = ReceivedFailureFilter::default().since(midpoint);
    assert_eq!(
        received_failed_count(harness.pool(), &table, &recent).await?,
        2
    );

    // Combined kind + since isolates the single newer Handler row.
    let recent_handlers = ReceivedFailureFilter::default()
        .kind(ReceivedFailureKind::Handler)
        .since(midpoint);
    let rows = received_failed_rows(harness.pool(), &table, &recent_handlers, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].idempotency_key, "idem-handler-new");

    Ok(())
}

#[tokio::test]
async fn replay_received_redrive_filters_by_kind_and_clears_history_on_request() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    // Two terminal rows of different kinds, each with attempts and two errors.
    for (idx, (label, kind)) in [
        ("rd-handler", ReceivedFailureKind::Handler),
        ("rd-invalid", ReceivedFailureKind::InvalidPayload),
    ]
    .into_iter()
    .enumerate()
    {
        let envelope = Envelope::new(OrderCreated {
            order_id: label.to_owned(),
        })
        .with_idempotency_key(format!("idem-{label}"));
        assert!(
            harness
                .insert_received(&envelope, 7, idx as i64, Some(label.as_bytes()))
                .await?
        );
        sqlx::query(&format!(
            "UPDATE {table_name}
             SET status = 'Failed',
                 attempts = 5,
                 errors = $1::jsonb,
                 last_failed_at = now(),
                 last_failure_kind = $3
             WHERE idempotency_key = $2"
        ))
        .bind(failure_errors_json(kind, 2))
        .bind(idem_key(&format!("idem-{label}")).to_string())
        .bind(kind.discriminant())
        .execute(harness.pool())
        .await?;
    }

    // One redrive that preserves forensics for Handler rows, and a second that
    // erases history for InvalidPayload rows.
    let cfg = harness.config();
    let ctx = MigrationContext::default().with_context("prod");
    let redrive = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, OrderCreated::descriptor()?),
        Replay::received::<OrderCreated>(10_002)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(10)
            .failure_kind(ReceivedFailureKind::Handler)
            .contexts(&["prod"]),
        Replay::received::<OrderCreated>(10_003)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(10)
            .failure_kind(ReceivedFailureKind::InvalidPayload)
            .clear_history()
            .contexts(&["prod"]),
    ];
    migrate(harness.pool(), &cfg, &ctx, &redrive).await?;

    // Handler row redriven with forensics intact.
    let handler_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rd-handler")
        .await?;
    assert_eq!(handler_row.status, ReceiveStatus::Pending);
    assert_eq!(handler_row.attempts, 5);
    assert_eq!(handler_row.errors.len(), 2);

    // InvalidPayload row redriven to a clean slate.
    let invalid_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rd-invalid")
        .await?;
    assert_eq!(invalid_row.status, ReceiveStatus::Pending);
    assert_eq!(invalid_row.attempts, 0);
    assert!(invalid_row.errors.is_empty());

    Ok(())
}

#[tokio::test]
async fn receive_insert_deduplicates_by_idempotency_key() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
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

    let mut conflicting_message_id = Envelope::new(OrderCreated {
        order_id: "order-456".to_owned(),
    })
    .with_idempotency_key("idem-conflicting-message-id");
    conflicting_message_id.message_id = first.message_id;
    assert!(
        !harness
            .insert_received(&conflicting_message_id, 0, 12, Some(b"order-456"))
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

    let err = harness
        .received_row_by_idempotency_key::<OrderCreated>("missing-idempotency-key")
        .await
        .expect_err("missing receive lookup should return an error");
    assert!(err.to_string().contains("missing-idempotency-key"));
    let err = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-conflicting-message-id")
        .await
        .expect_err("conflicting message id should not insert a new receive row");
    assert!(err.to_string().contains("idem-conflicting-message-id"));

    Ok(())
}

#[tokio::test]
async fn receive_insert_reports_message_id_conflict_separately_from_redelivery() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let _table = harness.received_table::<OrderCreated>().await?;
    let cfg = harness.config();

    let first = Envelope::new(OrderCreated {
        order_id: "order-conflict-first".to_owned(),
    })
    .with_idempotency_key("idem-conflict-first");
    let mut tx = harness.pool().begin().await?;
    let outcome =
        insert_received_with_outcome(&mut tx, &cfg, &first, 0, 20, Some(b"order-conflict-first"))
            .await?;
    tx.commit().await?;
    assert_eq!(outcome, ReceivedInsertOutcome::Inserted);

    let redelivery = Envelope::new(OrderCreated {
        order_id: "order-conflict-redelivery".to_owned(),
    })
    .with_idempotency_key("idem-conflict-first");
    let mut tx = harness.pool().begin().await?;
    let outcome = insert_received_with_outcome(
        &mut tx,
        &cfg,
        &redelivery,
        0,
        21,
        Some(b"order-conflict-redelivery"),
    )
    .await?;
    tx.commit().await?;
    assert_eq!(outcome, ReceivedInsertOutcome::DuplicateIdempotencyKey);

    let mut conflicting_message_id = Envelope::new(OrderCreated {
        order_id: "order-conflict-second".to_owned(),
    })
    .with_idempotency_key("idem-conflict-second");
    conflicting_message_id.message_id = first.message_id;
    let mut tx = harness.pool().begin().await?;
    let outcome = insert_received_with_outcome(
        &mut tx,
        &cfg,
        &conflicting_message_id,
        0,
        22,
        Some(b"order-conflict-second"),
    )
    .await?;
    tx.commit().await?;
    assert_eq!(outcome, ReceivedInsertOutcome::MessageIdConflict);

    let err = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-conflict-second")
        .await
        .expect_err("message-id conflict must not insert a second logical row");
    assert!(err.to_string().contains("idem-conflict-second"));

    Ok(())
}

#[tokio::test]
async fn harness_can_enqueue_after_receive_registration_for_same_type() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let _received = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-send-after-receive".to_owned(),
    })
    .with_idempotency_key("idem-send-after-receive");
    harness.enqueue(&envelope).await?;

    let row = harness
        .outbox_row::<OrderCreated>(envelope.message_id)
        .await?;
    assert_eq!(row.status, OutboxStatus::Pending);
    assert_eq!(row.payload["order_id"], "order-send-after-receive");

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
