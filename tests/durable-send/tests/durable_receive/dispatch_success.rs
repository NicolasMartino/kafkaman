//! Dispatch when the handler succeeds: what commits, and what the handler is told.
use super::*;

#[tokio::test]
async fn receive_table_uses_required_idempotency_key() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
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
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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
    let (_postgres, harness) = start_harness().await?;
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
    assert_eq!(meta.idempotency_key, idem_key("idem-meta"));
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
