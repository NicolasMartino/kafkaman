//! Dispatch when the handler fails: what is unwound, and how the failure is classified.
use super::*;

#[tokio::test]
async fn dispatch_failure_rolls_back_effect_and_parks_retryable() -> TestResult {
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
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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
    let (_postgres, harness) = start_harness().await?;
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
    let (_postgres, harness) = start_harness().await?;
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
            assert_eq!(context.idempotency_key, idem_key("idem-rollback-failure"));
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
    let (_postgres, harness) = start_harness().await?;
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
