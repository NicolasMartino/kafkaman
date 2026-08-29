//! Spending a row's retry budget: backoff, locking, and the bounded error history.
use super::*;

#[tokio::test]
async fn max_attempts_moves_received_row_to_failed_with_bounded_errors() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_retry_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 2, 1)).await?;
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
    // Backoff carries equal jitter, so the retry lands in [base/2, base] rather
    // than on an exact instant. Pinning the instant would be pinning the absence
    // of jitter, which is the thundering-herd bug the jitter exists to prevent.
    assert_retry_within(row.next_attempt_at, now, 2);
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
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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
            assert_eq!(
                context.idempotency_key,
                idem_key("idem-single-flight-failure")
            );
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
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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

    // The crash is an aborted task, not a panicking handler.
    //
    // It used to be a panic, which was the shorter way to interrupt a dispatch
    // and is no longer an interruption at all: a handler panic is caught at the
    // call boundary and recorded as an ordinary handler failure, so it would
    // leave `attempts = 1` and a `Retryable` row rather than the untouched one
    // this test is about. `dispatch_handler_panic.rs` covers that contract.
    //
    // What is under test here is the other thing entirely — the transaction
    // guarantee when a dispatch simply stops mid-flight, which is what a killed
    // process looks like to Postgres. Aborting the task drops the future and its
    // open transaction with no failure accounting whatsoever, which is exactly
    // that, and is a truer simulation than the panic ever was.
    let handler_started = Arc::new(Notify::new());
    let never_finish = Arc::new(Notify::new());
    let handler_started_for_handler = Arc::clone(&handler_started);
    let never_finish_for_handler = Arc::clone(&never_finish);
    let crash_router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let handler_started = Arc::clone(&handler_started_for_handler);
        let never_finish = Arc::clone(&never_finish_for_handler);
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            // Park with the write done and the transaction still open, so the
            // abort below lands in the window this test exists to cover.
            handler_started.notify_one();
            never_finish.notified().await;
            Ok(())
        })
    });
    let crash_pool = harness.pool().clone();
    let crash_table = table.clone();
    let crashing = tokio::spawn(async move {
        dispatch_once(
            &crash_pool,
            &crash_table,
            &crash_router,
            OffsetDateTime::now_utc(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), handler_started.notified()).await?;
    crashing.abort();
    assert!(crashing
        .await
        .expect_err("the aborted dispatch task should not report a result")
        .is_cancelled());

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
