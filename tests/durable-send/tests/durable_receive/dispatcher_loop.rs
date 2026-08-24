//! The dispatcher run loop, and how it shuts down.
use super::*;

#[tokio::test]
async fn dispatcher_loop_processes_due_rows_and_stops_on_cancellation() -> TestResult {
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
        LifecycleEmission::default(),
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
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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
        LifecycleEmission::default(),
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
