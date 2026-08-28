//! A handler's own enqueue sharing the dispatch transaction: both land, or neither does.
use super::*;

/// The consume-then-produce recovery path: when a handler's enqueue is rejected,
/// nothing it wrote survives, the receive row is never acknowledged, and the
/// message is delivered again. This is what makes rolling back an invalid send
/// safe rather than lossy — the work is recovered by redelivery, not by the
/// audit row that the rollback discards.
#[tokio::test]
async fn handler_enqueue_failure_is_rolled_back_and_the_message_redelivered() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let received_table = harness.received_table::<OrderCreated>().await?;
    let outbox_table = harness.outbox_table::<OrderAccepted>().await?;
    let outbox_name = outbox_table.qualified_name();

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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
    let (_postgres, harness) = start_harness().await?;
    let received_table = harness.received_table::<OrderCreated>().await?;
    let outbox_table = harness.outbox_table::<OrderAccepted>().await?;
    let outbox_name = outbox_table.qualified_name();

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
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
        None,
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
