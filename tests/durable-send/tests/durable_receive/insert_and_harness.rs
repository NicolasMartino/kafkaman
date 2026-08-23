use super::*;

#[tokio::test]
async fn receive_insert_deduplicates_by_idempotency_key() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;

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
    assert_eq!(row.idempotency_key, idem_key("idem-123"));
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
    let (_postgres, harness) = start_harness().await?;
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
    let (_postgres, harness) = start_harness().await?;
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
