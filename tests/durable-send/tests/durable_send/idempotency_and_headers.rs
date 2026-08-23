use super::*;

#[tokio::test]
async fn missing_idempotency_is_recorded_as_failed_outbox_row_when_committed() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    sqlx::query("CREATE TABLE committed_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-missing-idem-commit".to_owned(),
    });
    let message_id = event.message_id;

    let mut tx = harness.pool().begin().await?;
    // The caller's own write shares the transaction with the audit row. That is
    // what makes the commit/rollback choice meaningful: kafkaman never decides
    // the fate of business data it does not own.
    sqlx::query("INSERT INTO committed_orders (order_id) VALUES ($1)")
        .bind("order-missing-idem-commit")
        .execute(&mut *tx)
        .await?;
    let error = enqueue(&mut tx, &cfg, &event)
        .await
        .expect_err("missing idempotency must return an error");
    assert!(error.to_string().contains("idempotency"));
    // Committing is the caller electing to keep the business row plus a durable
    // record of why no event accompanies it.
    tx.commit().await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM committed_orders")
            .fetch_one(harness.pool())
            .await?,
        1,
        "committing must keep the caller's business row"
    );

    let row = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(row.status, OutboxStatus::Failed);
    assert_eq!(row.idempotency_key, None);
    assert_eq!(row.idempotency_source, None);
    assert!(row
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("missing idempotency")));

    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 0);
    let claimed = {
        let mut tx = harness.pool().begin().await?;
        let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
        tx.commit().await?;
        claimed
    };
    assert!(claimed.is_empty());

    Ok(())
}

#[tokio::test]
async fn missing_idempotency_audit_row_rolls_back_with_caller_transaction() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    sqlx::query("CREATE TABLE rolled_back_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-missing-idem-rollback".to_owned(),
    });
    let message_id = event.message_id;

    let mut tx = harness.pool().begin().await?;
    sqlx::query("INSERT INTO rolled_back_orders (order_id) VALUES ($1)")
        .bind("order-missing-idem-rollback")
        .execute(&mut *tx)
        .await?;
    let error = enqueue(&mut tx, &cfg, &event)
        .await
        .expect_err("missing idempotency must return an error");
    assert!(error.to_string().contains("idempotency"));
    // Rolling back is the caller electing atomicity over forensics: no order may
    // exist without its event. The audit row goes with it, and the work is
    // recovered by retry rather than by the audit trail.
    tx.rollback().await?;

    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(message_id)
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(count, 0);

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM rolled_back_orders")
            .fetch_one(harness.pool())
            .await?,
        0,
        "rolling back must discard the caller's business row with the audit row"
    );

    Ok(())
}

/// Pins the current asymmetry in the error-row rule: a reserved-header rejection
/// returns before the insert, so unlike a missing idempotency identity it leaves
/// the caller nothing to commit. This records the behaviour rather than
/// endorsing it — the two invalid-send paths arguably should agree.
#[tokio::test]
async fn reserved_header_rejection_leaves_no_audit_row_to_commit() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    let mut event = Envelope::new(OrderCreated {
        order_id: "order-reserved-audit".to_owned(),
    })
    .with_idempotency_key("idem-order-reserved-audit");
    event
        .headers
        .insert("kafkaman-message-id".to_owned(), "spoofed".to_owned());
    let message_id = event.message_id;

    let mut tx = harness.pool().begin().await?;
    let error = enqueue(&mut tx, &cfg, &event)
        .await
        .expect_err("reserved header must return an error");
    assert!(
        error.to_string().contains("reserved"),
        "unexpected error: {error}"
    );
    // Even electing to commit yields no record of the rejected send.
    tx.commit().await?;

    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(message_id)
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test]
async fn enqueue_rejects_reserved_kafkaman_headers() -> TestResult {
    let (_postgres, harness) = start_harness().await?;

    let mut event = Envelope::new(OrderCreated {
        order_id: "order-reserved".to_owned(),
    });
    event
        .headers
        .insert("kafkaman-message-id".to_owned(), "spoofed".to_owned());

    let error = harness.enqueue(&event).await.expect_err("must be rejected");
    assert!(
        error.to_string().contains("reserved"),
        "unexpected error: {error}"
    );

    Ok(())
}
