use super::*;

#[tokio::test]
async fn stale_claim_cannot_mark_row_published() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-stale".to_owned(),
    })
    .with_idempotency_key("idem-order-stale");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let outcome = mark_published(harness.pool(), &table, message_id, Uuid::new_v4()).await?;
    assert_eq!(outcome, MarkOutcome::StaleClaim);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Publishing)
        .await?;

    let outcome = mark_published(harness.pool(), &table, message_id, claimed[0].claim_id).await?;
    assert_eq!(outcome, MarkOutcome::Updated);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    Ok(())
}

#[tokio::test]
async fn ack_before_mark_republishes_after_claim_lease_expiry() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-dup".to_owned(),
    })
    .with_idempotency_key("idem-order-dup");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let publisher = harness.publisher();

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "manual-crash", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    publisher.publish(&claimed[0]).await?;
    assert_eq!(publisher.records_on(OrderCreated::TOPIC).len(), 1);

    let expire_sql = format!(
        "UPDATE {} SET claim_expires_at = now() - interval '1 second' WHERE message_id = $1",
        table.qualified_name()
    );
    sqlx::query(&expire_sql)
        .bind(message_id)
        .execute(harness.pool())
        .await?;

    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;
    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 2);

    Ok(())
}

#[tokio::test]
async fn mark_publish_failed_rejects_stale_claim() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-failed-stale".to_owned(),
    })
    .with_idempotency_key("idem-order-failed-stale");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let outcome = mark_publish_failed(
        harness.pool(),
        &table,
        message_id,
        Uuid::new_v4(),
        "wrong claim",
        Duration::from_secs(1),
    )
    .await?;
    assert_eq!(outcome, MarkOutcome::StaleClaim);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Publishing)
        .await?;

    Ok(())
}
