//! Superseding an entity's pending row when a newer snapshot is enqueued.
use super::*;

#[tokio::test]
async fn first_concurrent_enqueues_for_entity_serialize() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let cfg = harness.config();

    let old = product("p-1", "old").with_idempotency_key("idem-p1-old");
    let new = product("p-1", "new").with_idempotency_key("idem-p1-new");

    let mut first_tx = harness.pool().begin().await?;
    enqueue(&mut first_tx, &cfg, &old).await?;

    let pool_for_new = harness.pool().clone();
    let cfg_for_new = cfg.clone();
    let new_for_task = new.clone();
    let new_enqueue = tokio::spawn(async move {
        let mut tx = pool_for_new.begin().await?;
        enqueue(&mut tx, &cfg_for_new, &new_for_task).await?;
        tx.commit().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    tokio::task::yield_now().await;
    first_tx.commit().await?;
    // Three `?`: the timeout, the join, and the spawned task's own result.
    // Collapsing to two would silently discard a failed concurrent enqueue.
    timeout(Duration::from_secs(5), new_enqueue).await???;

    let old_row = harness
        .outbox_row::<ProductSnapshot>(old.message_id)
        .await?;
    let new_row = harness
        .outbox_row::<ProductSnapshot>(new.message_id)
        .await?;
    assert_eq!(old_row.status, OutboxStatus::Superseded);
    assert_eq!(new_row.status, OutboxStatus::Pending);
    assert_eq!(old_row.entity_key.as_deref(), Some("p-1"));
    assert_eq!(new_row.entity_key.as_deref(), Some("p-1"));

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].row.message_id, new.message_id);

    Ok(())
}

#[tokio::test]
async fn supersede_collapses_queued_updates() -> TestResult {
    let (_postgres, harness) = start_harness().await?;

    let first = product("p-2", "first").with_idempotency_key("idem-p2-first");
    let second = product("p-2", "second").with_idempotency_key("idem-p2-second");
    let third = product("p-2", "third").with_idempotency_key("idem-p2-third");

    harness.enqueue(&first).await?;
    harness.enqueue(&second).await?;
    harness.enqueue(&third).await?;

    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(first.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(second.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(third.message_id)
            .await?
            .status,
        OutboxStatus::Pending
    );

    let stats = harness.relay_once::<ProductSnapshot>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    let records = harness.published_on(ProductSnapshot::TOPIC);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].message_id, third.message_id);
    assert_eq!(records[0].payload["name"], "third");

    Ok(())
}

#[tokio::test]
async fn publishing_entity_blocks_newer_pending_claim_until_published() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;

    let first = product("p-3", "first").with_idempotency_key("idem-p3-first");
    let second = product("p-3", "second").with_idempotency_key("idem-p3-second");

    harness.enqueue(&first).await?;
    let mut tx = harness.pool().begin().await?;
    let first_claim = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].row.message_id, first.message_id);

    harness.enqueue(&second).await?;
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(first.message_id)
            .await?
            .status,
        OutboxStatus::Publishing
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(second.message_id)
            .await?
            .status,
        OutboxStatus::Pending
    );

    let mut blocked_tx = harness.pool().begin().await?;
    let blocked_claim = claim_batch(
        &mut blocked_tx,
        &table,
        "worker-b",
        Duration::from_secs(30),
        10,
    )
    .await?;
    blocked_tx.commit().await?;
    assert!(
        blocked_claim.is_empty(),
        "newer entity row must wait while an older row is publishing"
    );

    let outcome = mark_published(
        harness.pool(),
        &table,
        first.message_id,
        first_claim[0].claim_id,
    )
    .await?;
    assert_eq!(outcome, MarkOutcome::Updated);

    let mut next_tx = harness.pool().begin().await?;
    let next_claim = claim_batch(
        &mut next_tx,
        &table,
        "worker-b",
        Duration::from_secs(30),
        10,
    )
    .await?;
    next_tx.commit().await?;
    assert_eq!(next_claim.len(), 1);
    assert_eq!(next_claim[0].row.message_id, second.message_id);

    Ok(())
}
