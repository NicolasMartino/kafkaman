//! Keeping publish order per entity when a claim or a publish fails.
use super::*;

#[tokio::test]
async fn a_failed_publish_never_republishes_state_a_newer_row_has_overtaken() -> TestResult {
    // Regression. `enqueue` supersedes only rows that are Pending *at that
    // moment*, so a row that is Publishing when the next state arrives survives
    // — and `mark_publish_failed` then returns it to Pending. That leaves two
    // Pending rows for one entity, and the newer one wins the race to publish
    // because it carries `next_attempt_at = now()` from insert while the retried
    // older one carries `now() + retry_after`. The older state would then land at
    // the HIGHER offset, and every consumer's convergence guard would apply it
    // as newest — correctly by its own rules, permanently, and silently. This is
    // decision point 9's failure mode reached through retry rather than replay.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;

    let old = product("p-retry", "old").with_idempotency_key("idem-p-retry-old");
    let new = product("p-retry", "new").with_idempotency_key("idem-p-retry-new");

    harness.enqueue(&old).await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].row.message_id, old.message_id);

    // The newer state arrives while the older one is in flight, so supersede
    // cannot collapse it on the write path.
    harness.enqueue(&new).await?;

    // The in-flight publish fails transiently and the row returns to Pending.
    let outcome = mark_publish_failed(
        harness.pool(),
        &table,
        old.message_id,
        claimed[0].claim_id,
        "broker unavailable",
        // A real backoff, not zero: in production the retried row is scheduled
        // into the future while the newer row is due immediately, and that gap is
        // exactly what lets the newer state publish first. A zero backoff would
        // make both rows due and quietly test an easier case.
        Duration::from_secs(60),
    )
    .await?;
    assert_eq!(outcome, MarkOutcome::Updated);
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(old.message_id)
            .await?
            .status,
        OutboxStatus::Pending,
        "precondition: the retry path really does return the row to Pending"
    );

    // Both rows are now Pending for one entity. The claim must hand back only
    // the newer state and abandon the overtaken one.
    let mut retry_tx = harness.pool().begin().await?;
    let retry_claim = claim_batch(
        &mut retry_tx,
        &table,
        "worker-a",
        Duration::from_secs(30),
        10,
    )
    .await?;
    retry_tx.commit().await?;

    assert_eq!(
        retry_claim.len(),
        1,
        "one entity must never have two rows in flight"
    );
    assert_eq!(
        retry_claim[0].row.message_id, new.message_id,
        "the claimed row must be the newest state, not the retried older one"
    );

    let old_row = harness
        .outbox_row::<ProductSnapshot>(old.message_id)
        .await?;
    assert_eq!(
        old_row.status,
        OutboxStatus::Superseded,
        "the overtaken row must be abandoned, or it republishes stale state at a higher offset"
    );
    assert_eq!(
        old_row.last_error.as_deref(),
        Some("superseded by newer pending state for the same entity"),
        "the abandonment reason has to be legible during triage"
    );

    // And it must stay abandoned: no later cycle may resurrect it.
    let mut final_tx = harness.pool().begin().await?;
    let final_claim = claim_batch(
        &mut final_tx,
        &table,
        "worker-a",
        Duration::from_secs(30),
        10,
    )
    .await?;
    final_tx.commit().await?;
    assert!(
        final_claim.is_empty(),
        "the superseded row must not become claimable again"
    );

    Ok(())
}

#[tokio::test]
async fn the_collapse_touches_only_the_overtaken_row() -> TestResult {
    // The risk in rewriting a statement for speed is that its `WHERE` comes out
    // wider than intended, and a too-wide supersede here destroys queued state
    // permanently and silently. So: several entities each holding one Pending row,
    // one entity holding an overtaken pair, and exactly one row may be dropped.
    //
    // The opposite ordering — an older row due before a newer one — is not tested
    // because it is unreachable: while the older row is `Publishing` the newer one
    // cannot be claimed, and any cycle that does claim the newer one supersedes the
    // older one first.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;

    let overtaken = product("p-multi-1", "old").with_idempotency_key("idem-multi-1-old");
    let winner = product("p-multi-1", "new").with_idempotency_key("idem-multi-1-new");
    let bystander_a = product("p-multi-2", "only").with_idempotency_key("idem-multi-2");
    let bystander_b = product("p-multi-3", "only").with_idempotency_key("idem-multi-3");

    for event in [&overtaken, &bystander_a, &bystander_b] {
        harness.enqueue(event).await?;
    }

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 3);

    // A newer state for the first entity only, while its predecessor is in flight.
    harness.enqueue(&winner).await?;

    // Everything fails, so every row is back to Pending and eligible together.
    for row in &claimed {
        mark_publish_failed(
            harness.pool(),
            &table,
            row.row.message_id,
            row.claim_id,
            "broker unavailable",
            Duration::from_secs(0),
        )
        .await?;
    }

    let mut tx = harness.pool().begin().await?;
    let after = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
    tx.commit().await?;

    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(overtaken.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    for survivor in [&winner, &bystander_a, &bystander_b] {
        assert_eq!(
            harness
                .outbox_row::<ProductSnapshot>(survivor.message_id)
                .await?
                .status,
            OutboxStatus::Publishing,
            "an entity with nothing newer queued must not be collapsed"
        );
    }

    let mut claimed_ids: Vec<_> = after.iter().map(|row| row.row.message_id).collect();
    claimed_ids.sort();
    let mut expected = vec![
        winner.message_id,
        bystander_a.message_id,
        bystander_b.message_id,
    ];
    expected.sort();
    assert_eq!(claimed_ids, expected);

    Ok(())
}
