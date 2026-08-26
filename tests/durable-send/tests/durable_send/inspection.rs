//! Outbox depth, row age, and expired-claim reporting.
use super::*;

#[tokio::test]
async fn outbox_inspection_reports_depth_age_and_expired_claims() -> TestResult {
    let postgres = postgres().await?;
    let database_url = postgres.url();
    let harness = Harness::connect(database_url).await?;
    let table = harness.outbox_table::<OrderCreated>().await?;

    for index in 0..2 {
        harness
            .enqueue(
                &Envelope::new(OrderCreated {
                    order_id: format!("order-observe-{index}"),
                })
                .with_idempotency_key(format!("idem-order-observe-{index}")),
            )
            .await?;
    }

    // Backdated, not raced. Every age assertion below is about a row *older than
    // the threshold*, and a 1ms threshold made that a bet on the test reaching
    // the next statement more slowly than a millisecond — true almost always,
    // and a flake the rest of the time, on a suite whose flakes teach people to
    // rerun rather than to read. Five minutes states the same property with no
    // clock in it, and stays well inside the hour-long threshold the negative
    // cases use.
    sqlx::query(&format!(
        "UPDATE {} SET created_at = created_at - interval '5 minutes'",
        table.qualified_name()
    ))
    .execute(harness.pool())
    .await?;

    // Mark one row terminal so the summary has to keep the buckets apart: the
    // pending count must not absorb it, and its age must not raise a backlog
    // warning however aggressive the threshold.
    let mut tx = harness.pool().begin().await?;
    let terminal = claim_batch(
        &mut tx,
        &table,
        "observability-terminal-worker",
        Duration::from_secs(30),
        1,
    )
    .await?;
    tx.commit().await?;
    let terminal_row = terminal.first().expect("one row should be claimable");
    mark_published(
        harness.pool(),
        &table,
        terminal_row.message_id(),
        terminal_row.claim_id,
    )
    .await?;

    let before = OffsetDateTime::now_utc();
    let summary = outbox_status_summary(
        harness.pool(),
        &table,
        OffsetDateTime::now_utc(),
        Duration::from_secs(60),
    )
    .await?;
    let pending = summary
        .iter()
        .find(|entry| entry.status == OutboxStatus::Pending)
        .expect("pending rows should be summarized");
    assert_eq!(pending.message_type, OrderCreated::MESSAGE_TYPE);
    assert_eq!(
        pending.count, 1,
        "the published row must leave the pending bucket"
    );

    // The age is real, not merely present: bounded below by zero and above by
    // the wall-clock time this test has been running.
    let oldest_created_at = pending
        .oldest_created_at
        .expect("a non-empty bucket has an oldest row");
    let age_ms = pending
        .oldest_age_ms
        .expect("a non-empty bucket has a measurable age");
    assert!(oldest_created_at <= before);
    let elapsed_ms =
        u64::try_from((OffsetDateTime::now_utc() - oldest_created_at).whole_milliseconds())
            .unwrap_or(u64::MAX);
    assert!(
        age_ms <= elapsed_ms,
        "reported age {age_ms}ms cannot exceed real elapsed {elapsed_ms}ms"
    );
    assert!(
        pending.over_max_queue_age,
        "a pending row five minutes past a 60s threshold is a backlog"
    );

    let published = summary
        .iter()
        .find(|entry| entry.status == OutboxStatus::Published)
        .expect("the published row should be summarized");
    assert_eq!(published.count, 1);
    assert!(
        !published.over_max_queue_age,
        "a published row awaiting retention is history, not a backlog"
    );

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(
        &mut tx,
        &table,
        "observability-test-worker",
        Duration::from_millis(1),
        1,
    )
    .await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let stuck = outbox_stuck_rows(
        harness.pool(),
        &table,
        OffsetDateTime::now_utc() + time::Duration::seconds(1),
        Duration::from_millis(1),
        10,
    )
    .await?;
    assert_eq!(stuck.len(), 1);
    assert_eq!(stuck[0].status, OutboxStatus::Publishing);
    assert_eq!(
        stuck[0].claimed_by.as_deref(),
        Some("observability-test-worker")
    );
    assert!(
        stuck[0].claim_expires_at.is_some(),
        "an expired-claim report must say when the claim expired"
    );

    // A claim that has not yet outlived the threshold is not stuck. Without this
    // the query would report every in-flight publish as a fault.
    let healthy = outbox_stuck_rows(
        harness.pool(),
        &table,
        OffsetDateTime::now_utc(),
        Duration::from_secs(3600),
        10,
    )
    .await?;
    assert!(
        healthy.is_empty(),
        "a claim inside its lease is in flight, not stuck"
    );

    Ok(())
}
