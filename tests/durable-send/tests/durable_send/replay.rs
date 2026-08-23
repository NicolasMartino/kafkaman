use super::*;

#[tokio::test]
async fn outbox_replay_is_rejected_as_unsafe_for_entity_snapshots() -> TestResult {
    // The README promises "state-sourced republish for repair, never replaying
    // stale outbox rows as truth", and the entity-first decision (point 9)
    // explains why: a republished row is written to Kafka at a NEW, HIGHER
    // offset, so every consumer's convergence guard sees stale state carrying
    // the newest ordinal and applies it. The guard cannot detect this, because
    // in the log the record genuinely is newest. The corruption is silent and
    // permanent, so the constructor must refuse rather than warn.
    let err =
        Replay::outbox::<OrderCreated>(3).expect_err("row-sourced outbox replay must be rejected");

    assert!(
        matches!(
            &err,
            kafkaman_sqlx::Error::UnsafeOutboxReplay { message_type }
                if message_type == OrderCreated::MESSAGE_TYPE
        ),
        "expected UnsafeOutboxReplay, got {err:?}"
    );
    // The message has to point at the supported alternative, or an operator who
    // hits this during an incident has no next step.
    let rendered = err.to_string();
    assert!(rendered.contains("state-sourced"), "{rendered}");
    assert!(rendered.contains("Replay::received"), "{rendered}");

    Ok(())
}

#[tokio::test]
async fn published_rows_stay_terminal_without_row_sourced_replay() -> TestResult {
    // Repair must not be reachable by accident: once a row is Published there is
    // no supported path that moves it back to Pending, so the relay can never
    // re-emit an old snapshot at a fresh offset.
    let (_postgres, harness) = start_harness().await?;

    let events = ["replay-a", "replay-b", "replay-c"]
        .into_iter()
        .map(|order_id| {
            Envelope::new(OrderCreated {
                order_id: order_id.to_owned(),
            })
            .with_idempotency_key(format!("idem-{order_id}"))
        })
        .collect::<Vec<_>>();
    for event in &events {
        harness.enqueue(event).await?;
    }
    assert_eq!(harness.relay_once::<OrderCreated>().await?.published, 3);

    let table = harness.outbox_table::<OrderCreated>().await?;
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Published).await?,
        3
    );

    // A second relay pass finds nothing: Published is terminal.
    assert_eq!(harness.relay_once::<OrderCreated>().await?.claimed, 0);
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Published).await?,
        3
    );
    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 3);

    Ok(())
}
