use super::*;

#[tokio::test]
async fn worker_run_loop_relays_until_shutdown() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    for index in 0..3 {
        harness
            .enqueue(
                &Envelope::new(OrderCreated {
                    order_id: format!("order-run-{index}"),
                })
                .with_idempotency_key(format!("idem-order-run-{index}")),
            )
            .await?;
    }

    let publisher = harness.publisher();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        table,
        cfg.relay.clone(),
        shutdown.clone(),
    ));

    // Wait for the background loop to drain the backlog, then stop it.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while harness.published_on(OrderCreated::TOPIC).len() < 3 {
        if std::time::Instant::now() > deadline {
            panic!("worker did not publish all rows before the deadline");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.cancel();
    worker.await??;

    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 3);
    Ok(())
}

/// A relay handed an already-cancelled token must publish nothing.
///
/// The loop used to check for shutdown only *after* a cycle, so a relay started
/// during a shutdown still claimed a batch and made broker calls the shutdown
/// had already asked it not to make. `run_dispatcher` and `run_purger` both
/// guarded at the top of the loop; this one did not.
#[tokio::test]
async fn a_cancelled_relay_publishes_nothing() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    harness
        .enqueue(
            &Envelope::new(OrderCreated {
                order_id: "order-never-relayed".to_owned(),
            })
            .with_idempotency_key("idem-order-never-relayed"),
        )
        .await?;

    let shutdown = tokio_util::sync::CancellationToken::new();
    shutdown.cancel();

    kafkaman_worker::run(
        harness.pool().clone(),
        harness.publisher(),
        table,
        cfg.relay.clone(),
        shutdown,
    )
    .await?;

    assert!(
        harness.published_on(OrderCreated::TOPIC).is_empty(),
        "a cancelled relay must not claim or publish a batch"
    );
    Ok(())
}

#[tokio::test]
async fn durable_send_publishes_record_and_marks_row() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-1".to_owned(),
    })
    .with_idempotency_key("idem-order-1");
    let message_id = event.message_id;

    harness.enqueue(&event).await?;

    // The idempotency key the caller set must be persisted durably, not dropped.
    let stored = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(stored.idempotency_key, Some(idem_key("idem-order-1")));
    assert_eq!(
        stored.idempotency_source,
        Some(serde_json::json!("idem-order-1"))
    );

    let stats = harness.relay_once::<OrderCreated>().await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    let published = harness.published_on(OrderCreated::TOPIC);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message_id, message_id);
    assert_eq!(published[0].key.as_deref(), Some("order-1"));

    Ok(())
}

#[tokio::test]
async fn publish_error_requeues_row_with_last_error() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-error".to_owned(),
    })
    .with_idempotency_key("idem-order-error");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let cfg = harness.config();
    let table = harness.outbox_table::<OrderCreated>().await?;
    let stats =
        kafkaman_worker::relay_once(harness.pool(), &FailingPublisher, &table, &cfg.relay).await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.failed, 1);

    let row = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(row.status, OutboxStatus::Pending);
    assert_eq!(row.attempts, 1);
    assert!(row.claim_id.is_none());
    assert!(row.claimed_by.is_none());
    assert!(row.claim_expires_at.is_none());
    assert!(row
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("synthetic publish failure")));

    Ok(())
}

#[tokio::test]
async fn concurrent_registration_on_one_harness_is_safe() -> TestResult {
    let postgres = postgres().await?;
    let database_url = postgres.url();
    let harness = std::sync::Arc::new(Harness::connect(database_url).await?);

    // Many tasks register and use the same not-yet-migrated message type at once.
    // None may observe the type registered before its table exists.
    let mut handles = Vec::new();
    for index in 0..8 {
        let harness = harness.clone();
        handles.push(tokio::spawn(async move {
            let event = Envelope::new(InvoiceCreated {
                invoice_id: format!("invoice-{index}"),
            })
            .with_idempotency_key(format!("idem-invoice-{index}"));
            harness.enqueue(&event).await?;
            harness.relay_once::<InvoiceCreated>().await?;
            Ok::<(), kafkaman_test::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    assert_eq!(harness.published_on(InvoiceCreated::TOPIC).len(), 8);
    Ok(())
}
