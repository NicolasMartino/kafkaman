//! Narrowing a redrive by failure kind, and erasing history on the way through.
use super::*;

#[tokio::test]
async fn replay_received_redrive_filters_by_kind_and_clears_history_on_request() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    // Two terminal rows of different kinds, each with attempts and two errors.
    for (idx, (label, kind)) in [
        ("rd-handler", ReceivedFailureKind::Handler),
        ("rd-invalid", ReceivedFailureKind::InvalidPayload),
    ]
    .into_iter()
    .enumerate()
    {
        let envelope = Envelope::new(OrderCreated {
            order_id: label.to_owned(),
        })
        .with_idempotency_key(format!("idem-{label}"));
        assert!(
            harness
                .insert_received(&envelope, 7, idx as i64, Some(label.as_bytes()))
                .await?
        );
        sqlx::query(&format!(
            "UPDATE {table_name}
             SET status = 'Failed',
                 attempts = 5,
                 errors = $1::jsonb,
                 last_failed_at = now(),
                 last_failure_kind = $3
             WHERE idempotency_key = $2"
        ))
        .bind(failure_errors_json(kind, 2))
        .bind(idem_key(&format!("idem-{label}")).to_string())
        .bind(kind.discriminant())
        .execute(harness.pool())
        .await?;
    }

    // One redrive that preserves forensics for Handler rows, and a second that
    // erases history for InvalidPayload rows.
    let cfg = harness.config();
    let ctx = MigrationContext::default().with_context("prod");
    let redrive = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, OrderCreated::descriptor()?),
        Replay::received::<OrderCreated>(10_002)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(10)
            .failure_kind(ReceivedFailureKind::Handler)
            .contexts(&["prod"]),
        Replay::received::<OrderCreated>(10_003)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(10)
            .failure_kind(ReceivedFailureKind::InvalidPayload)
            .clear_history()
            .contexts(&["prod"]),
    ];
    migrate(harness.pool(), &cfg, &ctx, &redrive).await?;

    // Handler row redriven with forensics intact.
    let handler_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rd-handler")
        .await?;
    assert_eq!(handler_row.status, ReceiveStatus::Pending);
    assert_eq!(handler_row.attempts, 5);
    assert_eq!(handler_row.errors.len(), 2);

    // InvalidPayload row redriven to a clean slate.
    let invalid_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rd-invalid")
        .await?;
    assert_eq!(invalid_row.status, ReceiveStatus::Pending);
    assert_eq!(invalid_row.attempts, 0);
    assert!(invalid_row.errors.is_empty());

    Ok(())
}
