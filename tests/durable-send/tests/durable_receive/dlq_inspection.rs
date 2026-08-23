//! Reading the terminal-failure backlog for triage.
use super::*;

#[tokio::test]
async fn received_failed_rows_inspect_surface_lists_terminal_dlq_rows() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_dlq_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 1, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    // Three rows that exhaust their single attempt and land in terminal `Failed`,
    // plus one younger row left `Pending` to prove the inspect surface excludes
    // non-terminal rows.
    let failing = ["dlq-a", "dlq-b", "dlq-c"];
    for (idx, order_id) in failing.iter().enumerate() {
        let envelope = Envelope::new(OrderCreated {
            order_id: (*order_id).to_owned(),
        })
        .with_idempotency_key(format!("idem-{order_id}"));
        assert!(
            harness
                .insert_received(&envelope, 4, idx as i64, Some(order_id.as_bytes()))
                .await?
        );
    }
    let pending = Envelope::new(OrderCreated {
        order_id: "dlq-pending".to_owned(),
    })
    .with_idempotency_key("idem-dlq-pending");
    assert!(
        harness
            .insert_received(&pending, 4, 99, Some(b"dlq-pending"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move { Err(kafkaman_sqlx::Error::Handler("dlq-bound".to_owned())) })
    });

    let now = OffsetDateTime::from_unix_timestamp(1_700_020_000)?;
    for _ in 0..failing.len() {
        let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
        assert_eq!(stats.failed, 1);
    }

    // Count and list reflect exactly the three terminal rows; the pending row is
    // excluded.
    let all = ReceivedFailureFilter::default();
    assert_eq!(
        received_failed_count(harness.pool(), &table, &all).await?,
        3
    );

    let listed = received_failed_rows(harness.pool(), &table, &all, 10).await?;
    assert_eq!(listed.len(), 3);
    assert!(listed.iter().all(|row| row.status == ReceiveStatus::Failed));
    assert!(listed.iter().all(|row| row.attempts == 1));
    // Forensic error history is preserved on every terminal row.
    assert!(listed.iter().all(|row| !row.errors.is_empty()));
    // Oldest failure first. All three failed in the same dispatch pass and so
    // share a `last_failed_at`; the order below is guaranteed by the `created_at`
    // tiebreak, which resolves a tied group by row age rather than by random
    // message id.
    assert_eq!(
        listed
            .iter()
            .map(|row| row.idempotency_key)
            .collect::<Vec<_>>(),
        [
            idem_key("idem-dlq-a"),
            idem_key("idem-dlq-b"),
            idem_key("idem-dlq-c")
        ]
    );
    // The non-terminal pending row never appears in the DLQ inspect surface.
    assert!(listed
        .iter()
        .all(|row| row.idempotency_key != idem_key("idem-dlq-pending")));

    // `limit` bounds the page to the oldest terminal rows.
    let page = received_failed_rows(harness.pool(), &table, &all, 2).await?;
    assert_eq!(
        page.iter()
            .map(|row| row.idempotency_key)
            .collect::<Vec<_>>(),
        [idem_key("idem-dlq-a"), idem_key("idem-dlq-b")]
    );

    Ok(())
}

#[tokio::test]
async fn received_failed_filter_narrows_by_kind_and_since() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    // Seed three terminal rows with controlled business time and most-recent
    // failure time/kind. Business time is deliberately misleading: the old
    // business event can fail recently, and a newer business event can have an
    // older failure.
    let seed = [
        (
            "handler-old",
            1_700_100_000_i64,
            1_700_000_000_i64,
            ReceivedFailureKind::Handler,
        ),
        (
            "handler-new",
            1_700_000_000_i64,
            1_700_100_000_i64,
            ReceivedFailureKind::Handler,
        ),
        (
            "invalid-new",
            1_700_100_000_i64,
            1_700_100_000_i64,
            ReceivedFailureKind::InvalidPayload,
        ),
    ];
    for (idx, (label, _, _, _)) in seed.iter().enumerate() {
        let envelope = Envelope::new(OrderCreated {
            order_id: (*label).to_owned(),
        })
        .with_idempotency_key(format!("idem-{label}"));
        assert!(
            harness
                .insert_received(&envelope, 7, idx as i64, Some(label.as_bytes()))
                .await?
        );
    }
    for (label, business_epoch, failure_epoch, kind) in seed {
        let errors =
            failure_errors_json_at(kind, 1, OffsetDateTime::from_unix_timestamp(failure_epoch)?);
        sqlx::query(&format!(
            "UPDATE {table_name}
             SET status = 'Failed',
                 occurred_at = to_timestamp($1),
                 errors = $2::jsonb,
                 last_failed_at = to_timestamp($4),
                 last_failure_kind = $5
             WHERE idempotency_key = $3"
        ))
        .bind(business_epoch)
        .bind(errors)
        .bind(idem_key(&format!("idem-{label}")).to_string())
        .bind(failure_epoch)
        .bind(kind.discriminant())
        .execute(harness.pool())
        .await?;
    }

    let midpoint = OffsetDateTime::from_unix_timestamp(1_700_050_000)?;

    // Kind narrows to the two Handler rows, oldest first by created_at.
    let handlers = ReceivedFailureFilter::default().kind(ReceivedFailureKind::Handler);
    assert_eq!(
        received_failed_count(harness.pool(), &table, &handlers).await?,
        2
    );
    assert_eq!(
        received_failed_rows(harness.pool(), &table, &handlers, 10)
            .await?
            .iter()
            .map(|row| row.idempotency_key)
            .collect::<Vec<_>>(),
        [idem_key("idem-handler-old"), idem_key("idem-handler-new")]
    );

    // Since narrows to the two newer rows regardless of kind.
    let recent = ReceivedFailureFilter::default().since(midpoint);
    assert_eq!(
        received_failed_count(harness.pool(), &table, &recent).await?,
        2
    );

    // Combined kind + since isolates the single newer Handler row.
    let recent_handlers = ReceivedFailureFilter::default()
        .kind(ReceivedFailureKind::Handler)
        .since(midpoint);
    let rows = received_failed_rows(harness.pool(), &table, &recent_handlers, 10).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].idempotency_key, idem_key("idem-handler-new"));

    Ok(())
}
