//! Reading the terminal-failure backlog for triage.
use super::*;

#[tokio::test]
async fn received_ingest_failure_summary_reports_quarantine_growth_without_payloads() -> TestResult
{
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let cfg = harness.config();

    let seed = [
        (
            11,
            "orders",
            "orders",
            "order_created",
            ReceivedIngestFailureKind::InvalidPayload,
            "payload was not json",
        ),
        (
            12,
            "orders",
            "orders",
            "order_created",
            ReceivedIngestFailureKind::InvalidPayload,
            "payload was still not json",
        ),
        (
            21,
            "wrong-orders",
            "orders",
            "order_created",
            ReceivedIngestFailureKind::UnexpectedTopic,
            "unexpected topic",
        ),
    ];
    for (offset, source_topic, expected_topic, message_type, kind, error) in seed {
        let failure = ReceivedIngestFailure {
            source_topic: source_topic.to_owned(),
            source_partition: 0,
            source_offset: offset,
            key: Some(format!("key-{offset}").into_bytes()),
            headers: serde_json::json!({ "kafka": ["header"] }),
            payload: Some(format!("payload-{offset}").into_bytes()),
            message_type: message_type.to_owned(),
            expected_topic: expected_topic.to_owned(),
            kind,
            error: error.to_owned(),
        };
        let mut tx = harness.pool().begin().await?;
        assert!(
            insert_received_ingest_failure(&mut tx, &cfg, &failure).await?,
            "each source offset is unique and should insert"
        );
        tx.commit().await?;
    }

    let summary =
        received_ingest_failure_summary(harness.pool(), &cfg, OffsetDateTime::now_utc()).await?;
    assert_eq!(summary.len(), 2);

    let invalid_payload = summary
        .iter()
        .find(|row| row.failure_kind == ReceivedIngestFailureKind::InvalidPayload)
        .expect("invalid payload bucket should be present");
    assert_eq!(invalid_payload.message_type, "order_created");
    assert_eq!(invalid_payload.expected_topic, "orders");
    assert_eq!(invalid_payload.count, 2);
    assert!(
        invalid_payload.oldest_age_ms < 60_000,
        "a just-written quarantine bucket should report a bounded age, got {}ms",
        invalid_payload.oldest_age_ms
    );

    let unexpected_topic = summary
        .iter()
        .find(|row| row.failure_kind == ReceivedIngestFailureKind::UnexpectedTopic)
        .expect("unexpected-topic bucket should be present");
    assert_eq!(unexpected_topic.expected_topic, "orders");
    assert_eq!(unexpected_topic.count, 1);

    Ok(())
}

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

#[tokio::test]
async fn received_inspection_reports_depth_stuck_rows_and_redrives_dlq() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = durable_send_tests::unique_schema("kafkaman_observe_receive");
    // Two attempts, so the first failure lands in `Retryable` with a scheduled
    // `next_attempt_at` — the branch the stuck-row query indexes separately from
    // `Pending` — and the second exhausts the budget into the DLQ.
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 2, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-observe-receive".to_owned(),
    })
    .with_idempotency_key("idem-order-observe-receive");
    assert!(
        harness
            .insert_received(&event, 0, 7, Some(b"order-observe-receive"))
            .await?
    );

    // Backdated, not raced. The assertion below is that a row older than the
    // threshold is a backlog, and a 1ms threshold turned that into a bet on the
    // test taking longer than a millisecond to reach the next statement. Five
    // minutes states it with no clock in it, and stays well inside the hour-long
    // threshold the negative case below uses.
    sqlx::query(&format!(
        "UPDATE {} SET created_at = created_at - interval '5 minutes'",
        table.qualified_name()
    ))
    .execute(harness.pool())
    .await?;

    // Queued work past the threshold is a backlog — and a terminal bucket must
    // never be flagged, however old.
    let summary = received_status_summary(
        harness.pool(),
        &table,
        OffsetDateTime::now_utc(),
        Duration::from_secs(60),
    )
    .await?;
    let pending = summary
        .iter()
        .find(|entry| entry.status == ReceiveStatus::Pending)
        .expect("pending receive rows should be summarized");
    assert_eq!(pending.message_type, OrderCreated::MESSAGE_TYPE);
    assert_eq!(pending.count, 1);
    assert!(
        pending.over_max_queue_age,
        "a pending row five minutes past a 60s max_queue_age is a backlog"
    );

    // With a threshold longer than the row has existed, nothing is a backlog.
    // Five minutes of backdating is deliberately far inside this hour.
    let quiet = received_status_summary(
        harness.pool(),
        &table,
        OffsetDateTime::now_utc(),
        Duration::from_secs(3600),
    )
    .await?;
    assert!(
        quiet.iter().all(|entry| !entry.over_max_queue_age),
        "no bucket should breach an hour-long max_queue_age"
    );

    let stuck = received_stuck_rows(
        harness.pool(),
        &table,
        OffsetDateTime::now_utc() + time::Duration::seconds(1),
        Duration::from_millis(1),
        10,
    )
    .await?;
    assert_eq!(stuck.len(), 1);
    assert_eq!(stuck[0].status, ReceiveStatus::Pending);
    // A Pending row carries no `next_attempt_at`, so it is due from creation.
    assert!(stuck[0].next_attempt_at.is_none());
    assert_eq!(stuck[0].due_at, stuck[0].created_at);

    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move { Err(kafkaman_sqlx::Error::Handler("boom".to_owned())) })
    });

    // First failure leaves a retry budget, so the row lands in Retryable with a
    // scheduled `next_attempt_at`. That is the branch the stuck query indexes on
    // separately from Pending, and it must not report a row inside its backoff.
    let retryable =
        dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(retryable.failed, 1);
    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-receive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    let scheduled = row
        .next_attempt_at
        .expect("a retryable row must carry a scheduled next attempt");

    let not_yet_due = received_stuck_rows(
        harness.pool(),
        &table,
        scheduled - time::Duration::seconds(1),
        Duration::from_millis(1),
        10,
    )
    .await?;
    assert!(
        not_yet_due.is_empty(),
        "a row still inside its backoff window is waiting, not stuck"
    );

    let overdue = received_stuck_rows(
        harness.pool(),
        &table,
        scheduled + time::Duration::seconds(60),
        Duration::from_millis(1),
        10,
    )
    .await?;
    assert_eq!(overdue.len(), 1);
    assert_eq!(overdue[0].status, ReceiveStatus::Retryable);
    assert_eq!(overdue[0].next_attempt_at, Some(scheduled));
    assert_eq!(overdue[0].due_at, scheduled);
    // The overdue clock, which is what `stuck_after` filtered on. `age_ms`
    // measures from `created_at` instead — the row existed before it was due,
    // so it is necessarily the larger of the two and answers a different
    // question.
    assert!(overdue[0].stuck_for_ms >= 60_000);
    assert!(
        overdue[0].age_ms >= overdue[0].stuck_for_ms,
        "a row cannot have been overdue for longer than it has existed"
    );

    // Exhaust the retry budget so the row reaches the DLQ.
    let mut attempts = row.attempts;
    while harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-receive")
        .await?
        .status
        != ReceiveStatus::Failed
    {
        let due = harness
            .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-receive")
            .await?
            .next_attempt_at
            .unwrap_or_else(OffsetDateTime::now_utc);
        dispatch_once(
            harness.pool(),
            &table,
            &router,
            due + time::Duration::seconds(1),
        )
        .await?;
        attempts += 1;
        assert!(
            attempts < 10,
            "retry budget should exhaust well before this"
        );
    }
    let failed_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-receive")
        .await?;
    assert!(
        !failed_row.errors.is_empty(),
        "a dead-lettered row must carry its failure history"
    );
    let recorded_attempts = failed_row.attempts;
    let recorded_errors = failed_row.errors.len();

    // The descriptor path is what the admin route uses: it must resolve the same
    // retry policy and drive the same redrive as the typed path.
    let descriptor_table =
        ReceivedTable::for_descriptor(&harness.config(), OrderCreated::descriptor()?)?;
    assert_eq!(
        descriptor_table.retry.max_attempts,
        table.retry.max_attempts
    );

    let replay = Replay::received_descriptor(Replay::RUNTIME_VERSION, OrderCreated::descriptor()?)
        .max_rows(10);
    let redriven = redrive_received(harness.pool(), &harness.config(), &replay).await?;
    assert_eq!(redriven, 1);
    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-receive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Pending);
    // Default redrive preserves triage history — that is the documented guarantee.
    assert_eq!(row.attempts, recorded_attempts);
    assert_eq!(row.errors.len(), recorded_errors);
    assert!(row.next_attempt_at.is_none());

    // An unbounded redrive is rejected rather than silently replaying everything.
    let unbounded =
        Replay::received_descriptor(Replay::RUNTIME_VERSION, OrderCreated::descriptor()?);
    assert!(
        redrive_received(harness.pool(), &harness.config(), &unbounded)
            .await
            .is_err(),
        "redrive without max_rows must fail"
    );

    Ok(())
}
