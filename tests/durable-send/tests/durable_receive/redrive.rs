//! Moving terminal rows back to `Pending` for one more pass.
use super::*;

#[tokio::test]
async fn replay_received_redrives_failed_rows_without_replaying_processed_rows() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let events = [
        "replay-received-a",
        "replay-received-b",
        "replay-received-c",
    ]
    .into_iter()
    .map(|order_id| {
        Envelope::new(OrderCreated {
            order_id: order_id.to_owned(),
        })
        .with_idempotency_key(format!("idem-{order_id}"))
    })
    .collect::<Vec<_>>();
    for (idx, event) in events.iter().enumerate() {
        assert!(
            harness
                .insert_received(
                    event,
                    6,
                    idx as i64,
                    Some(event.payload.order_id.as_bytes())
                )
                .await?
        );
    }

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    for _ in 0..events.len() {
        let stats =
            dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
        assert_eq!(stats.processed, 1);
    }

    let processed = ReceiveStatus::Processed.sql_literal();
    let pending = ReceiveStatus::Pending.sql_literal();
    let failed = ReceiveStatus::Failed.sql_literal();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {processed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );

    // Give the processed rows a failure history too, so the redrive below is
    // proven to skip them on status rather than on absence of failure metadata.
    sqlx::query(&format!(
        "UPDATE {table_name}
         SET attempts = 3,
             errors = jsonb_build_array(jsonb_build_object('detail', 'old', 'occurred_at', now())),
             last_failed_at = now(),
             last_failure_kind = 'Handler',
             processed_at = now()
         WHERE status = {processed}"
    ))
    .execute(harness.pool())
    .await?;

    let failed_events = ["replay-failed-a", "replay-failed-b", "replay-failed-c"]
        .into_iter()
        .map(|order_id| {
            Envelope::new(OrderCreated {
                order_id: order_id.to_owned(),
            })
            .with_idempotency_key(format!("idem-{order_id}"))
        })
        .collect::<Vec<_>>();
    for (idx, event) in failed_events.iter().enumerate() {
        assert!(
            harness
                .insert_received(
                    event,
                    6,
                    100 + idx as i64,
                    Some(event.payload.order_id.as_bytes())
                )
                .await?
        );
    }
    // Match on stored digests: `idempotency_key` holds a SHA-256 hex digest, so
    // a prefix LIKE against the plaintext key would silently match no rows.
    let failed_keys = failed_events
        .iter()
        .map(|event| idem_key(&format!("idem-{}", event.payload.order_id)).to_string())
        .collect::<Vec<_>>();
    let seeded = sqlx::query(&format!(
        "UPDATE {table_name}
         SET status = {failed},
             attempts = 3,
             next_attempt_at = NULL,
             errors = jsonb_build_array(jsonb_build_object('message', 'old', 'occurred_at', now())),
             last_failed_at = now(),
             last_failure_kind = 'Handler'
         WHERE idempotency_key = ANY($1)"
    ))
    .bind(&failed_keys)
    .execute(harness.pool())
    .await?;
    assert_eq!(seeded.rows_affected(), failed_events.len() as u64);

    let cfg = harness.config();
    let skipped = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, OrderCreated::descriptor()?),
        Replay::received::<OrderCreated>(10_001)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(2)
            .contexts(&["prod"]),
    ];
    let skipped_report = migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default().with_context("staging"),
        &skipped,
    )
    .await?;
    assert_eq!(
        skipped_report.steps()[2].action,
        MigrationAction::SkippedContext
    );

    let replay = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, OrderCreated::descriptor()?),
        Replay::received::<OrderCreated>(10_002)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(2)
            .contexts(&["staging"]),
    ];
    let ctx = MigrationContext::default().with_context("staging");
    let dry_run = migrate_dry_run(harness.pool(), &cfg, &ctx, &replay).await?;
    let preview = dry_run.steps()[2].preview.as_deref().unwrap_or_default();
    assert!(preview.contains("received"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {processed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );

    let applied = migrate(harness.pool(), &cfg, &ctx, &replay).await?;
    assert_eq!(applied.steps()[2].action, MigrationAction::Applied);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {processed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {pending}"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name}
             WHERE status = {pending}
               AND attempts = 3
               AND jsonb_array_length(errors) = 1
               AND processed_at IS NULL"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );

    let rerun = migrate(harness.pool(), &cfg, &ctx, &replay).await?;
    assert_eq!(
        rerun.steps()[2].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {pending}"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        1
    );

    Ok(())
}

#[tokio::test]
async fn redrive_with_clear_history_resets_attempts_and_errors() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = durable_send_tests::unique_schema("kafkaman_observe_clear");
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 1, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-observe-clear".to_owned(),
    })
    .with_idempotency_key("idem-order-observe-clear");
    assert!(harness.insert_received(&event, 0, 9, None).await?);

    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move { Err(kafkaman_sqlx::Error::Handler("boom".to_owned())) })
    });
    // max_attempts is 1 in this config, so one dispatch exhausts the budget.
    dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    let failed = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-clear")
        .await?;
    assert_eq!(failed.status, ReceiveStatus::Failed);
    assert!(failed.attempts > 0);
    assert!(!failed.errors.is_empty());

    let replay = Replay::received_descriptor(Replay::RUNTIME_VERSION, OrderCreated::descriptor()?)
        .max_rows(10)
        .clear_history();
    assert_eq!(
        redrive_received(harness.pool(), &harness.config(), &replay).await?,
        1
    );

    let cleared = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-order-observe-clear")
        .await?;
    assert_eq!(cleared.status, ReceiveStatus::Pending);
    assert_eq!(cleared.attempts, 0, "clear_history resets the retry budget");
    assert!(
        cleared.errors.is_empty(),
        "clear_history drops recorded failures"
    );

    Ok(())
}

#[tokio::test]
async fn a_redrive_leaves_rows_another_redrive_already_claimed() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = durable_send_tests::unique_schema("kafkaman_redrive_concurrent");
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 1, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = table.qualified_name();

    // Four exhausted rows, failed the way production fails them so the redrive
    // filter matches on real `last_failed_at`/`last_failure_kind` values.
    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move { Err(kafkaman_sqlx::Error::Handler("boom".to_owned())) })
    });
    for idx in 0..4 {
        let event = Envelope::new(OrderCreated {
            order_id: format!("order-concurrent-{idx}"),
        })
        .with_idempotency_key(format!("idem-order-concurrent-{idx}"));
        assert!(harness.insert_received(&event, 0, idx, None).await?);
        dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    }
    let failed = ReceiveStatus::Failed.sql_literal();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        4,
        "all four rows exhausted their retry budget"
    );

    // Stand in for a redrive already in flight: another transaction holding the
    // first two candidates, in the order the redrive itself would take them.
    let mut in_flight = harness.pool().begin().await?;
    let claimed = sqlx::query_scalar::<_, uuid::Uuid>(&format!(
        "SELECT message_id FROM {table_name}
         WHERE status = {failed}
         ORDER BY last_failed_at, created_at, message_id
         LIMIT 2
         FOR UPDATE"
    ))
    .fetch_all(&mut *in_flight)
    .await?;
    assert_eq!(claimed.len(), 2);

    // Bounded, because the failure this guards against is a *hang*: without
    // `SKIP LOCKED` this redrive waits on the transaction above instead of
    // taking the rows nobody holds.
    let replay = Replay::received_descriptor(Replay::RUNTIME_VERSION, OrderCreated::descriptor()?)
        .max_rows(10);
    let moved = tokio::time::timeout(
        Duration::from_secs(10),
        redrive_received(harness.pool(), &harness.config(), &replay),
    )
    .await
    .expect("a redrive must not block on rows another redrive is holding")?;
    assert_eq!(
        moved, 2,
        "the two held rows belong to the redrive holding them; this one takes \
         the other two and reports only what it moved"
    );

    // The held rows are untouched, so the operator who is holding them still
    // decides their fate — including whether their failure history survives.
    let held = claimed
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join("','");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name}
             WHERE status = {failed} AND message_id IN ('{held}')"
        ))
        .fetch_one(harness.pool())
        .await?,
        2
    );

    in_flight.rollback().await?;

    // Once nobody holds them they redrive normally, and the second call reports
    // two rather than re-reporting the first call's work.
    let rest = redrive_received(harness.pool(), &harness.config(), &replay).await?;
    assert_eq!(rest, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {table_name} WHERE status = {failed}"
        ))
        .fetch_one(harness.pool())
        .await?,
        0,
        "the two redrives between them moved every failed row exactly once"
    );

    Ok(())
}
