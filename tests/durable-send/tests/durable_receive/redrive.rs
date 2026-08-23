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
    // The legacy `message` key is deliberate: it exercises the compatibility
    // alias that keeps pre-problem-detail rows readable.
    sqlx::query(&format!(
        "UPDATE {table_name}
         SET attempts = 3,
             errors = jsonb_build_array(jsonb_build_object('message', 'old', 'occurred_at', now())),
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
