use super::*;

#[tokio::test]
async fn retry_after_newer_applied_does_not_regress_cache() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let old = product("p-1", "old").with_idempotency_key("idem-p1-old");
    let new = product("p-1", "new").with_idempotency_key("idem-p1-new");
    assert!(harness.insert_received(&old, 0, 10, Some(b"p-1")).await?);
    assert!(harness.insert_received(&new, 0, 11, Some(b"p-1")).await?);

    let router = MessageRouter::new().handler::<ProductSnapshot>(|_conn, meta, _msg| {
        Box::pin(async move {
            if meta.source_offset == 10 && meta.attempts == 0 {
                Err(kafkaman_sqlx::Error::Handler(
                    "transient old state".to_owned(),
                ))
            } else {
                Ok(())
            }
        })
    });

    let now = OffsetDateTime::now_utc();
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.failed, 1);

    let second = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(second.processed, 1);
    assert_cache_state(&harness, "p-1", "new", 11).await?;

    let retried = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + TimeDuration::seconds(10),
    )
    .await?;
    assert_eq!(retried.processed, 1);
    assert_cache_state(&harness, "p-1", "new", 11).await?;

    Ok(())
}

#[tokio::test]
async fn redrive_after_newer_applied_does_not_regress_cache() -> TestResult {
    let schema = unique_schema("kafkaman_entity");
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 1, 20)).await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let old = product("p-2", "old").with_idempotency_key("idem-p2-old");
    let new = product("p-2", "new").with_idempotency_key("idem-p2-new");
    assert!(harness.insert_received(&old, 0, 20, Some(b"p-2")).await?);
    assert!(harness.insert_received(&new, 0, 21, Some(b"p-2")).await?);

    let router = MessageRouter::new().handler::<ProductSnapshot>(|_conn, meta, _msg| {
        Box::pin(async move {
            if meta.source_offset == 20 && meta.attempts == 0 {
                Err(kafkaman_sqlx::Error::Handler(
                    "terminal old state".to_owned(),
                ))
            } else {
                Ok(())
            }
        })
    });

    let now = OffsetDateTime::now_utc();
    let failed = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(failed.failed, 1);
    let old_row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("idem-p2-old")
        .await?;
    assert_eq!(old_row.status, ReceiveStatus::Failed);

    let newer = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(newer.processed, 1);
    assert_cache_state(&harness, "p-2", "new", 21).await?;

    let cfg = harness.config();
    let ctx = MigrationContext::default().with_context("prod");
    let redrive = changelog![
        InitSchema,
        CreateReceivedTable::new(10_000, ProductSnapshot::descriptor()?),
        Replay::received::<ProductSnapshot>(30_000)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(10)
            .contexts(&["prod"]),
    ];
    migrate(harness.pool(), &cfg, &ctx, &redrive).await?;

    let redriven = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(redriven.processed, 1);
    assert_cache_state(&harness, "p-2", "new", 21).await?;

    Ok(())
}

#[tokio::test]
async fn concurrent_dispatch_of_two_states_converges_to_newer() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let old = product("p-3", "old").with_idempotency_key("idem-p3-old");
    let new = product("p-3", "new").with_idempotency_key("idem-p3-new");
    assert!(harness.insert_received(&old, 0, 30, Some(b"p-3")).await?);
    assert!(harness.insert_received(&new, 0, 31, Some(b"p-3")).await?);

    let old_started = Arc::new(Notify::new());
    let release_old = Arc::new(Notify::new());
    let old_started_for_handler = Arc::clone(&old_started);
    let release_old_for_handler = Arc::clone(&release_old);
    let router = Arc::new(MessageRouter::new().handler::<ProductSnapshot>(
        move |_conn, meta, _msg| {
            let old_started = Arc::clone(&old_started_for_handler);
            let release_old = Arc::clone(&release_old_for_handler);
            Box::pin(async move {
                if meta.source_offset == 30 {
                    old_started.notify_one();
                    release_old.notified().await;
                }
                Ok(())
            })
        },
    ));

    let now = OffsetDateTime::now_utc();
    let pool_for_old = harness.pool().clone();
    let table_for_old = table.clone();
    let router_for_old = Arc::clone(&router);
    let old_dispatch = tokio::spawn(async move {
        dispatch_once(&pool_for_old, &table_for_old, router_for_old.as_ref(), now).await
    });

    old_started.notified().await;

    let pool_for_new = harness.pool().clone();
    let table_for_new = table.clone();
    let router_for_new = Arc::clone(&router);
    let new_dispatch = tokio::spawn(async move {
        dispatch_once(&pool_for_new, &table_for_new, router_for_new.as_ref(), now).await
    });

    let new_stats = new_dispatch.await??;
    assert_eq!(new_stats.claimed, 1);
    assert_eq!(new_stats.processed, 1);
    assert_cache_state(&harness, "p-3", "new", 31).await?;

    release_old.notify_one();
    let old_stats = old_dispatch.await??;
    assert_eq!(old_stats.claimed, 1);
    assert_eq!(old_stats.processed, 1);
    assert_cache_state(&harness, "p-3", "new", 31).await?;

    Ok(())
}

#[tokio::test]
async fn received_redrive_is_context_targeted_dry_runnable_and_applied_once() -> TestResult {
    // The replay change-engine machinery (context gating, counted dry-run
    // preview, checksum-guarded single application) is exercised here on
    // `Replay::received`, which is the only supported replay target: outbox
    // replay is rejected outright as unsafe for entity snapshots.
    let schema = unique_schema("kafkaman_entity");
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 1, 20)).await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let doomed = product("p-4", "doomed").with_idempotency_key("idem-p4-doomed");
    assert!(
        harness
            .insert_received(&doomed, 0, 40, Some(b"p-4"))
            .await?
    );

    let router = MessageRouter::new().handler::<ProductSnapshot>(|_conn, meta, _msg| {
        Box::pin(async move {
            if meta.attempts == 0 {
                Err(kafkaman_sqlx::Error::Handler("terminal".to_owned()))
            } else {
                Ok(())
            }
        })
    });

    let now = OffsetDateTime::now_utc();
    assert_eq!(
        dispatch_once(harness.pool(), &table, &router, now)
            .await?
            .failed,
        1
    );
    assert_eq!(
        harness
            .received_row_by_idempotency_key::<ProductSnapshot>("idem-p4-doomed")
            .await?
            .status,
        ReceiveStatus::Failed
    );

    let cfg = harness.config();
    let redrive =
        || -> Result<Vec<Box<dyn Changeset>>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(changelog![
                InitSchema,
                CreateReceivedTable::new(10_000, ProductSnapshot::descriptor()?),
                Replay::received::<ProductSnapshot>(30_000)?
                    .since(OffsetDateTime::UNIX_EPOCH)
                    .max_rows(10)
                    .contexts(&["prod"]),
            ])
        };

    // 1. A non-matching context skips the replay entirely.
    let skipped = migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default().with_context("staging"),
        &redrive()?,
    )
    .await?;
    assert_eq!(skipped.steps()[2].action, MigrationAction::SkippedContext);
    assert_eq!(
        harness
            .received_row_by_idempotency_key::<ProductSnapshot>("idem-p4-doomed")
            .await?
            .status,
        ReceiveStatus::Failed,
        "a context-skipped replay must not touch rows"
    );

    // 2. A dry run reports the real candidate count and mutates nothing.
    let ctx = MigrationContext::default().with_context("prod");
    let dry_run = migrate_dry_run(harness.pool(), &cfg, &ctx, &redrive()?).await?;
    let preview = dry_run.steps()[2]
        .preview
        .as_deref()
        .expect("replay dry-run should include a preview");
    assert!(preview.contains("~1"), "{preview}");
    assert_eq!(
        harness
            .received_row_by_idempotency_key::<ProductSnapshot>("idem-p4-doomed")
            .await?
            .status,
        ReceiveStatus::Failed,
        "a dry run must not requeue"
    );

    // 3. Applying requeues the row exactly once; re-running is a no-op.
    let applied = migrate(harness.pool(), &cfg, &ctx, &redrive()?).await?;
    assert_eq!(applied.steps()[2].action, MigrationAction::Applied);
    assert_eq!(
        harness
            .received_row_by_idempotency_key::<ProductSnapshot>("idem-p4-doomed")
            .await?
            .status,
        ReceiveStatus::Pending
    );

    let rerun = migrate(harness.pool(), &cfg, &ctx, &redrive()?).await?;
    assert_eq!(
        rerun.steps()[2].action,
        MigrationAction::SkippedAlreadyApplied
    );

    // 4. The redriven row now processes, and its original source_offset is
    //    preserved — that is why received redrive is safe where outbox replay
    //    is not.
    let redriven = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(redriven.processed, 1);
    assert_cache_state(&harness, "p-4", "doomed", 40).await?;

    Ok(())
}
