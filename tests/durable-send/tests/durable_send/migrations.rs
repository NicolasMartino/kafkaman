use super::*;

#[tokio::test]
async fn migrate_is_idempotent_and_template_generalizes() -> TestResult {
    let (_postgres, harness) = start_harness().await?;

    harness.outbox_table::<OrderCreated>().await?;
    harness.outbox_table::<InvoiceCreated>().await?;
    let cfg = harness.config();
    let changesets = changelog_for_two_messages()?;

    migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default(),
        &changesets,
    )
    .await?;
    migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default(),
        &changesets,
    )
    .await?;

    let table_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.tables
         WHERE table_schema = $1
           AND table_name IN ('outbox_order_created', 'outbox_invoice_created')",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(table_count, 2);

    let history_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {}.{}",
        cfg.schema.quoted(),
        "changelog_history"
    ))
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(history_count, 3);

    Ok(())
}

#[tokio::test]
async fn add_idempotency_key_upgrades_a_pre_idempotency_outbox_table() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = OrderCreated::descriptor()?;
    let table = OutboxTable::new(cfg.schema.clone(), descriptor.clone())?;

    // Simulate a table created before idempotency support: no idempotency_key
    // column, and changeset v2 already recorded as applied so migrate() will not
    // recreate it.
    let legacy_ddl = format!(
        "CREATE TABLE {} (
            message_id UUID PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'Pending',
            topic TEXT NOT NULL,
            correlation_id UUID NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL
        )",
        table.qualified_name()
    );
    sqlx::query(&legacy_ddl).execute(pool).await?;
    sqlx::query(&format!(
        "INSERT INTO {}.\"changelog_history\" (version, name) VALUES (2, 'create_outbox_table')",
        cfg.schema.quoted()
    ))
    .execute(pool)
    .await?;

    assert_eq!(idempotency_key_columns(pool, &cfg).await?, 0);

    let changesets: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(2, descriptor.clone())),
        Box::new(AddIdempotencyKey::new(3, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;

    // The upgrade changeset must have added the column, and re-running is a no-op.
    assert_eq!(idempotency_key_columns(pool, &cfg).await?, 1);
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(idempotency_key_columns(pool, &cfg).await?, 1);

    Ok(())
}

#[tokio::test]
async fn dry_run_does_not_mutate_legacy_history() -> TestResult {
    let postgres = postgres().await?;
    let database_url = postgres.url();
    let pool = sqlx::PgPool::connect(database_url).await?;
    let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
    let cfg = kafkaman_sqlx::ResolvedConfig::new(schema).with_message(OrderCreated::descriptor()?);
    let history = format!("{}.{}", cfg.schema().quoted(), "\"changelog_history\"");

    // Seed a pre-idempotency, pre-audit M1-shaped history table with one row.
    sqlx::query(&format!("CREATE SCHEMA {}", cfg.schema().quoted()))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "CREATE TABLE {history} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {history} (version, name) VALUES (1, 'init_schema')"
    ))
    .execute(&pool)
    .await?;

    let changesets = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ];
    let ctx = MigrationContext::default().with_applied_by("dry-runner");
    let report = migrate_dry_run(&pool, &cfg, &ctx, &changesets).await?;
    assert_eq!(
        report.steps()[0].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(report.steps()[1].action, MigrationAction::WouldApply);

    // The bootstrap that adds + backfills `applied_by` ran only inside the
    // rolled-back dry-run transaction, so the column must not exist afterwards.
    let applied_by_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'changelog_history' \
         AND column_name = 'applied_by')",
    )
    .bind(cfg.schema().as_str())
    .fetch_one(&pool)
    .await?;
    assert!(
        !applied_by_exists,
        "dry-run must not persist the applied_by column"
    );

    let row_count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {history}"))
        .fetch_one(&pool)
        .await?;
    assert_eq!(row_count, 1, "dry-run must not insert changelog rows");

    Ok(())
}

#[tokio::test]
async fn migrate_records_report_checksum_and_applied_by() -> TestResult {
    let postgres = postgres().await?;
    let database_url = postgres.url();
    let pool = sqlx::PgPool::connect(database_url).await?;
    let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
    let cfg = kafkaman_sqlx::ResolvedConfig::new(schema)
        .with_message(OrderCreated::descriptor()?)
        .with_message(InvoiceCreated::descriptor()?);
    let changesets = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ];
    let ctx = MigrationContext::default().with_applied_by("integration-test");

    let report = migrate(&pool, &cfg, &ctx, &changesets).await?;
    assert_eq!(report.applied_count(), 2);

    let history = format!("{}.{}", cfg.schema().quoted(), "\"changelog_history\"");
    let rows = sqlx::query(&format!(
        "SELECT version, checksum, applied_by FROM {history} ORDER BY version"
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 2);
    for row in rows {
        let checksum: Option<String> = sqlx::Row::try_get(&row, "checksum")?;
        let applied_by: Option<String> = sqlx::Row::try_get(&row, "applied_by")?;
        assert!(checksum.as_deref().is_some_and(
            |value| value.starts_with("sha256:") && value.len() == "sha256:".len() + 64
        ));
        assert_eq!(applied_by.as_deref(), Some("integration-test"));
    }

    let second = migrate(&pool, &cfg, &ctx, &changesets).await?;
    assert!(second
        .steps()
        .iter()
        .all(|step| step.action == MigrationAction::SkippedAlreadyApplied));

    let mutated = changelog![
        InitSchema,
        CreateOutboxTable::new(2, InvoiceCreated::descriptor()?),
    ];
    let err = migrate(&pool, &cfg, &ctx, &mutated)
        .await
        .expect_err("mutated applied changeset must be rejected");
    assert!(matches!(err, kafkaman_sqlx::Error::ChecksumMismatch { .. }));

    Ok(())
}

#[tokio::test]
async fn legacy_null_checksum_history_upgrades_without_mismatch() -> TestResult {
    let postgres = postgres().await?;
    let database_url = postgres.url();
    let pool = sqlx::PgPool::connect(database_url).await?;
    let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
    let cfg = kafkaman_sqlx::ResolvedConfig::new(schema).with_message(OrderCreated::descriptor()?);
    let history = format!("{}.{}", cfg.schema().quoted(), "\"changelog_history\"");

    sqlx::query(&format!("CREATE SCHEMA {}", cfg.schema().quoted()))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "CREATE TABLE {history} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {history} (version, name) VALUES (1, 'init_schema')"
    ))
    .execute(&pool)
    .await?;

    let changesets = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ];
    let report = migrate(
        &pool,
        &cfg,
        &MigrationContext::default().with_applied_by("upgrade-test"),
        &changesets,
    )
    .await?;

    assert_eq!(
        report.steps()[0].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(report.steps()[1].action, MigrationAction::Applied);

    let legacy = sqlx::query(&format!(
        "SELECT checksum, applied_by FROM {history} WHERE version = 1"
    ))
    .fetch_one(&pool)
    .await?;
    let checksum: Option<String> = sqlx::Row::try_get(&legacy, "checksum")?;
    let applied_by: Option<String> = sqlx::Row::try_get(&legacy, "applied_by")?;
    assert!(checksum.is_none());
    assert_eq!(applied_by.as_deref(), Some("unknown"));

    Ok(())
}

#[test]
fn changelog_macro_rejects_disordered_or_duplicate_versions() {
    let disordered = std::panic::catch_unwind(|| {
        let _ = changelog![
            CreateOutboxTable::new(2, OrderCreated::descriptor().unwrap()),
            InitSchema,
        ];
    });
    assert!(disordered.is_err());

    let duplicate = std::panic::catch_unwind(|| {
        let _ = changelog![
            InitSchema,
            CreateOutboxTable::new(2, OrderCreated::descriptor().unwrap()),
            CreateOutboxTable::new(2, InvoiceCreated::descriptor().unwrap()),
        ];
    });
    assert!(duplicate.is_err());
}
