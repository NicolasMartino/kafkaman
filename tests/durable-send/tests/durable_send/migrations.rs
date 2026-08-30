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
