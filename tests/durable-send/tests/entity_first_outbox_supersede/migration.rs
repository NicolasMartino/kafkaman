//! Upgrading an outbox table that predates the entity key column.
use super::*;

#[tokio::test]
async fn add_outbox_entity_key_upgrades_legacy_outbox_table() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = ProductSnapshot::descriptor()?;
    let table = OutboxTable::new(cfg.schema.clone(), descriptor.clone())?;

    let legacy_ddl = format!(
        "CREATE TABLE {} (
            message_id UUID PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'Pending',
            topic TEXT NOT NULL,
            correlation_id UUID NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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

    assert_eq!(entity_key_columns(pool, &cfg).await?, 0);

    let changesets: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(2, descriptor.clone())),
        Box::new(AddOutboxEntityKey::new(3, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(entity_key_columns(pool, &cfg).await?, 1);
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(entity_key_columns(pool, &cfg).await?, 1);

    Ok(())
}

async fn entity_key_columns(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'outbox_product_snapshot'
           AND column_name = 'entity_key'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
}
