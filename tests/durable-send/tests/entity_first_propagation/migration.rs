use super::*;

#[tokio::test]
async fn add_received_entity_key_upgrades_a_legacy_received_table() -> TestResult {
    // `AddReceivedEntityKey` is public API and step 1 of the documented upgrade
    // path, and nothing exercised it — the outbox twin was covered, this was not.
    // The risky part is not the `ADD COLUMN` but what happens to rows that were
    // already there: they keep `entity_key IS NULL` forever, so the cache has to
    // converge them through the record-key fallback or not at all.
    let schema = unique_schema("kafkaman_legacy_rx");
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 3, 20)).await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = ProductSnapshot::descriptor()?;
    let table = ReceivedTable::new(cfg.schema.clone(), descriptor.clone())?;

    // Reproduce the pre-column shape by building the current table and dropping
    // the column. Hand-writing the old DDL would drift from the real one and
    // stop testing the migration the day a column is added elsewhere.
    let base: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateReceivedTable::new(2, descriptor.clone())),
        Box::new(CreateCacheTable::new(3, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &base).await?;
    sqlx::query(&format!(
        "ALTER TABLE {} DROP COLUMN entity_key",
        table.qualified_name()
    ))
    .execute(pool)
    .await?;
    assert_eq!(received_entity_key_columns(pool, &cfg).await?, 0);

    // A row written by the old binary: no entity key anywhere but the record key.
    let legacy_id = uuid::Uuid::new_v4();
    sqlx::query(&format!(
        "INSERT INTO {} (
            message_id, idempotency_key, status, source_topic, source_partition,
            source_offset, key, message_type, payload, occurred_at
         ) VALUES ($1, $2, 'Pending', $3, 0, 12, $4, $5, $6, now())",
        table.qualified_name()
    ))
    .bind(legacy_id)
    .bind("a".repeat(64))
    .bind(ProductSnapshot::TOPIC)
    .bind(b"p-legacy".to_vec())
    .bind(ProductSnapshot::MESSAGE_TYPE)
    .bind(serde_json::json!({ "product_id": "p-legacy", "name": "from-before" }))
    .execute(pool)
    .await?;

    let upgraded: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateReceivedTable::new(2, descriptor.clone())),
        Box::new(CreateCacheTable::new(3, descriptor.clone())),
        Box::new(AddReceivedEntityKey::new(4, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &upgraded).await?;
    assert_eq!(received_entity_key_columns(pool, &cfg).await?, 1);

    // Migrations must converge, not just apply: running the same changelog twice
    // is the property the whole engine is built on.
    migrate(pool, &cfg, &MigrationContext::default(), &upgraded).await?;
    assert_eq!(received_entity_key_columns(pool, &cfg).await?, 1);

    // The pre-existing row is not backfilled. That is deliberate — nothing can
    // recover the entity key for a row whose payload the migration cannot type —
    // so the fallback is what has to work.
    let backfilled: Option<String> = sqlx::query_scalar(&format!(
        "SELECT entity_key FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(legacy_id)
    .fetch_one(pool)
    .await?;
    assert!(
        backfilled.is_none(),
        "the changeset must not invent an entity key for rows it cannot type"
    );

    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let stats = dispatch_once(pool, &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(
        stats.processed, 1,
        "a legacy row must still dispatch after the upgrade"
    );

    // `new` rather than `for_message`: this config was built for a hand-rolled
    // changelog and never registered the descriptor.
    let cache = CacheTable::new(cfg.schema.clone(), descriptor.clone())?;
    let keys: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT entity_key FROM {}",
        cache.qualified_name()
    ))
    .fetch_all(pool)
    .await?;
    assert_eq!(
        keys,
        vec!["p-legacy".to_owned()],
        "a NULL-entity_key row must converge through the record key, not a fabricated one"
    );

    Ok(())
}
