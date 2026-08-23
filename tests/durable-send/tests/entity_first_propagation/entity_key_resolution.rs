use super::*;

#[tokio::test]
async fn entity_key_survives_a_partition_key_that_is_not_the_entity_key() -> TestResult {
    // Regression: the entity key used to be smuggled to the consumer in the
    // `kafkaman-entity-key` header, but ingest strips the entire `kafkaman-`
    // namespace from user headers. For any type whose partition key differs from
    // its entity key, the cache therefore keyed on the partition key (or worse,
    // on `message_id`) and could never converge. The entity key is now resolved
    // from the typed payload and stored as a column.
    let (_postgres, harness) = start_harness().await?;

    // Send side: the row records both identities, and they are different.
    let outbound = RegionalProduct::envelope("prod-1", "eu-west", "first")
        .with_idempotency_key("idem-regional-1");
    harness.enqueue(&outbound).await?;
    let outbox = harness
        .outbox_row::<RegionalProduct>(outbound.message_id)
        .await?;
    assert_eq!(outbox.entity_key.as_deref(), Some("prod-1"));
    assert_eq!(outbox.partition_key.as_deref(), Some("eu-west"));
    // Co-location still follows the declared partition key.
    assert_eq!(outbox.record_key(), Some("eu-west"));
    // The header is still emitted for foreign consumers that cannot deserialize
    // the typed payload, even though kafkaman's own ingest no longer needs it.
    assert_eq!(
        outbox
            .headers
            .get("kafkaman-entity-key")
            .map(String::as_str),
        Some("prod-1")
    );

    // Receive side: the cache must key on the entity, not the partition key.
    let table = harness.received_table::<RegionalProduct>().await?;
    let inbound = RegionalProduct::envelope("prod-1", "eu-west", "second")
        .with_idempotency_key("idem-regional-2");
    assert!(
        harness
            .insert_received(&inbound, 0, 10, Some(b"eu-west"))
            .await?
    );

    let received = harness
        .received_row_by_idempotency_key::<RegionalProduct>("idem-regional-2")
        .await?;
    assert_eq!(
        received.entity_key.as_deref(),
        Some("prod-1"),
        "the received row must carry the entity key, not the partition key"
    );

    let router = MessageRouter::new()
        .handler::<RegionalProduct>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.processed, 1);

    let cache = CacheTable::for_message::<RegionalProduct>(&harness.config())?;
    let keys: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT entity_key FROM {}",
        cache.qualified_name()
    ))
    .fetch_all(harness.pool())
    .await?;
    assert_eq!(
        keys,
        vec!["prod-1".to_owned()],
        "cache must be keyed by entity, not by partition key or message id"
    );

    Ok(())
}

#[tokio::test]
async fn a_type_without_a_partition_key_publishes_under_its_entity_key() -> TestResult {
    // Regression: the record key came only from `partition_key()`. A type
    // declaring none produced keyless records, which Kafka routes round-robin —
    // scattering one entity's snapshots across partitions, where the convergence
    // guard (which compares offsets only within one partition) can never
    // reconcile them.
    let (_postgres, harness) = start_harness().await?;

    let event = KeylessProduct::envelope("prod-9", "only").with_idempotency_key("idem-keyless-1");
    harness.enqueue(&event).await?;

    let row = harness
        .outbox_row::<KeylessProduct>(event.message_id)
        .await?;
    assert_eq!(row.partition_key, None);
    assert_eq!(row.entity_key.as_deref(), Some("prod-9"));

    let stats = harness.relay_once::<KeylessProduct>().await?;
    assert_eq!(stats.published, 1);

    let records = harness.published_on(KeylessProduct::TOPIC);
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].key.as_deref(),
        Some("prod-9"),
        "a keyless entity type must still be published under a stable entity key"
    );

    Ok(())
}
