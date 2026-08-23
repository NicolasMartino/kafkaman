use super::*;

#[tokio::test]
async fn cache_apply_halts_when_an_entity_changes_partition() -> TestResult {
    // Offsets are comparable only within one topic and partition. If an entity
    // moves partition, the guard's predicate can never be true again, so the
    // cache row would freeze forever with no error and no metric. Detection must
    // halt loudly and demand a re-bootstrap instead.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let now = OffsetDateTime::now_utc();

    let first = product("p-move", "from-p0").with_idempotency_key("idem-move-1");
    assert!(
        harness
            .insert_received(&first, 0, 5, Some(b"p-move"))
            .await?
    );
    assert_eq!(
        dispatch_once(harness.pool(), &table, &router, now)
            .await?
            .processed,
        1
    );
    assert_cache_state(&harness, "p-move", "from-p0", 5).await?;

    // Same entity, different partition: not a stale record, a broken invariant.
    let moved = product("p-move", "from-p1").with_idempotency_key("idem-move-2");
    assert!(
        harness
            .insert_received(&moved, 1, 6, Some(b"p-move"))
            .await?
    );
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.processed, 0);
    assert_eq!(
        stats.failed, 1,
        "a partition change must not be silently ignored"
    );

    let row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("idem-move-2")
        .await?;
    let detail = row
        .errors
        .last()
        .map(|error| error.detail.clone())
        .unwrap_or_default();
    assert!(
        detail.contains("re-bootstrapped"),
        "the failure must name the required operator action, got: {detail}"
    );
    // Terminal on the first attempt. The guard's predicate can never become true
    // again for this row, so burning the retry budget would only postpone the
    // operator signal by the length of that budget.
    assert_eq!(
        row.status,
        ReceiveStatus::Failed,
        "a repartition is not something a retry can fix"
    );
    assert!(
        row.next_attempt_at.is_none(),
        "a terminal row must not remain claimable by a due-time query"
    );
    assert_eq!(
        row.attempts, 1,
        "it must not consume the whole retry budget"
    );

    // The applied state is untouched: the guard refused, it did not regress.
    assert_cache_state(&harness, "p-move", "from-p0", 5).await?;

    Ok(())
}

#[tokio::test]
async fn cache_apply_refuses_a_row_with_no_resolvable_entity_key() -> TestResult {
    // Falling back to `message_id` would give every message its own cache row:
    // unbounded growth that never converges and looks healthy until read.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let event = product("p-orphan", "state").with_idempotency_key("idem-orphan");
    assert!(harness.insert_received(&event, 0, 1, None).await?);

    // Simulate a row from before the entity-key column existed, with no header
    // and no record key to fall back to.
    sqlx::query(&format!(
        "UPDATE {} SET entity_key = NULL, key = NULL WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(event.message_id)
    .execute(harness.pool())
    .await?;

    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.failed, 1);

    // Also terminal: re-reading the same row cannot conjure an entity key.
    let row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("idem-orphan")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Failed);
    assert!(row.next_attempt_at.is_none());

    let cache = CacheTable::for_message::<ProductSnapshot>(&harness.config())?;
    let cached: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {}", cache.qualified_name()))
            .fetch_one(harness.pool())
            .await?;
    assert_eq!(
        cached, 0,
        "no cache row may be created under a fabricated key"
    );

    Ok(())
}
