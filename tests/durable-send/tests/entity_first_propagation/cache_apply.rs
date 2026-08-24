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

#[tokio::test]
async fn a_rebuilt_topic_migrates_a_cache_stranded_on_the_retired_one() -> TestResult {
    // The recovery a topic rebuild depends on. After republishing every entity
    // onto a new topic, each consumer's cache still holds an offset from the
    // retired one — and offsets compare only within a topic-partition, so the
    // ordinary guard can never accept another record for that entity again.
    // Arriving on the topic the type *declares* is what authorizes resetting it.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let now = OffsetDateTime::now_utc();

    let first = product("p-rebuild", "from-retired").with_idempotency_key("idem-rebuild-1");
    assert!(
        harness
            .insert_received(&first, 0, 5, Some(b"p-rebuild"))
            .await?
    );
    assert_eq!(
        dispatch_once(harness.pool(), &table, &router, now)
            .await?
            .processed,
        1
    );

    // The offset is far ahead of anything the new topic will produce, so if the
    // record below is applied it can only be because the guard was reset rather
    // than satisfied.
    strand_cache_on_retired_topic(&harness, "p-rebuild", "products.retired", 9_999).await?;

    let rebuilt = product("p-rebuild", "from-rebuilt").with_idempotency_key("idem-rebuild-2");
    assert!(
        harness
            .insert_received(&rebuilt, 0, 1, Some(b"p-rebuild"))
            .await?
    );
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(
        stats.failed, 0,
        "a record on the declared topic must not fail against a stranded cache"
    );
    assert_eq!(stats.processed, 1);

    // Applied at offset 1 despite the cache holding 9_999: the guard was reset,
    // not out-raced.
    assert_cache_state(&harness, "p-rebuild", "from-rebuilt", 1).await?;

    let cache = CacheTable::for_message::<ProductSnapshot>(&harness.config())?;
    let applied_topic: String = sqlx::query(&format!(
        "SELECT applied_topic FROM {} WHERE entity_key = $1",
        cache.qualified_name()
    ))
    .bind("p-rebuild")
    .fetch_one(harness.pool())
    .await?
    .try_get("applied_topic")?;
    assert_eq!(
        applied_topic,
        ProductSnapshot::TOPIC,
        "the cache must adopt the declared topic, not keep the retired one"
    );

    Ok(())
}

#[tokio::test]
async fn a_straggler_from_a_retired_topic_is_dropped_quietly() -> TestResult {
    // The other half of a cutover: once the cache has moved to the declared
    // topic, records still draining out of the received table from the retired
    // one are stale. Failing them would fill the error table for the length of
    // the migration, so they are ignored exactly like any other stale record.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let now = OffsetDateTime::now_utc();

    let current = product("p-straggler", "current").with_idempotency_key("idem-straggler-1");
    assert!(
        harness
            .insert_received(&current, 0, 7, Some(b"p-straggler"))
            .await?
    );
    assert_eq!(
        dispatch_once(harness.pool(), &table, &router, now)
            .await?
            .processed,
        1
    );

    let straggler = product("p-straggler", "stale").with_idempotency_key("idem-straggler-2");
    assert!(
        harness
            .insert_received(&straggler, 0, 99, Some(b"p-straggler"))
            .await?
    );
    restamp_received_source_topic(&harness, 99, "products.retired").await?;

    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(
        stats.failed, 0,
        "a straggler from a retired topic is stale, not broken"
    );
    assert_eq!(stats.processed, 1);

    // Its offset was higher, so only the origin check kept it out.
    assert_cache_state(&harness, "p-straggler", "current", 7).await?;

    Ok(())
}

#[tokio::test]
async fn an_origin_change_to_an_undeclared_topic_still_fails() -> TestResult {
    // The reason the authorization is a declared *topic* rather than "any new
    // origin": a consumer wired to the wrong topic must not be able to
    // overwrite a cache with another domain's state just by being different.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let router = MessageRouter::new()
        .handler::<ProductSnapshot>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let now = OffsetDateTime::now_utc();

    let first = product("p-wrong", "original").with_idempotency_key("idem-wrong-1");
    assert!(
        harness
            .insert_received(&first, 0, 3, Some(b"p-wrong"))
            .await?
    );
    assert_eq!(
        dispatch_once(harness.pool(), &table, &router, now)
            .await?
            .processed,
        1
    );
    // Neither the cached origin nor the incoming one is the declared topic.
    strand_cache_on_retired_topic(&harness, "p-wrong", "products.retired", 3).await?;

    let stray = product("p-wrong", "from-elsewhere").with_idempotency_key("idem-wrong-2");
    assert!(
        harness
            .insert_received(&stray, 0, 4, Some(b"p-wrong"))
            .await?
    );
    restamp_received_source_topic(&harness, 4, "some.other.topic").await?;

    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(
        stats.failed, 1,
        "an unauthorized origin change must still fail terminally"
    );
    assert_eq!(stats.processed, 0);

    let row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("idem-wrong-2")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Failed);
    assert_cache_state(&harness, "p-wrong", "original", 3).await?;

    Ok(())
}
