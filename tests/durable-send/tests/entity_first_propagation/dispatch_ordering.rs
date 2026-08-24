//! Where an application handler runs relative to the cache upsert.
//!
//! Every assertion here is behavioural. The savepoint placement in particular is
//! the sort of thing that reads correct in either position, so it is proven by
//! failing a handler and then reading the cache row — never by inspecting where
//! the savepoint call sits.

use super::*;

/// Read the cache row a handler would see, from inside that handler.
async fn cached_name(
    conn: &mut sqlx::PgConnection,
    cache: &str,
    entity_key: &str,
) -> Option<String> {
    let sql = format!("SELECT payload->>'name' FROM {cache} WHERE entity_key = $1");
    sqlx::query_scalar(&sql)
        .bind(entity_key)
        .fetch_optional(conn)
        .await
        .ok()
        .flatten()
}

async fn cache_name_now(harness: &Harness, entity_key: &str) -> TestResult<Option<String>> {
    let cache = CacheTable::for_message::<ProductSnapshot>(&harness.config())?;
    let sql = format!(
        "SELECT payload->>'name' FROM {} WHERE entity_key = $1",
        cache.qualified_name()
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(entity_key)
        .fetch_optional(harness.pool())
        .await?)
}

async fn cache_table_name(harness: &Harness) -> TestResult<Arc<str>> {
    Ok(
        CacheTable::for_message::<ProductSnapshot>(&harness.config())?
            .qualified_name()
            .into(),
    )
}

/// The headline reversal. A handler that derives from its own cache used to see
/// the entity exactly one version stale and had to compensate; now it sees the
/// incoming record already applied.
#[tokio::test]
async fn a_post_upsert_handler_sees_its_own_record_already_applied() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let cache = cache_table_name(&harness).await?;
    let seen: Arc<std::sync::Mutex<Vec<Option<String>>>> = Arc::default();

    let router = {
        let seen = Arc::clone(&seen);
        MessageRouter::new().handler::<ProductSnapshot>(move |conn, _meta, _msg| {
            let seen = Arc::clone(&seen);
            let cache = Arc::clone(&cache);
            Box::pin(async move {
                let observed = cached_name(conn, &cache, "p-order").await;
                seen.lock().unwrap().push(observed);
                Ok(())
            })
        })
    };

    let first = product("p-order", "v1").with_idempotency_key("order-1");
    assert!(
        harness
            .insert_received(&first, 0, 1, Some(b"p-order"))
            .await?
    );
    dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;

    let second = product("p-order", "v2").with_idempotency_key("order-2");
    assert!(
        harness
            .insert_received(&second, 0, 2, Some(b"p-order"))
            .await?
    );
    dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;

    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![Some("v1".to_owned()), Some("v2".to_owned())],
        "the handler must see the incoming record, not the previous version"
    );
    Ok(())
}

/// A pre-upsert handler is the only way to see the previous version, which is
/// exactly why it exists as a separate opt-in.
#[tokio::test]
async fn a_pre_upsert_handler_sees_the_previous_version() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let cache = cache_table_name(&harness).await?;
    let seen: Arc<std::sync::Mutex<Vec<Option<String>>>> = Arc::default();

    let router = {
        let seen = Arc::clone(&seen);
        MessageRouter::new().handler_before::<ProductSnapshot>(move |conn, _meta, _msg| {
            let seen = Arc::clone(&seen);
            let cache = Arc::clone(&cache);
            Box::pin(async move {
                let observed = cached_name(conn, &cache, "p-pre").await;
                seen.lock().unwrap().push(observed);
                Ok(HandlerFlow::Continue)
            })
        })
    };

    for (offset, name, idem) in [(1_i64, "v1", "pre-1"), (2, "v2", "pre-2")] {
        let envelope = product("p-pre", name).with_idempotency_key(idem);
        assert!(
            harness
                .insert_received(&envelope, 0, offset, Some(b"p-pre"))
                .await?
        );
        dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    }

    assert_eq!(
        seen.lock().unwrap().clone(),
        vec![None, Some("v1".to_owned())],
        "absent before the first snapshot, then the previous version"
    );
    // Registering only the pre-upsert hook is a complete registration: the cache
    // still converged.
    assert_eq!(
        cache_name_now(&harness, "p-pre").await?.as_deref(),
        Some("v2")
    );
    Ok(())
}

/// A record at or behind the applied offset carries no new state, so re-deriving
/// from it is wasted work at best and a stale recomputation at worst.
#[tokio::test]
async fn a_replayed_record_is_ignored_and_the_post_upsert_handler_does_not_run() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let runs = Arc::new(AtomicUsize::new(0));

    let router = {
        let runs = Arc::clone(&runs);
        MessageRouter::new().handler::<ProductSnapshot>(move |_conn, _meta, _msg| {
            runs.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(()) })
        })
    };

    let newer = product("p-ignore", "v2").with_idempotency_key("ignore-2");
    assert!(
        harness
            .insert_received(&newer, 0, 9, Some(b"p-ignore"))
            .await?
    );
    dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Behind the applied offset: the guard refuses the write.
    let older = product("p-ignore", "v1").with_idempotency_key("ignore-1");
    assert!(
        harness
            .insert_received(&older, 0, 3, Some(b"p-ignore"))
            .await?
    );
    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the handler must be skipped"
    );
    // The row is still claimed, still marked processed, and still counted. The
    // stats are a row-disposition count, not a handler-execution count.
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(
        cache_name_now(&harness, "p-ignore").await?.as_deref(),
        Some("v2"),
        "the ignored record must not regress the cache"
    );
    Ok(())
}

/// Skipping downstream work is a cost decision the application is entitled to
/// make. Skipping convergence is not, and there is no API for it.
#[tokio::test]
async fn a_pre_upsert_skip_stops_the_handler_but_not_the_upsert() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let runs = Arc::new(AtomicUsize::new(0));

    let router = {
        let runs = Arc::clone(&runs);
        MessageRouter::new()
            .handler_before::<ProductSnapshot>(|_conn, _meta, _msg| {
                Box::pin(async move { Ok(HandlerFlow::SkipHandler) })
            })
            .handler::<ProductSnapshot>(move |_conn, _meta, _msg| {
                runs.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(()) })
            })
    };

    let envelope = product("p-skip", "v1").with_idempotency_key("skip-1");
    assert!(
        harness
            .insert_received(&envelope, 0, 4, Some(b"p-skip"))
            .await?
    );
    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;

    assert_eq!(runs.load(Ordering::SeqCst), 0, "the skip must be honoured");
    assert_eq!(stats.processed, 1, "the row is still processed");
    assert_eq!(
        cache_name_now(&harness, "p-skip").await?.as_deref(),
        Some("v1"),
        "a handler must never be able to suppress convergence"
    );
    Ok(())
}

/// **The savepoint gate.** If the savepoint opened after the upsert, this
/// handler failure would commit an advanced cache row; the retry would then find
/// the record at the applied offset, get `Ignored`, and skip the handler
/// forever. Asserted by reading the cache, not by reading the savepoint code.
#[tokio::test]
async fn a_failing_post_upsert_handler_unwinds_the_cache_upsert_too() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let fail = Arc::new(AtomicUsize::new(1));

    let router = {
        let fail = Arc::clone(&fail);
        MessageRouter::new().handler::<ProductSnapshot>(move |_conn, _meta, _msg| {
            let should_fail = fail.load(Ordering::SeqCst) == 1;
            Box::pin(async move {
                if should_fail {
                    return Err(kafkaman_sqlx::Error::Handler("derivation blew up".into()));
                }
                Ok(())
            })
        })
    };

    let envelope = product("p-savepoint", "v1").with_idempotency_key("savepoint-1");
    assert!(
        harness
            .insert_received(&envelope, 0, 11, Some(b"p-savepoint"))
            .await?
    );

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.failed, 1);
    assert_eq!(
        cache_name_now(&harness, "p-savepoint").await?,
        None,
        "a failed handler must leave the cache exactly where it was; otherwise the \
         retry yields `Ignored` and the handler never runs again"
    );

    // And the retry genuinely re-runs the handler, which is the property the
    // assertion above exists to protect.
    fail.store(0, Ordering::SeqCst);
    let row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("savepoint-1")
        .await?;
    assert_eq!(
        row.status,
        ReceiveStatus::Retryable,
        "a handler failure is retryable, so the row stays claimable"
    );

    let due = OffsetDateTime::now_utc() + TimeDuration::hours(1);
    let stats = dispatch_once(harness.pool(), &table, &router, due).await?;
    assert_eq!(stats.processed, 1);
    assert_eq!(
        cache_name_now(&harness, "p-savepoint").await?.as_deref(),
        Some("v1")
    );
    Ok(())
}

/// A failing *pre*-upsert handler must unwind the same way, because the
/// savepoint covers both hooks.
#[tokio::test]
async fn a_failing_pre_upsert_handler_leaves_the_cache_untouched() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;

    let router = MessageRouter::new().handler_before::<ProductSnapshot>(|_conn, _meta, _msg| {
        Box::pin(async move {
            Err(kafkaman_sqlx::Error::Handler(
                "pre-image check failed".into(),
            ))
        })
    });

    let envelope = product("p-pre-fail", "v1").with_idempotency_key("pre-fail-1");
    assert!(
        harness
            .insert_received(&envelope, 0, 2, Some(b"p-pre-fail"))
            .await?
    );

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.failed, 1);
    assert_eq!(cache_name_now(&harness, "p-pre-fail").await?, None);
    Ok(())
}

/// The missing-handler lookup stays ahead of the upsert. A replica that has not
/// been deployed yet must not silently converge a cache it has no code to derive
/// from.
#[tokio::test]
async fn an_unregistered_type_parks_its_row_without_advancing_the_cache() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let router = MessageRouter::new();

    let envelope = product("p-unrouted", "v1").with_idempotency_key("unrouted-1");
    assert!(
        harness
            .insert_received(&envelope, 0, 6, Some(b"p-unrouted"))
            .await?
    );

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.processed, 0);

    let row = harness
        .received_row_by_idempotency_key::<ProductSnapshot>("unrouted-1")
        .await?;
    assert_eq!(
        row.status,
        ReceiveStatus::Retryable,
        "parked for a later deploy, not dropped"
    );
    assert_eq!(cache_name_now(&harness, "p-unrouted").await?, None);
    Ok(())
}

/// A type routed only pre-upsert is fully registered. Treating it as unrouted
/// would park every one of its rows.
#[tokio::test]
async fn a_pre_upsert_only_registration_is_not_a_missing_handler() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<ProductSnapshot>().await?;
    let router = MessageRouter::new().handler_before::<ProductSnapshot>(|_conn, _meta, _msg| {
        Box::pin(async move { Ok(HandlerFlow::Continue) })
    });

    let envelope = product("p-preonly", "v1").with_idempotency_key("preonly-1");
    assert!(
        harness
            .insert_received(&envelope, 0, 8, Some(b"p-preonly"))
            .await?
    );

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(
        cache_name_now(&harness, "p-preonly").await?.as_deref(),
        Some("v1")
    );
    Ok(())
}
