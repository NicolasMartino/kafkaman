//! Rebuilding outboxes from current state.
use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use kafkaman::recovery::Failure;
use kafkaman_sqlx::enqueue;
use tokio::sync::Notify;

fn failed(error: RecoveryError) -> (String, Report, Failure) {
    match error {
        RecoveryError::Failed {
            source_name,
            report,
            cause,
        } => (source_name, *report, *cause),
        other => panic!("expected a failed run, got {other}"),
    }
}

/// T1 and T10: pages of two from current state, a key that vanished between
/// listing and loading, and a second run under its own identity.
#[tokio::test]
async fn a_resync_rebuilds_every_page_from_current_state() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products(), regional()]).await?;
    fixture
        .seed(
            "product_state",
            &[
                ("p-01", "a"),
                ("p-02", "b"),
                ("p-03", "c"),
                ("p-04", "d"),
                ("p-05", "e"),
            ],
        )
        .await?;
    fixture.fault("p-03", "missing").await?;
    let mut recovery = Recovery::new(vec![
        product_source("products"),
        regional_source("regional"),
    ])?;
    let tasks_before = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();

    let events = Events::default();
    let first = {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await?
    };
    assert_eq!(first.failed, None);
    let started = &events.named("recovery started")[0]["fields"];
    assert_eq!(started["sources"], "products,regional");
    assert!(started.get("source").is_none());
    let completed = events.named("recovery source completed");
    assert_eq!(completed.len(), 2, "{completed:?}");
    assert_eq!(completed[0]["fields"]["source"], "products");
    assert_eq!(completed[0]["fields"]["last_key"], "p-05");
    assert_eq!(completed[0]["fields"]["enqueued"], 4);
    assert_eq!(completed[1]["fields"]["source"], "regional");
    assert!(completed[1]["fields"].get("last_key").is_none());
    assert!(first.not_run.is_empty());
    assert_eq!(
        first
            .completed
            .iter()
            .map(|outcome| (outcome.source.as_str(), outcome.progress.clone()))
            .collect::<Vec<_>>(),
        [
            ("products", resync_progress(4, 1, Some("p-05"))),
            ("regional", resync_progress(0, 0, None)),
        ]
    );
    assert_eq!(
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks(),
        tasks_before,
        "recovery runs in the caller's task and leaves nothing running"
    );
    let rows = fixture.outbox::<ProductSnapshot>().await?;
    assert_eq!(
        rows.iter()
            .map(|(key, name, _)| (key.as_str(), name.as_str()))
            .collect::<Vec<_>>(),
        [("p-01", "a"), ("p-02", "b"), ("p-04", "d"), ("p-05", "e")]
    );

    // State moves on; the next run publishes the new state under new keys.
    fixture.seed("product_state", &[("p-01", "a2")]).await?;
    let second = fixture
        .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
        .await?;
    assert_ne!(first.run_id, second.run_id);
    let rows = fixture.outbox::<ProductSnapshot>().await?;
    assert_eq!(rows.len(), 8);
    assert_eq!((rows[4].0.as_str(), rows[4].1.as_str()), ("p-01", "a2"));
    let identities = rows
        .iter()
        .map(|(_, _, identity)| identity.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        identities.len(),
        8,
        "every run's envelope is a new identity"
    );
    let sources: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT idempotency_source::text FROM {} ORDER BY enqueue_order",
        OutboxTable::for_message::<ProductSnapshot>(&fixture.resolved())?.qualified_name()
    ))
    .fetch_all(&fixture.pool)
    .await?;
    assert!(sources[..4]
        .iter()
        .all(|s| s.contains(&first.run_id.to_string())));
    assert!(sources[4..]
        .iter()
        .all(|s| s.contains(&second.run_id.to_string())));
    Ok(())
}

/// T4 and T9: every selected topic is verified before any table exists, and
/// a refusal in preparation reports the cursor it was given.
#[tokio::test]
async fn verification_covers_every_selected_topic_before_any_storage() -> TestResult {
    let fixture = Fixture::start().await?;
    let (broker, brokers) = broker_with(&[products()]).await?;
    fixture
        .seed("product_state", &[("p-00", "z"), ("p-01", "a")])
        .await?;
    fixture.seed("regional_state", &[("r-01", "a")]).await?;
    let mut recovery = Recovery::new(vec![
        product_source("products"),
        regional_source("regional"),
    ])?;

    let (source, report, cause) = failed(
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await
            .unwrap_err(),
    );
    assert_eq!(source, "regional");
    assert!(matches!(cause, Failure::Topics(_)), "{cause}");
    assert!(report.completed.is_empty());
    assert_eq!(report.not_run, ["products"]);
    assert!(!fixture.outbox_exists::<ProductSnapshot>().await?);
    assert!(!fixture.outbox_exists::<RegionalProduct>().await?);
    assert_eq!(fixture.applied_changesets().await?, 0);
    let admin = TopicAdmin::from_brokers(&brokers)?;
    assert!(
        converge_topics(&admin, TopicMode::Verify, &prefixed(&[regional()]))
            .await
            .is_err(),
        "verification must not have created the missing topic"
    );

    // A resume refused in preparation keeps its cursor and counts nothing.
    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(
                &mut recovery,
                Some(&brokers),
                resync_inputs(Some("regional"), Some("r-00")),
            )
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `r-00`"),
        "{error}"
    );
    let (_, report, _) = failed(error);
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("r-00"))
    );
    let failures = events.named("recovery failed");
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["fields"]["last_key"], "r-00");
    assert_eq!(failures[0]["fields"]["source"], "regional");
    assert_eq!(failures[0]["fields"]["run_id"], report.run_id.to_string());

    // A broker that is down is a preparation failure too; once it is back,
    // the resume starts after its cursor.
    broker.stop_with_timeout(Some(5)).await?;
    let resume = || resync_inputs(Some("products"), Some("p-00"));
    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), resume())
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `p-00`"),
        "{error}"
    );
    let (_, report, cause) = failed(error);
    assert!(
        matches!(cause, Failure::Topics(_) | Failure::Transport(_)),
        "{cause}"
    );
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("p-00"))
    );
    assert_eq!(
        events.named("recovery failed")[0]["fields"]["last_key"],
        "p-00"
    );
    assert!(!fixture.outbox_exists::<ProductSnapshot>().await?);

    restart(&broker, &brokers, &[products()]).await?;
    let report = fixture.run(&mut recovery, Some(&brokers), resume()).await?;
    assert_eq!(
        report.completed[0].progress,
        resync_progress(1, 0, Some("p-01"))
    );
    Ok(())
}

fn counting_keys(
    table: &'static str,
    listed: Arc<AtomicUsize>,
) -> impl for<'a> FnMut(&'a mut PgConnection, Option<&'a str>, usize) -> ResyncFuture<'a, Vec<String>>
       + Send
       + 'static {
    move |conn, after, limit| {
        listed.fetch_add(1, Ordering::SeqCst);
        keys_from(table, conn, after, limit)
    }
}

/// T4 and T9: a migration that fails after every topic verified stops the
/// run before any key is listed, and keeps the resume cursor.
#[tokio::test]
async fn a_migration_failure_stops_the_run_before_any_key_is_listed() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products(), regional()]).await?;
    fixture.seed("product_state", &[("p-01", "a")]).await?;
    fixture
        .seed("regional_state", &[("r-00", "z"), ("r-01", "a")])
        .await?;
    // A view where regional's outbox belongs: the table is skipped as
    // existing and the first index on it fails.
    fixture.init_schema().await?;
    let blocked = OutboxTable::for_message::<RegionalProduct>(&fixture.resolved())?;
    sqlx::query(&format!(
        "CREATE VIEW {} AS SELECT 1 AS placeholder",
        blocked.qualified_name()
    ))
    .execute(&fixture.pool)
    .await?;
    let listed = Arc::new(AtomicUsize::new(0));
    let mut recovery = Recovery::new(vec![
        Source::resync::<ProductSnapshot, _, _>(
            "products",
            counting_keys("product_state", listed.clone()),
            |conn, key, run_id| {
                Box::pin(async move {
                    Ok(current("product_state", conn, key)
                        .await?
                        .map(|(id, name)| {
                            ProductSnapshot::envelope(&id, &name)
                                .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                        }))
                })
            },
        ),
        Source::resync::<RegionalProduct, _, _>(
            "regional",
            counting_keys("regional_state", listed.clone()),
            |_, _, _| Box::pin(async { Ok(None) }),
        ),
    ])?;

    let (source, report, cause) = failed(
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await
            .unwrap_err(),
    );
    assert_eq!(source, "regional");
    assert!(matches!(cause, Failure::Migrate(_)), "{cause}");
    assert_eq!(report.not_run, ["products"]);
    assert_eq!(listed.load(Ordering::SeqCst), 0, "no key was listed");
    assert!(fixture.outbox::<ProductSnapshot>().await?.is_empty());

    let resume = || resync_inputs(Some("regional"), Some("r-00"));
    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), resume())
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `r-00`"),
        "{error}"
    );
    let (_, report, cause) = failed(error);
    assert!(matches!(cause, Failure::Migrate(_)), "{cause}");
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("r-00"))
    );
    assert_eq!(
        events.named("recovery failed")[0]["fields"]["last_key"],
        "r-00"
    );
    assert_eq!(listed.load(Ordering::SeqCst), 0);

    sqlx::query(&format!("DROP VIEW {}", blocked.qualified_name()))
        .execute(&fixture.pool)
        .await?;
    let report = fixture.run(&mut recovery, Some(&brokers), resume()).await?;
    assert_eq!(
        report.completed[0].progress,
        resync_progress(0, 1, Some("r-01")),
        "the resume starts after r-00"
    );
    Ok(())
}

/// T5: the run stops at the first failing source and reports the last key it
/// committed; resuming from there loads the next key, not that one.
#[tokio::test]
async fn a_failure_stops_the_run_at_the_last_committed_key() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products(), regional(), keyless()]).await?;
    fixture.seed("regional_state", &[("r-01", "a")]).await?;
    fixture
        .seed(
            "product_state",
            &[("p-01", "a"), ("p-02", "b"), ("p-03", "c"), ("p-04", "d")],
        )
        .await?;
    fixture.seed("keyless_state", &[("k-01", "a")]).await?;
    fixture.fault("p-03", "mismatch").await?;
    let mut recovery = Recovery::new(vec![
        regional_source("regional"),
        product_source("products"),
        keyless_source("keyless"),
    ])?;

    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `p-02`"),
        "{error}"
    );
    let (source, report, cause) = failed(error);
    assert_eq!(source, "products");
    assert!(
        matches!(
            cause,
            Failure::Resync(kafkaman_sqlx::Error::InvalidResync(_))
        ),
        "{cause}"
    );
    assert_eq!(report.completed.len(), 1);
    assert_eq!(report.completed[0].source, "regional");
    assert_eq!(
        report.completed[0].progress,
        resync_progress(1, 0, Some("r-01"))
    );
    assert_eq!(
        report.failed.as_ref().unwrap().progress,
        resync_progress(2, 0, Some("p-02"))
    );
    assert_eq!(report.not_run, ["keyless"]);
    let failure = &events.named("recovery failed")[0]["fields"];
    assert_eq!(failure["last_key"], "p-02");
    assert_eq!(failure["enqueued"], 2);
    assert_eq!(failure["not_run"], "keyless");
    let completed = events.named("recovery source completed");
    assert_eq!(completed.len(), 1, "{completed:?}");
    assert_eq!(completed[0]["fields"]["source"], "regional");
    assert_eq!(completed[0]["fields"]["last_key"], "r-01");
    let messages = events.messages();
    let position = |message: &str| messages.iter().position(|seen| seen == message);
    assert!(
        position("recovery source completed") < position("recovery failed"),
        "{messages:?}"
    );
    assert_eq!(
        fixture
            .outbox::<ProductSnapshot>()
            .await?
            .iter()
            .map(|(key, _, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["p-01", "p-02"]
    );
    assert!(
        fixture.outbox_exists::<KeylessProduct>().await?,
        "every selected source is prepared before the first key"
    );
    assert!(fixture.outbox::<KeylessProduct>().await?.is_empty());

    // A resume that fails before its first commit keeps the cursor.
    fixture.clear_faults().await?;
    fixture.fault("p-03", "error").await?;
    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(
                &mut recovery,
                Some(&brokers),
                resync_inputs(Some("products"), Some("p-02")),
            )
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `p-02`"),
        "{error}"
    );
    let (_, report, cause) = failed(error);
    assert!(matches!(cause, Failure::Resync(_)), "{cause}");
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("p-02"))
    );
    assert_eq!(
        events.named("recovery failed")[0]["fields"]["last_key"],
        "p-02"
    );

    // With the fault gone, the resume starts strictly after the cursor.
    fixture.clear_faults().await?;
    let report = fixture
        .run(
            &mut recovery,
            Some(&brokers),
            resync_inputs(Some("products"), Some("p-02")),
        )
        .await?;
    assert_eq!(
        report.completed[0].progress,
        resync_progress(2, 0, Some("p-04"))
    );
    assert_eq!(
        fixture
            .outbox::<ProductSnapshot>()
            .await?
            .iter()
            .map(|(key, _, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["p-01", "p-02", "p-03", "p-04"]
    );
    Ok(())
}

/// T6 and T7: a rejected COMMIT is not progress, and the resume from the last
/// observed key loads the rejected key again under a new run.
#[tokio::test]
async fn a_rejected_commit_is_not_progress_and_the_resume_reloads_it() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products(), regional()]).await?;
    let mut recovery = Recovery::new(vec![
        product_source("products"),
        regional_source("regional"),
    ])?;
    // An empty run creates the tables, so the fault can be planted on them.
    fixture
        .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
        .await?;
    let outbox = OutboxTable::for_message::<ProductSnapshot>(&fixture.resolved())?.qualified_name();
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION reject_p03() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.entity_key = 'p-03' THEN RAISE EXCEPTION 'commit rejected'; END IF;
           RETURN NEW;
         END $$;
         CREATE CONSTRAINT TRIGGER reject_p03 AFTER INSERT ON {outbox}
           DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_p03();"
    ))
    .execute(&fixture.pool)
    .await?;
    fixture
        .seed(
            "product_state",
            &[("p-01", "a"), ("p-02", "b"), ("p-03", "c"), ("p-04", "d")],
        )
        .await?;
    fixture.seed("regional_state", &[("r-01", "a")]).await?;

    let (source, report, cause) = failed(
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await
            .unwrap_err(),
    );
    assert_eq!(source, "products");
    assert!(cause.to_string().contains("commit rejected"), "{cause}");
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(2, 0, Some("p-02"))
    );
    assert_eq!(report.not_run, ["regional"]);
    assert!(fixture.outbox::<RegionalProduct>().await?.is_empty());
    let rows = fixture.outbox::<ProductSnapshot>().await?;
    assert_eq!(
        rows.iter()
            .map(|(key, _, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["p-01", "p-02"],
        "the rejected key's row rolled back"
    );

    sqlx::query(&format!("DROP TRIGGER reject_p03 ON {outbox}"))
        .execute(&fixture.pool)
        .await?;
    let events = Events::default();
    let resumed = {
        let _capture = events.capture();
        fixture
            .run(
                &mut recovery,
                Some(&brokers),
                resync_inputs(Some("products"), Some("p-02")),
            )
            .await?
    };
    assert_ne!(resumed.run_id, report.run_id);
    assert_eq!(
        events.named("recovery started")[0]["fields"]["run_id"],
        resumed.run_id.to_string()
    );
    assert_eq!(
        resumed.completed[0].progress,
        resync_progress(2, 0, Some("p-04"))
    );
    let p03: String = sqlx::query_scalar(&format!(
        "SELECT idempotency_source::text FROM {outbox} WHERE entity_key = 'p-03'"
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert!(p03.contains(&resumed.run_id.to_string()), "{p03}");
    Ok(())
}

/// T8: a failing query fails the run once, with no retry inside it; the
/// operator's rerun then succeeds.
#[tokio::test]
async fn a_database_fault_is_returned_once_and_not_retried() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture
        .seed("product_state", &[("p-01", "a"), ("p-02", "b")])
        .await?;
    fixture.fault("p-01", "error").await?;
    let loads = Arc::new(AtomicUsize::new(0));
    let counted = loads.clone();
    let mut recovery = Recovery::new(vec![Source::resync::<ProductSnapshot, _, _>(
        "products",
        |conn, after, limit| keys_from("product_state", conn, after, limit),
        move |conn, key, run_id| {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(current("product_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        ProductSnapshot::envelope(&id, &name)
                            .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )])?;

    let (_, report, cause) = failed(
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await
            .unwrap_err(),
    );
    assert!(matches!(cause, Failure::Resync(_)), "{cause}");
    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(report.failed.unwrap().progress, resync_progress(0, 0, None));
    assert!(fixture.outbox::<ProductSnapshot>().await?.is_empty());

    fixture.clear_faults().await?;
    let report = fixture
        .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
        .await?;
    assert_eq!(
        report.completed[0].progress,
        resync_progress(2, 0, Some("p-02"))
    );
    assert_eq!(loads.load(Ordering::SeqCst), 3);
    Ok(())
}

/// T11: a live write for the entity being loaded waits on its lock, so the
/// live snapshot is the one the receiver's cache keeps.
#[tokio::test]
async fn a_live_write_waits_for_the_key_being_loaded() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture
        .seed("product_state", &[("p-01", "old"), ("p-02", "b")])
        .await?;
    let loaded = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (loaded_tx, release_rx) = (loaded.clone(), release.clone());
    let mut recovery = Recovery::new(vec![Source::resync::<ProductSnapshot, _, _>(
        "products",
        |conn, after, limit| keys_from("product_state", conn, after, limit),
        move |conn, key, run_id| {
            let (loaded, release) = (loaded_tx.clone(), release_rx.clone());
            Box::pin(async move {
                let state = current("product_state", conn, key).await?;
                if key == "p-01" {
                    loaded.notify_one();
                    // Bounded, so a write that never waits fails the test
                    // instead of hanging it.
                    let _ = tokio::time::timeout(Duration::from_secs(10), release.notified()).await;
                }
                Ok(state.map(|(id, name)| {
                    ProductSnapshot::envelope(&id, &name)
                        .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                }))
            })
        },
    )])?;
    // Tables first, so the live write below has somewhere to go. Nothing is
    // after the cursor, and the empty run keeps it.
    let mut setup = Recovery::new(vec![product_source("products")])?;
    let empty = resync_inputs(Some("products"), Some("p-99"));
    let setup = fixture.run(&mut setup, Some(&brokers), empty).await?;
    assert_eq!(
        setup.completed[0].progress,
        resync_progress(0, 0, Some("p-99"))
    );

    let request = RecoveryInputs::default();
    let pool = fixture.pool.clone();
    let cfg = fixture.resolved();
    let run = fixture.run(&mut recovery, Some(&brokers), request);
    let live = async {
        loaded.notified().await;
        let mut tx = pool.begin().await?;
        sqlx::query("UPDATE product_state SET name = 'live' WHERE id = 'p-01'")
            .execute(&mut *tx)
            .await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await?;
        let write = async {
            let live = ProductSnapshot::envelope("p-01", "live").with_idempotency_key("live-p-01");
            enqueue(&mut tx, &cfg, &live).await?;
            tx.commit().await?;
            Ok::<_, durable_send_tests::BoxError>(())
        };
        let watch = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let waiting: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT FROM pg_locks WHERE locktype = 'advisory'
                         AND NOT granted AND pid = $1)",
                    )
                    .bind(pid)
                    .fetch_one(&pool)
                    .await?;
                    if waiting {
                        return Ok::<_, sqlx::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            release.notify_one();
            Ok::<_, durable_send_tests::BoxError>(())
        };
        tokio::try_join!(write, watch)?;
        Ok::<_, durable_send_tests::BoxError>(())
    };
    let (report, live) = tokio::join!(run, live);
    live?;
    let report = report?;
    assert_eq!(
        report.completed[0].progress,
        resync_progress(2, 0, Some("p-02"))
    );
    // The lock orders one entity's rows; p-02 may land before or after the
    // live row.
    let rows = fixture.outbox::<ProductSnapshot>().await?;
    let names = |wanted: &str| {
        rows.iter()
            .filter(|(key, _, _)| key == wanted)
            .map(|(_, name, _)| name.as_str())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names("p-01"),
        ["old", "live"],
        "the live snapshot follows the one loaded under the lock"
    );
    assert_eq!(names("p-02"), ["b"]);
    assert_eq!(rows.len(), 3);

    // Through the broker to a receiver's cache. The live write superseded
    // the pending recovery row for p-01, so two records go out.
    relay::<ProductSnapshot>(&fixture, &brokers, 2).await?;
    fixture.migrate_handler(products()).await?;
    let topic = cfg.descriptor_for::<ProductSnapshot>()?.topic;
    let consumer = kafkaman_rdkafka::RdkafkaConsumer::from_brokers(
        &brokers,
        &format!("recovery-{}", Uuid::new_v4()),
    )?;
    consumer.subscribe(&[topic.as_str()])?;
    let mut inserted = 0;
    while inserted < 2 {
        let stats = tokio::time::timeout(
            Duration::from_secs(30),
            consumer.ingest_once::<ProductSnapshot>(&fixture.pool, &cfg),
        )
        .await??;
        inserted += stats.inserted;
    }
    let received = kafkaman_sqlx::ReceivedTable::for_message::<ProductSnapshot>(&cfg)?;
    let router = kafkaman_sqlx::MessageRouter::new()
        .handler::<ProductSnapshot>(|_, _, _| Box::pin(async { Ok(()) }));
    let mut processed = 0;
    loop {
        let stats = kafkaman_sqlx::dispatch_once(
            &fixture.pool,
            &received,
            &router,
            time::OffsetDateTime::now_utc(),
        )
        .await?;
        if stats.processed == 0 {
            break;
        }
        processed += stats.processed;
    }
    assert_eq!(processed, 2);
    let cache = kafkaman_sqlx::CacheTable::for_message::<ProductSnapshot>(&cfg)?;
    let cached: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT entity_key, payload->>'name' FROM {} ORDER BY entity_key",
        cache.qualified_name()
    ))
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        cached,
        [
            ("p-01".to_owned(), "live".to_owned()),
            ("p-02".to_owned(), "b".to_owned())
        ]
    );
    let p02: String = sqlx::query_scalar(&format!(
        "SELECT idempotency_source::text FROM {} WHERE entity_key = 'p-02'",
        received.qualified_name()
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert!(p02.contains(&report.run_id.to_string()), "{p02}");
    Ok(())
}

/// One call to a source's `keys`: when it started, the limit it was given,
/// and how many keys it returned.
type KeysCall = (Instant, usize, usize);

/// A products source that records every keys call and every load, and lists
/// at most `caps[n]` keys on its `n`th call (the limit past the end).
fn recorded_source(
    caps: Vec<usize>,
    calls: Arc<Mutex<Vec<KeysCall>>>,
    loads: Arc<Mutex<Vec<Instant>>>,
) -> Source {
    Source::resync::<ProductSnapshot, _, _>(
        "products",
        move |conn, after, limit| {
            let calls = calls.clone();
            let cap = caps
                .get(calls.lock().unwrap().len())
                .copied()
                .unwrap_or(limit);
            let started = Instant::now();
            Box::pin(async move {
                let keys = keys_from("product_state", conn, after, cap).await?;
                calls.lock().unwrap().push((started, limit, keys.len()));
                Ok(keys)
            })
        },
        move |conn, key, run_id| {
            loads.lock().unwrap().push(Instant::now());
            Box::pin(async move {
                Ok(current("product_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        ProductSnapshot::envelope(&id, &name)
                            .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )
}

const FIVE: [(&str, &str); 5] = [
    ("p-01", "a"),
    ("p-02", "b"),
    ("p-03", "c"),
    ("p-04", "d"),
    ("p-05", "e"),
];

fn progress_events(events: &Events) -> Vec<serde_json::Value> {
    events
        .named("recovery progress")
        .iter()
        .map(|event| event["fields"]["enqueued"].clone())
        .collect()
}

/// The file's page size and pause, not the Job's old variables, pace a run,
/// and progress is logged once per completed page.
#[tokio::test]
async fn the_file_paces_the_run_and_each_page_logs_progress() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture.seed("product_state", &FIVE).await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let loads = Arc::new(Mutex::new(Vec::new()));
    let mut recovery = Recovery::new(vec![recorded_source(
        Vec::new(),
        calls.clone(),
        loads.clone(),
    )])?;
    let config = fixture.config_with("[recovery]\nbatch_size = 2\npause_ms = 300\n");

    let events = Events::default();
    let report = {
        let _legacy = LegacyPacing::set("7", "50");
        let _capture = events.capture();
        fixture
            .run_with(
                &config,
                &mut recovery,
                Some(&brokers),
                RecoveryInputs::default(),
            )
            .await?
    };
    assert_eq!(
        report.completed[0].progress,
        resync_progress(5, 0, Some("p-05"))
    );
    let calls = calls.lock().unwrap().clone();
    let loads = loads.lock().unwrap().clone();
    assert_eq!(
        calls.iter().map(|(_, limit, _)| *limit).collect::<Vec<_>>(),
        [2, 2, 2, 2]
    );
    assert_eq!(
        calls.iter().map(|(_, _, len)| *len).collect::<Vec<_>>(),
        [2, 2, 1, 0]
    );
    // After each page, the pause, then the next keys call.
    let mut loaded = 0;
    for (page, next) in calls.iter().zip(&calls[1..]) {
        loaded += page.2;
        let gap = next.0.duration_since(loads[loaded - 1]);
        assert!(gap >= Duration::from_millis(300), "{gap:?}");
    }
    assert_eq!(progress_events(&events), [2, 4, 5]);
    Ok(())
}

/// A short page in the middle ends a page too.
#[tokio::test]
async fn a_short_page_in_the_middle_logs_its_own_progress() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture.seed("product_state", &FIVE).await?;
    let mut recovery = Recovery::new(vec![recorded_source(
        vec![2, 1, 2],
        Arc::default(),
        Arc::default(),
    )])?;
    let events = Events::default();
    {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await?;
    }
    assert_eq!(progress_events(&events), [2, 3, 5]);
    Ok(())
}

/// A failure inside a page reports the key committed before it, which no
/// page-end progress line carried.
#[tokio::test]
async fn a_failure_inside_a_page_reports_the_last_committed_key() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture.seed("product_state", &FIVE).await?;
    fixture.fault("p-02", "mismatch").await?;
    let mut recovery = Recovery::new(vec![product_source("products")])?;
    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `p-01`"),
        "{error}"
    );
    let (_, report, _) = failed(error);
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(1, 0, Some("p-01"))
    );
    assert_eq!(
        events.named("recovery failed")[0]["fields"]["last_key"],
        "p-01"
    );
    assert!(progress_events(&events).is_empty());
    Ok(())
}

/// T5: a resume whose first keys query fails keeps its cursor, and once the
/// query works it starts strictly after it.
#[tokio::test]
async fn a_resume_whose_keys_query_fails_keeps_its_cursor() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture.seed("product_state", &FIVE).await?;
    let broken = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let asked = Arc::new(Mutex::new(Vec::new()));
    let loaded = Arc::new(Mutex::new(Vec::new()));
    let (fail, seen, loads) = (broken.clone(), asked.clone(), loaded.clone());
    let mut recovery = Recovery::new(vec![Source::resync::<ProductSnapshot, _, _>(
        "products",
        move |conn, after, limit| {
            seen.lock().unwrap().push(after.map(str::to_owned));
            let fail = fail.load(Ordering::SeqCst);
            Box::pin(async move {
                if fail {
                    sqlx::query("SELECT 1 / 0").execute(&mut *conn).await?;
                }
                keys_from("product_state", conn, after, limit).await
            })
        },
        move |conn, key, run_id| {
            loads.lock().unwrap().push(key.to_owned());
            Box::pin(async move {
                Ok(current("product_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        ProductSnapshot::envelope(&id, &name)
                            .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )])?;
    let resume = || resync_inputs(Some("products"), Some("p-02"));

    let events = Events::default();
    let error = {
        let _capture = events.capture();
        fixture
            .run(&mut recovery, Some(&brokers), resume())
            .await
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("after last_key `p-02`"),
        "{error}"
    );
    let (_, report, cause) = failed(error);
    assert!(matches!(cause, Failure::Resync(_)), "{cause}");
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("p-02"))
    );
    assert_eq!(
        events.named("recovery failed")[0]["fields"]["last_key"],
        "p-02"
    );
    assert_eq!(*asked.lock().unwrap(), [Some("p-02".to_owned())]);
    assert!(loaded.lock().unwrap().is_empty());
    assert!(fixture.outbox::<ProductSnapshot>().await?.is_empty());

    broken.store(false, Ordering::SeqCst);
    let report = fixture.run(&mut recovery, Some(&brokers), resume()).await?;
    assert_eq!(
        report.completed[0].progress,
        resync_progress(3, 0, Some("p-05"))
    );
    assert_eq!(loaded.lock().unwrap()[0], "p-03");
    Ok(())
}

/// T9: once the topics are verified the run only writes outboxes, so losing
/// the broker mid-run still queues every row, and the relay sends them once
/// it is back.
#[tokio::test]
async fn losing_the_broker_after_preparation_still_queues_every_row() -> TestResult {
    let fixture = Fixture::start().await?;
    let (broker, brokers) = broker_with(&[products()]).await?;
    let broker = Arc::new(broker);
    fixture
        .seed("product_state", &[("p-01", "a"), ("p-02", "b")])
        .await?;
    let stop = Arc::new(tokio::sync::OnceCell::new());
    let (container, stopped) = (broker.clone(), stop.clone());
    let mut recovery = Recovery::new(vec![Source::resync::<ProductSnapshot, _, _>(
        "products",
        |conn, after, limit| keys_from("product_state", conn, after, limit),
        move |conn, key, run_id| {
            let (container, stopped) = (container.clone(), stopped.clone());
            Box::pin(async move {
                stopped
                    .get_or_init(|| async {
                        container.stop_with_timeout(Some(5)).await.unwrap();
                    })
                    .await;
                Ok(current("product_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        ProductSnapshot::envelope(&id, &name)
                            .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )])?;

    let report = fixture
        .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
        .await?;
    assert!(stop.initialized());
    assert_eq!(
        report.completed[0].progress,
        resync_progress(2, 0, Some("p-02"))
    );
    assert_eq!(fixture.outbox::<ProductSnapshot>().await?.len(), 2);

    restart(&broker, &brokers, &[products()]).await?;
    relay::<ProductSnapshot>(&fixture, &brokers, 2).await?;
    Ok(())
}

/// Overrides the service file sets for every declared type, including the
/// ones a run does not select.
const OVERRIDES: &str = "[recovery]
batch_size = 2
[retry.messages.regional_product]
max_attempts = 3
[observability.messages.regional_product]
lifecycle = \"per-message\"
[retry.messages.keyless_product]
max_attempts = 3
";

/// T12: the file is checked against every declared source, while only the
/// selected ones are verified, prepared and run.
#[tokio::test]
async fn a_run_checks_the_file_against_every_source_and_prepares_its_selection() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture
        .seed("product_state", &[("p-01", "a"), ("p-02", "b")])
        .await?;
    fixture.seed("regional_state", &[("r-01", "a")]).await?;
    let config = fixture.config_with(&format!("{RETRY_DEFAULTS}{OVERRIDES}"));
    let mut recovery = mixed();

    // Regional's topic is missing, so verifying it would fail this run.
    let named = fixture
        .run_with(
            &config,
            &mut recovery,
            Some(&brokers),
            resync_inputs(Some("products"), None),
        )
        .await?;
    assert_eq!(
        named.completed[0].progress,
        resync_progress(2, 0, Some("p-02"))
    );
    let resumed = fixture
        .run_with(
            &config,
            &mut recovery,
            Some(&brokers),
            resync_inputs(Some("products"), Some("p-01")),
        )
        .await?;
    assert_eq!(
        resumed.completed[0].progress,
        resync_progress(1, 0, Some("p-02"))
    );
    assert!(!fixture.outbox_exists::<RegionalProduct>().await?);
    let keyless_received =
        kafkaman_sqlx::ReceivedTable::for_message::<KeylessProduct>(&fixture.resolved())?;
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(keyless_received.qualified_name())
        .fetch_one(&fixture.pool)
        .await?;
    assert!(!exists);
    assert!(!fixture.outbox_exists::<KeylessProduct>().await?);

    // A named redrive of the received source, under the same file.
    fixture.migrate_handler(keyless()).await?;
    let redrive = RecoveryInputs {
        operation: Some("redrive".to_owned()),
        source: Some("keyless".to_owned()),
        max_rows: Some("5".to_owned()),
        ..RecoveryInputs::default()
    };
    let redriven = fixture
        .run_with(&config, &mut recovery, None, redrive)
        .await?;
    assert_eq!(
        redriven.completed[0].progress,
        SourceProgress::Redrive { moved: 0 }
    );

    // The default resync runs the resync sources in order and leaves the
    // received one out entirely.
    let admin = TopicAdmin::from_brokers(&brokers)?;
    for topic in prefixed(&[regional()]) {
        admin
            .create(&topic.topic, 1, None, &topic.topic_spec)
            .await?;
    }
    let all = fixture
        .run_with(
            &config,
            &mut recovery,
            Some(&brokers),
            RecoveryInputs::default(),
        )
        .await?;
    assert_eq!(
        all.completed
            .iter()
            .map(|outcome| outcome.source.as_str())
            .collect::<Vec<_>>(),
        ["products", "regional"]
    );
    assert!(all.failed.is_none() && all.not_run.is_empty());
    assert!(!fixture.outbox_exists::<KeylessProduct>().await?);
    Ok(())
}
