//! Rebuilding outboxes from current state.
use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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

    let first = fixture
        .run(&mut recovery, Some(&brokers), RecoveryInputs::default())
        .await?;
    assert_eq!(first.failed, None);
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
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture.seed("product_state", &[("p-01", "a")]).await?;
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

    // An unreachable broker is a preparation failure too.
    let unreachable = fixture.config();
    let request =
        recovery.request_from(&unreachable, resync_inputs(Some("products"), Some("p-00")))?;
    let (_, report, cause) = failed(
        recovery
            .run(
                &fixture.pool,
                &unreachable,
                Some("127.0.0.1:1"),
                &MigrationContext::default(),
                request,
            )
            .await
            .unwrap_err(),
    );
    assert!(
        matches!(cause, Failure::Topics(_) | Failure::Transport(_)),
        "{cause}"
    );
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("p-00"))
    );
    assert!(!fixture.outbox_exists::<ProductSnapshot>().await?);
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
    fixture.seed("regional_state", &[("r-01", "a")]).await?;
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

    let (_, report, cause) = failed(
        fixture
            .run(
                &mut recovery,
                Some(&brokers),
                resync_inputs(Some("regional"), Some("r-00")),
            )
            .await
            .unwrap_err(),
    );
    assert!(matches!(cause, Failure::Migrate(_)), "{cause}");
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("r-00"))
    );
    assert_eq!(listed.load(Ordering::SeqCst), 0);
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
        !fixture.outbox_exists::<KeylessProduct>().await?
            || fixture.outbox::<KeylessProduct>().await?.is_empty()
    );

    // A resume that fails before its first commit keeps the cursor.
    fixture.clear_faults().await?;
    fixture.fault("p-03", "error").await?;
    let (_, report, cause) = failed(
        fixture
            .run(
                &mut recovery,
                Some(&brokers),
                resync_inputs(Some("products"), Some("p-02")),
            )
            .await
            .unwrap_err(),
    );
    assert!(matches!(cause, Failure::Resync(_)), "{cause}");
    assert_eq!(
        report.failed.unwrap().progress,
        resync_progress(0, 0, Some("p-02"))
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
/// live snapshot is the last one in the outbox.
#[tokio::test]
async fn a_live_write_waits_for_the_key_being_loaded() -> TestResult {
    let fixture = Fixture::start().await?;
    let (_broker, brokers) = broker_with(&[products()]).await?;
    fixture.seed("product_state", &[("p-01", "old")]).await?;
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
                loaded.notify_one();
                release.notified().await;
                Ok(state.map(|(id, name)| {
                    ProductSnapshot::envelope(&id, &name)
                        .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                }))
            })
        },
    )])?;
    // Tables first, so the live write below has somewhere to go.
    let mut setup = Recovery::new(vec![product_source("products")])?;
    let empty = resync_inputs(Some("products"), Some("p-99"));
    fixture.run(&mut setup, Some(&brokers), empty).await?;

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
    assert_eq!(
        report?.completed[0].progress,
        resync_progress(1, 0, Some("p-01"))
    );
    let rows = fixture.outbox::<ProductSnapshot>().await?;
    assert_eq!(
        rows.iter()
            .map(|(key, name, _)| (key.as_str(), name.as_str()))
            .collect::<Vec<_>>(),
        [("p-01", "old"), ("p-01", "live")],
        "the live snapshot follows the one loaded under the lock"
    );
    Ok(())
}
