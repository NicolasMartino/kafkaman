//! State-sourced republish, including both sides of the read/enqueue race.
use super::*;

use std::{num::NonZeroUsize, sync::Arc};

use kafkaman_sqlx::{resync, Error, Replay, ResyncFuture, ResyncOptions, ResyncProgress};
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool};
use tokio::{sync::Notify, time::Instant};
use uuid::Uuid;

async fn seed(pool: &PgPool, count: usize) -> TestResult {
    sqlx::query("CREATE TABLE entity_state (id text PRIMARY KEY, name text NOT NULL)")
        .execute(pool)
        .await?;
    for i in 1..=count {
        sqlx::query("INSERT INTO entity_state VALUES ($1, 'v5')")
            .bind(format!("p-{i:02}"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

fn keys<'a>(
    conn: &'a mut PgConnection,
    after: Option<&'a str>,
    limit: usize,
) -> ResyncFuture<'a, Vec<String>> {
    Box::pin(async move {
        Ok(sqlx::query_scalar(
            "SELECT id FROM entity_state WHERE ($1::text IS NULL OR id COLLATE \"C\" > $1)
             ORDER BY id COLLATE \"C\" LIMIT $2",
        )
        .bind(after)
        .bind(i64::try_from(limit).unwrap())
        .fetch_all(conn)
        .await?)
    })
}

fn load<'a>(
    conn: &'a mut PgConnection,
    key: &'a str,
) -> ResyncFuture<'a, Option<Envelope<ProductSnapshot>>> {
    Box::pin(async move {
        let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *conn)
            .await?;
        assert_eq!(isolation, "read committed");
        let name: Option<String> =
            sqlx::query_scalar("SELECT name FROM entity_state WHERE id = $1")
                .bind(key)
                .fetch_optional(conn)
                .await?;
        Ok(name.map(|name| product(key, &name).with_idempotency_key(Uuid::new_v4().to_string())))
    })
}

async fn wait_for_advisory_waiter(pool: &PgPool, pid: Option<i32>) -> TestResult {
    timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE locktype = 'advisory'
                 AND NOT granted AND ($1::int IS NULL OR pid = $1))",
            )
            .bind(pid)
            .fetch_one(pool)
            .await?;
            if waiting {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn resync_republishes_every_key_in_paced_batches_and_counts_missing_entities() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), 5).await?;
    let mut checkpoints = Vec::new();
    let mut pages = Vec::new();
    let started = Instant::now();
    let done = resync(
        harness.pool(),
        &harness.config(),
        ResyncOptions {
            batch_size: NonZeroUsize::new(2).unwrap(),
            pause: Duration::from_millis(30),
            start_after: None,
        },
        |conn, after, limit| {
            pages.push(after.map(str::to_owned));
            Box::pin(async move {
                let batch = keys(conn, after, limit).await?;
                // Deletion after enumeration is a normal resync race.
                if batch.iter().any(|key| key == "p-03") {
                    sqlx::query("DELETE FROM entity_state WHERE id = 'p-03'")
                        .execute(conn)
                        .await?;
                }
                Ok(batch)
            })
        },
        load,
        |done| checkpoints.push(done.clone()),
    )
    .await?;
    assert_eq!(done.enqueued, 4);
    assert_eq!(done.skipped, 1);
    assert_eq!(done.last_key.as_deref(), Some("p-05"));
    assert_eq!(checkpoints.len(), 5);
    assert_eq!(checkpoints[2].last_key.as_deref(), Some("p-03"));
    assert_eq!(checkpoints[2].skipped, 1);
    assert_eq!(
        pages,
        [
            None,
            Some("p-02".into()),
            Some("p-04".into()),
            Some("p-05".into())
        ]
    );
    assert!(started.elapsed() >= Duration::from_millis(60));

    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 4);
    let mut published: Vec<_> = harness
        .published_on(ProductSnapshot::TOPIC)
        .iter()
        .map(|record| record.payload["product_id"].as_str().unwrap().to_owned())
        .collect();
    published.sort();
    assert_eq!(published, ["p-01", "p-02", "p-04", "p-05"]);
    Ok(())
}

#[tokio::test]
async fn resync_resumes_after_last_committed_key_on_loader_failure() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), 5).await?;
    let cfg = harness.config();
    let mut checkpoint = ResyncProgress::default();
    let failed = resync(
        harness.pool(),
        &cfg,
        ResyncOptions::default(),
        keys,
        |conn, key| {
            Box::pin(async move {
                if key == "p-03" {
                    // The failed key's side effects must roll back too.
                    sqlx::query("UPDATE entity_state SET name = 'uncommitted' WHERE id = $1")
                        .bind(key)
                        .execute(conn)
                        .await?;
                    return Err(Error::Handler("loader failed".into()));
                }
                load(conn, key).await
            })
        },
        |done| checkpoint = done.clone(),
    )
    .await;
    assert!(matches!(failed, Err(Error::Handler(_))));
    assert_eq!(checkpoint.enqueued, 2);
    assert_eq!(checkpoint.last_key.as_deref(), Some("p-02"));
    let done = resync(
        harness.pool(),
        &cfg,
        ResyncOptions {
            start_after: checkpoint.last_key,
            ..ResyncOptions::default()
        },
        keys,
        load,
        |_| {},
    )
    .await?;
    assert_eq!(done.enqueued, 3);
    assert_eq!(done.last_key.as_deref(), Some("p-05"));
    let count: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {}", table.qualified_name()))
            .fetch_one(harness.pool())
            .await?;
    assert_eq!(
        count, 5,
        "completed keys must not be loaded or enqueued again"
    );
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 5);
    assert!(harness
        .published_on(ProductSnapshot::TOPIC)
        .iter()
        .all(|row| row.payload["name"] == "v5"));
    Ok(())
}

#[tokio::test]
async fn resync_racing_live_update_loses_to_live_update() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), 1).await?;
    let read = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let pool = harness.pool().clone();
    let cfg = harness.config();
    let repair = tokio::spawn({
        let read = read.clone();
        let resume = resume.clone();
        async move {
            resync(
                &pool,
                &cfg,
                ResyncOptions::default(),
                keys,
                move |conn, key| {
                    let read = read.clone();
                    let resume = resume.clone();
                    Box::pin(async move {
                        let state = load(conn, key).await?;
                        read.notify_one();
                        // Pause AFTER the v5 read but BEFORE enqueue. The broken
                        // read-then-enqueue design lets the live v6 commit here.
                        timeout(Duration::from_secs(10), resume.notified())
                            .await
                            .unwrap();
                        Ok(state)
                    })
                },
                |_| {},
            )
            .await
        }
    });
    timeout(Duration::from_secs(5), read.notified()).await?;
    let mut live_tx = harness.pool().begin().await?;
    sqlx::query("UPDATE entity_state SET name = 'v6' WHERE id = 'p-01'")
        .execute(&mut *live_tx)
        .await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *live_tx)
        .await?;
    let live = product("p-01", "v6").with_idempotency_key("live-v6");
    let cfg = harness.config();
    let writer = tokio::spawn(async move {
        enqueue(&mut live_tx, &cfg, &live).await?;
        live_tx.commit().await?;
        Ok::<_, Error>(())
    });
    // Database evidence, not a scheduling sleep: the live enqueue has reached
    // the entity lock and cannot commit in the repair's read/enqueue window.
    wait_for_advisory_waiter(harness.pool(), Some(pid)).await?;
    resume.notify_one();
    assert_eq!(timeout(Duration::from_secs(5), repair).await???.enqueued, 1);
    timeout(Duration::from_secs(5), writer).await???;
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 1);
    assert_eq!(
        harness.published_on(ProductSnapshot::TOPIC)[0].payload["name"],
        "v6"
    );
    Ok(())
}

#[tokio::test]
async fn resync_reads_fresh_state_after_wait_even_with_repeatable_read_pool_default() -> TestResult
{
    let (postgres, harness) = start_harness().await?;
    harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), 1).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                )
                .execute(conn)
                .await?;
                Ok(())
            })
        })
        .connect(postgres.url())
        .await?;
    let cfg = harness.config();
    let mut live_tx = harness.pool().begin().await?;
    sqlx::query("UPDATE entity_state SET name = 'v6' WHERE id = 'p-01'")
        .execute(&mut *live_tx)
        .await?;
    enqueue(
        &mut live_tx,
        &cfg,
        &product("p-01", "v6").with_idempotency_key("live-v6"),
    )
    .await?;
    let repair = tokio::spawn(async move {
        resync(&pool, &cfg, ResyncOptions::default(), keys, load, |_| {}).await
    });
    // The repair's lock SELECT started while v6 was still uncommitted.
    wait_for_advisory_waiter(harness.pool(), None).await?;
    live_tx.commit().await?;
    assert_eq!(timeout(Duration::from_secs(5), repair).await???.enqueued, 1);
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 1);
    assert_eq!(
        harness.published_on(ProductSnapshot::TOPIC)[0].payload["name"],
        "v6"
    );
    Ok(())
}

#[tokio::test]
async fn resync_rejects_unordered_keys_and_a_loader_for_a_different_entity() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), 2).await?;
    let cfg = harness.config();
    let unordered = resync(
        harness.pool(),
        &cfg,
        ResyncOptions::default(),
        |_, _, _| Box::pin(async { Ok(vec!["p-02".into(), "p-01".into()]) }),
        load,
        |_| panic!("no progress before validating the whole batch"),
    )
    .await;
    assert!(matches!(unordered, Err(Error::InvalidResync(_))));
    let mismatch = resync(
        harness.pool(),
        &cfg,
        ResyncOptions::default(),
        keys,
        |conn, _| load(conn, "p-02"),
        |_| panic!("mismatch must not commit"),
    )
    .await;
    assert!(matches!(mismatch, Err(Error::InvalidResync(_))));
    let count: i64 =
        sqlx::query_scalar(&format!("SELECT count(*) FROM {}", table.qualified_name()))
            .fetch_one(harness.pool())
            .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[test]
fn outbox_replay_still_refuses_and_names_the_resync_api() {
    let error = Replay::outbox::<ProductSnapshot>().unwrap_err();
    assert!(matches!(error, Error::UnsafeOutboxReplay { .. }));
    assert!(error.to_string().contains("kafkaman::sqlx::resync"));
}
