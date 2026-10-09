//! State-sourced repair of snapshot topics through the ordinary outbox.

use std::{future::Future, num::NonZeroUsize, pin::Pin, time::Duration};

use kafkaman_core::{Envelope, KafkaMessage};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};

use crate::catch_panic::catch_application_panic;
use crate::outbox_enqueue::lock_outbox_entity;
use crate::{enqueue, Error, OutboxTable, ResolvedConfig, Result};

/// A callback future that can borrow the supplied connection and cursor/key.
pub type ResyncFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Controls key pagination and the pause between batches of a [`resync()`].
#[derive(Clone, Debug)]
pub struct ResyncOptions {
    /// Maximum number of keys requested per batch (default: 100).
    pub batch_size: NonZeroUsize,
    /// Time to wait after a batch, with no transaction or connection held.
    pub pause: Duration,
    /// Exclusive cursor from a previous run's committed progress.
    pub start_after: Option<String>,
}

impl Default for ResyncOptions {
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(100).unwrap_or(NonZeroUsize::MIN),
            pause: Duration::ZERO,
            start_after: None,
        }
    }
}

/// Committed progress for this run, including missing keys in the cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResyncProgress {
    pub enqueued: u64,
    pub skipped: u64,
    /// Last committed key, or the input cursor if no keys have completed.
    pub last_key: Option<String>,
}

/// Refill a snapshot topic by loading current entity state into the outbox.
///
/// `keys` returns at most `batch_size` entity keys in strictly increasing Rust
/// string order, all greater than its exclusive cursor; an empty page ends the
/// run. Use the same stable order in the source query (for text in PostgreSQL,
/// `COLLATE "C"`). Enumeration must return keys only, never cached payloads.
/// Keys inserted behind the cursor need a later run or normal live propagation.
///
/// For each key, a new READ COMMITTED transaction takes the same advisory lock
/// as [`enqueue`] **before** calling `load`. `load` must query current state on
/// that connection and return a fresh, idempotency-keyed envelope, or `None` if
/// the entity no longer exists. Its envelope's entity key must match the locked
/// key. It must not change isolation, commit/roll back the transaction, read
/// through another connection, or use a payload read before this callback.
/// Prefer a single non-locking SELECT to construct a consistent snapshot;
/// taking domain row locks here can deadlock with a live writer that updates
/// those rows before enqueueing. Live writes must commit their state change and
/// normal enqueue atomically in the same database transaction.
///
/// A live writer holding the entity lock finishes before `load` reads; READ
/// COMMITTED gives that read a fresh snapshot after the wait. A live writer
/// arriving during `load` waits until this repair commits and enqueues later.
/// INSERT-time outbox ordering makes that live state win even if its transaction
/// began earlier or its snapshot cannot see the repair row. The relay collapses
/// any remaining pending predecessors once an in-flight publish settles.
/// No stored outbox row is replayed and this function never contacts
/// the broker; the relay publishes the resulting rows.
///
/// `progress` runs after **each key's commit**, including skipped keys. Persist
/// its `last_key` to resume after errors or cancellation; counts cover only this
/// invocation. An error rolls back the current key and stops the run, preserving
/// earlier commits. A crash between commit and checkpoint can repeat a key on
/// resume, safely loading its current state again. Use fresh envelope identities
/// on each attempt. A missing key does not synthesize a deletion snapshot.
///
/// ```no_run
/// # async fn example(pool: &sqlx::PgPool, cfg: &kafkaman_sqlx::ResolvedConfig)
/// # -> kafkaman_sqlx::Result<()> {
/// use kafkaman_sqlx::{resync, ResyncOptions};
/// # use kafkaman_core::{Envelope, KafkaMessage};
/// # use serde::Serialize;
/// # #[derive(Serialize)]
/// # struct Snapshot;
/// # impl KafkaMessage for Snapshot {
/// # const MESSAGE_TYPE: &'static str = "snapshot";
/// # const TOPIC: &'static str = "snapshots";
/// # fn entity_key(&self) -> String { "entity".into() }
/// # }
/// # async fn load_snapshot(_: &mut sqlx::PgConnection, _: &str)
/// # -> kafkaman_sqlx::Result<Option<Envelope<Snapshot>>> { Ok(None) }
/// let done = resync(
///     pool, cfg, ResyncOptions::default(),
///     |conn, after, limit| Box::pin(async move {
///         Ok(sqlx::query_scalar(
///             "SELECT id FROM entities WHERE ($1::text IS NULL OR id COLLATE \"C\" > $1) \
///              ORDER BY id COLLATE \"C\" LIMIT $2"
///         ).bind(after).bind(limit as i64).fetch_all(conn).await?)
///     }),
///     |conn, key| Box::pin(load_snapshot(conn, key)),
///     |done| println!("committed cursor: {:?}", done.last_key),
/// ).await?;
/// println!("enqueued {}, skipped {}", done.enqueued, done.skipped);
/// # Ok(()) }
/// ```
pub async fn resync<P, Keys, Load, Progress>(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    options: ResyncOptions,
    mut keys: Keys,
    mut load: Load,
    mut progress: Progress,
) -> Result<ResyncProgress>
where
    P: KafkaMessage + Serialize,
    Keys: for<'a> FnMut(
        &'a mut PgConnection,
        Option<&'a str>,
        usize,
    ) -> ResyncFuture<'a, Vec<String>>,
    Load: for<'a> FnMut(&'a mut PgConnection, &'a str) -> ResyncFuture<'a, Option<Envelope<P>>>,
    Progress: FnMut(&ResyncProgress),
{
    let table = OutboxTable::for_message::<P>(cfg)?;
    let mut done = ResyncProgress {
        last_key: options.start_after,
        ..ResyncProgress::default()
    };
    loop {
        let batch = {
            let mut conn = pool.acquire().await?;
            keys(
                &mut conn,
                done.last_key.as_deref(),
                options.batch_size.get(),
            )
            .await?
        };
        if batch.is_empty() {
            return Ok(done);
        }
        if batch.len() > options.batch_size.get()
            || batch.windows(2).any(|pair| pair[0] >= pair[1])
            || done.last_key.as_ref().is_some_and(|last| batch[0] <= *last)
        {
            return Err(Error::InvalidResync(
                "keys must be strictly increasing, after the cursor, and within batch_size".into(),
            ));
        }
        for key in batch {
            let mut tx = pool.begin().await?;
            // Set explicitly: a pool/session default of REPEATABLE READ would
            // freeze the snapshot at the lock SELECT, before a possible wait.
            sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                .execute(&mut *tx)
                .await?;
            lock_outbox_entity(&mut tx, &table, &key).await?;
            let envelope = load(&mut tx, &key).await?;
            let enqueued = if let Some(envelope) = envelope {
                let actual =
                    catch_application_panic(P::MESSAGE_TYPE, "resolve entity key", || {
                        envelope.payload.entity_key()
                    })?;
                if actual != key {
                    return Err(Error::InvalidResync(format!(
                        "loader returned entity key `{actual}` while holding the lock for `{key}`"
                    )));
                }
                enqueue(&mut tx, cfg, &envelope).await?;
                true
            } else {
                false
            };
            tx.commit().await?;
            if enqueued {
                done.enqueued += 1;
            } else {
                done.skipped += 1;
            }
            done.last_key = Some(key);
            progress(&done);
        }
        if !options.pause.is_zero() {
            tokio::time::sleep(options.pause).await;
        }
    }
}
