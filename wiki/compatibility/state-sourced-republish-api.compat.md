# State-Sourced Republish API

- Document Class: Compatibility Note
- Status: Implemented
- Date: 2026-10-09
- Category: Library API
- Scope: State-sourced snapshot repair through the PostgreSQL outbox
- Sources:
  - crates/kafkaman-sqlx/src/resync.rs
  - crates/kafkaman-sqlx/src/outbox_enqueue.rs
  - crates/kafkaman-sqlx/src/replay.rs
  - tests/durable-send/tests/entity_first_outbox_supersede/resync.rs
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/plans/entity-first-propagation.plan.md

## Public surface and compatibility

`kafkaman-sqlx` adds `resync`, `ResyncOptions`, `ResyncProgress`, and
`ResyncFuture`; the existing facade re-export exposes them at `kafkaman::sqlx`.
`Error::InvalidResync(String)` joins the non-exhaustive error enum and reports
invalid pagination or an envelope whose entity key differs from the locked key.
It is classified as a configuration problem. `Replay::outbox` still always
returns `UnsafeOutboxReplay`, whose text now points at `kafkaman::sqlx::resync`.
`Replay::received` is unchanged.

This is an additive, minor-release API change. Existing enqueue signatures,
database schema, wire payloads, and stored outbox rows do not change. No migration
is required. The SQLx crate now uses Tokio's timer for pacing; its PostgreSQL
runtime already uses Tokio.

```rust,ignore
pub type ResyncFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub async fn resync<P, Keys, Load, Progress>(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    options: ResyncOptions,
    keys: Keys,
    load: Load,
    progress: Progress,
) -> Result<ResyncProgress>
where
    P: KafkaMessage + Serialize,
    Keys: for<'a> FnMut(&'a mut PgConnection, Option<&'a str>, usize)
        -> ResyncFuture<'a, Vec<String>>,
    Load: for<'a> FnMut(&'a mut PgConnection, &'a str)
        -> ResyncFuture<'a, Option<Envelope<P>>>,
    Progress: FnMut(&ResyncProgress);
```

`ResyncOptions` contains `batch_size: NonZeroUsize` (default 100),
`pause: Duration` (default zero), and `start_after: Option<String>` (default none).
`ResyncProgress` contains `enqueued: u64`, `skipped: u64`, and
`last_key: Option<String>`.

## Caller contract and resumption

The key callback implements keyset pagination, returning at most the requested
limit, strictly increasing keys after the exclusive cursor, or an empty page to
finish. Ordering must match Rust string ordering; use PostgreSQL `COLLATE "C"`
for text IDs. The whole page is validated before processing any key. Keep payload
construction in the loader, using the supplied connection and a fresh envelope
with an idempotency identity. Returning `None` skips a disappeared entity; it
does not emit a deletion snapshot. Keys inserted behind the cursor require normal
live propagation or a later sweep.

Each key commits independently, so transactions hold one entity lock at a time.
The progress callback runs after every commit, including missing entities, and
the pause holds no connection or transaction. On failure, only the current key
rolls back; resume with the last callback's cursor. Counts are per invocation.
If the process dies between commit and checkpoint, resuming can repeat a key:
the loader must build a fresh envelope from current state again. Persisting the
cursor is the caller's responsibility. No broker connection is needed by the
resync job; the ordinary relay publishes.

## Concurrency and evidence

Resync takes the same transaction-scoped advisory lock as enqueue before loading
state, explicitly at READ COMMITTED isolation. After waiting for a live writer,
the loader sees the writer's commit. A live writer arriving after the load waits
until repair commits, then supersedes repair through normal enqueue. Live writers
must commit domain state and enqueue atomically. Loaders must use the provided
connection, avoid changing transaction state, and preferably read their snapshot
with a single non-locking query to avoid domain-row/advisory-lock inversion.
PostgreSQL's [isolation documentation](https://www.postgresql.org/docs/current/transaction-iso.html)
explains why a REPEATABLE READ snapshot taken at the waiting lock statement
cannot serve this purpose.

The integration target `entity_first_outbox_supersede` covers every enumerated
key, paced batches, missing-entity counts, cursor resumption after loader failure,
rollback, invalid key ordering, mismatched entity keys, and continued outbox
replay refusal. `resync_racing_live_update_loses_to_live_update` pauses after
loading v5 but before enqueue, verifies the live v6 writer is blocked on the
advisory lock, then verifies only v6 publishes. The complementary test starts
the repair behind an uncommitted writer using a REPEATABLE READ pool default and
verifies that the repair loads and publishes v6.

Phase 5 and automatic topic-lifecycle recovery are outside this API.
