# State-Sourced Republish API

- Document Class: Compatibility Note
- Status: Implemented
- Date: 2026-10-09
- Category: Library API
- Scope: State-sourced snapshot repair through the PostgreSQL outbox
- Sources:
  - crates/kafkaman-sqlx/src/resync.rs
  - crates/kafkaman-sqlx/src/outbox_enqueue.rs
  - crates/kafkaman-sqlx/src/outbox_claim.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
  - crates/kafkaman-sqlx/src/generated_changelog.rs
  - tests/durable-send/tests/entity_first_outbox_supersede/enqueue_order.rs
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

This is an additive, minor-release API and schema change. Enqueue and resync
signatures, public row structs, and wire payloads remain compatible. Outboxes
now require the slot-1 `AddOutboxEnqueueOrder` migration before the new relay
runs. The SQLx crate uses Tokio's timer for pacing; its PostgreSQL runtime
already uses Tokio.

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

## Outbox schema and deployment

Each outbox gains `enqueue_order BIGINT NOT NULL`, with a table-owned BIGINT
sequence default (`CACHE 1`). INSERT allocates the value while enqueue holds the
entity advisory lock, so a later lock holder receives a higher value regardless
of transaction start time or isolation level. `created_at` remains the audit
and retention timestamp. No UUID tie-breaker is needed for sequence values.

The shipped slot-0 create statements and their checksums remain unchanged.
Generated changelogs append `add_outbox_enqueue_order` at outbox template slot 1
and use the existing migration runner's transactional, checksum-checked history.
Both fresh and existing tables run this upgrade. Hand-written changelogs must
append `AddOutboxEnqueueOrder::new(next_version, descriptor)` for each outbox;
the manual product example demonstrates this. Re-running the upgrade is safe,
including adopting generated history after manual migration.

Migration takes an exclusive table lock, backfills existing rows in
`(created_at, message_id)` order, sets the sequence above the backfill, and
rebuilds the entity/state index with `enqueue_order`. Existing rows keep their
IDs, payloads and statuses. Their original enqueue order was never recorded
and cannot be reconstructed; backfill preserves the previous deterministic
ordering rather than promising to repair historical inversions. Drain legacy
pending work before upgrading when that distinction matters, then use resync
to republish authoritative state. Migration locks and rewrites existing rows,
so schedule it for the table's size. Stop old relays before resuming with the
new build: old binaries still order by timestamps. The schema change is additive,
but the corrected guarantee requires migrated tables and the new relay.

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
until repair commits, then enqueues after it. Enqueue supersedes visible pending
predecessors while holding the entity lock. A fixed snapshot may miss a repair
row, and an in-flight repair may later return to pending; relay collapse and
claim selection therefore use enqueue order as well. The claim statement also
supersedes its candidates' older pending siblings in the same snapshot, covering
a fixed-snapshot enqueue committed between preliminary collapse and claim.
A `Publishing` row still
blocks any newer publication for that entity until it settles. Live writers
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

The three `enqueue_order_survives_*` resync schedules start a live v6 transaction
before repair reads committed v5: REPEATABLE READ, SERIALIZABLE, and READ
COMMITTED with repair claimed and transiently failed. All three failed on
`2ad8a88` by publishing only v5 and pass after the ordering fix. The additional
enqueue-order module covers migration from slot 0 with existing rows, sequence
continuation, idempotent migration, claim limits and returned order, blocked
entities outside the collapse window, newer-row backoff, and two live writers
without resync.

Pagination tests separately reject duplicates, oversized pages, and keys equal
to or before the cursor. An initially empty page preserves its input cursor
without loading or reporting progress. Pacing is measured from the last commit
callback in a batch to the next page request, excluding database work from the
interval under test.

## Fix-round regression evidence (2026-10-09)

All names below belong to `entity_first_outbox_supersede`. “Old code” means
`2ad8a88`; the first three schedules ran before production edits. For the next
five, the old claim implementation and generated migration registration were
temporarily restored. Guard mutations changed only the named condition and
were restored before verification. The tests remain in the normal full suite.

| Test | Before / mutation result | Fixed result |
| --- | --- | --- |
| `enqueue_order_survives_earlier_repeatable_read_writer` | Old code fails: publishes v5 | Pass: publishes v6 |
| `enqueue_order_survives_earlier_serializable_writer` | Old code fails: publishes v5 | Pass: publishes v6 |
| `enqueue_order_survives_read_committed_repair_retry` | Old code fails: publishes v5 | Pass: publishes v6 |
| `migration_backfills_legacy_order_and_advances_new_inserts` | Old registration fails: zero upgrades | Pass: legacy order, continued sequence, repeat migration and claims |
| `claim_limit_and_return_order_follow_enqueue_order` | Old claim fails limited selection; reverting only returned sort also fails | Pass: limited and returned INSERT order |
| `enqueue_order_survives_live_writer_retry_without_resync` | Old claim fails to supersede the delayed predecessor | Pass: only live v6 survives |
| `pending_sibling_outside_collapse_window_cannot_be_claimed` | Old claim leaves the stale predecessor pending | Pass: blocked entity does not hide the target pair |
| `newer_pending_backoff_blocks_older_due_sibling` | Old claim publishes the predecessor | Pass: no premature publication |
| `enqueue_between_collapse_and_claim_cannot_leave_a_stale_retry` | Without atomic claim-time supersede, a missed predecessor publishes after v6 | Pass: the same-snapshot cleanup supersedes it |
| `resync_rejects_duplicate_keys` | Changing `>=` to `>` fails the test | Pass |
| `resync_rejects_oversized_page` | Removing the page-size check fails the test | Pass |
| `resync_rejects_key_at_cursor` | Removing the cursor guard fails the test | Pass |
| `resync_rejects_key_before_cursor` | Removing the cursor guard fails the test | Pass |
| `resync_initially_empty_page_preserves_input_cursor` | Dropping the input cursor fails the test | Pass |
| `resync_republishes_every_key_in_paced_batches_and_counts_missing_entities` | Removing the sleep fails the batch-boundary assertion | Pass |

The in-flight guard and enqueue-time supersede predicate are unchanged; the
new retry schedules also assert that the live row cannot publish while its
predecessor is `Publishing`. No resync signature change was needed.

Final verification passed: `just lint`, `just features`, `just msrv`, and
`just test all`, with `RUST_TEST_THREADS=1 CARGO_BUILD_JOBS=2`. The expanded
outbox/resync target passes all 25 tests. Only one container-backed target ran
at a time during the final gates.

Phase 5 and automatic topic-lifecycle recovery are outside this API.
