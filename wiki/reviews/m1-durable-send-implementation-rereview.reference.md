# M1 Durable Send Implementation Re-Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-21
- Category: Code review
- Scope: Fresh review of the current M1 durable-send working tree after attempted fixes for the first implementation review. This review challenges the current code, tests, examples, and wiki status against the M1 implementation plan and active M1 spec.
- Sources:
  - `wiki/plans/m1-durable-send-implementation.plan.md`
  - `wiki/specs/m1-durable-send.spec.md`
  - `wiki/reviews/m1-durable-send-implementation-review.reference.md`
  - `crates/kafkaman-core/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-worker/src/lib.rs`
  - `crates/kafkaman-test/src/lib.rs`
  - `crates/kafkaman-rdkafka/src/lib.rs`
  - `crates/kafkaman/src/lib.rs`
  - `examples/axum-outbox/src/main.rs`
  - `tests/durable-send/tests/durable_send.rs`
- Related:
  - `wiki/reviews/m1-durable-send-implementation-review.reference.md`
  - `wiki/plans/m1-durable-send-implementation.plan.md`
  - `wiki/specs/m1-durable-send.spec.md`

## Verdict

The fixes materially improved the implementation, but M1 should still not be
closed as fully clean.

The core Postgres-backed durable-send path passes tests, and several high-value
issues from the first review are now fixed or mostly fixed:

- status SQL generation now comes from `OutboxStatus`;
- claim and retry scheduling now use the database clock;
- `run()` no longer exits on the first relay cycle error;
- `idempotency_key` is persisted and forwarded on fresh schemas;
- `migrate()` now serializes concurrent migration runs with a Postgres advisory
  lock.

However, the current state still has one release-blocking issue and several
scope and correctness gaps:

1. **Blocker:** the new `idempotency_key` column has no upgrade migration for
   already-created M1 outbox tables. Fresh schemas pass; existing schemas break.
2. **High:** the Redpanda/full-loop Harness path promised by the implementation
   plan is still absent.
3. **High:** the Harness registration race is still present. The new advisory
   lock prevents duplicate changelog-history insert failure, but it does not
   prevent concurrent callers from getting a table handle before migration
   completes.
4. **Medium:** strict clippy fails on the new migration helper signature.
5. **Medium:** kafkaman metadata headers can be duplicated/spoofed by user
   envelope headers because reserved header names are not rejected.

The implementation is much closer, but the "all issues fixed" claim does not
hold.

## Verification

Commands run against the current working tree:

- `cargo fmt --check` passed.
- `cargo check --workspace --all-features` passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` failed.
  Clippy reports `clippy::borrowed-box` at
  `crates/kafkaman-sqlx/src/lib.rs:249` for `sorted: &[&Box<dyn Changeset>]`.
- `cargo test --workspace` failed inside the sandbox because testcontainers
  could not create Docker containers. Re-running with Docker access passed:
  `11 passed (16 suites, 4.69s)`.
- `cargo test --workspace --all-features` with Docker access passed:
  `11 passed (16 suites, 3.08s)`.

The passing tests prove the fresh Postgres schema plus capturing-publisher path.
They do not prove broker behavior, schema upgrade compatibility from pre-fix M1
tables, or concurrent Harness registration safety.

## Fixed Or Mostly Fixed

### Status String Centralization

Mostly fixed.

`OutboxStatus` now exposes the enum set and SQL literal helpers:

- `OutboxStatus::ALL`: `crates/kafkaman-core/src/lib.rs:197-206`
- `OutboxStatus::sql_literal()`: `crates/kafkaman-core/src/lib.rs:217-222`
- `OutboxStatus::sql_literal_list()`: `crates/kafkaman-core/src/lib.rs:224-232`

The SQLx storage layer now builds status SQL from these helpers rather than
hard-coded string literals:

- DDL default and CHECK list use `pending` and `status_list`:
  `crates/kafkaman-sqlx/src/lib.rs:350-374`
- enqueue uses `OutboxStatus::Pending.sql_literal()`:
  `crates/kafkaman-sqlx/src/lib.rs:398-405`
- claim, mark-published, and mark-failed use status literals from `OutboxStatus`:
  `crates/kafkaman-sqlx/src/lib.rs:430-532`

This satisfies the spirit of the implementation plan's centralized-status
requirement. It still relies on `as_str()` returning safe ASCII status names,
but that is acceptable because statuses are fixed enum variants.

### Database Clock For Lease And Retry Scheduling

Fixed for the prior app-clock/DB-clock bug.

The claim update now writes the lease expiry from the database clock:

- lease duration converted to seconds: `crates/kafkaman-sqlx/src/lib.rs:449`
- `claim_expires_at = now() + make_interval(secs => $4)`:
  `crates/kafkaman-sqlx/src/lib.rs:450-457`

Publish-failure retry scheduling now also uses the database clock:

- `mark_publish_failed` accepts `retry_after: Duration`:
  `crates/kafkaman-sqlx/src/lib.rs:505-512`
- `next_attempt_at = now() + make_interval(secs => $4)`:
  `crates/kafkaman-sqlx/src/lib.rs:515-523`
- the worker passes `cfg.retry_after`, not `OffsetDateTime::now_utc() +
  retry_after`: `crates/kafkaman-worker/src/lib.rs:57-66`

Residual concern: durations are passed as `f64` seconds using
`Duration::as_secs_f64()`. The integration tests exercise normal durations, but
there is no config validation for zero or very small lease/retry durations. A
zero lease would make a claim immediately eligible for another worker.

### Worker Loop Resilience

Fixed for the original availability bug.

`run()` now catches `relay_once` errors, logs them with `tracing::error!`, and
continues after `poll_interval`:
`crates/kafkaman-worker/src/lib.rs:85-108`.

This fixes the "first transient DB error kills the worker forever" problem.

Residual concern: repeated relay failures are only logged. There is still no
metric, failure counter, callback, or supervisor signal. That is acceptable for
M1, but production users will need an observability contract.

### Migration Race Between Processes

Mostly fixed for the migration engine.

`migrate()` now acquires a session-scoped advisory lock before bootstrapping
history and applying changesets:

- connection acquisition and lock key: `crates/kafkaman-sqlx/src/lib.rs:223-230`
- `SELECT pg_advisory_lock($1)`: `crates/kafkaman-sqlx/src/lib.rs:231-234`
- migrations run on the same connection: `crates/kafkaman-sqlx/src/lib.rs:236`
- unlock attempted afterward: `crates/kafkaman-sqlx/src/lib.rs:238-243`

This prevents two application processes from applying the same schema's
changesets concurrently and racing the `changelog_history` primary key.

Residual concern: unlock errors are ignored. The session lock should be released
when the connection closes, but an unlock failure deserves at least debug/error
logging if the connection is returned to the pool.

## Blocker

### 1. `idempotency_key` Breaks Existing M1 Tables

The idempotency fix is incomplete and migration-unsafe.

Fresh outbox tables now include an `idempotency_key` column:

- table DDL: `crates/kafkaman-sqlx/src/lib.rs:350-352`

The runtime now assumes that column exists:

- enqueue insert column list includes `idempotency_key`:
  `crates/kafkaman-sqlx/src/lib.rs:399-401`
- enqueue binds `evt.idempotency_key`: `crates/kafkaman-sqlx/src/lib.rs:407-410`
- row mapping reads `idempotency_key`: `crates/kafkaman-sqlx/src/lib.rs:607`
- Rdkafka publisher forwards it as `kafkaman-idempotency-key` when present:
  `crates/kafkaman-rdkafka/src/lib.rs:66-70`

But there is no `ALTER TABLE ... ADD COLUMN idempotency_key` path and no new
changeset for existing outbox tables. Searching the SQLx crate shows no
`ALTER TABLE` or `ADD COLUMN` handling for this change.

Because `CreateOutboxTable` uses `CREATE TABLE IF NOT EXISTS`, rerunning
`migrate()` against a schema created before this fix will not add the column.
The next enqueue into that table will fail with a database error like
`column "idempotency_key" does not exist`.

This is a blocker if any M1 database already exists. It also violates the
library-pack expectation that public schema changes record migration and
compatibility impact.

Required fix:

- add a dedicated changeset that runs `ALTER TABLE <outbox_table> ADD COLUMN IF
  NOT EXISTS idempotency_key TEXT` for every configured message table;
- add an integration test that creates an old-shape outbox table, runs
  `migrate()`, then enqueues and reads a row with an idempotency key;
- add a compatibility note under `wiki/compatibility/` because persisted schema
  behavior changed.

## High Findings

### 2. Redpanda / Full-Loop Path Is Still Missing

Still open.

The implementation plan says Redpanda helpers and testcontainers live behind an
explicit full-loop feature, and it shows a Harness API with `connect_redpanda`:
`wiki/plans/m1-durable-send-implementation.plan.md`.

Current code still does not implement that path:

- `crates/kafkaman-test/Cargo.toml:10` only wires the `redpanda` feature to
  `dep:kafkaman-rdkafka`;
- `crates/kafkaman-test/src/lib.rs:95` still declares `HarnessPublisher`, but it
  has no Redpanda variant in practice;
- there is no `connect_redpanda` symbol;
- `tests/durable-send/tests/durable_send.rs:272-274` starts Postgres only;
- `cargo test --workspace --all-features` passes, but it does not start or
  assert against Redpanda.

The active spec documents this as deferred, saying broker-level assertions are
future full-loop work. The implementation plan remains `Status: Completed`,
which is still overstated unless the plan outcome explicitly records the
deferral.

Required fix:

- either implement the Redpanda Harness/test path, or update the plan closure to
  say that the full-loop gate was intentionally deferred and is not part of the
  completed M1 proof.

### 3. Harness Registration Can Still Return Before Migration Completes

Still open, even after the advisory-lock fix.

`Harness::ensure_message` updates the shared config under a mutex, releases the
mutex, and only then runs migration:

- config check and mutation: `crates/kafkaman-test/src/lib.rs:201-214`
- config clone and migration after releasing the lock:
  `crates/kafkaman-test/src/lib.rs:216-224`

The advisory lock prevents two migrations from corrupting changelog history, but
it does not prevent this sequence:

1. task A registers message type P, releases the config mutex, and starts
   `migrate()`;
2. task B calls `ensure_message::<P>()`, sees P already registered, sets
   `needs_migration = false`, and returns an `OutboxTable`;
3. task B calls enqueue/relay before task A's migration has created the table.

That can still fail with a missing-table error. This is test-only today, but the
Harness exists specifically to model behavior above the storage boundary.

Required fix:

- serialize registration and migration together, or track per-message migration
  state so later callers wait until the table exists;
- add a concurrent `ensure_message::<P>` test using one Harness and multiple
  tasks.

## Medium Findings

### 4. Strict Clippy Fails

Machine-verified.

`cargo clippy --workspace --all-targets --all-features -- -D warnings` fails:

```text
error: you seem to be trying to use `&Box<T>`. Consider using just `&T`
   --> crates/kafkaman-sqlx/src/lib.rs:249:15
```

The issue comes from sorting changesets as `Vec<&Box<dyn Changeset>>` and
passing borrowed boxes into `run_migrations`:

- `let mut sorted: Vec<&Box<dyn Changeset>> = changesets.iter().collect();`:
  `crates/kafkaman-sqlx/src/lib.rs:220`
- `run_migrations` receives that list: `crates/kafkaman-sqlx/src/lib.rs:246-250`

This is not a runtime correctness bug, but it means the current code does not
meet a strict lint gate. Use `Vec<&dyn Changeset>` or sort indices.

### 5. User Headers Can Collide With Kafkaman Metadata Headers

New finding.

The Rdkafka publisher copies user envelope headers first, then appends kafkaman
metadata headers:

- user headers copied: `crates/kafkaman-rdkafka/src/lib.rs:47-52`
- kafkaman message/correlation headers appended:
  `crates/kafkaman-rdkafka/src/lib.rs:54-64`
- idempotency header appended: `crates/kafkaman-rdkafka/src/lib.rs:66-70`

Kafka permits duplicate header keys. If a caller supplies
`kafkaman-message-id`, `kafkaman-correlation-id`, `kafkaman-causation-id`, or
`kafkaman-idempotency-key` in `Envelope::headers`, the published record can
contain conflicting metadata. Depending on consumer behavior, the user-supplied
value may shadow or confuse the system value.

Required fix:

- reserve the `kafkaman-` header namespace and reject such user headers at
  enqueue time, or
- strip/overwrite reserved keys before publishing and document precedence.

This should be tested at the publisher-boundary level.

### 6. `InitSchema` Duplication Still Exists

Still open.

The migration bootstrap creates schema and changelog history before changeset
execution:

- bootstrap call: `crates/kafkaman-sqlx/src/lib.rs:251`
- schema/history bootstrap implementation:
  `crates/kafkaman-sqlx/src/lib.rs:287-295`

`InitSchema` still generates the same DDL as version 1:

- `InitSchema`: `crates/kafkaman-sqlx/src/lib.rs:156-177`

This is not failing because `IF NOT EXISTS` makes it idempotent, but there are
still two owners for the changelog-history table shape.

Recommended fix: keep bootstrap as the private owner and remove `InitSchema`, or
make `InitSchema` the sole owner and reduce bootstrap to the minimum needed to
query history.

### 7. Facade Crate Still Is Not Proven

Still open.

The facade exports only:

- `kafkaman_core::*`: `crates/kafkaman/src/lib.rs:1`
- `kafkaman_sqlx as sqlx`: `crates/kafkaman/src/lib.rs:2`
- `kafkaman_worker as worker`: `crates/kafkaman/src/lib.rs:3`

The Axum example still imports underlying crates directly:

- `kafkaman_core`: `examples/axum-outbox/src/main.rs:9`
- `kafkaman_rdkafka`: `examples/axum-outbox/src/main.rs:10`
- `kafkaman_sqlx`: `examples/axum-outbox/src/main.rs:11`

The example also depends on `kafkaman` without using it. Either use the facade
in the example or drop the unused dependency until the facade has a real role.

### 8. Duplicate Message Descriptors Are Still Accepted

Still open.

`ResolvedConfig::with_message` still appends descriptors without checking
duplicate `message_type` values:

- `crates/kafkaman-sqlx/src/lib.rs:51-53`

Duplicate message types can generate repeated table changesets with different
versions but the same table/index names. `CREATE TABLE IF NOT EXISTS` can make
that look successful while changelog history records misleading work.

Recommended fix: reject duplicates when constructing `ResolvedConfig`, or expose
a fallible builder method and make callers use it.

## Minor / Deferred Findings

### 9. Claim Still Uses One Update Per Row

Still open and acceptable for M1.

`claim_batch` selects candidates and then loops, issuing one
`UPDATE ... RETURNING *` per row. This keeps distinct claim IDs simple, but it
is still N+1 relative to the batched SQL shape in the plan.

This is not a correctness issue under the held transaction. Revisit when batch
sizes or relay throughput matter.

### 10. `Missing` And `StaleClaim` Still Share One Worker Counter

Still open.

The storage layer distinguishes `MarkOutcome::StaleClaim` and
`MarkOutcome::Missing`, but worker stats still collapse both into `stats.stale`:

- published path: `crates/kafkaman-worker/src/lib.rs:53-55`
- failed path: `crates/kafkaman-worker/src/lib.rs:58-69`
- `RelayStats` still only has `stale`: `crates/kafkaman-core/src/lib.rs:334-338`

This is fine for M1, but operations will eventually need to distinguish a lost
lease from a deleted/missing row.

### 11. State Index Name Truncation Can Still Collide

Still open.

`state_index_name()` truncates to the prefix that fits inside
`SqlIdentifier::MAX_LEN`:

- `crates/kafkaman-sqlx/src/lib.rs:116-119`
- max identifier length: `crates/kafkaman-core/src/lib.rs:28`

Two long message types sharing the same retained prefix can still produce the
same index name. Add collision detection or a short deterministic hash when
truncation occurs.

### 12. `Error::Publish` Still Looks Unused

Still open.

`kafkaman-worker::Error::Publish` remains in the public error enum, but current
relay behavior records publish errors into the outbox row and does not return
them through this variant. Remove it or document when it will be used.

## Documentation / Wiki Gaps

### Compatibility Note Missing For Schema Change

The idempotency persistence change modifies the outbox table shape. The project
rules include a library compatibility pack and `wiki/compatibility/` folder.
This change should have a compatibility note describing:

- new nullable `idempotency_key TEXT` column;
- migration requirement for existing M1 tables;
- public API impact from changing `mark_publish_failed` to use retry duration
  rather than absolute retry timestamp;
- expected behavior for idempotency Kafka headers.

### Plan Status Still Overstates Redpanda Completion

`wiki/plans/m1-durable-send-implementation.plan.md` remains `Status: Completed`
while the Redpanda/full-loop path in that plan remains absent. The active spec
does say Redpanda assertions are deferred, but the plan outcome should say the
same thing explicitly.

## Close Criteria Before Calling The Fix Pass Complete

Required before closing the fix pass:

1. Add an idempotency column migration and upgrade test for old-shape outbox
   tables.
2. Decide and record Redpanda/full-loop status: implement it or explicitly defer
   it in the plan outcome.
3. Fix Harness concurrent registration so callers cannot observe registered
   config before table migration completes.
4. Fix strict clippy.
5. Reject or sanitize reserved `kafkaman-*` envelope headers.

Recommended before broader M1 closure:

1. Remove or consolidate `InitSchema` duplication.
2. Make the Axum example use the facade or remove the unused facade dependency.
3. Reject duplicate message descriptors.
4. Split missing-row and stale-claim worker stats.
5. Add index-name collision handling.
6. Add a compatibility note for schema/API changes.

## Bottom Line

The current implementation is stronger than the first review target, and the
fresh-schema durable-send path is green. But the most dangerous current bug is
exactly the kind that tests often miss: a schema-shape change added through
`CREATE TABLE IF NOT EXISTS` without an upgrade migration. Fix that before
treating the idempotency work as complete.
