# M1 Durable Send Implementation Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-20
- Category: Code review
- Scope: Verification of the post-implementation review for `wiki/plans/m1-durable-send-implementation.plan.md`, including line-by-line checks against the current Rust implementation and M1 wiki status.
- Sources:
  - `wiki/plans/m1-durable-send-implementation.plan.md`
  - `wiki/specs/m1-durable-send.spec.md`
  - `crates/kafkaman-core/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-worker/src/lib.rs`
  - `crates/kafkaman-test/src/lib.rs`
  - `crates/kafkaman-rdkafka/src/lib.rs`
  - `crates/kafkaman/src/lib.rs`
  - `examples/axum-outbox/src/main.rs`
  - `examples/axum-outbox/Cargo.toml`
- Related:
  - `wiki/plans/m1-durable-send-implementation.plan.md`
  - `wiki/specs/m1-durable-send.spec.md`

## Verdict

The submitted review is substantially correct. The durable-send core is implemented
and tested for the Postgres plus capturing-publisher path, but the plan status is
too broad if interpreted as every planned M1 item being complete.

The most important confirmed issues are:

1. outbox status strings are not centralized despite the explicit plan requirement;
2. lease and retry timing mix application-clock writes with database-clock eligibility;
3. the Redpanda/full-loop Harness path promised by the implementation plan does not exist;
4. `idempotency_key` is an envelope field only and is not durable or forwarded by the relay;
5. `kafkaman-worker::run` exits permanently on the first relay error;
6. `migrate()` is not concurrency-safe and will race the `changelog_history`
   primary key when multiple application instances boot at once (see Additional
   Issue A). This is a production-impacting bug, not just a test-harness edge, and
   shares a root cause with the Harness migration race (Submitted Review Claim 7).

The strongest next step is to fix the low-risk correctness items now: centralized
status SQL generation, database-clock lease/retry writes, resilient worker loop
error handling, and an explicit `idempotency_key` decision. Redpanda should either
be implemented as the plan promised or clearly deferred by updating the plan status
and closure notes to match the active M1 spec.

## Verification Performed

- `cargo test --workspace` initially failed in the sandbox because testcontainers
  could not create Docker containers.
- Re-running with Docker access passed: `10 passed (16 suites, 5.53s)`.
- `cargo check -p kafkaman-test --features redpanda` passed, which proves the
  feature compiles, but it does not prove any Redpanda behavior because there is
  no Redpanda Harness implementation or broker-backed test path.
- Live container lifecycle was observed by polling `docker ps` during a single
  test run: a `postgres:16-alpine` container appeared for ~2 seconds and was gone
  the instant the test process exited. This confirms the containers are ephemeral
  and `Drop`-cleaned (see Additional Issues E and F), which is why they are not
  visible in Docker Desktop after a run.

## Submitted Review Claims

### 1. Status Strings Are Not Centralized

Confirmed.

The plan explicitly says the Rust enum and SQL strings must stay centralized:
`wiki/plans/m1-durable-send-implementation.plan.md:230-231`.

The implementation has `OutboxStatus::as_str()` in
`crates/kafkaman-core/src/lib.rs:192-199`, but SQL text does not use it. Status
strings are repeated in the SQLx layer:

- DDL default: `crates/kafkaman-sqlx/src/lib.rs:312`
- DDL check constraint: `crates/kafkaman-sqlx/src/lib.rs:328`
- enqueue insert value: `crates/kafkaman-sqlx/src/lib.rs:360`
- claim eligibility: `crates/kafkaman-sqlx/src/lib.rs:388-389`
- claim update: `crates/kafkaman-sqlx/src/lib.rs:408`
- publish mark: `crates/kafkaman-sqlx/src/lib.rs:442,447`
- publish-failed mark: `crates/kafkaman-sqlx/src/lib.rs:463,469`

The review understated one related surface: parsing also has a separate string
match in `crates/kafkaman-core/src/lib.rs:214-217`. That parser is expected, but
it should be deliberately tied to the same status constants or covered by a test
that proves SQL literals, display strings, and parsing stay aligned.

Recommended fix: expose status constants or SQL literal helper functions from
core, build the CHECK constraint and query fragments from those values, and add
a unit test that fails if the enum/string/SQL status set diverges.

### 2. App Clock And DB Clock Are Mixed

Confirmed.

The plan's claim SQL uses database time for the lease write:
`claim_expires_at = now() + $lease_for` in
`wiki/plans/m1-durable-send-implementation.plan.md:296-302`.

The implementation uses database `now()` for eligibility:

- pending eligibility: `crates/kafkaman-sqlx/src/lib.rs:388`
- expired publishing eligibility: `crates/kafkaman-sqlx/src/lib.rs:389`

But the claim write binds a Rust-computed timestamp:

- `let lease_until = OffsetDateTime::now_utc() + lease_for`:
  `crates/kafkaman-sqlx/src/lib.rs:401`
- `claim_expires_at = $4`: `crates/kafkaman-sqlx/src/lib.rs:412`

The retry path has the same shape:

- `let retry_at = OffsetDateTime::now_utc() + cfg.retry_after`:
  `crates/kafkaman-worker/src/lib.rs:59`
- `next_attempt_at = $4`: `crates/kafkaman-sqlx/src/lib.rs:465`

That means application-host clock skew against Postgres changes both lease
duration and retry eligibility. For a durable-send primitive, lease ownership
should be based on one clock. The plan already chose the database clock for
leases.

Recommended fix: change claim update SQL to compute `claim_expires_at` from
Postgres time. For retry, prefer changing `mark_publish_failed` to accept a retry
duration and compute `next_attempt_at` in SQL from database time. If the M1 API
must keep `retry_at`, document that retry scheduling is app-clock based and
defer the API cleanup.

### 3. Redpanda / Full-Loop Harness Path Is Missing

Confirmed, with one documentation nuance.

The implementation plan requires an opt-in Redpanda/full-loop Harness path. It
shows a Harness API with `connect_redpanda` and a `HarnessPublisher` Redpanda
variant in `wiki/plans/m1-durable-send-implementation.plan.md:382-396`.

Current code does not implement that path:

- `HarnessPublisher` has only `Capturing`:
  `crates/kafkaman-test/src/lib.rs:95-97`
- `Harness` stores a `CapturingPublisher` directly:
  `crates/kafkaman-test/src/lib.rs:101-102`
- there is no `connect_redpanda` symbol in the source tree;
- the `redpanda` feature only enables `dep:kafkaman-rdkafka`:
  `crates/kafkaman-test/Cargo.toml:8-10`.

`RdkafkaPublisher` does exist and compiles. The Axum example uses it, and
`cargo check -p kafkaman-test --features redpanda` passes. The missing piece is
behavioral proof through a real broker.

Nuance: the active M1 spec already says the automated full-loop gate uses a
capturing publisher and Redpanda broker assertions are deferred:
`wiki/specs/m1-durable-send.spec.md:80-82`. That makes this a plan/spec status
conflict as much as a code gap. The plan should not remain simply "Completed"
unless the Redpanda item is implemented or explicitly marked deferred in the
plan outcome.

### 4. `idempotency_key` Is Dropped

Confirmed.

The envelope carries `idempotency_key`:
`crates/kafkaman-core/src/lib.rs:149-157`.

The outbox DDL has no `idempotency_key` column:
`crates/kafkaman-sqlx/src/lib.rs:310-328`.

The enqueue insert column list also omits it:
`crates/kafkaman-sqlx/src/lib.rs:357-360`, with binds in
`crates/kafkaman-sqlx/src/lib.rs:364-372`.

This means the field is not carried durably through the M1 outbox. It is only
present on the in-memory envelope API. If M3 dedup requires durable
idempotency, it will need a migration later.

The "carried for envelope compatibility" framing in the plan undersells the
hazard. `idempotency_key` is on the public `Envelope` API, so callers will
reasonably set it and assume it is persisted and delivered. Silently discarding
a value the user explicitly provided is worse than not offering the field at
all: it looks supported but is a no-op. The correct M1 stance is one of two
honest options, not silent drop.

Additional note: the Rdkafka publisher cannot forward `idempotency_key` either,
because the claimed row does not contain it. The publisher maps row headers plus
message/correlation/causation IDs in `crates/kafkaman-rdkafka/src/lib.rs:47-71`;
there is no idempotency header.

Recommended fix: either add a nullable `idempotency_key TEXT` column now and
map it through enqueue/outbox rows/publisher headers, or explicitly document in
the M1 spec and plan outcome that idempotency is API-shape-only and non-durable
until a future migration.

### 5. `InitSchema` Duplicates Bootstrap DDL

Confirmed.

`migrate()` always calls `bootstrap_history()` before processing changesets:
`crates/kafkaman-sqlx/src/lib.rs:214-220`.

`bootstrap_history()` creates the schema and history table directly:

- schema: `crates/kafkaman-sqlx/src/lib.rs:248-249`
- history table: `crates/kafkaman-sqlx/src/lib.rs:251-255`

`InitSchema` then generates the same DDL as version 1:

- schema: `crates/kafkaman-sqlx/src/lib.rs:169-172`
- history table: `crates/kafkaman-sqlx/src/lib.rs:173-177`

Because the SQL uses `IF NOT EXISTS`, this is not an immediate behavior bug.
It is still two owners for the same schema/history-table definition.

Recommended fix: keep bootstrap as the single owner and remove `InitSchema`, or
make `InitSchema` the single public owner while keeping bootstrap as a private
minimal prereq with no duplicate table shape. If version 1 history is needed,
record it with an explicit no-op changeset or documented bootstrap marker.

### 6. `run()` Dies Permanently On First Relay Error

Confirmed.

`run()` propagates any `relay_once` error out of the loop:
`crates/kafkaman-worker/src/lib.rs:80-97`, especially line 88.

This includes transient claim/mark database errors. Publish errors themselves
are handled inside `relay_once` by marking the row failed/retryable, but if the
marking database call fails, `relay_once` returns an error and `run()` exits.

The review's "silently" wording is true for common spawned usage, but with a
nuance: the function does return the error if the caller awaits the join handle.
The Axum example spawns the worker and awaits it only after server shutdown:
`examples/axum-outbox/src/main.rs:76-97`. If the worker exits early, the server
can continue accepting requests while outbox delivery has stopped until shutdown
reveals the error.

The worker crate declares `tracing` as a dependency but has no instrumentation:
`crates/kafkaman-worker/Cargo.toml:17`.

Recommended fix: make `run()` log relay errors and continue after
`poll_interval`, unless shutdown has been requested. Consider a stats/error hook
or supervisor-friendly return mode later.

### 7. Harness `ensure_message` Has A Migration Race

Confirmed.

`ensure_message` mutates the in-memory config under a mutex and records whether
migration is needed:
`crates/kafkaman-test/src/lib.rs:201-214`.

It then reacquires the mutex only to clone the config and runs migration after
the lock has been released:
`crates/kafkaman-test/src/lib.rs:216-223`.

A concurrent `ensure_message::<P>` on the same `Harness` can observe the type as
registered and return a config/table before the corresponding migration has
finished. Concurrent migrations can also race on changelog history inserts.

Current tests do not appear to exercise concurrent registration on one Harness,
so this is latent. Since the Harness is meant to model concurrency-sensitive
durable-send behavior, it should either serialize registration plus migration
or clearly document that dynamic message registration is single-threaded.

### 8. Claim Is N+1

Confirmed.

The claim path first selects candidates with `FOR UPDATE SKIP LOCKED`, then loops
over candidates and issues one `UPDATE ... RETURNING *` per row:

- loop: `crates/kafkaman-sqlx/src/lib.rs:403`
- per-row update SQL: `crates/kafkaman-sqlx/src/lib.rs:406-414`

This is correct while the transaction holds row locks, but it is not the batched
`UPDATE ... WHERE message_id IN (...)` shape sketched in the plan:
`wiki/plans/m1-durable-send-implementation.plan.md:296-302`.

This is acceptable for M1. If batch sizes grow, replace it with a batched update
using a generated `(message_id, claim_id)` values table so each row still gets a
distinct claim ID.

### 9. Facade Crate Is Not Proved By The Example

Confirmed.

The facade currently re-exports only:

- `kafkaman_core::*`: `crates/kafkaman/src/lib.rs:1`
- `kafkaman_sqlx as sqlx`: `crates/kafkaman/src/lib.rs:2`
- `kafkaman_worker as worker`: `crates/kafkaman/src/lib.rs:3`

The Axum example depends on `kafkaman` but imports the implementation crates
directly:

- dependency: `examples/axum-outbox/Cargo.toml:10`
- direct imports: `examples/axum-outbox/src/main.rs:9-11`
- direct worker call: `examples/axum-outbox/src/main.rs:77`

The facade may be intentionally minimal for M1, but it is not currently
demonstrated as usable by the example. Either make the example consume the
facade or remove the unused dependency until the facade has a clearer public API.

### 10. `enqueue` Builds The Descriptor Twice

Confirmed.

`enqueue` first resolves `OutboxTable::for_message::<P>(cfg)`, which validates
and stores the descriptor:
`crates/kafkaman-sqlx/src/lib.rs:350`.

It then calls `P::descriptor()?` again:
`crates/kafkaman-sqlx/src/lib.rs:354`.

This is minor duplication. Use `table.descriptor.topic.clone()` or otherwise
reuse the descriptor returned by the table resolution.

### 11. `RelayStats` Blurs Missing And Stale

Confirmed, scoped to worker stats.

The storage API distinguishes the outcomes:

- existing row with wrong claim: `MarkOutcome::StaleClaim`
- missing row: `MarkOutcome::Missing`
- implementation: `crates/kafkaman-sqlx/src/lib.rs:521-525`

`RelayStats` has only `stale` for both cases:
`crates/kafkaman-core/src/lib.rs:301-305`.

The worker increments `stats.stale` for both `StaleClaim` and `Missing`:

- publish mark: `crates/kafkaman-worker/src/lib.rs:54-57`
- failed mark: `crates/kafkaman-worker/src/lib.rs:60-72`

This is a reasonable M1 simplification, but operationally a missing row and a
lost lease are different. Split the counters when relay observability becomes
part of the public surface.

### 12. State Index Name Truncation Can Collide

Confirmed.

`state_index_name()` keeps only the table-name prefix that fits between `idx_`
and `_state`:
`crates/kafkaman-sqlx/src/lib.rs:117-120`.

With `SqlIdentifier::MAX_LEN = 63` in `crates/kafkaman-core/src/lib.rs:28`, the
middle portion is 53 characters. Two distinct long outbox table names sharing
the same first 53 characters produce the same index name.

This is unlikely, but silent. Since this is generated DDL, either detect and
error on collision during migration planning or append a short deterministic
hash when truncation occurs.

## Additional Issues Found

### A. `migrate()` Is Not Concurrency-Safe (Top-Tier)

This is a production-impacting bug, not an "additional" nicety, and it is the
root cause that Submitted Review Claim 7 (the Harness race) is a symptom of. Both
should be fixed by one mechanism (a Postgres advisory lock around the migration
run), after which the Harness race resolves for free.

The Axum example runs `migrate()` on every boot
(`examples/axum-outbox/src/main.rs:67`). Deploying that with more than one
replica means simultaneous startup migrations, and the `changelog_history`
primary key race below will crash-loop a pod on boot.

`migrate()`
checks whether a changeset exists inside a transaction:
`crates/kafkaman-sqlx/src/lib.rs:230-232`.

It then executes statements and inserts a history row:
`crates/kafkaman-sqlx/src/lib.rs:236-241`.

The history insert is a plain `INSERT`:
`crates/kafkaman-sqlx/src/lib.rs:291-296`.

Two processes running the same migration concurrently can both observe a missing
version and then race to insert the same primary key. One will fail. This is not
only a test Harness edge; it can affect concurrent application startup if
multiple instances run migrations.

Recommended fix: use a Postgres advisory lock for the migration run, or make the
history insert conflict-tolerant and re-check after conflict. Advisory locking
is simpler and preserves exactly-once changeset execution semantics.

### B. Worker Failure Visibility In The Example Is Weak

The submitted review correctly identifies `run()` exiting on errors. The Axum
example amplifies that issue because it spawns the worker, serves HTTP, and only
awaits the worker after server shutdown:
`examples/axum-outbox/src/main.rs:76-97`.

If the relay exits immediately after startup, the example can continue accepting
orders and enqueueing outbox rows while publishing has stopped. This is
acceptable for a small example only if clearly documented. A better example would
supervise the worker task or use a `tokio::select!` that terminates the process
when either the server or worker fails.

### C. Duplicate Message Descriptors Are Not Rejected

`ResolvedConfig::with_message` simply appends a descriptor:
`crates/kafkaman-sqlx/src/lib.rs:52-53`.

There is no config-level check for duplicate message types. A duplicated message
type can generate repeated `CreateOutboxTable` changesets with different
versions but the same table/index names. The DDL uses `IF NOT EXISTS`, so this
may not fail, but it can leave misleading changelog history.

This is less urgent than the top concerns, but the migration/config boundary
should eventually reject duplicate message descriptors.

### D. Worker `Error::Publish` Is Unused

`kafkaman-worker::Error` contains a `Publish(BoxError)` variant:
`crates/kafkaman-worker/src/lib.rs:21-22`.

Current relay behavior does not return publish errors through this variant; it
records them in the outbox row via `mark_publish_failed`. This is harmless, but
it is dead public surface unless a future API uses it.

### E. Test Containers Rely On `Drop` Cleanup, Not Ryuk

Live observation during a single test run showed the ephemeral
`postgres:16-alpine` container appear and then disappear the instant the test
process exited, with no Ryuk reaper container present. The integration tests
therefore depend on Rust `Drop` unwinding of the `ContainerAsync` handle
(`tests/durable-send/tests/durable_send.rs:267-284`) to remove containers.

This is the reason the containers are never visible after a run, and it is the
correct mental model to document for anyone debugging the suite: to see a
container you must inspect `docker ps` (or `docker events`) while a test is
mid-flight, not afterwards.

The risk: `Drop`-based cleanup does not run if a test process is `SIGKILL`ed
(for example a hard CI timeout or `kill -9`). Without the Ryuk reaper those
containers leak. If leaked containers become a problem in CI, either enable the
Ryuk reaper or add an explicit cleanup step. For local development the current
behavior is acceptable and is mostly a documentation gap.

### F. Container-Level And Schema-Level Isolation Are Redundant

Each integration test calls `start_postgres()` independently
(`tests/durable-send/tests/durable_send.rs:267`), so a full run boots roughly one
Postgres container per test. At the same time, `Harness::connect` already
isolates every connection inside a unique ephemeral schema
(`kafkaman_test_<uuid>`, `crates/kafkaman-test/src/lib.rs:108`).

The suite is therefore paying for two layers of isolation that each fully
isolate the test: a fresh container and a fresh schema. One shared Postgres
container for the whole suite, relying on the per-schema isolation that already
exists, would be faster and would make the container easy to observe because it
would live for the entire run. This is a test-ergonomics and speed improvement,
not a correctness issue.

## Closure Recommendation

Do not close the implementation plan as fully complete without one of these two
actions:

1. implement the missing Redpanda/full-loop Harness path and keep the plan's
   original scope intact; or
2. update the plan outcome to say Redpanda/full-loop was explicitly deferred,
   aligning it with `wiki/specs/m1-durable-send.spec.md:80-82`.

Before closing M1 as durable-send complete, fix or document:

- centralized status SQL generation;
- database-clock lease and retry writes;
- resilient worker loop behavior with tracing;
- durable/non-durable `idempotency_key` semantics;
- concurrent migration behavior.

The existing implementation is a solid M1 Postgres-backed durable-send core, but
the completed-plan label currently overstates the implemented and proven scope.
