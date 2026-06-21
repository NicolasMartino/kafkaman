# Deep Durability Testing

Document Class: Proposal
Status: Proposed
Date: 2026-06-21
Category: Test strategy
Scope: Catalog adversarial concurrency, crash, ingest, send, and durability test designs for kafkaman's durable-execution paths, plus the repeatable technique used to discover them.
Sources:
- wiki/plans/m3-durable-receive.plan.md
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- tests/durable-send/tests/durable_receive.rs
- crates/kafkaman-sqlx/src/lib.rs
Related:
- wiki/specs/m1-durable-send.spec.md
- wiki/specs/m2-change-engine-config.spec.md
- wiki/decisions/message-identity-and-header-namespace.decision.md
- wiki/decisions/retry-backoff-dlq-policy.decision.md

## Context

kafkaman's correctness promise is effective-once durable execution: a message is
written to Postgres, de-duplicated by identity, dispatched through the handler
stack from the durable row, and marked with a durable outcome. Handler health,
broker redelivery, crashes, retries, and scheduler concurrency must not corrupt
state, lose records, or stall broker flow.

The first M3 receive tests prove the happy path, single-call failure path,
duplicate redelivery convergence, crash rollback, and bounded error history.
Those tests are necessary but not sufficient. The M3 review found H1, a
high-severity effective-once hole, by reasoning about interleavings rather than
by observing a failing test. This proposal keeps that reasoning technique as a
living test catalog.

The current review is:

- The original A-G catalog is still valuable.
- Some entries are now landed or partially landed and should be tracked as
  regression assets, not only future ideas.
- The proposal needs more rare-failure tests around ambiguous commits,
  cancellation, poisoned transactions, Kafka offset uncertainty, idempotency
  collisions, consume-then-produce atomicity, schema/config drift, status
  variant drift, timestamp precision, and send-side mirrors.
- Follow-up verification confirmed the H1 guard and landed regression claims,
  corrected B2's mechanism description, and confirmed that parked retryable rows
  are already test-pinned by
  `dispatch_failure_rolls_back_effect_and_parks_retryable`.

## Test Discovery Technique

For every durable operation, look for a boundary where the logical operation is
larger than one physical commit, lock, broker acknowledgement, or process
lifetime. Then force the boundary open.

1. Identify the invariant.
   Examples: no business effect without `Processed`; no `Processed` without the
   business effect; no Kafka offset commit before durable insert; no duplicate
   outbox message for one processed input.
2. Find the escape point.
   Examples: rollback before failure accounting, broker offset commit after row
   insert, handler cancellation while a transaction is open, DB connection loss
   after `COMMIT` is sent but before the client sees the acknowledgement.
3. Enumerate adversaries.
   Use a second worker, duplicate delivery, process kill, task abort, connection
   kill, pool exhaustion, clock jump, migration lock, malformed payload, or
   conflicting identity.
4. Assert the externally observable invariant.
   Avoid only checking function return values. Inspect the durable tables and,
   when relevant, consumed/produced Kafka records.
5. Prefer deterministic control first, randomized schedules second.
   Channel barriers, test-only pause points, explicit two-step primitives,
   `pg_terminate_backend`, Toxiproxy/netem, and state-machine property tests all
   have a place. The deterministic test should capture the minimal failing
   interleaving before a chaos variant is added.

## Existing Receive Catalog

### Class A - Concurrency

#### A1. Concurrent failure-then-success on one row (H1 regression)

- Mechanism: Worker A claims a row and the handler fails. Earlier M3 slices
  rolled the transaction back before recording failure accounting, which
  released the row lock and allowed Worker B to process the same row first.
- Invariant: failure accounting should hold the claimed row lock until the
  retryable failure row commits; a competing dispatcher must skip the locked row
  instead of processing it.
- Current status: mitigated by handler savepoint rollback and pinned by
  `failure_recording_holds_row_lock_until_retryable_commit`.
- Remaining test: run the same interleaving through two long-running dispatcher
  loops.

#### A2. N-way concurrent dispatch on one message family

- Mechanism: seed one row or a mix of duplicates, then run many dispatch loops
  concurrently with randomized handler delay and success/failure outcomes.
- Invariant: at most one committed business effect per idempotency key; no
  deadlock; all due non-poison rows eventually settle.
- Current status: sequential randomized redelivery convergence is landed.
- Remaining test: add real concurrent workers and include a failing-handler
  branch to statistically reproduce A1-like timing.

#### A3. Concurrent duplicate insert

- Mechanism: two `insert_received` calls with the same `idempotency_key` from
  different connections, including the case where dispatch holds the winning
  row's `FOR UPDATE` lock.
- Invariant: exactly one insert returns `true`; no second row; no deadlock; the
  loser does not change the winner's payload, headers, source offset, or ids.
- Current status: sequential dedup is landed; concurrent insert is unproven.

### Class B - Crash and Failure Accounting

#### B1. Crash during handler, before commit

- Mechanism: begin dispatch, run a handler that writes a business row, then
  panic, abort the task, close the connection, or drop the transaction before
  commit.
- Invariant: no business effect committed; row remains claimable or parked by
  explicit failure accounting; never `Processed`.
- Current status: panic rollback is landed in
  `crash_during_dispatch_rolls_back_and_can_be_redriven`.
- Remaining test: add explicit transaction-drop, task-abort, and connection-kill
  variants because panic, future cancellation, and connection loss exercise
  different cleanup paths.

#### B2. Crash in failure-accounting window

- Mechanism: handler fails; dispatch rolls back; process crashes before
  `record_received_failure` runs.
- Invariant option A: failed attempt is durably counted.
- Invariant option B: document attempts as best-effort crash accounting and
  prove the row is safely retried without a business effect.
- Prediction: current receive path likely preserves safety but loses the
  attempt count. The precise mechanism is that receive claim is a pure
  `SELECT ... FOR UPDATE`; unlike send-side `claim_batch`, it never increments
  `attempts` before user code runs. If the process dies before
  `record_received_failure`, no committed operation ever touched `attempts`.
  The existing crash-redrive test already observes `attempts == 0` after a
  handler panic.

### Class C - Robustness and Poison Messages

#### C1. Unregistered-handler row causes head-of-line blocking

- Mechanism: seed a row whose `message_type` has no registered handler, then
  call `dispatch_once` repeatedly with newer valid rows behind it.
- Invariant: one bad row must not stall newer rows.
- Prediction: reveals a gap. `MissingHandler` is returned before failure
  accounting, so `dispatch_once` itself returns `Err`. Because claiming orders
  by `created_at` and limits to one row, an old bad row is not merely stuck; it
  can hard-block the entire loop and prevent newer valid rows from being seen.
- Design forced: decide whether `MissingHandler` parks the row, fails the whole
  scheduler, or moves to a terminal/DLQ state.

#### C2. Undeserializable payload parks rather than crashes

- Mechanism: insert a row whose stored JSON payload cannot deserialize into the
  registered message type.
- Invariant: dispatch does not panic; row is parked or moved terminal; never
  `Processed`.
- Prediction: should park as handler error today, but M4 still needs a terminal
  poison/DLQ state to avoid infinite manual replay.

#### C3. Handler returns `Ok` after poisoning its transaction

- Mechanism: a handler issues a SQL statement that fails, catches/ignores that
  DB error, and returns `Ok(())`. Postgres marks the transaction aborted, so the
  success branch's later `mark_received_processed` fails with the transaction in
  an aborted state.
- Invariant: the row must not become `Processed`; no handler writes commit; the
  failure must be classified deliberately instead of escaping as an unaccounted
  scheduler error.
- Prediction: likely reveals a gap. The current `dispatch_once` success branch
  uses `?` on `mark_received_processed`; if that fails, it returns `Err` without
  entering the handler-error branch and without `record_received_failure`.
- Why it matters: this has the same operational shape as `MissingHandler` but is
  reached through a handler that appears to have succeeded.

### Class D - Atomicity Invariants

#### D1. Concurrent reader never sees a torn commit

- Mechanism: while a slow handler runs, a separate connection repeatedly reads
  `(business_row_count, received.status)`.
- Invariant: never observe business effect without `Processed`, never
  `Processed` without the business effect, and never observe committed
  `Processing`.
- Prediction: should pass because the handler effect and `Processed` update
  share one transaction.

#### D2. Multi-table handler rollback

- Mechanism: handler writes two business tables and then fails.
- Invariant: both writes roll back together; row is parked/retryable; no
  half-committed business state.
- Prediction: should pass while the handler shares kafkaman's transaction.

#### D3. Status-guard asymmetry stays explicit

- Mechanism: inspect or test the two durable status writers. The success path's
  `mark_received_processed` has no status guard and relies on the held
  `FOR UPDATE` row lock plus the still-open transaction. The failure path's
  `record_received_failure` runs after rollback on a fresh pooled connection, so
  it must retain its `status IN ('Pending','Retryable')` guard.
- Invariant: success may stay unguarded only while it is lock-protected inside
  the dispatch transaction; any future success-path refactor that moves the mark
  outside the lock must add an equivalent guard/token. Failure accounting must
  remain guarded because it is intentionally outside the handler transaction.
- Why it matters: this asymmetry is the load-bearing reasoning behind the H1 fix
  and should be documented as an invariant, not rediscovered from the SQL.

### Class E - Bounded Resources

#### E1. Errors ring past the cap

- Mechanism: drive more than 20 failures against one row, tagging each error
  message with its attempt number.
- Invariant: `errors.len() == 20`, `attempts` reflects all attempts, and the
  retained errors are the most recent 20 in deterministic order.
- Current status: landed in
  `received_failure_errors_keep_most_recent_twenty_entries`.

#### E2. Pool exhaustion under held-transaction model

- Mechanism: pool size 1 or 2, multiple concurrent dispatch loops, slow
  handlers, and failure accounting that needs a fresh connection after rollback.
- Invariant: eventual drain or bounded timeout; no permanent deadlock; no hidden
  dependency on pool size greater than configured worker count.
- Prediction: should pass if rollback releases the connection before failure
  accounting requests another one.

### Class F - Clock Scheduling

#### F1. Due-boundary and retry-window correctness

- Mechanism: one row with `next_attempt_at == due_at`; another retryable row
  with future `next_attempt_at`; use injected clock only.
- Invariant: boundary row is claimable; future row waits until the clock reaches
  it; SQL never inlines `now()` in test-controlled dispatch paths.
- Prediction: should pass and pin the injected-clock contract.

#### F2. Parked retryable rows are not auto-redriven in M3

- Mechanism: a handler failure records `Retryable` with `next_attempt_at = NULL`;
  subsequent `dispatch_once` calls run with due timestamps after the failure.
- Invariant: the parked row is not claimed again until a manual replay/M4 retry
  scheduler sets a concrete due timestamp.
- Current status: landed in
  `dispatch_failure_rolls_back_effect_and_parks_retryable`, which asserts
  `next_attempt_at == None` and then asserts a second `dispatch_once` claims
  zero rows.
- Why it matters: this pins M3's deliberate "park, do not auto-retry" behavior so
  M4 retry scheduling changes are explicit.

### Class G - Kafka Ingest Edge

#### G1. Crash between durable insert and Kafka offset commit

- Mechanism: once ingest exists, kill the scheduler after the received row
  insert commits but before the Kafka offset commits.
- Invariant: on restart the record is re-consumed and `insert_received` dedups
  it; one row exists; message loss is impossible.
- Prediction: future test for M3 step 7.

#### G2. Offset commit uncertainty after durable insert

- Mechanism: durable insert succeeds; offset commit times out, returns
  retriable error, or the consumer loses partition ownership during commit.
- Invariant: duplicate broker delivery converges to the same received row and
  one handler effect.
- Why it matters: production offset commits can be ambiguous in ways that a
  clean crash test does not cover.

#### G3. Forbidden ordering guard

- Mechanism: inject a crash or panic after offset commit is attempted but before
  received row insertion would have committed.
- Invariant: kafkaman code path must make this ordering unreachable. If a test
  hook can force it, the implementation has inverted the critical ordering.

## Additional Rare-Failure Classes

### Class H - Ambiguous Commit and Async Cancellation

#### H1. Postgres commit acknowledgement lost

- Mechanism: handler writes business effect and marks `Processed`; the client
  sends `COMMIT`; a proxy drops the server acknowledgement or the backend
  connection is killed exactly around commit.
- Invariant: after reconnect, the durable database state is self-describing:
  either both business effect and `Processed` committed, or neither committed.
  A retry must not create a second effect.
- Tooling: Toxiproxy/netem is more realistic than only dropping Rust futures.
- Priority: high, because ambiguous commit outcomes are rare but production
  realistic.

#### H2. Tokio task cancellation while transaction is open

- Mechanism: abort the `dispatch_once` task after the handler performs one
  write but before it returns.
- Invariant: transaction is dropped or rolled back; row is not `Processed`;
  business write is absent; a later dispatch can safely retry.
- Difference from B1: cancellation drops futures without normal error handling,
  so it tests cleanup under scheduler shutdown and timeouts.

#### H3. Graceful shutdown between claim and handler completion

- Mechanism: trigger worker shutdown after claim but before handler completion.
- Invariant: kafkaman stops claiming new rows, lets in-flight transaction finish
  or rolls it back, and does not leave committed `Processing`.
- Future target: worker runtime once receive schedulers are spawnable units.

### Class I - Identity Collisions and Dedup Integrity

#### I1. Same idempotency key, different payload

- Mechanism: insert two received envelopes with the same `idempotency_key` but
  different payload, headers, source topic/partition/offset, or correlation id.
- Invariant: exactly one row is durable, but the collision is observable through
  a return value, metric, error, audit row, or explicit documented behavior.
- Risk: `ON CONFLICT DO NOTHING` can hide producer bugs that otherwise look like
  correct dedup.
- Design forced: decide whether conflicting duplicate content is benign, a
  metric-only anomaly, or a hard error.

#### I2. Same Kafka source offset, different idempotency key

- Mechanism: replay the same topic/partition/offset with a different
  idempotency key, simulating a producer or envelope bug.
- Invariant: choose and prove the contract. Either source offset identity is
  advisory only, or the table rejects/flags the second row to prevent duplicate
  effects for one Kafka record.
- Priority: high before full Kafka ingest because it defines the defense line
  between broker identity and application idempotency identity.

#### I3. Header/key byte edge cases

- Mechanism: ingest records with null key, empty key, invalid UTF-8 headers,
  duplicate header names, very large headers, and user headers using reserved
  `kafkaman-*` prefixes.
- Invariant: reserved metadata cannot be spoofed; bad metadata does not commit
  an offset unless the durable handling decision is recorded.

### Class J - Consume-Then-Produce Atomicity

Landed implementation evidence: J1-J3 are covered by the M3 closure-based
handler surface. Handlers receive `&mut PgConnection` plus `ReceivedMeta`, can
perform business SQL, and can call `enqueue_on_connection` in the same receive
transaction. Treat this class as a landed regression target.

#### J1. Handler business write plus follow-up outbox enqueue

- Mechanism: receive a message; handler writes a business table and enqueues a
  follow-up outbox message in the same transaction; dispatch succeeds.
- Invariant: business row, received `Processed`, and outbox `Pending` appear
  atomically.
- Why it matters: this is the common service-to-service choreography path.

#### J2. Failure after follow-up enqueue

- Mechanism: handler writes business row, enqueues outbox, then returns error.
- Invariant: business row and outbox row both roll back; received row is parked
  or retryable; no follow-up message can publish from a failed consume.

#### J3. Duplicate redelivery after consume-then-produce success

- Mechanism: after J1 commits, reinsert/redeliver the same received record.
- Invariant: no second business effect and no second outbox row for the same
  downstream command.
- Design forced: downstream outbox enqueue needs its own idempotency key policy,
  not just receive-row idempotency.

### Class K - Runtime Schema and Config Drift

#### K1. Dispatch while migration lock is held

- Mechanism: hold the kafkaman migration advisory lock or run a long migration
  while dispatch loops execute.
- Invariant: dispatch either proceeds against the old compatible schema or
  fails fast with a bounded operational error; no partial changeset and no
  deadlock.

#### K2. Send and receive registration for the same message type

- Mechanism: register both outbox and received tables for the same message type,
  then enqueue after receive registration.
- Invariant: both tables exist and use distinct qualified names; receive
  migration does not mask outbox migration.
- Current status: landed as a Harness regression in
  `harness_can_enqueue_after_receive_registration_for_same_type`.

#### K3. Retry config changes between attempts

- Mechanism: a row fails once under one retry/backoff config, then runtime
  config changes before replay or next attempt.
- Invariant: the next durable state follows the documented config ownership:
  persisted row data remains truthful, while newly computed scheduling uses the
  active resolved config.
- Future target: M4 retry/backoff/DLQ runtime config.

#### K4. `Processing` and `Failed` are reserved but never written in M3

- Mechanism: exercise success, handler failure, crash, missing handler,
  malformed payload, and replay paths; then inspect all received rows.
- Invariant: M3 dispatch writes only `Pending`, `Retryable`, and `Processed`.
  `Processing` and `Failed` may remain enum/DDL-allowed reserved states, but no
  M3 path should silently start committing them.
- Current evidence: `ReceiveStatus::ALL` includes `Processing` and `Failed`, but
  current `kafkaman-sqlx` code has no writes using those variants.
- Why it matters: M4 DLQ/terminal-state work should introduce `Failed`
  deliberately, with a focused migration/test update.

#### K5. Received `message_version` is a dead field in M3

- Mechanism: insert received rows through the public helper and inspect
  `ReceivedRow.message_version`.
- Invariant: either M3 intentionally hard-codes version `1`, or future
  schema-evolution tests need an API that can persist and dispatch non-`1`
  versions.
- Current evidence: received-table DDL exposes `message_version`, and
  `ReceivedRow` reads it back, but `insert_received` currently inserts the SQL
  literal `1`.
- Why it matters: versioning should be an explicit compatibility decision before
  message schema evolution lands, not a silently dead column.

### Class L - Locking, Starvation, and Fairness

#### L1. Oldest row locked, younger due rows must progress

- Mechanism: one connection locks the oldest due row with `FOR UPDATE`; worker
  dispatch loops run against several younger due rows.
- Invariant: `FOR UPDATE SKIP LOCKED` lets younger rows process; when the old
  lock releases, the oldest row is still claimable.
- Why it matters: this distinguishes healthy lock contention from head-of-line
  blocking caused by poison rows.

#### L2. Long handler does not starve unrelated message types

- Mechanism: one table/type has a slow handler; another table/type has fast due
  rows; run worker topology as it will exist in production.
- Invariant: worker scheduling and pool sizing do not let one message type
  starve all others indefinitely.
- Future target: worker runtime topology.

#### L3. Pool saturation with mixed success and failure

- Mechanism: small pool, concurrent slow successes, concurrent failures that
  need failure accounting, and at least one no-row dispatch.
- Invariant: no deadlock; no lost failure accounting except the intentionally
  documented B2 crash window; no connection leak after panic or cancellation.

### Class M - Observability and Payload Safety

#### M1. Error history and logs do not leak payload

- Mechanism: process a message with sensitive payload, key, and headers; make
  handler fail with a controlled error.
- Invariant: durable `errors`, logs, traces, and metrics include bounded error
  context but not raw payload or unsafe headers unless explicitly configured.
- Related proposal: observability/logging policy.

#### M2. Metrics/tracing failures cannot affect durability

- Mechanism: install a tracing/metrics sink that panics, returns error, or
  blocks during success and failure paths.
- Invariant: observer failure cannot roll back a committed handler transaction,
  mark a row processed incorrectly, or prevent failure accounting from running.

#### M3. Suppressed stale failure accounting must not over-report failures

- Mechanism: reproduce the H1 interleaving after the status guard fix. Worker B
  commits the row as `Processed`; Worker A's guarded `record_received_failure`
  update correctly affects zero rows.
- Invariant: durable state remains safe, and dispatch statistics/metrics reflect
  what was durably recorded. If the failure accounting update was suppressed,
  decide whether `DispatchStats.failed` should be `0`, or whether it deliberately
  counts attempted handler failures rather than durable failure records.
- Current evidence: `record_received_failure` discards `rows_affected`, and the
  `dispatch_once` error branch always returns `failed: 1`. The stale-failure
  regression currently asserts `failed == 1` even though the row remains
  `Processed` and `errors.len() == 0`.
- Why it matters: the H1 fix protects durability, but it can still double-count
  observability for one logical row unless the stats contract is clarified.

### Class N - Send-Side Mirror Tests

#### N1. Publish succeeds, mark-published write is lost

- Mechanism: broker accepts the message, then the process or DB connection dies
  before `mark_published` commits.
- Invariant: row remains reclaimable and may publish again; this is
  at-least-once send behavior, not message loss.
- Required assertion: duplicate publish is acceptable only if documented and
  observable.

#### N2. Stale outbox claim cannot mark a newer claim

- Mechanism: Worker A claims outbox row, lease expires, Worker B reclaims and
  publishes, then Worker A attempts `mark_published` or `mark_failed`.
- Invariant: stale token update is a no-op; newer claim outcome wins.
- Status: should remain a send-side regression gate even as receive work grows.

#### N3. Broker acknowledgement ambiguity

- Mechanism: producer send reaches Kafka but acknowledgement is lost to the
  client.
- Invariant: kafkaman treats the outbox row according to documented
  at-least-once semantics; no row is marked published unless the implementation
  can prove broker acceptance.

### Class O - Stateful Chaos and Model Checking

#### O1. Database-backed state-machine property test

- Mechanism: model a received row and randomly execute operations: duplicate
  insert, claim, handler success, handler failure, panic, rollback, stale failure
  accounting, replay, clock advance, clock regression, and restart.
- Invariant: database state always matches the model's allowed states; no state
  contains a business effect without a durable processed marker.
- Tooling: `proptest` for operation generation; deterministic seed printed on
  failure.

#### O2. Async scheduler model test

- Mechanism: isolate Rust-level worker coordination and run under `shuttle` or
  `loom` where Postgres is replaced by a small in-memory state machine.
- Invariant: scheduler cancellation, shutdown, and worker coordination do not
  drop in-flight work or double-complete a claim.
- Boundary: this cannot prove SQL behavior; it complements, not replaces,
  Postgres integration tests.

#### O3. Nightly chaos harness

- Mechanism: run Postgres and Redpanda behind fault-injection proxies; randomize
  kill points around insert, offset commit, handler commit, publish, and
  mark-published.
- Invariant: after every restart, durable tables and broker observations satisfy
  the same effective-once/at-least-once contracts as deterministic tests.
- Placement: ignored/nightly suite, not normal `cargo test`.

### Class P - Test Oracle Precision

#### P1. TIMESTAMPTZ precision in exact timestamp assertions

- Mechanism: bind an `OffsetDateTime::now_utc()` as `processed_at`, read it back
  from Postgres `TIMESTAMPTZ`, and compare exactly to the original Rust value.
- Invariant: tests should not depend on sub-microsecond precision that Postgres
  cannot store. Either truncate the Rust timestamp to microseconds before
  dispatch or compare with a microsecond tolerance.
- Current evidence: `dispatch_once_commits_handler_effect_and_processed_status`
  asserts `row.processed_at == Some(now)`. This has passed locally, but it is a
  brittle oracle if a platform returns nanosecond values not aligned to
  Postgres' timestamp precision.
- Why it matters: this is a live test reliability issue, not only future
  durability coverage.

## Priority Order

1. A1 real two-dispatch-worker H1 regression — landed with
   `dispatch_once_with_hooks` stale-failure interleaving test.
2. C3 handler-swallowed DB error leaving a poisoned transaction.
3. C1 unregistered-handler head-of-line blocking.
4. B2 crash in failure-accounting window; choose crash-durable or best-effort
   attempt accounting.
5. M3 dispatch statistics for suppressed stale failure accounting.
6. P1 timestamp precision test-oracle hardening.
7. J1/J2/J3 consume-then-produce atomicity and handler API support — landed.
8. H1/H2 ambiguous commit and cancellation cleanup.
9. I1/I2 identity collision and source-offset conflict tests.
10. G1/G2 ingest crash and offset-commit uncertainty — landed with Redpanda
    post-durable-write/pre-offset-commit hook tests.
11. L1/L3 lock starvation and pool saturation.
12. M1 payload-safety observability test.
13. O1 database-backed state-machine property test.

## Coverage Matrix

| ID | Name | Class | Expected result | Status |
| --- | --- | --- | --- | --- |
| A1 | Concurrent failure-then-success | Concurrency | Passes after H1 fix | Landed |
| A2 | N-way concurrent dispatch | Concurrency | Passes or finds races | Landed sequential; add concurrent |
| A3 | Concurrent duplicate insert | Concurrency | Passes | Landed sequential; add concurrent |
| B1 | Crash during handler | Crash | Passes | Landed panic; add drop/abort/connection variants |
| B2 | Crash in failure accounting | Crash | Safety passes; attempts lost | Proposed |
| C1 | Missing handler head-of-line | Robustness | Reveals gap | Landed |
| C2 | Undeserializable payload | Robustness | Parks or terminal | Proposed |
| C3 | Handler returns Ok after poisoning tx | Robustness | Reveals gap | Landed |
| D1 | Torn commit reader | Atomicity | Passes | Proposed |
| D2 | Multi-table rollback | Atomicity | Passes | Proposed |
| D3 | Status-guard asymmetry | Atomicity | Passes by design | Proposed |
| E1 | Errors ring cap | Bounded resources | Passes | Landed |
| E2 | Pool exhaustion | Bounded resources | Passes | Proposed |
| F1 | Due-boundary retry window | Clock | Passes | Proposed |
| F2 | Parked retryable not auto-redriven | Clock | Passes | Landed |
| G1 | Insert before offset commit crash | Ingest | Passes | Landed |
| G2 | Offset commit uncertainty | Ingest | Passes | Landed |
| G3 | Forbidden offset-before-row ordering | Ingest | Unreachable | Future |
| H1 | Commit acknowledgement lost | Ambiguous commit | Passes | Proposed |
| H2 | Task cancellation in transaction | Cancellation | Passes | Proposed |
| H3 | Graceful shutdown mid-dispatch | Cancellation | Passes | Landed |
| I1 | Same idempotency key, different payload | Identity | Design-forcing | Proposed |
| I2 | Same source offset, different idempotency key | Identity | Design-forcing | Proposed |
| I3 | Header/key byte edge cases | Identity | Design-forcing | Future |
| J1 | Consume success enqueues outbox | Atomic chain | Needs API support | Landed |
| J2 | Failure after outbox enqueue | Atomic chain | Needs API support | Landed |
| J3 | Duplicate consume after follow-up send | Atomic chain | Passes | Landed |
| K1 | Dispatch while migration lock held | Schema/config | Passes or bounded error | Proposed |
| K2 | Send/receive same type registration | Schema/config | Passes | Landed |
| K3 | Retry config changes between attempts | Schema/config | Design-forcing | Future |
| K4 | Reserved status variants are not written | Schema/config | Passes | Proposed |
| K5 | Received message_version is hard-coded | Schema/config | Design-forcing | Proposed |
| L1 | Oldest locked row does not block younger rows | Locking | Passes | Proposed |
| L2 | Slow type does not starve other types | Scheduling | Passes | Future |
| L3 | Pool saturation mixed outcomes | Resources | Passes | Proposed |
| M1 | Payload safety in errors/logs | Observability | Passes | Proposed |
| M2 | Metrics sink failure cannot affect durability | Observability | Passes | Future |
| M3 | Suppressed stale failure stats | Observability | Design-forcing | Landed |
| N1 | Publish success, mark-published lost | Send side | At-least-once | Proposed |
| N2 | Stale outbox claim cannot mark newer claim | Send side | Passes | Proposed |
| N3 | Broker acknowledgement ambiguity | Send side | At-least-once | Future |
| O1 | DB-backed state-machine property | Chaos/model | Finds rare bugs | Proposed |
| O2 | Async scheduler model test | Chaos/model | Finds races | Future |
| O3 | Nightly proxy chaos harness | Chaos/model | Finds rare bugs | Future |
| P1 | TIMESTAMPTZ exact assertion precision | Test oracle | Prevents flakes | Proposed |

## Open Questions

- Should receive `attempts` be crash-durable, or explicitly documented as
  best-effort crash accounting?
- Should `MissingHandler` park the row, move it terminal, or stop the scheduler?
- Should infrastructure errors that occur after a handler returns `Ok` be routed
  into failure accounting, treated as scheduler-fatal, or split into a distinct
  operational error class?
- Should `DispatchStats.failed` count attempted handler failures, or only
  failures durably recorded by `record_received_failure`?
- Should same-idempotency-key/different-payload collisions be observable errors,
  metrics-only anomalies, or documented as silent dedup?
- Should Kafka topic/partition/offset be a uniqueness guard in addition to
  application `idempotency_key`?
- Are `Processing` and `Failed` explicitly reserved states until M4, and should
  tests fail if M3 starts writing them?
- Is received `message_version` intentionally fixed at `1` until schema evolution
  work, or should receive ingest expose a version-aware API earlier?
- Which controlled-interleaving primitive should `kafkaman-test` expose:
  channel barriers, injected pause points, explicit two-step claim/complete, or
  all three behind a test-only feature?
- Which fault-injection tool should own ambiguous commit and broker
  acknowledgement tests: Toxiproxy, Redpanda/testcontainers hooks, or a custom
  local proxy?

## Next Steps

- Convert priority items 1-4 into concrete tests under
  `tests/durable-send/tests/durable_receive.rs` or a receive-focused test crate.
- Fix P1 as a small test-hygiene change whenever the receive tests are next
  edited.
- Add a small test-only pause-point mechanism only where manual SQL simulation
  cannot faithfully represent the production path.
- Update this catalog's status column whenever a test lands or reveals a real
  defect.
- Fold any new defect back into the M3 review, the active plan's verification
  gates, and the relevant spec or decision once behavior is validated.
