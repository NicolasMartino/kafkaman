# M3 Durable Receive Implementation Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-21
- Category: Code review
- Scope: Post-implementation review of the first M3 durable-receive slice in
  `wiki/plans/m3-durable-receive.plan.md` against the landed Rust code, covering
  the received-table types/DDL, deduplicating received insert, `dispatch_once()`
  held-transaction success/failure paths, the bounded `errors` ring, the minimal
  `MessageRouter`, the Harness receive helpers, and the Postgres-only receive
  tests. The plan's remaining items (property gate, crash gate,
  `Replay::received`, injected `Clock` type, macros, Kafka ingest, full-loop) are
  assessed only as scope deltas, not as defects.
- Sources:
  - `wiki/plans/m3-durable-receive.plan.md`
  - `wiki/decisions/message-consumption-and-handler-model.decision.md`
  - `wiki/decisions/message-identity-and-header-namespace.decision.md`
  - `wiki/decisions/retry-backoff-dlq-policy.decision.md`
  - `crates/kafkaman-core/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-test/src/lib.rs`
  - `tests/durable-send/tests/durable_receive.rs`

## Verdict

The first durable-receive slice is well-built and faithful to the plan's stated
increment. The success path is genuinely effective-once: `dispatch_once()` runs
the handler inside the same kafkaman-owned transaction that holds the row lock,
so business writes and the `Processed` mark commit atomically. Dedup-as-log,
nullable-`next_attempt_at` parking, and deterministic clock binding work as the
plan describes and are covered by the existing integration tests. The bounded
`errors` ring SQL looks correct by static inspection, but the 20-entry cap is
not yet test-pinned; only a single-error case is covered.

There is one real effective-once correctness bug on the failure path (H1): the
out-of-transaction failure accounting update is unguarded, so under concurrent
dispatch it can clobber a row another worker has already committed as
`Processed`, producing a double effect. The current tests cannot catch it
because they drive a single `dispatch_once()` per row; it is exactly the class
of defect the not-yet-written randomized effective-once and crash gates exist to
expose. Fix H1 before closing the slice as "effective-once" or before any
multi-worker dispatcher lands.

A secondary theme is scope drift between the plan's "In Scope" handler surface
(`FromMessage`, `Rx`, state, Tower `Service`/`Layer`, metadata access) and what
shipped (a single payload-only closure). The plan's Progress section already
flags most remaining items, so this is plan-accuracy work, not hidden breakage.

## Verification Performed

- `cargo fmt --all -- --check` - clean.
- `cargo check --workspace --all-features` - clean.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` - clean.
- `cargo test -p kafkaman-sqlx --lib` - 6 passed.
- Postgres-only receive integration tests
  (`tests/durable-send/tests/durable_receive.rs`) were re-executed during the
  independent double-check on 2026-06-21 with Docker/testcontainers: 5 passed.
  The first sandboxed attempt failed at Docker container creation with
  `Operation not permitted`; the escalated rerun passed.
- A temporary send-after-receive Harness regression test was added, run, and
  removed during the independent double-check. It failed with PostgreSQL
  `42P01` missing relation for the outbox table after receive-side registration,
  confirming M2.

## High Findings

### H1. Failure accounting can clobber a concurrently-`Processed` row (effective-once hole)

On handler failure, `dispatch_once()` rolls the held transaction back and then
records failure accounting on a **separate pool connection**
(`crates/kafkaman-sqlx/src/lib.rs:1468-1477`):

```rust
Err(err) => {
    let message = err.to_string();
    tx.rollback().await?;                 // row lock released; status reverts to Pending/Retryable
    record_received_failure(pool, table, row.message_id, message, due_at).await?;
```

`record_received_failure` (`crates/kafkaman-sqlx/src/lib.rs:1545-1581`) updates
with an **unguarded** predicate:

```sql
UPDATE {name}
SET status = 'Retryable', attempts = attempts + 1, next_attempt_at = NULL, errors = ...
WHERE message_id = $1
```

The plan calls this the "short follow-up transaction"
(`wiki/plans/m3-durable-receive.plan.md:46-53`), and it must be separate so the
rollback can undo the handler's business writes without also undoing the
accounting. But because the rollback releases the row lock and reverts the row
to its claimable state, another worker can claim the same row before the
follow-up update lands. Concurrent interleaving:

1. Worker A claims the row, handler fails, transaction **rolls back** — no
   business effect, lock released, status back to `Pending`/`Retryable` with a
   due `next_attempt_at`.
2. Worker B claims the now-claimable row, handler **succeeds**, commits the
   business effect plus `status = 'Processed'`.
3. Worker A's `record_received_failure` runs: `WHERE message_id = $1` with no
   status guard overwrites the row to `status = 'Retryable'`,
   `next_attempt_at = NULL`, blowing away B's `Processed`. A later
   `Replay::received` or M4 re-drive makes the row due again, and the handler
   effect runs a **second** time.

This violates the effective-once guarantee that is the entire point of M3
(`wiki/plans/m3-durable-receive.plan.md:23-26`). The
`message-consumption-and-handler-model` decision's held-transaction model is
sound for the success path, but the failure path's escape from the transaction
reintroduces a check-then-act race with no guard.

Note `claim_received_row` uses `FOR UPDATE SKIP LOCKED`
(`crates/kafkaman-sqlx/src/lib.rs:1481-1507`), which only prevents two workers
holding the *same* lock simultaneously. It does not protect the post-rollback
window, because A no longer holds the lock when the follow-up update fires.

**Fix:** guard the follow-up update so it cannot advance a row another worker
has already moved out of the claimable states:

```sql
WHERE message_id = $1 AND status IN ('Pending', 'Retryable')
```

With the guard, step 3's `UPDATE` blocks on B's row lock, then finds the row in
`Processed` and no-ops — exactly one effect, accounting correctly suppressed.
The two-workers-both-fail case still increments `attempts` twice, which is
acceptable over-counting, not a wrong effect.

This finding should be treated as a blocker for advertising the slice as
effective-once and for shipping any concurrent dispatcher.

## Medium Findings

### M1. Handler surface is well below the plan's "In Scope"

The plan's In Scope and Execution Step 4
(`wiki/plans/m3-durable-receive.plan.md:72-74, 148-154`) call for `FromMessage`
extractors, `Rx`, state, Tower `Service`/`Layer` compatibility, and receive
transaction extraction. What shipped is a single erased closure
`Fn(&mut PgConnection, P)` (`crates/kafkaman-sqlx/src/lib.rs:361-377`). Concrete
consequences today:

- Handlers receive **only the deserialized payload**. There is no access to
  `message_id`, `idempotency_key`, `attempts`, `headers`, `correlation_id`, or
  `causation_id` — all of which are already persisted on `ReceivedRow`
  (`crates/kafkaman-core/src/lib.rs:348-368`) and which most real handlers need
  (at minimum correlation for tracing).
- The raw `&mut PgConnection` is the transaction seam rather than an `Rx`
  abstraction, so if the receive transaction later needs to carry more than a
  bare connection, every handler signature changes.

The Progress section honestly labels this "minimal `MessageRouter`." The
recommendation is to either trim the plan's In Scope to what the slice delivers
and move the rest to explicit pending steps, or land at least metadata access
before promoting an M3 spec, so the spec does not overstate the handler
contract the way the M2 spec overstated boot-time guarantees.

### M2. Harness send/receive registration paths conflict for the same message type

`ensure_message` gates its outbox migration on `!already_registered`
(`crates/kafkaman-test/src/lib.rs:318-356`), while `ensure_received_message`
registers into the **same** `cfg.messages()` list and always migrates
(`crates/kafkaman-test/src/lib.rs:358-387`). So if a test touches the receive
side of a type first (`received_table` / `insert_received`), the descriptor is
now registered, and a subsequent `enqueue` / `outbox_table` for that same type
sees `already_registered == true` and **skips the outbox-table migration
entirely** — the outbox INSERT then fails against a missing relation.

No current test exercises both sides of one type, so this is latent. But it is a
sharp edge for the harness's own users, who will reasonably both send and
receive the same message. The two `ensure_*` routines should share one
registration+migration path that provisions both tables (or at least must not
let one side's registration suppress the other side's DDL). Related: the two
paths assign changeset versions from list position (outbox at `idx + 2`,
received at `10_000 + idx`, `crates/kafkaman-test/src/lib.rs:390-407`), which is
stable within a single harness lifetime but couples version numbers to
registration order.

### M3. Missing verification gates the plan explicitly requires

The plan's Verification Gates and Evidence sections
(`wiki/plans/m3-durable-receive.plan.md:186-209`) call for several tests that
are not yet present, and H1 makes two of them load-bearing rather than
box-ticking:

- **Randomized effective-once property test** for redelivery orderings and
  duplicates. A concurrent variant would very likely surface H1.
- **Crash-during-dispatch gate** proving rollback leaves the row claimable and
  commits no double effect.
- **Bounded-errors ring test** proving the ring stays at its limit while
  `attempts` keeps counting. The ring SQL exists and is correct
  (`crates/kafkaman-sqlx/src/lib.rs:1561-1570`), but only a single-error
  assertion (`tests/durable-send/tests/durable_receive.rs:159-160`) covers it;
  the 20-element bound is never driven.

These are tracked as "Remaining M3 work" in Progress, so this finding is about
sequencing: land the property/crash gates next, because they are the cheapest
way to prove H1 is fixed and stays fixed.

## Low Findings

### L1. `mark_received_processing` is a dead write

`dispatch_once()` calls `mark_received_processing`
(`crates/kafkaman-sqlx/src/lib.rs:1456, 1509-1524`) to set `status =
'Processing'` inside the dispatch transaction. On success it is overwritten by
`Processed` in the same transaction; on failure it is rolled back. It is
therefore never observable outside the transaction — the plan itself says
`Processing` is "an in-transaction transition ... not a durable lease state"
(`wiki/plans/m3-durable-receive.plan.md:36-39`). The write produces row-version
and WAL churn with no reader. Remove it, or document why a transient marker is
retained (e.g. for a future `RETURNING`/notification hook).

### L2. `received_row_by_idempotency_key` reports a nil UUID on miss

When a lookup by idempotency key misses, the Harness helper returns
`Error::MissingRow(Uuid::nil())` (`crates/kafkaman-test/src/lib.rs:304-316`). A
nil UUID inside a "missing row" error is misleading during test debugging; a
`MissingReceivedRow(String)` variant carrying the key would name the actual
lookup input.

### L3. Received `correlation_id` is nullable while the envelope always supplies one

`create_received_table_sql` declares `correlation_id UUID` (nullable)
(`crates/kafkaman-sqlx/src/lib.rs:1319-1326`), and `ReceivedRow.correlation_id`
is `Option<Uuid>` (`crates/kafkaman-core/src/lib.rs:363`). Every `Envelope`
carries a non-null `correlation_id` (`crates/kafkaman-core/src/lib.rs:175,
187`), so rows inserted through `insert_received` always have one. The nullable
column presumably anticipates Kafka ingest of records without kafkaman
correlation metadata, but that path does not exist yet (Step 7). This is a
forward-compatibility choice worth a one-line note in the eventual M3 spec so
the nullability is intentional rather than accidental drift.

## Solidly Implemented

- `dispatch_once()` runs the handler against the same `Transaction` that holds
  the `FOR UPDATE SKIP LOCKED` row lock (`&mut tx` deref-coerces to
  `&mut PgConnection`), so business writes and `status = 'Processed'` commit
  atomically (`crates/kafkaman-sqlx/src/lib.rs:1440-1479`). The success-path
  effective-once test and the duplicate-convergence test both pass on this.
- Parking is correct: failure sets `next_attempt_at = NULL`, and the claim
  predicate selects `Retryable` only when `next_attempt_at <= $1`, so
  `NULL <= $1` (never true) excludes parked rows. The "second dispatch claims 0"
  assertion proves no hot-loop
  (`tests/durable-send/tests/durable_receive.rs:167-170`).
- Dedup-as-log via `ON CONFLICT (idempotency_key) DO NOTHING` returning
  `rows_affected() == 1` (`crates/kafkaman-sqlx/src/lib.rs:1407-1437`); the table
  is greenfield `idempotency_key TEXT NOT NULL` plus a unique index, verified
  against `information_schema`
  (`tests/durable-send/tests/durable_receive.rs:28-59`).
- By static SQL inspection, the bounded `errors` ring keeps the most-recent 20
  entries in chronological order while `attempts` counts independently,
  expressed as a single set-based `jsonb` statement
  (`crates/kafkaman-sqlx/src/lib.rs:1561-1570`). The 20-entry cap still needs the
  verification gate called out in M3.
- Deterministic clock: the injected `due_at` timestamp is threaded through claim
  eligibility, `processed_at`, and error `occurred_at`; no `now()` is inlined on
  the dispatch path, matching the plan's clock requirement
  (`wiki/plans/m3-durable-receive.plan.md:55-58`).
- Reserved-header enforcement is mirrored from the send side into
  `insert_received` (`crates/kafkaman-sqlx/src/lib.rs:1397-1399`), honoring the
  `message-identity-and-header-namespace` decision.
- `ReceiveStatus` derives its CHECK constraint and `IN (...)` lists from one
  `ALL` array (`crates/kafkaman-core/src/lib.rs:286-318`), so the database
  cannot drift from the Rust enum — consistent with the M1/M2 status-centralization
  pattern.
- The received state index `(status, next_attempt_at, created_at)`
  (`crates/kafkaman-sqlx/src/lib.rs:1336-1342`) covers the claim predicate's
  filter and ordering.

## Recommended Fix Order

1. Fix H1: add the `status IN ('Pending', 'Retryable')` guard to
   `record_received_failure`, and add a concurrent/interleaved regression test
   that proves a failure-then-success race leaves exactly one effect and a
   `Processed` row.
2. Land M3's randomized effective-once and crash-during-dispatch gates (they
   pin H1 and several plan gates at once).
3. Add the bounded-errors ring test that drives past the 20-entry limit.
4. Reconcile the Harness `ensure_message` / `ensure_received_message`
   registration so a type can be both sent and received in one harness (M2).
5. Decide the handler-surface scope: either trim the plan's In Scope to the
   shipped payload-only closure, or add metadata/`Rx` access before promoting an
   M3 spec (M1).
6. Address L1-L3 polish before stabilizing the receive public surface.

## Status

Resolved. Findings were recorded against commit `96d9a11` ("Implement initial M3
durable receive"); the two dated resolution blocks below land all H/M/L findings.
H1 and the gating tests landed in the 2026-06-21 block; M1 (metadata access) and
L3 (nullability note) landed in the 2026-06-22 block. What remains is *new* M3
surface area the original review explicitly scoped out as deltas, not defects:
`Replay::received`, the injected Harness clock, the `FromMessage`/`Rx`/state and
Tower handler abstractions beyond metadata access, derive/test macros, the Kafka
ingest scheduler, and full-loop coverage. Those are tracked in the plan's
remaining-work list, not as open findings here.

## Resolution Log

### 2026-06-21 follow-up implementation

Partially resolved.

Resolved in this fix slice:

- H1: `record_received_failure` now guards stale failure accounting with
  `status IN ('Pending', 'Retryable')`, so it cannot clobber a row that another
  dispatcher has already committed as `Processed`.
- M2: Harness send and receive registration paths now both run their relevant
  idempotent migrations after descriptor registration, so one message type can be
  both received and enqueued in the same harness.
- Missing gates: `tests/durable-send/tests/durable_receive.rs` now includes the
  stale failure interleaving regression, crash-during-dispatch redrive gate,
  randomized duplicate redelivery convergence gate, and bounded 20-entry error
  ring gate.
- L1: the transient in-transaction `mark_received_processing` write was removed;
  the `FOR UPDATE SKIP LOCKED` row lock remains the dispatch claim.
- L2: Harness receive lookup misses now report
  `MissingReceivedRow(idempotency_key)` instead of a nil UUID placeholder.

Verification:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo test -p kafkaman-sqlx --lib`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`

Remaining open from this review: M1 handler-surface scope, the L3 received
`correlation_id` nullability note, and the larger M3 surfaces not covered by
this fix slice (`Replay::received`, injected Harness clock, macros, Kafka
ingest, and full-loop coverage).

### 2026-06-22 follow-up implementation

Resolved the two remaining review findings; only out-of-scope M3 surface area is
left (see updated Status).

Resolved in this slice:

- M1: dispatch handlers now receive message metadata. A new `ReceivedMeta`
  (`crates/kafkaman-core/src/lib.rs`) carries `message_id`, `idempotency_key`,
  `message_type`/`message_version`, `attempts` (count at claim time), `headers`,
  `source_topic`/`source_partition`/`source_offset`/`key`, `correlation_id`, and
  `causation_id`. `MessageRouter::handler` and the erased handler trait now take
  `Fn(&mut PgConnection, ReceivedMeta, P)`; `dispatch_once` builds the meta from
  the claimed row and passes it in. This clears the review's bar of "land at
  least metadata access before promoting an M3 spec." The broader
  `FromMessage`/`Rx`/state and Tower abstractions remain deferred M3 surface, not
  an open finding. New gate
  `dispatch_exposes_message_metadata_to_handler` asserts every metadata field is
  visible to the handler.
- L3: the nullable `correlation_id`/`causation_id` columns now carry an inline
  comment in `create_received_table_sql` documenting that the nullability is an
  intentional forward-compat choice for the future Kafka-ingest path, not drift.

Verification:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test -p kafkaman-sqlx --lib` (6 passed)
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
  (11 passed, including the new metadata gate)
