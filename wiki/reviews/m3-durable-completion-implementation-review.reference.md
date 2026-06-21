# M3 Durable Completion Implementation Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-22
- Category: Code review
- Scope: Adversarial post-implementation review of the M3 durable-completion slice
  in `wiki/plans/m3-durable-completion.plan.md` against the landed Rust code. The
  `dispatch_once` seam itself was reviewed in
  `m3-durable-receive-implementation-review.reference.md`; this review concentrates
  on the engine wrapped around that seam — the Kafka ingest path
  (`RdkafkaConsumer::ingest_once`), the receive dispatcher loop (`run_dispatcher`),
  `Replay::received`, and the parked-row recovery story — and on failure modes the
  deep-durability catalog does **not** yet anticipate. It also checks the plan's
  Progress claims against shipped code.
- Sources:
  - `wiki/plans/m3-durable-completion.plan.md`
  - `wiki/proposals/05-deep-durability-testing.proposal.md`
  - `wiki/decisions/kafka-ingest-identity-and-ordering.decision.md`
  - `wiki/decisions/missing-handler-dispatch-policy.decision.md`
  - `wiki/decisions/dispatch-infrastructure-error-classification.decision.md`
  - `wiki/decisions/dispatch-stats-semantics.decision.md`
  - `wiki/decisions/receive-handler-surface-scope.decision.md`
  - `crates/kafkaman-core/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-rdkafka/src/lib.rs`
  - `crates/kafkaman-worker/src/lib.rs`
  - `tests/durable-send/tests/durable_receive.rs`
  - `tests/durable-send/tests/redpanda_full_loop.rs`
- Related:
  - `wiki/reviews/m3-durable-receive-implementation-review.reference.md`

## Verdict

The `dispatch_once` seam holds up. The H1 status guard, the
rollback-then-fresh-connection failure accounting, the C3 poisoned-transaction
handling (`mark_received_processed` error routed to `record_received_failure`),
and the `DispatchStats.failed` = durable-records contract all survive the
interleavings traced here. The six new decision docs are internally consistent.

The engine wrapped around the seam is where the risk concentrates. The Kafka
ingest path has two distinct **partition-stall** failure modes that the catalog
does not cover (F1, F2). The dispatcher loop does not drain (F3), contradicting
the plan's own wording. Parked failure/missing-handler rows have **no in-library
recovery path** (F4), and `Replay::received` silently re-executes handler side
effects on already-processed rows (F5). Finally, two of the plan's Phase gates
are claimed as met by the Progress note but are not present in the shipped tests.

None of F1–F6 is caught by the current suite, by design: the suite drives ingest
and dispatch one clean call at a time. They are exactly the boundary-forcing
cases the deep-testing technique is meant to surface.

## HIGH — uncatalogued failure modes

### F1. Ingest-level poison message permanently stalls the partition

`RdkafkaConsumer::ingest_once` (`crates/kafkaman-rdkafka/src/lib.rs:157-206`)
runs: `recv → from_slice::<P> → build envelope → insert_received → tx.commit →
commit_message(offset)`. Any error **before** the offset commit returns `Err`
with the broker offset un-advanced:

- undeserializable payload (`serde_json::from_slice::<P>` → `Error::Serde`)
- `MissingPayload` (tombstone / null body)
- `MissingIdempotencyKey` (external producer without `kafkaman-idempotency-key`)
- invalid UUID in any `kafkaman-*` header (`parse_uuid_header`)
- any transient DB error during insert/commit

Because the offset never moves, the next `recv()` redelivers the **same** record,
forever, and the entire partition is blocked behind it. There is no park, skip,
or DLQ at the ingest layer.

The catalog's C2 covers an undeserializable *stored row* at dispatch (which
parks). It does not cover a record that cannot become a row at all — and that is
strictly worse, because no durable row exists and the offset never advances. This
is the single biggest gap.

- Invariant: a malformed or un-routable broker record must not block delivery of
  valid records behind it on the same partition.
- Test: produce a record with a valid `kafkaman-idempotency-key` but a body that
  will not deserialize into `P`, then a valid record behind it on the same
  partition; assert the valid record is still ingestable. Today it is not.
- Design forced: ingest needs a poison policy (skip-with-durable-record,
  dead-letter, or quarantine offset) symmetric to the dispatch-side park decision.

### F2. PK-vs-conflict-target mismatch is a second stall vector

`insert_received` (`crates/kafkaman-sqlx/src/lib.rs:1515-1568`) uses
`ON CONFLICT (idempotency_key) DO NOTHING`, but `message_id` is the table PRIMARY
KEY. For an external (non-kafkaman) producer there is no `kafkaman-message-id`
header, so `Envelope::new` assigns a fresh random `message_id` on every consume.
A pair of records that collide on `message_id` but differ on `idempotency_key`
(or any producer bug reusing a `message_id` with a new key) hits the **PRIMARY
KEY** violation, which is not the `ON CONFLICT` target → uncaught `sqlx` error →
the same infinite partition stall as F1.

- Invariant: duplicate-or-conflicting identity must resolve to a clean dedup
  (`inserted = false`) or a recorded anomaly, never an uncaught insert error that
  stalls ingest.
- Test: insert two received rows sharing `message_id`, differing
  `idempotency_key`; today the second is an uncaught unique violation.

## MEDIUM — uncatalogued

### F3. The dispatcher does not drain — throughput is one row per poll interval

`run_dispatcher` (`crates/kafkaman-worker/src/lib.rs:122-151`) calls
`dispatch_once` once (which is `LIMIT 1` in `claim_received_row`), then
unconditionally `sleep(poll_interval)`. The plan (Phase 3.4 and the Phase 3
verification gate) says "poll-interval **drain** of due rows." It does not drain;
it processes exactly one row per interval. With the default `poll_interval =
250ms` that is ~4 rows/sec/worker regardless of backlog, and a backlog of N takes
`N × poll_interval` to clear — it never catches up under sustained arrival. The
send-side `relay_once` correctly batches via `claim_batch(batch_limit)`; the
dispatcher has no batch and no inner drain-until-empty.

- Invariant: a finite backlog of due rows drains in bounded time independent of
  poll interval.
- Fix options: inner `while stats.claimed > 0` loop before sleeping, or a batched
  `dispatch_batch`.
- Test: seed 100 due rows, run the dispatcher for `2 × poll_interval`, assert the
  processed count is ~2 (demonstrates the limit); after the fix, assert all 100
  drain.

### F4. Parked failure / missing-handler rows are unrecoverable through the shipped API

A handler failure or `MissingHandler` parks the row as `status = Retryable,
next_attempt_at = NULL` (`record_received_failure`,
`crates/kafkaman-sqlx/src/lib.rs:1700-1718`, and the missing-handler decision).
The claim query (`claim_received_row`, line 1640) requires `status = retryable
AND next_attempt_at <= $1`; since `NULL <= ts` evaluates to `NULL` (false),
dispatch never reclaims it (correct per F2 "no auto-retry in M3"). But
`Replay::received` (`replay_received_filter_sql`, line 991) hardcodes
`status = Processed`. So a parked row is simultaneously:

- not reclaimable by the dispatcher, and
- not matched by the only operational replay surface.

**There is no in-library path to re-drive a parked receive failure in M3** — not
even after the missing handler is registered. Recovery requires manual SQL. The
missing-handler decision frames parking as the resilience mechanism but ships no
re-drive. The plan defers retry to M4, but park-without-recovery is a live
operational dead-end, not a deferred feature.

- Invariant: any durable receive state must have a documented, in-library path
  back to dispatchable, or be an explicitly terminal/DLQ state.
- Test: seed a missing-handler row → dispatch (parks Retryable/NULL) → register
  the handler → `Replay::received` → assert it does not pick up the parked row
  (Processed-only filter) and a subsequent `dispatch_once` claims 0 rows.
- Design forced: either a `Replay::received` variant that targets `Retryable`, or
  a documented operational query, before M3 spec promotion.

### F5. `Replay::received` silently re-executes side effects on already-processed rows

`replay_received_update_sql` (lines 951-973) selects `status = Processed` rows,
resets them to `Pending`, clears `processed_at`, and sets `next_attempt_at =
NULL` → claimable. On the next dispatch the handler runs **again**. Receive
idempotency is enforced only at `insert_received` (the `ON CONFLICT`), not at
handler execution. So replaying a received message re-applies its business effect
with zero dedup unless the handler is itself idempotent. For outbox replay this
is benign at-least-once publish; for receive it is a duplicate business write.
Nothing in the replay or identity decisions warns about this asymmetry, and J3's
idempotency reasoning explicitly does not cover the operator-initiated replay
path.

- Invariant: the effective-once promise must state clearly whether operator
  replay is exempt; if so, the hazard must be documented at the call site.
- Test: process a received row to `Processed` with a non-idempotent handler
  (e.g. `INSERT`), `Replay::received`, dispatch, assert the business row count —
  it will be 2.

### F6. Failure states are conflated; no operator-distinguishable cause

Missing-handler, undeserializable-stored-payload, and transient handler failure
all land in the identical `Retryable, next_attempt_at = NULL` shape with an entry
in the `errors` ring. `Processing`/`Failed` are reserved for M4 (K4). Combined
with F4, an operator cannot distinguish "stuck on config/deploy" from "awaiting
M4 retry," and has no terminal state to move rows to. Accepted by the decisions,
but the operational consequence (F4) was not drawn out.

## Plan-accuracy challenges

The 2026-06-22 Progress note claims the slice landed "through the Phase 4
Postgres/Redpanda gates." Two gates are not met by shipped code:

- **Phase 1A.4 — two real `dispatch_once` workers coordinated by the Phase 0
  primitive.** The landed `failure_recording_holds_row_lock_until_retryable_commit`
  (`tests/durable-send/tests/durable_receive.rs:389-497`) uses one real
  `dispatch_once` task plus manual SQL simulating the winning worker (lines
  438-476). The Phase 0 controlled-interleaving primitive
  (`dispatch-test-interleaving-primitive.decision.md`) does not exist in
  `kafkaman-test`; there are no pause-points or barriers anywhere. A1 is therefore
  still the narrow regression Phase 1A.4 was meant to replace.
- **Phase 4.3 — consume-then-produce full-loop with redelivery (J3).** The
  redpanda test `full_loop_ingests_from_redpanda_and_dispatches_received_row`
  (`tests/durable-send/tests/redpanda_full_loop.rs:128-194`) ingests once and
  dispatches once, receive-only. There is no broker-level duplicate/redelivery and
  no handler-enqueues-outbox over the broker. J1/J2 are Postgres-only; J3 is still
  `Proposed` in the matrix.

Neither is a code bug, but the plan's Progress note overclaims relative to the
catalog matrix (which lists J3 `Proposed` and A1 "Landed narrow; expand"). The
plan status should be corrected: Phase 0, A1-real, and J3 remain open.

## LOW / confirmations

- **G3 holds by construction**: `tx.commit()` strictly precedes
  `commit_message(offset)` in `ingest_once`, and no code path commits the offset
  first. True but untested (no hook exists that could invert it).
- **No backoff / circuit-breaker** in `run` or `run_dispatcher`: a permanent error
  (e.g. a missing table) logs every `poll_interval` forever. Known pattern; worth a
  bounded-error decision.
- `claim_received_row` orders by `created_at` with **no `message_id` tiebreaker**
  (replay has one). Harmless under `SKIP LOCKED`, but inconsistent.
- User headers pass through `String::from_utf8_lossy` (silent corruption on
  invalid UTF-8); duplicate header names resolve last-wins for user headers but
  first-wins for `kafkaman-*` (`user_headers` vs `header_value`). I3 territory.

## Follow-up verification addendum

Manual double-check against the current worktree confirms the core review result:
F1-F6 are real gaps, and the plan Progress note overclaims relative to shipped
tests. The following clarifications tighten the scope without changing the
priority of the findings.

- **F1 precision:** deterministic poison records are the production-risk case.
  Transient database errors before offset commit should not be grouped with
  poison input; holding the broker offset until the database write succeeds is
  the intended durable behavior.
- **F1 precision:** invalid UUID parsing applies to the message, correlation,
  and causation metadata headers, not every `kafkaman-*` header. The
  `kafkaman-idempotency-key` header is required but stored as a string.
- **F2 precision:** ordinary external producers without a `kafkaman-message-id`
  get a fresh random `message_id` on each consume. The practical primary-key
  stall is a conflicting or reused kafkaman message id with a different
  idempotency key.

Additional gaps found during the double-check:

- **F7. Case-variant reserved headers poison ingest.** `user_headers` filters
  only lowercase `kafkaman-`, but `reserved_header` rejects that namespace
  case-insensitively during `insert_received`. A consumed record containing
  `Kafkaman-Message-Id` is copied as a user header, then rejected before offset
  commit, producing another F1-style partition stall.
- **F8. Ingest source topic provenance is inferred, not checked.**
  `ingest_once::<P>` does not verify that the consumed Kafka record's topic
  matches `P::TOPIC`, and `insert_received` persists `source_topic` from the
  message descriptor rather than the broker record. This is harmless in the
  current single-topic clean-path tests but weak if a consumer is subscribed to
  the wrong topic or multiple topics.
- **F9. Graceful shutdown mid-dispatch is not actually landed.** `run_dispatcher`
  observes the cancellation token only after `dispatch_once` returns. The
  shipped dispatcher test cancels after the handler finishes, so it proves
  between-cycle shutdown, not cancellation while a handler transaction is open.
  The proposal matrix row H3 should not be `Landed` until a mid-dispatch test
  exists or the contract is narrowed.

The LOW header notes also need a small correction: duplicate non-reserved user
headers are last-wins in the `BTreeMap`, while duplicate reserved kafkaman
metadata is first-wins through `header_value`; invalid UTF-8 is silently lossy
decoded rather than rejected.

## Recommended next steps

Highest-value new tests, in order: F1/F7 (ingest poison stall, including
case-variant reserved headers), F4 (parked-row unrecoverable), F5 (replay
re-executes effects), F3 (no drain), F2 (PK/conflict stall), F8 (topic
provenance mismatch), and F9/H3 (mid-dispatch shutdown). F1/F7 and F4 are the
production-biting cases outside the current catalog.

1. Add F1/F2/F4 as a new catalog class (proposed Class **Q — Ingest poison and
   recovery**) with matrix rows, and add F3/F5 rows to the existing classes, in
   `wiki/proposals/05-deep-durability-testing.proposal.md`.
2. Correct the plan's Progress note and reopen Phase 0, A1-real, and J3.
3. Write failing tests for F1, F4, and F5 before deciding fixes, so the defects are
red before the contract changes.

## Implementation Follow-up - 2026-06-22

The review findings above were fixed in the follow-up implementation slice.

- **F1/F7 fixed:** `RdkafkaConsumer::ingest_once` now separates deterministic
  malformed broker records from transient DB failures. Missing payload,
  missing idempotency key, invalid typed payload, invalid reserved UUID header,
  case-variant reserved metadata headers, and unexpected topic records are
  skipped by committing the Kafka offset and returning `IngestStats.skipped =
  1`; SQL insert/commit errors still hold the offset.
- **F2 fixed:** `insert_received` now uses untargeted `ON CONFLICT DO NOTHING`,
  so a reused `kafkaman-message-id` with a different idempotency key is treated
  as a duplicate/conflict instead of surfacing as a transient DB failure.
- **F3 fixed:** `run_dispatcher` drains immediately while `dispatch_once`
  claims rows, instead of sleeping for `poll_interval` after each row.
- **F4/F5 fixed:** `Replay::received` now targets parked
  `Retryable / next_attempt_at IS NULL` rows only. It no longer selects
  `Processed` rows, so the default replay path does not re-run handler side
  effects.
- **F6 fixed:** receive failure ring entries now include a structured
  `ReceivedFailureKind` (`MissingHandler`, `InvalidPayload`, `Infrastructure`,
  `Handler`) with backward-compatible defaulting for older JSON entries.
- **F8 fixed:** `ingest_once::<P>` verifies the broker record topic equals
  `P::TOPIC`; unexpected-topic records are skipped and are not persisted under
  the descriptor topic.
- **F9/H3 fixed:** dispatcher shutdown is now tested while a handler is still
  in flight. The loop finishes the current dispatch, observes cancellation
  before draining the next row, and exits.

Evidence:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests` - 35
  tests passed.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features
  redpanda --test redpanda_full_loop -- --test-threads=1` - 4 tests passed.
- `rtk git diff --check` - passed.

Remaining plan-level caveat: these fixes close the review defects, but they do
not by themselves complete the broader M3 closure gates. Phase 0 controlled
interleaving, real two-worker A1 coverage, G1/G2 crash/offset-uncertainty
hooks, Phase 4 consume-then-produce Redpanda duplicate J3, handler-model
reconciliation, and M3 spec promotion remain plan work.

## Quarantine Policy Follow-up - 2026-06-22

The subsequent review correctly observed that the F1-F5 fixes moved risk from
stalling to committed drops. The follow-up implementation addresses those
G-series concerns:

- **G1/G2 fixed:** deterministic ingest skips are written to
  `<schema>.received_ingest_failures` before offset commit, and repeated
  deserialize/schema failures trip `ConsecutiveSkipLimitExceeded` without
  committing the breaker record's offset.
- **G3 fixed:** receive insert now exposes `ReceivedInsertOutcome`, separating
  normal idempotency redelivery from `MessageIdConflict`; rdkafka ingest
  quarantines message-id conflicts before committing.
- **G4 fixed:** handler-returned SQL errors are classified as `Handler`;
  infrastructure SQL errors from dispatch marking remain `Infrastructure`.
- **G5 fixed:** `Replay::received` preserves attempts and error history while
  unparking retryable rows.

Evidence:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests` - 37
  tests passed.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features
  redpanda --test redpanda_full_loop -- --test-threads=1` - 5 tests passed.
- `rtk cargo test --workspace --all-features` - 61 tests passed.
