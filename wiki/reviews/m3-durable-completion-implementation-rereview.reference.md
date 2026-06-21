# M3 Durable Completion Implementation Re-Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-22
- Category: Code review
- Scope: Second-pass review after the 2026-06-22 review-fix slice that addressed
  F1–F5 from `m3-durable-completion-implementation-review.reference.md`. Confirms
  each prior finding against the landed code and tests, then evaluates the failure
  modes the fixes themselves introduce — principally the shift from *partition
  stall* to *silent drop* on the ingest path. Static review; the Postgres/Redpanda
  suites were read, not executed here.
- Sources:
  - `wiki/reviews/m3-durable-completion-implementation-review.reference.md`
  - `wiki/compatibility/m3-durable-receive-review-fix-api.compat.md`
  - `crates/kafkaman-core/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-rdkafka/src/lib.rs`
  - `crates/kafkaman-worker/src/lib.rs`
  - `tests/durable-send/tests/durable_receive.rs`
  - `tests/durable-send/tests/redpanda_full_loop.rs`
- Related:
  - `wiki/reviews/m3-durable-completion-implementation-review.reference.md`
  - `wiki/proposals/05-deep-durability-testing.proposal.md`

## Verdict

All five prior findings are genuinely fixed and pinned by tests. The seam remains
correct. The risk has moved, not disappeared: the ingest fixes resolve the stalls
by **committing the offset and dropping the record**, and the dedup fix resolves
the conflict crash by **swallowing any uniqueness violation as a duplicate**. Both
are correct for true poison and true duplicates, but they also silently discard
data in two realistic non-poison situations — a breaking producer-side schema
change (G2) and a `message_id` collision between two distinct logical messages
(G3). For a system whose thesis is a durable execution ledger, a skipped record
leaves **no durable trace at all** (G1). These are the findings worth testing
against next; none is a regression of the seam, but G2 in particular can turn a
deploy-ordering mistake into silent topic-wide data loss.

## Prior findings — confirmed closed

- **F1 (ingest poison stall) — CLOSED.** `ingest_once`
  (`crates/kafkaman-rdkafka/src/lib.rs:164-211`) now classifies deterministic
  failures via `Error::is_deterministic_ingest_skip` (line 215), commits the
  offset, and returns `skipped: 1`. Transient DB errors still propagate without
  committing the offset (correct — those must retry). Pinned by
  `ingest_skips_poison_record_then_deduplicates_redelivery` and
  `ingest_skips_records_from_unexpected_source_topic`.
- **F2 (PK vs conflict-target stall) — CLOSED.** `insert_received` uses untargeted
  `ON CONFLICT DO NOTHING` (`crates/kafkaman-sqlx/src/lib.rs:1549`); any uniqueness
  violation now resolves to `inserted = false` instead of an uncaught error. See
  residual G3 for the cost of the untargeted form.
- **F3 (dispatcher does not drain) — CLOSED.** `run_dispatcher`
  (`crates/kafkaman-worker/src/lib.rs:122-166`) now loops without sleeping while
  `claimed > 0` and only sleeps when a cycle claims nothing; shutdown is checked at
  the top of every iteration. Pinned by
  `dispatcher_loop_processes_due_rows_and_stops_on_cancellation` and
  `dispatcher_loop_finishes_in_flight_dispatch_before_shutdown`.
- **F4 (parked rows unrecoverable) — CLOSED.** `replay_received_filter_sql`
  (`crates/kafkaman-sqlx/src/lib.rs:991-1008`) now targets `status = Retryable AND
  next_attempt_at IS NULL` — exactly the parked state — and resets to `Pending`.
  Parked rows are re-drivable in-library. Pinned by
  `replay_received_unparks_retryable_rows_without_replaying_processed_rows`.
- **F5 (replay re-executes processed rows) — CLOSED.** The same filter change means
  replay no longer touches `Processed` rows, so it cannot re-run a completed
  handler's side effects. Same test asserts the `Processed` row is untouched.
- **F6 (failure-cause conflation) — PARTIALLY CLOSED.** `ReceivedError.kind`
  (`ReceivedFailureKind`, `crates/kafkaman-core/src/lib.rs:341-356`) now records
  `MissingHandler / InvalidPayload / Infrastructure / Handler`, giving operators a
  triage signal. See residual G4 — the classification is derived from the Rust
  error variant, not the failure domain, so it still mis-buckets some cases. There
  is still no terminal `Failed`/DLQ state (deferred to M4 per K4).

## New / residual findings

### G1. Deterministic ingest skip is silent, irreversible data loss (MEDIUM)

A skipped record has its Kafka offset committed and is never written anywhere
(`crates/kafkaman-rdkafka/src/lib.rs:171-186`). There is no dead-letter row, no
quarantine table, and no log line on the skip path — the only signal is the
in-memory `IngestStats.skipped` returned to the caller. After the offset advances
the record is unrecoverable. `ingest_skips_poison_record_then_deduplicates_redelivery`
asserts exactly this (skip, commit, no row) and moves on, locking in the
drop-without-trace behavior. For a durable-ledger system this is the sharpest
gap: the one path that destroys data keeps no durable evidence it happened.

- Invariant: a record kafkaman chooses to drop must leave a durable, queryable
  trace (dead-letter row or quarantine) so the loss is auditable and recoverable.
- Test: ingest a poison record, then assert some durable artifact exists (today
  none does); assert an operator can enumerate dropped records after the fact.
- Design forced: a dead-letter/quarantine sink for ingest skips, symmetric to the
  dispatch-side park decision.

### G2. Schema skew is treated as poison → silent topic-wide drop (HIGH)

`record_envelope` deserializes strictly into `P` (`from_slice::<P>`, line 240) and
classifies any `serde` failure as a deterministic skip (line 222). serde ignores
unknown fields, so additive producer changes are safe — but a breaking change
(renamed/removed required field, retyped field), or simply deploying the producer
before the consumer, makes **every** new record fail to deserialize and be
skipped with its offset committed. The result is silent, topic-wide data loss
during a bad deploy ordering. The pre-fix behavior (stall) was safer for data: it
halted loudly and preserved the records until the mismatch was fixed. The fix
optimized for "one bad record can't block the partition" but applied the same
policy to "every record is currently unreadable," which are operationally
opposite situations.

- Invariant: a sustained inability to deserialize a topic must surface as a loud,
  halting condition, not as steady-state silent skipping.
- Test: ingest N consecutive records that fail to deserialize into `P`; assert
  kafkaman does not silently commit past all of them (e.g. a poison-rate threshold
  halts ingest, or each lands in a dead-letter). Today all N are dropped.
- Design forced: distinguish "isolated corrupt record" from "schema mismatch
  across the stream" — a consecutive-skip threshold, a dead-letter requirement, or
  a circuit-breaker on the skip path.

### G3. Untargeted `ON CONFLICT` silently drops a *different* logical message (MEDIUM)

The F2 fix makes `insert_received` swallow any uniqueness violation as a
duplicate. For the idempotency-key index this is correct dedup. But the table also
has a `message_id` PRIMARY KEY, and the two constraints now collapse to the same
"duplicate" outcome. If two distinct logical messages (different `idempotency_key`,
different payload) collide on `message_id` — a producer reusing the
`kafkaman-message-id` header, or an external producer — the second is silently
dropped (`inserted = false`, offset committed) and is indistinguishable in
`IngestStats` from a legitimate idempotency-key redelivery (both report
`duplicates += 1`). The compat note documents this, but it converts a detectable
identity bug into silent loss. The catalog's I2 (same offset, different
idempotency key) is the mirror of this and is still `Proposed`.

- Invariant: a uniqueness conflict on `message_id` with a *different*
  `idempotency_key` is an identity anomaly, not a benign duplicate; it must be
  observable (distinct stat, error, or dead-letter), not folded into the dedup
  count.
- Test: insert two rows sharing `message_id` and differing `idempotency_key`;
  assert the conflict is reported distinctly from an idempotency-key duplicate.

### G4. Failure-kind is derived from the error variant, not the failure domain (LOW–MEDIUM)

`received_failure_kind` (`crates/kafkaman-sqlx/src/lib.rs:1649-1657`) maps
`Error::Sqlx(_) → Infrastructure`. A handler that performs a business write and
hits a permanent constraint violation (unique/check) surfaces that as
`Error::Sqlx` via `?`, so a never-succeeds business-poison failure is recorded as
`Infrastructure`. Combined with M3's "park, no auto-retry" model, an operator
triaging by `kind` reads `Infrastructure` as transient and waits, when the row in
fact requires intervention. The new `kind` field exists precisely for triage, so
this mis-bucketing undercuts its purpose. Conversely, a genuinely transient infra
blip during `mark_received_processed` parks the row needing manual replay — the
row most likely to succeed on retry is the one requiring manual action (acceptable
under documented M3 scope, but worth noting alongside the taxonomy).

- Invariant: `kind` should reflect whether the failure is transient-retryable or
  permanent-needs-intervention, which the Rust error variant alone does not encode.
- Test: a handler that triggers a business unique-violation; assert the recorded
  `kind` is not `Infrastructure` (or document that `Sqlx`-from-handler is
  intentionally `Infrastructure`).

### G5. Replay erases the forensic history of exactly the rows it targets (LOW)

`replay_received_update_sql` (lines 951-973) resets `attempts = 0` and
`errors = '[]'`. Now that replay targets parked failed rows, it wipes the error
ring and attempt count that are the operator's only record of *why* the row
failed. A row that fails again after replay looks like a first-time failure. This
mirrors outbox replay, but outbox replay targeted published rows with no such
history; receive replay lands on the rows where the history matters most.

- Invariant: re-driving a failed row should preserve (or archive) its prior error
  history for forensics.
- Test: replay a parked row with a non-empty `errors` ring; assert the prior
  errors remain queryable somewhere after replay.

## Scope note

There is still no resilient `run_ingest` loop in `kafkaman-worker` (only `run` and
`run_dispatcher`); ingest is driven by test code calling `ingest_once` directly.
Consequently `IngestStats.skipped` — the sole signal that data was dropped (G1) —
has no production consumer yet. The plan's Phase 0 interleaving primitive and the
real two-worker A1 test also remain absent; the A1 regression is still the
one-real-worker + manual-SQL form, and J3 broker-level duplicate-after-
consume-then-produce is still not present in the Redpanda suite.

## Recommended next steps

Highest-value, in order: G2 (schema-skew silent mass drop), G1 (dead-letter for
ingest skips), G3 (identity-anomaly observability), then G4/G5. G2 and G1 are the
production-biting pair: together they mean a single bad deploy can silently
discard a topic with no recoverable record. Suggest a single decision —
`ingest-poison-and-dead-letter-policy.decision.md` — covering skip-vs-quarantine,
a consecutive-skip circuit breaker, and whether `message_id` conflicts are
anomalies, plus a new catalog class for the silent-loss tests.

## Resolution Follow-up - 2026-06-22

The recommended policy and tests were implemented.

- **G1/G2 closed:** deterministic ingest skips now write a durable
  `<schema>.received_ingest_failures` quarantine row before committing the Kafka
  offset. Repeated deserialize/schema failures trip
  `ConsecutiveSkipLimitExceeded`; the breaker row is quarantined but its offset
  is not committed. Pinned by
  `ingest_circuit_breaker_stops_committing_repeated_schema_failures` and the
  quarantine assertions in
  `ingest_skips_poison_record_then_deduplicates_redelivery`.
- **G3 closed:** `insert_received_with_outcome` returns
  `ReceivedInsertOutcome::{Inserted, DuplicateIdempotencyKey, MessageIdConflict}`.
  rdkafka ingest quarantines message-id conflicts as `MessageIdConflict`;
  storage behavior is pinned by
  `receive_insert_reports_message_id_conflict_separately_from_redelivery`.
- **G4 closed:** handler-returned SQL errors are classified as
  `ReceivedFailureKind::Handler`; SQL errors from dispatch marking remain
  infrastructure. Pinned by
  `handler_sql_constraint_error_is_recorded_as_handler_failure`.
- **G5 closed:** `Replay::received` preserves attempts and error history while
  unparking retryable rows. Pinned by
  `replay_received_unparks_retryable_rows_without_replaying_processed_rows`.

Policy record: `wiki/decisions/ingest-poison-quarantine-policy.decision.md`.

Evidence:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests` - 37
  tests passed.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features
  redpanda --test redpanda_full_loop -- --test-threads=1` - 5 tests passed.
- `rtk cargo test --workspace --all-features` - 61 tests passed.
