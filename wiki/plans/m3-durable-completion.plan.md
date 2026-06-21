# M3 Durable Completion Implementation Plan

Document Class: Plan
Status: Completed
Date: 2026-06-22
Category: Delivery execution
Scope: Sequenced completion of M3 after the first durable-receive slice and its
  review fixes. Hardens the `dispatch_once` seam against the deep-durability test
  catalog, then builds the operational surface (replay, injected clock), the
  Kafka ingest engine and dispatcher loop, the decision-gated handler surface,
  and finally chaos/model coverage and spec promotion. Test designs lead;
  implementation follows each gate.
Sources:
- wiki/plans/m3-durable-receive.plan.md
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/decisions/message-identity-and-header-namespace.decision.md
- wiki/decisions/retry-backoff-dlq-policy.decision.md
- crates/kafkaman-sqlx/src/lib.rs
- crates/kafkaman-worker/src/lib.rs
- crates/kafkaman-rdkafka/src/lib.rs
Related:
- wiki/specs/m1-durable-send.spec.md
- wiki/specs/m2-change-engine-config.spec.md
- wiki/specs/m3-durable-receive.spec.md
- wiki/roadmaps/path-to-v1.roadmap.md

## Deliverable

The first M3 slice proved `dispatch_once()` as an effective-once seam against
test-seeded rows. This plan turns that seam into a runnable, hardened durable
receive subsystem: a Kafka ingest path that writes a durable row before
committing the broker offset, a dispatcher loop that drains due rows mirroring
the send-side relay, operational replay, and a handler surface sufficient for
consume-then-produce choreography — all backed by the adversarial test catalog in
`05-deep-durability-testing.proposal.md` and promoted into an M3 spec when green.

## Strategy: Tests Lead, Implementation Follows

The M3 review's H1 hole was found by reasoning about interleavings, not by a
failing test. The deep-testing proposal predicts that several of its highest
priority designs reveal **real defects in the code that already exists**
(`dispatch_once`), and that several others **force design decisions that shape
the ingest contract**. Therefore the seam is hardened and its contracts decided
*before* an engine is wrapped around it. Within every phase, the gap-revealing
or contract-pinning test is written first; the implementation change and its
decision record follow.

## Decision Recording

Each design choice forced by a Phase 1–4 test is recorded as its own
`wiki/decisions/*.decision.md`, consistent with the existing decision corpus, and
back-linked from the phase that forced it. The plan's phase notes summarize the
outcome but the decision doc is the source of truth.

## Phases

### Phase 0 — Test scaffolding and oracle hygiene

Cheap, immediate, unblocks faithful Phase 1 tests.

1. Add a test-only controlled-interleaving primitive to `kafkaman-test` behind a
   test feature. Decision required (proposal open question): channel barriers vs
   injected pause-points vs explicit two-step claim/complete. Recommended: a
   minimal injected pause-point hook on the dispatch path, because the proposal
   notes manual SQL simulation (as used in the current A1 regression) does not
   faithfully represent two real `dispatch_once` workers.
   - Decision doc: `dispatch-test-interleaving-primitive.decision.md`.
2. P1: fix the brittle exact-`processed_at` oracle
   (`dispatch_once_commits_handler_effect_and_processed_status`) — truncate the
   injected timestamp to microseconds before dispatch, or assert with a
   microsecond tolerance, so the test cannot flake on sub-microsecond platform
   precision Postgres `TIMESTAMPTZ` cannot store.

### Phase 1 — Harden the `dispatch_once` seam (test-first, gap-revealing)

Split into must-fix blockers (Phase 1A) that change the `dispatch_once` contract
the Phase 3 loop will depend on, and invariant pins (Phase 1B) that record or
confirm behavior but do not block Kafka ingest. Each item: write the test,
observe the gap, decide, fix, regression-pin. **Phase 1A is a hard gate for
Phase 3; Phase 1B may run in parallel with or after Phase 3.**

#### Phase 1A — Blockers before Kafka ingest

1. **C1 (missing-handler head-of-line blocking):** an old unregistered-type row
   ordered first by `created_at` blocks all newer valid rows because
   `MissingHandler` returns `Err` before any parking. Under a polling loop this
   is catastrophic, so it must be fixed before Phase 3. Decide MissingHandler
   policy (park the row / move terminal / stop the scheduler) and fix so one bad
   row cannot stall the loop.
   - Decision doc: `missing-handler-dispatch-policy.decision.md`.
2. **C3 (poisoned transaction after `Ok`):** handler swallows a failed SQL
   statement and returns `Ok(())`; `mark_received_processed` then fails on the
   aborted transaction. Predicted gap: the success branch's `?` returns `Err`
   without entering failure accounting. Decide infra-error-after-`Ok`
   classification (route to failure accounting vs scheduler-fatal vs distinct
   operational class) and fix.
   - Decision doc: `dispatch-infrastructure-error-classification.decision.md`.
3. **M3-stats (suppressed stale accounting over-reporting):** after the H1 guard
   no-ops, `DispatchStats.failed` still returns 1 though nothing durable changed.
   The loop's logging/metrics depend on this contract, so settle it before the
   loop lands. Decide whether `failed` counts attempted handler failures or
   durable failure records, and align the stats contract (likely use
   `rows_affected` from `record_received_failure`).
   - Decision doc: `dispatch-stats-semantics.decision.md`.
4. **A1 (real workers):** replace the manual-SQL H1 simulation with two real
   `dispatch_once` workers coordinated by the Phase 0 primitive — fail-then-
   success on one row, exactly one effect, one `Processed`. Proves the seam is
   concurrency-safe before a loop runs many dispatchers against it.

Phase 1A gate output: a contract-clear, concurrency-safe `dispatch_once` whose
MissingHandler, infra-error, and stats contracts are decided and pinned.

#### Phase 1B — Invariant pins (record or defer; not a Phase 3 blocker)

5. **B2 (crash in failure-accounting window):** handler fails, rollback, process
   dies before `record_received_failure`. Decide whether receive `attempts` is
   crash-durable or explicitly best-effort, and pin the safety invariant (no
   business effect, row reclaimable).
   - Decision doc: `receive-attempts-crash-durability.decision.md`.
6. **Atomicity confirmations:** D1 torn-commit reader, D2 multi-table rollback,
   D3 status-guard-asymmetry invariant (documented, not just SQL). These are
   expected to pass by design; the value is the regression pin and the
   documented invariant.
7. **Resource / identity / schema pins:** E2 pool exhaustion under the
   held-transaction model, I1 same-key/different-payload collision policy, K4
   reserved-state (`Processing`/`Failed`) never written in M3, K5
   `message_version` hard-coded-`1` decision.
   - Decision docs as forced: `idempotency-collision-policy.decision.md`,
     `received-message-version-contract.decision.md`.

### Phase 2 — Operational surface (Postgres-only, no Kafka)

Self-contained and fully testable without a broker; lands the deterministic clock
the retry/replay work needs.

1. Injected Harness `Clock` for due-row and error-boundary tests; pins F1
   due-boundary/retry-window correctness and strengthens F2 (parked rows not
   auto-redriven).
2. `Replay::received::<T>` using the M2 operational changeset-guard pattern,
   mirroring the send-side replay surface.

### Phase 3 — Kafka ingest and dispatcher loop (the engine)

Turns the seam into a runnable subsystem. **Gated on Phase 1A** (the loop must
not wrap a seam with the C1/C3/M3-stats holes). Identity contract decided first.
Scope note: this phase proves effective-once for the receive→dispatch→business-
effect path only; consume-then-produce atomicity (handler enqueues outbox in the
receive transaction) is proven in Phase 4, not here.

1. **Ingest identity decision (gate):** I2 same-source-offset/different-
   idempotency-key and G3 forbidden ordering. Decide whether Kafka
   topic/partition/offset is a uniqueness guard in addition to application
   `idempotency_key`, and prove the offset-before-row ordering is unreachable.
   - Decision doc: `kafka-ingest-identity-and-ordering.decision.md`.
2. Add a `StreamConsumer` to `kafkaman-rdkafka` (currently producer-only).
3. Ingest path: consume record → `insert_received` (dedup) → commit offset, with
   the durable insert strictly before the offset commit.
4. Receive dispatcher `run()` loop mirroring the relay `run()` in
   `kafkaman-worker`: poll-interval drain of due rows, cycle-failure resilience,
   `CancellationToken` shutdown.
5. Tests: G1 crash between insert and offset commit (re-consume dedups, no loss),
   G2 offset-commit uncertainty, H2 task cancellation mid-transaction, H3
   graceful shutdown mid-dispatch, L1 oldest-locked-row does not block younger
   rows, and a full-loop Redpanda/testcontainers test proving **receive-only**
   effective-once (ingest→dispatch→business effect; no handler-side enqueue).

### Phase 4 — Handler surface (decision-gated)

1. **Keep-or-extend decision (gate):** the consume-then-produce class (J1–J3)
   needs a handler that can call the public enqueue helper inside its receive
   transaction; today's `Fn(&mut PgConnection, ReceivedMeta, P)` can issue raw
   SQL but not the public `enqueue`. Decide: keep the closure + `ReceivedMeta`
   model and add a minimal `Rx`/transaction handle, vs commit to the full
   `FromMessage`/`Rx`/state + Tower `Service`/`Layer` stack.
   - Decision doc: `receive-handler-surface-scope.decision.md`.
2. Implement the chosen scope; land J1 (consume success enqueues outbox
   atomically), J2 (failure rolls back business + outbox), J3 (duplicate
   redelivery, no second effect or outbox row) and the downstream-enqueue
   idempotency-key policy J3 forces.
3. Full-loop consume-then-produce gate: a Redpanda/testcontainers test that
   consumes a record, enqueues a follow-up outbox message inside the receive
   transaction, redelivers/duplicates the input, and proves no duplicate business
   row and no duplicate outbox row. This is the consume-then-produce counterpart
   to Phase 3's receive-only full-loop gate and is required before spec promotion.
4. Sugar last: `#[derive(KafkaMessage)]` and `#[kafkaman::test]` over the working
   explicit APIs.

### Phase 5 — Chaos, model checking, and spec promotion

1. O1 database-backed state-machine property test (`proptest`, deterministic
   seed) over insert/claim/success/failure/panic/rollback/replay/clock/restart.
2. Deferred/nightly: O3 proxy chaos harness, H1 ambiguous-commit (Toxiproxy/
   netem), M1/M2 observability and payload-safety, N1–N3 send-side mirrors.
3. Promote validated M3 behavior into `wiki/specs/m3-durable-receive.spec.md`.

## Verification Gates

- Every phase: `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `cargo test --workspace --all-features`.
- Phase 1A (gate for Phase 3): C1, C3, and M3-stats tests each fail before their
  fix and pass after; the real-worker A1 regression passes; the MissingHandler,
  infra-error, and stats-semantics decision docs are committed.
- Phase 1B: each pin test passes (or its deferral is recorded in the proposal's
  coverage matrix); each forced decision (B2 attempts, idempotency collision,
  message_version) has a committed decision doc.
- Phase 2: F1 due-boundary/retry-window test passes against the injected clock
  with no inlined `now()` on the dispatch path; `Replay::received` is verified for
  dry-run (no writes), apply, audit output, the M2 operational changeset
  guardrail, and idempotence (a second replay is a no-op).
- Phase 3: the receive-only full-loop Redpanda test proves ingest→dispatch
  effective-once for the business effect; G1/G2 ingest crash/uncertainty
  reconverge to one row; G3 offset-before-row ordering is demonstrably
  unreachable. This gate does not assert consume-then-produce.
- Phase 4: the consume-then-produce full-loop test proves no duplicate business
  or outbox row under redelivery; the handler-surface-scope decision is committed.
- Phase 5: spec promotion only after the Postgres-only suite, the Phase 3
  receive-only full-loop, and the Phase 4 consume-then-produce full-loop are all
  green, and the central `message-consumption-and-handler-model` decision is no
  longer Draft (see Closure Criteria).

## Evidence To Record

- Update the proposal's coverage matrix status column whenever a test lands or
  reveals a defect; fold real defects back into the M3 review and the relevant
  spec/decision.
- Record each phase's proof commands and outcomes in Progress.

## Progress

- 2026-06-22: Plan created. Predecessor `m3-durable-receive.plan.md` first slice
  and its review fixes (H1/M1/M2/M3-gates/L1/L2/L3) are landed; this plan
  sequences the remaining completion work test-first.
- 2026-06-22: Implemented the first completion slice through the Phase 4
  Postgres/Redpanda gates: MissingHandler rows are parked instead of blocking
  younger rows; poisoned post-handler transactions are recorded as durable
  receive failures; `DispatchStats.failed` now counts durable failure records;
  `Replay::received` supports guarded dry-run/apply/idempotence; the receive
  dispatcher loop drains due rows until cancellation; `RdkafkaConsumer` ingests
  records with durable insert before offset commit; handlers can enqueue outbox
  messages through `enqueue_on_connection` inside the receive transaction. Proof:
  `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`; `cargo test --workspace --all-features`;
  `cargo test --manifest-path tests/durable-send/Cargo.toml --tests`;
`cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda
--test redpanda_full_loop`. Remaining closure work: Phase 1B deferrals,
chaos/model checking, central handler-model reconciliation, and M3 spec
promotion.
- 2026-06-22: Follow-up review-fix slice closed F1-F9 from
  `m3-durable-completion-implementation-review.reference.md`: deterministic
  ingest poison skips with offset commit, broad receive insert conflict
  deduplication, dispatcher backlog drain, mid-dispatch shutdown proof,
  retryable-only `Replay::received`, structured receive failure causes, and
  Redpanda poison/redelivery/topic-provenance coverage. This does not close the
  broader plan gates for Phase 0 controlled interleaving, Phase 4
  consume-then-produce Redpanda duplicate J3, handler-model reconciliation, or
  M3 spec promotion.
  Proof: `rtk cargo test --manifest-path tests/durable-send/Cargo.toml
  --tests`; `rtk cargo test --manifest-path tests/durable-send/Cargo.toml
  --features redpanda --test redpanda_full_loop -- --test-threads=1`;
  `rtk git diff --check`.
- 2026-06-22: Follow-up ingest poison policy slice added durable ingest
  quarantine, a consecutive-skip circuit breaker, explicit receive insert
  outcomes for idempotency duplicate vs message-id conflict, handler-domain SQL
  failure classification, and replay history preservation. Proof:
  `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`;
  `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features
  redpanda --test redpanda_full_loop -- --test-threads=1`; `rtk cargo test
  --workspace --all-features`.

## Wiki Updates

- Update this plan as phases complete; update `wiki/index.md` and `wiki/log.md`
  for every wiki mutation.
- Create decision docs as Phase 1–4 gates force choices; back-link them here.
- Reconcile the central `message-consumption-and-handler-model.decision.md` (now
  `Status: Draft`) before spec promotion: accept it once Phase 4 settles the
  handler-surface scope, and supersede/update its Tower-handler-model section if
  Phase 4 chooses the closure + `ReceivedMeta` + minimal `Rx` model instead. The
  M3 spec must not rest on a Draft decision.
- Promote validated behavior into `wiki/specs/m3-durable-receive.spec.md` at
  closure.

- 2026-06-22: Production ingest runner slice added
  `RdkafkaConsumer::run_ingester::<P>` with `CancellationToken` shutdown,
  transient Kafka/SQL retry backoff, aggregate `IngestLoopStats`, and loud
  propagation of `ConsecutiveSkipLimitExceeded` without committing the breaker
  record. Redpanda gates prove normal loop ingest until cancellation and runner
  stop/redelivery on repeated schema poison. This does not close Phase 4
  consume-then-produce J3, handler model reconciliation, or M3 spec promotion.
- 2026-06-22: G1/G2 ingest uncertainty slice added the `test-hooks` gated
  post-durable-write/pre-offset-commit hook, plus Redpanda tests proving a
  crash-window error and runner-level offset-commit uncertainty both redeliver
  through the same consumer group, deduplicate to the existing received row,
  and then commit the broker offset.
- 2026-06-22: A1 real two-worker dispatch interleaving slice added the
  `kafkaman-sqlx` `test-hooks` gated `before_record_failure` hook and a
  production-path `dispatch_once_with_hooks` regression. Worker A fails and
  rolls back, worker B processes the same row with a normal `dispatch_once`,
  then worker A resumes stale failure accounting; final state remains
  `Processed` with one business effect and no stale failure count.
- 2026-06-22: Phase 4 consume-then-produce slice extended the atomic handler
  test through J3. The closure + `ReceivedMeta` handler surface can perform
  business SQL and call `enqueue_on_connection` on the receive transaction;
  success commits business row + follow-up outbox, failure rolls both back, and
  duplicate input redelivery deduplicates without a second business effect or
  outbox row. Redpanda full-loop coverage proves ingest→dispatch→follow-up
  relay publish and duplicate input deduplication. `message-consumption-and-handler-model.decision.md`
  is accepted and reconciled with the shipped M3 handler surface.
- 2026-06-22: Promoted validated M3 behavior into
  `wiki/specs/m3-durable-receive.spec.md` and marked this implementation plan
  completed. The remaining O1/O3 chaos and model-checking candidates remain
  tracked in `wiki/proposals/05-deep-durability-testing.proposal.md` as
  post-M3 hardening, not as M3 closure gates.

## Closure Criteria

This plan closes when the `dispatch_once` seam is hardened against the priority
deep-testing catalog with all forced decisions recorded, the operational replay
and injected clock are landed, Kafka ingest commits offsets only after durable
writes, a dispatcher loop drains due rows under the Phase 3 receive-only full-loop
test, the handler surface supports consume-then-produce under the Phase 4
full-loop test, the central `message-consumption-and-handler-model` decision is
accepted (no longer Draft) and reconciled with the shipped handler surface, and
the validated behavior is promoted into an M3 spec.
