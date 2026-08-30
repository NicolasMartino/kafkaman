# M3 Durable Receive Implementation Plan

- Document Class: Plan
- Status: Completed
- Date: 2026-06-21
- Category: Delivery execution
- Scope: Tactical implementation sequence for M3 durable receive, deterministic dispatch, and receive-side toolkit maturity.
- Sources:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
- Related:
  - wiki/specs/m1-durable-send.spec.md
  - wiki/specs/m2-change-engine-config.spec.md
  - wiki/specs/m3-durable-receive.spec.md
  - wiki/roadmaps/path-to-v1.roadmap.md

## Deliverable

M3 proves the receive half of kafkaman's core promise: a Kafka message can be
durably written to Postgres, de-duplicated by message identity, dispatched
through the user handler stack from the durable row, and marked with a durable
outcome without letting handler health stall broker consumption.

## Dispatch Transaction Model

M3 implements the held-transaction row-lock model required by the receive
decision. `dispatch_once()` claims due rows with `FOR UPDATE SKIP LOCKED`, runs
the handler stack inside the same kafkaman-owned transaction, and commits the
handler's business writes plus kafkaman's `Processed` mark together.

Received rows do not get M1-style persistent claim/lease columns in M3. The row
lock is the claim. Dispatch never commits a durable `Processing` state; a crash
or process kill during dispatch rolls back the transaction, leaving the row in
its prior claimable state with no committed business effect.

This accepts a deliberate tradeoff: a dispatch worker holds one Postgres
connection and one row lock while user code runs. M3 controls the blast radius
with dispatch concurrency limits and handler timeouts. If a future milestone
wants a committed claim-column lease model, that is a separate durable decision
because it changes the effective-once transaction boundary.

On handler failure, the handler transaction rolls back. kafkaman then records
failure accounting in a short follow-up transaction: append to bounded `errors`,
increment `attempts`, set `status = Retryable`, and park the row by setting
`next_attempt_at = NULL`. Because the claim predicate only selects rows whose
`next_attempt_at` is due, M3 records the failure without hot-looping. M4 owns
the real retry/backoff schedule that turns parked `Retryable` rows back into
due rows.

All due-row predicates bind the current timestamp from kafkaman's injected
`Clock`; SQL must not inline `now()` for dispatch tests. Production may use a
system or database-backed clock implementation, but tests advance time by
controlling the bound timestamp.

## In Scope

- Per-type received tables created through the M2 change engine.
- Greenfield received-table identity with `idempotency_key NOT NULL` and a
  per-type unique constraint from creation.
- Dedup-as-log insert behavior using per-type unique `idempotency_key`.
- Receive row state machine needed by dispatch: `Pending`, `Processed`,
  `Retryable`, and `Failed`.
- Nullable `next_attempt_at` for parked M3 retryables; due rows use an injected
  `Clock` timestamp.
- Bounded `errors` JSONB ring that M4 retry/backoff/DLQ will reuse.
- Deterministic `dispatch_once()` seam for tests and scheduler internals.
- `MessageRouter` explicit registration API, `FromMessage` extractors, state,
  and receive transaction extraction.
- Tower `Service`/`Layer` integration for generic message middleware.
- `#[derive(KafkaMessage)]` as opt-in sugar over the explicit message trait.
- `#[kafkaman::test]` test macro as opt-in sugar over explicit Harness setup.
- `kafkaman-test` receive helpers: seed received rows, run `dispatch_once()`,
  handler `oneshot` tests, injected `Clock`, and send capture from handlers.
- Consume-side `Replay::received::<T>` using the M2 operational changeset guard
  pattern.
- Kafka ingest scheduler that writes a durable row before committing the Kafka
  offset.
- Optional Redpanda/testcontainers full-loop coverage after Postgres-only
  receive behavior is green.

## Out Of Scope

- M4 retry/backoff/DLQ policy scheduling, DLQ movement, and poison-message
  operator flows.
- A persistent claim-column or lease-based receive dispatch model.
- Retention and purge enforcement.
- Per-key ordering guarantees beyond Kafka partition ordering and the documented
  `SKIP LOCKED` dispatch behavior.
- Non-Kafka transports or in-memory transport simulation.
- HTTP/Axum receive integration beyond proving the consumer tower can coexist
  with existing runtime composition.

## Crate Topology

- `kafkaman-core`: message trait, message metadata/request types,
  `MessageRouter`, `FromMessage`, `Rx`, and public status/error data shapes.
- `kafkaman-sqlx`: received-table DDL, receive-row SQL primitives,
  `dispatch_once()`, failure accounting, and consume-side replay changesets.
- `kafkaman-test`: Harness receive helpers, row assertions, injected clock,
  handler `oneshot` helpers, and send capture.
- `kafkaman-macros` or split proc-macro crates: `#[derive(KafkaMessage)]` and
  `#[kafkaman::test]`, re-exported from the public facade only when ready.
- `kafkaman-rdkafka` / `kafkaman-worker`: Kafka ingest edge and runtime wiring.

Adding `tower` to workspace dependencies is part of M3, but the HTTP-specific
`tower-http`/Axum extractor stack remains outside the Kafka message surface.

## Execution Sequence

1. Open with the consumer-facing test wish.
   - Add a failing `kafkaman-test` Harness test that seeds an `OrderCreated`
     received row, registers `MessageRouter::new().handler::<OrderCreated>(...)`,
     calls `dispatch_once()`, and asserts one durable effect plus `Processed`.
   - Add the duplicate example case in the same slice: two deliveries with the
     same `idempotency_key` converge to one durable effect.
   - Add a randomized effective-once property test for redelivery orderings and
     duplicates before calling the slice complete.

2. Add receive schema primitives.
   - Define received-table descriptor/types beside the existing outbox table
     machinery.
   - Add a `CreateReceivedTable` changeset using the M2 changelog engine.
   - Include identity, source topic/partition/offset, key, payload, message
     type/version, headers, correlation/causation IDs, status, attempts, bounded
     `errors`, and nullable `next_attempt_at`.
   - Do not add persistent claim/lease columns for received rows in M3.
   - Add the bounded-errors ring helper and tests while the JSONB shape is still
     small.

3. Implement deterministic dispatch.
   - Implement `dispatch_once()` over Postgres first.
   - Claim due `Pending`/`Retryable` rows with `FOR UPDATE SKIP LOCKED` and a
     due timestamp bound from the injected `Clock`.
   - Run the handler stack inside the kafkaman-owned transaction handed to the
     handler as `Rx`.
   - On success, commit handler business writes and `status = Processed`
     atomically in that transaction.
   - On failure, roll back handler writes, then record bounded error accounting
     in a short follow-up transaction with parked `Retryable` semantics.
   - Add the crash-during-dispatch gate: a killed worker leaves the row claimable
     and does not commit a double effect.

4. Build the explicit handler surface.
   - Introduce the explicit message trait and manual implementation path.
   - Implement `MessageRouter::handler::<T>()` before macro sugar.
   - Add `FromMessage` extractors for payload, metadata, state, and receive
     transaction access.
   - Add Tower layer compatibility for generic middleware that operates on the
     kafkaman message request.

5. Mature `kafkaman-test` around the new seam.
   - Add Harness helpers for received-row seeding and row assertions.
   - Add handler `oneshot` tests that do not require Kafka.
   - Add injected `Clock` support for due-row and error-boundary tests.
   - Add send capture for handlers that publish follow-up messages.
   - Add `#[kafkaman::test]` only after the explicit Harness path is stable.

6. Add consume-side replay.
   - Add `Replay::received::<T>` as an operational changeset guarded by the M2
     changelog/audit mechanism.
   - Keep the replay bounded and dry-run visible, matching the send-side replay
     guardrails.
   - Use replay to make parked `Retryable` rows claimable in controlled tests
     without introducing M4 backoff policy.

7. Add Kafka ingest after dispatch is proven.
   - Consume Kafka records at the edge.
   - Validate/reserve `kafkaman-*` metadata handling according to the identity
     decision.
   - Insert the durable received row with `ON CONFLICT DO NOTHING`.
   - Commit the Kafka offset only after the durable write/no-op is complete.
   - Keep user code out of ingest.

8. Add macro and full-loop polish.
   - Add `#[derive(KafkaMessage)]` only after the explicit trait API is stable.
   - Add an opt-in Redpanda/testcontainers full-loop test proving ingest writes
     durable rows and dispatch processes them.
   - Update examples only when the API is stable enough to teach.

## Verification Gates

- `cargo fmt --all -- --check`
- `cargo check --workspace`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Postgres-only integration tests for receive schema, `idempotency_key NOT NULL`
  uniqueness, dedup, dispatch success, atomic business-write plus `Processed`
  commit, handler failure parking, and deterministic clock behavior.
- Randomized effective-once property test for redelivery orderings and
  duplicates.
- Crash-during-dispatch gate proving rollback leaves the row claimable and does
  not commit a double effect.
- Bounded-errors ring test proving the ring stays within the configured limit
  while `attempts` keeps counting.
- Consume-side replay tests for dry-run report, bounded apply, audit entry, and
  controlled re-drive of parked received rows.
- `#[kafkaman::test]` macro smoke test plus equivalent non-macro Harness test.
- Optional full-loop Redpanda/testcontainers receive test behind the existing
  integration feature strategy.

## Evidence To Record

- Test names and commands that prove the Postgres-only receive path.
- Property-test seed or strategy summary for effective-once redelivery coverage.
- Crash-gate command/outcome for crash during dispatch.
- Any full-loop Redpanda/testcontainers command and outcome.
- Schema compatibility notes if received-table DDL or public APIs need migration
  guidance.
- Any new durable choice that is not already covered by the receive, identity,
  schema, runtime, or test-tooling decisions.

## Progress

- 2026-06-21: Implemented first Postgres durable-receive slice:
  `ReceiveStatus`/`ReceivedRow`, `ReceivedTable`, `CreateReceivedTable`,
  durable received insert with `ON CONFLICT DO NOTHING`, Harness receive helpers,
  minimal `MessageRouter`, and `dispatch_once()` held-transaction success and
  failure paths.
- Proof recorded 2026-06-21: `cargo fmt --all -- --check`,
  `cargo check --workspace --all-features`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `cargo test --workspace --all-features`, and
  `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`.
- 2026-06-21 (review follow-up): addressed the implementation review's H1
  failure-accounting clobber (status-guarded `record_received_failure`), the M2
  Harness send/receive registration conflict, and L1/L2 polish; added the
  stale-failure interleaving, crash-during-dispatch, randomized redelivery
  convergence, and bounded 20-entry error-ring gates.
- 2026-06-22 (review follow-up): handlers now receive message metadata via
  `ReceivedMeta` (`message_id`, `idempotency_key`, `attempts`, `headers`,
  `correlation_id`/`causation_id`, source topic/partition/offset/key);
  `MessageRouter::handler` and `dispatch_once` thread it through, with the
  `dispatch_exposes_message_metadata_to_handler` gate. This satisfies the review's
  "land at least metadata access" bar (M1) and documents the L3 nullability choice
  inline. The broader `FromMessage`/`Rx`/state and Tower handler abstractions
  remain pending.
- 2026-08-31 (closure): this plan is marked `Completed`. It had stood at `Active`
  since 2026-06-21 while every one of its closure criteria was met elsewhere —
  `wiki/specs/m3-durable-receive.spec.md` exists and the roadmap has recorded M3
  as completed since M4 opened.

  Of the "Remaining M3 work" this section previously listed, three shipped and
  four were dropped. Shipped: `Replay::received::<T>`, the Kafka ingest engine
  with offset-after-durable-write, and full-loop Redpanda coverage under
  `tests/durable-send/tests/redpanda_full_loop/`. Never built, and now recorded
  as deferred rather than pending: `FromMessage`/`Rx`/state extractors with Tower
  layer compatibility, the injected `Harness` `Clock`, `#[kafkaman::test]`, and
  `#[derive(KafkaMessage)]` — the last is still open as `Proposed` proposal 16.
  The accepted M3 handler surface is the minimal closure-based one, which the
  spec states directly. See the 2026-08-31 ratification notes on
  [consumer-test-tooling](../decisions/consumer-test-tooling.decision.md) and
  [library-test-strategy](../decisions/library-test-strategy.decision.md).

## Wiki Updates

- Update this plan as steps complete.
- Update `wiki/index.md` and `wiki/log.md` for every wiki mutation.
- Promote validated M3 behavior into a new `wiki/specs/m3-durable-receive.spec.md`
  when implementation and proof are complete.
- Update affected decisions only if implementation forces a durable choice not
  already recorded.

## Closure Criteria

This plan closes when M3 has a validated durable receive implementation, the
Postgres-only receive tests are green, the explicit handler API is documented by
tests, consume-side replay is implemented, Kafka ingest commits offsets after
durable writes, and the validated behavior is promoted into an M3 spec.
