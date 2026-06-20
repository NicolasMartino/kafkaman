# Wiki Index

Project: kafkaman
Stage: M3 durable receive plan active
Updated: 2026-06-21

One-line: A Rust library plus optional worker runtime for reliable Kafka-backed
service messaging, using Postgres as the durable execution ledger.

## Specs

- [specs/m1-durable-send.spec.md](specs/m1-durable-send.spec.md) - Validated M1
  durable-send behavior: transactional enqueue, per-type outbox DDL, minimal
  migrate, claim-lease relay, publisher boundary, Harness seed, and Axum example.
  Status: Active.
- [specs/m2-change-engine-config.spec.md](specs/m2-change-engine-config.spec.md) -
  Validated M2 behavior: `kafkaman.toml` loader, fail-fast resolved config,
  migration reports, checksums, `applied_by`, `changelog!`, dry-run, and guarded
  send-side `Replay`. Status: Active.

## Reviews

- [reviews/m1-durable-send-implementation-review.reference.md](reviews/m1-durable-send-implementation-review.reference.md)
  - Sourced verification of the post-implementation M1 durable-send review,
  confirming the main gaps around status centralization, clock ownership,
  Redpanda/full-loop scope, idempotency durability, worker resilience, and
  migration concurrency. Status: Sourced.
- [reviews/m1-durable-send-implementation-rereview.reference.md](reviews/m1-durable-send-implementation-rereview.reference.md)
  - Fresh re-review after attempted fixes. Confirms several fixes, but identifies
  the missing idempotency schema-upgrade migration as a blocker, with Redpanda
  full-loop scope, Harness concurrency, strict clippy, and reserved Kafka header
  handling still open. Status: Sourced.
- [reviews/m2-change-engine-config-implementation-review.reference.md](reviews/m2-change-engine-config-implementation-review.reference.md)
  - Line-by-line review of the M2 change-engine + config implementation. Confirms
  the engine and config loader are largely delivered, but flags retry-config
  validation never being wired into any boot path, the checksum being FNV-1a (not
  the specified SHA-256), a non-side-effect-free dry-run, and stale `attempts` on
  replay. Status: Sourced.

## Compatibility

- [compatibility/m1-durable-send-schema-and-api-changes.compatibility.md](compatibility/m1-durable-send-schema-and-api-changes.compatibility.md)
  - Records the review-fix schema/API changes: durable `idempotency_key` column
  plus its `AddIdempotencyKey` upgrade changeset for pre-existing tables,
  `mark_publish_failed` retry-duration signature, and reserved `kafkaman-` header
  rejection. Status: Active.
- [compatibility/m2-change-engine-config-schema-and-api.compat.md](compatibility/m2-change-engine-config-schema-and-api.compat.md)
  - M2 schema/API changes: nullable `checksum` and `applied_by`
  `changelog_history` columns, `migrate(..., MigrationContext, ...) ->
  MigrationReport` signature break, dry-run, and guarded replay behavior.
  Status: Draft.

## Decisions

- [decisions/messaging-scope-and-receive-model.decision.md](decisions/messaging-scope-and-receive-model.decision.md)
  - Durable-execution-first core; Kafka-only transport in v1; HTTP and synchronous
  outcomes deferred; receive is fire-and-forget plus durable status with
  `correlation_id`/`causation_id` in the envelope. Status: Accepted.
- [decisions/message-identity-and-header-namespace.decision.md](decisions/message-identity-and-header-namespace.decision.md)
  - V1 persisted messages require an `idempotency_key`; received tables dedup on
  per-type unique idempotency keys; `kafkaman-*` Kafka headers are reserved and
  user headers with that prefix are rejected. Status: Accepted.
- [decisions/retry-backoff-dlq-policy.decision.md](decisions/retry-backoff-dlq-policy.decision.md)
  - Retry/backoff/DLQ policy is runtime config in `kafkaman.toml`, with common
  defaults plus per-message-type overrides, injected-clock tests, and a
  table-backed V1 DLQ surface. Status: Accepted.
- [decisions/v1-roadmap-execution-policy.decision.md](decisions/v1-roadmap-execution-policy.decision.md)
  - Draft decisions may contain accepted sub-decisions while full status waits
  for milestone validation; V1 work may use dependency-aware parallel worktrees.
  Status: Accepted.
- [decisions/schema-and-change-management.decision.md](decisions/schema-and-change-management.decision.md)
  - Dedicated `kafkaman` schema; distinct per-type tables from one template; a
  Rust Flyway-style change engine; no SQL functions. Status: Accepted.
- [decisions/configuration-and-environment-model.decision.md](decisions/configuration-and-environment-model.decision.md)
  - One flat `kafkaman.toml`, rendered per environment by CI/CD from vault;
  `apply(env)` selects values not structure; tunable settings, including
  per-message retry/backoff/DLQ policy, are runtime config, not changesets.
  Status: Accepted.
- [decisions/runtime-composition-and-topology.decision.md](decisions/runtime-composition-and-topology.decision.md)
  - Schedulers are spawnable units; topology is a host choice; send-side Axum UX
  is opinionated around request transactions but M1 implements only the generic
  enqueue/relay core. Status: Draft.
- [decisions/message-consumption-and-handler-model.decision.md](decisions/message-consumption-and-handler-model.decision.md)
  - Receive side uses ingest and dispatch schedulers, per-type received tables,
  required idempotency-key dedup-as-log, bounded errors, and a Tower-style
  message handler stack. Status: Draft.
- [decisions/library-test-strategy.decision.md](decisions/library-test-strategy.decision.md)
  - kafkaman tests itself with unit, Postgres integration, and full-loop tiers;
  dogfooding-first where tests sit at or above toolkit abstractions. Status:
  Draft.
- [decisions/consumer-test-tooling.decision.md](decisions/consumer-test-tooling.decision.md)
  - `kafkaman-test` is the consumer-facing test toolkit with Harness,
  deterministic one-step drivers, and future macro sugar. Status: Draft.

## Roadmaps

- [roadmaps/path-to-v1.roadmap.md](roadmaps/path-to-v1.roadmap.md) - Six
  milestones to V1: M1 durable send completed, then change-engine/config,
  durable receive/toolkit maturity, retry/DLQ, observability, and hardening.
  Retry/DLQ and parallel-worktree execution policies are now accepted. Status:
  Draft.

## References

- [references/rust-kafka-outbox-ecosystem.reference.md](references/rust-kafka-outbox-ecosystem.reference.md)
  - Rust Kafka clients, nearest Rust outbox crate, and JVM/.NET comparables.
  Status: Sourced.

## Proposals

- [proposals/01-kafkaman-objectives.proposal.md](proposals/01-kafkaman-objectives.proposal.md)
  - What kafkaman is, its core promise, crate shape, and design requirements.
  Status: Proposed.
- [proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md](proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md)
  - Kafka-command-first vs propagation-first vs durable-execution-first. Status:
  Accepted, promoted to the messaging-scope decision.
- [proposals/03-direct-transport-mode.proposal.md](proposals/03-direct-transport-mode.proposal.md)
  - Explicit non-durable direct Kafka producer and consumer modes for
  high-throughput or low-durability workloads. Status: Proposed.
- [proposals/04-observability-logging-policy.proposal.md](proposals/04-observability-logging-policy.proposal.md)
  - Configurable tracing, logging, metrics, payload safety, and per-message-type
  observability policy. Status: Proposed.

## Plans

- [plans/first-poc-outbox-publisher.plan.md](plans/first-poc-outbox-publisher.plan.md)
  - Smallest durable-send slice: per-type outbox table, minimal `migrate()`,
  claim-lease relay, publisher, Axum example, and crash/idempotency gates. Status:
  Completed.
- [plans/m1-durable-send-implementation.plan.md](plans/m1-durable-send-implementation.plan.md)
- Code-level M1 implementation plan for workspace/crate layout, core types,
SQLx DDL/primitives, relay, Harness, tests, and example. Status: Completed.
- [plans/m3-durable-receive.plan.md](plans/m3-durable-receive.plan.md)
- Active M3 execution plan for durable receive: received tables, deterministic
  dispatch, handler API, Harness maturity, and Kafka ingest. First Postgres
  storage/dispatch slice implemented 2026-06-21. Status: Active.

## Checklists

(none yet)

## Archive

(none yet)
