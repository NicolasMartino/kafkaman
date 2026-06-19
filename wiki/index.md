# Wiki Index

Project: kafkaman
Stage: M1 durable-send PoC implemented; V1 roadmap drafted
Updated: 2026-06-20

One-line: A Rust library plus optional worker runtime for reliable Kafka-backed
service messaging, using Postgres as the durable execution ledger.

## Specs

- [specs/m1-durable-send.spec.md](specs/m1-durable-send.spec.md) - Validated M1
  durable-send behavior: transactional enqueue, per-type outbox DDL, minimal
  migrate, claim-lease relay, publisher boundary, Harness seed, and Axum example.
  Status: Active.

## Reviews

- [reviews/m1-durable-send-implementation-review.reference.md](reviews/m1-durable-send-implementation-review.reference.md)
  - Sourced verification of the post-implementation M1 durable-send review,
  confirming the main gaps around status centralization, clock ownership,
  Redpanda/full-loop scope, idempotency durability, worker resilience, and
  migration concurrency. Status: Sourced.

## Decisions

- [decisions/messaging-scope-and-receive-model.decision.md](decisions/messaging-scope-and-receive-model.decision.md)
  - Durable-execution-first core; Kafka-only transport in v1; HTTP and synchronous
  outcomes deferred; receive is fire-and-forget plus durable status with
  `correlation_id`/`causation_id` in the envelope. Status: Accepted.
- [decisions/schema-and-change-management.decision.md](decisions/schema-and-change-management.decision.md)
  - Dedicated `kafkaman` schema; distinct per-type tables from one template; a
  Rust Flyway-style change engine; no SQL functions. Status: Accepted.
- [decisions/configuration-and-environment-model.decision.md](decisions/configuration-and-environment-model.decision.md)
  - One flat `kafkaman.toml`, rendered per environment by CI/CD from vault;
  `apply(env)` selects values not structure; tunable settings are runtime config,
  not changesets. Status: Accepted.
- [decisions/runtime-composition-and-topology.decision.md](decisions/runtime-composition-and-topology.decision.md)
  - Schedulers are spawnable units; topology is a host choice; send-side Axum UX
  is opinionated around request transactions but M1 implements only the generic
  enqueue/relay core. Status: Draft.
- [decisions/message-consumption-and-handler-model.decision.md](decisions/message-consumption-and-handler-model.decision.md)
  - Receive side uses ingest and dispatch schedulers, per-type received tables,
  dedup-as-log, bounded errors, and a Tower-style message handler stack. Status:
  Draft.
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
  Status: Draft.

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

## Plans

- [plans/first-poc-outbox-publisher.plan.md](plans/first-poc-outbox-publisher.plan.md)
  - Smallest durable-send slice: per-type outbox table, minimal `migrate()`,
  claim-lease relay, publisher, Axum example, and crash/idempotency gates. Status:
  Completed.
- [plans/m1-durable-send-implementation.plan.md](plans/m1-durable-send-implementation.plan.md)
  - Code-level M1 implementation plan for workspace/crate layout, core types,
  SQLx DDL/primitives, relay, Harness, tests, and example. Status: Completed.

## Checklists

(none yet)

## Archive

(none yet)
