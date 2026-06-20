# First PoC: Durable Send - per-type outbox -> rdkafka via minimal `kafkaman::migrate()`

- Document Class: Plan
- Status: Completed
- Date: 2026-06-20
- Category: Proof of concept
- Scope: Smallest end-to-end slice proving the durable-send promise using the decided per-type table layout and `kafkaman::migrate()` entry point; excludes consume/inbox, full retry/DLQ policy, operational changesets, and full config/env maturity.
- Sources:
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/research-summary.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/plans/m1-durable-send-implementation.plan.md

## Deliverable This Executes

**Promise:** an Axum service inserts a message inside its own SQLx Postgres
transaction into a per-type outbox table in the `kafkaman` schema, provisioned by
`kafkaman::migrate()`. A relay then publishes the row to Kafka after commit. The
PoC proves durable at-least-once publication: no message is lost across the tested
crash windows.

**Delivery semantics:** the relay is publish-then-mark. If Kafka accepts the
record but the DB mark fails or the process crashes first, the row is reclaimed
after its claim lease expires and republished. That is **at-least-once**, not
exactly-once.

## In Scope

- **`kafkaman-core`:** envelope fields (`message_id`, `correlation_id`,
  `causation_id`, headers, payload, occurred time), typed message descriptors,
  validated SQL identifiers, and send-side outbox status/claim types.
- **`kafkaman-sqlx`:** minimal `migrate()`, object-safe changesets,
  `InitSchema`, `CreateOutboxTable`, transactional enqueue, claim lease query,
  publish mark, and publish-failure requeue.
- **Per-type table plumbing:** create two structurally identical outbox tables
  from one template to prove the design generalizes, while publishing only one
  message type end-to-end.
- **`kafkaman-rdkafka`:** publisher that maps a claimed outbox row to a Kafka
  record and surfaces delivery ack/error. It performs no DB writes.
- **`kafkaman-worker`:** minimal relay loop: claim lease, publish outside the
  claim transaction, mark by `claim_id`, or requeue on publish error. A stuck
  `Publishing` row is reclaimable after lease expiry.
- **`kafkaman-test` seed:** Docker-free Harness over a caller-provided database
  URL with ephemeral-schema `migrate()`, enqueue helper, `relay_once()`, a
  capturing publisher by default, published-record assertions, and row-state
  assertions. Redpanda/full-loop support is opt-in.
- **`apps/axum-outbox`:** runnable Axum + SQLx example with a local
  `changelog.rs`, two-phase `main` (`migrate()` then relay run), and an endpoint
  that writes business state plus `enqueue(&mut tx, ...)` in one transaction.

## Out Of Scope

- Consume/inbox side, idempotent consumer, receive transaction, handler router,
  receive offset handling.
- Full retry/backoff/DLQ policy. M1 has only fixed-delay requeue on publish error
  and claim lease reclaim.
- `kafkaman-axum`, `Sender` extractor, `send_non_transactional`, admin routes,
  observability beyond basic tracing spans.
- Operational changesets, checksum/audit maturity, rendered env config, retention
  jobs, partitioning, CDC, and ordering guarantees beyond Kafka's own partition
  behavior.

## Steps

1. Create workspace + compiling crate stubs: `kafkaman-core`, `kafkaman-sqlx`,
   `kafkaman-rdkafka`, `kafkaman-worker`, `kafkaman-test`, optional facade, and
   `apps/axum-outbox`.
2. Write the headline failing Harness test for durable send: enqueue in a SQL tx,
   commit, run `relay_once()`, assert one published/captured record and row
   `Published`.
3. Implement core envelope, message descriptor, validated SQL identifier, status,
   row, claim, and object-safe changeset types.
4. Implement minimal `migrate()` plus `InitSchema` / `CreateOutboxTable`.
5. Implement transactional enqueue, claim lease, stale-claim-safe mark published,
   and publish-failure requeue.
6. Implement `kafkaman-worker::relay_once()` and `run()` generic over a publisher
   trait; green the headline test with the capturing publisher.
7. Implement `kafkaman-rdkafka` publisher and opt-in Redpanda/full-loop test path.
8. Flesh out the Harness seed: ephemeral schema, PoC changelog, capture sink,
   row assertions, and explicit `relay_once()` driver.
9. Build the Axum example.
10. Add crash gates through the Harness.

White-box tests are expected for layers below the Harness boundary: identifier
validation, rendered DDL, claim SQL, stale mark rejection, publish-failure requeue,
and rdkafka mapping. Behavior-level gates should go through the Harness.

## Verification Gates

- `cargo check --workspace` passes.
- `cargo nextest run` passes.
- **`migrate()` is idempotent:** running it twice converges; per-type tables and
  `changelog_history` exist in the configured schema; each version is recorded
  once.
- **Template generalizes:** two `CreateOutboxTable` changesets produce two
  structurally identical per-type tables.
- **Normal delivery:** POST to the endpoint or enqueue through the Harness, run
  `relay_once()`, observe a captured/real Kafka record, and observe the outbox row
  marked `Published`.
- **Crash between commit and publish:** after business commit but before relay
  publish, restart/run relay and observe the message is still published.
- **Crash after Kafka ack before mark:** after ack but before DB mark, row remains
  `Publishing`; after lease expiry it is reclaimed and republished.
- **Stale claim safety:** a relay with an expired/replaced `claim_id` cannot mark
  a row `Published`.
- **Publish error path:** publish error records `last_error`, increments attempts
  through claim, clears claim fields, and requeues with `next_attempt_at`.

## Evidence To Record

- Test command output and any compose/testcontainers setup used.
- Schema snapshot for the generated per-type tables and `changelog_history`.
- Example README or run command.
- A short note documenting the duplicate window and why this PoC proves
  at-least-once publication, not exactly-once.

## Wiki Pages To Update When Done

- Promote the validated M1 subset into a spec: envelope shape, per-type outbox DDL,
  M1 `migrate()` behavior, transactional enqueue, relay claim/lease semantics, and
  send-side Harness API.
- Update `wiki/index.md`.
- Append to `wiki/log.md`.

## What Closes This Plan

All verification gates pass and the validated M1 behavior is recorded as a spec.
Future-only ideas remain in decisions, roadmap milestones, or later plans.
