# kafkaman Project Objectives

- Document Class: Proposal
- Status: Proposed
- Date: 2026-06-20
- Category: Project objectives
- Scope: Defines what kafkaman is, its core promise, and how it fits a modern Rust microservice stack.
- Sources:
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/research-summary.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/manifest.md
- Related:
  - wiki/references/rust-kafka-outbox-ecosystem.reference.md
  - wiki/proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Proposal

kafkaman is a robust **Rust library plus optional worker runtime for reliable
Kafka-backed service communication, using Postgres as the durable execution
ledger**.

This is deliberately narrower than "a Kafka wrapper" and broader than "an
outbox table helper." The product is the **durable execution model around
Kafka**, not the Kafka client itself.

**Guiding stance on the reference architecture.** The cqrs-fullstack / RepForge
project (see [the ecosystem reference](../references/rust-kafka-outbox-ecosystem.reference.md)
and the migration evidence note) is *evidence of what worked for one team and a
starting point for discussion* — not a blueprint to replicate. kafkaman's design
is driven by what we want to achieve; the reference informs it, it does not bind
it.

## Core Promise

1. **Durable send.** When a service intends to send a message, the intent is
   stored in Postgres inside the caller's transaction *before* broker dispatch.
   Dispatch happens asynchronously after commit.
2. **Durable receive.** When a service receives a message, receipt is recorded
   durably and the offset is committed **right after that durable write — not
   after the handler** — so the broker keeps flowing. Handler execution is then
   tracked durably and driven from the table (see the
   [message-consumption decision](../decisions/message-consumption-and-handler-model.decision.md)).
3. **Built-in reliability.** Retries, backoff, poison-message handling,
   idempotency, ordering, and operator visibility are part of the library, not
   left to each caller.
4. **Fits, does not replace.** kafkaman plugs into existing modern Rust
   service stacks (Tokio, Axum, SQLx/Postgres, tracing) rather than requiring a
   platform rewrite.

## Execution Model (agreed direction)

kafkaman is a **durable message runtime**, not just an outbox helper. This
realizes the original project statement ("a scheduler would run the code to
consume/send and handle retries"):

- **Two schedulers → durable table → handler.** An *ingest* scheduler consumes
  Kafka, writes the received row into a per-type table, and commits the offset
  immediately (messages must flow); a *dispatch* scheduler claims rows
  (`FOR UPDATE SKIP LOCKED`) and drives the user's handler stack. Handlers are
  registered explicitly, axum-style (`.handler::<OrderShipped>(on_shipped)`), and
  the payload type carries its `topic`/`message_type` via a derive. The handler
  stack is a Tower `Service<Message>` with kafkaman extractors (its own thing, not
  HTTP). kafkaman owns Kafka I/O, polling, retries, backoff, and DLQ. See the
  [message-consumption decision](../decisions/message-consumption-and-handler-model.decision.md).
- **The transaction relocates, it does not disappear.**
  - *Receive:* kafkaman owns the transaction and hands the handler a
    `&mut Transaction`, so the handler's business write and kafkaman's
    "mark processed" commit together (idempotent / effective-once).
  - *Send:* the user owns the transaction; kafkaman joins it to insert the row.
- **Send enqueue offers two modes** (tradeoff is explicit):
  - transactional enqueue (atomic with the business write — no dual-write loss;
    the reliable path),
  - fire-and-forget enqueue (convenience for messages not tied to a DB state
    change; accepts dual-write risk).
- **Schema & change management:** kafkaman ships its own Flyway/Liquibase-style
  change engine in Rust. Versioned changesets — structural (per-type tables) and
  one-shot operations (replay) — apply in order and are recorded with checksums
  in `kafkaman.changelog_history`. Tunable settings (retention, batch sizes) are
  per-env runtime config from a flat `kafkaman.toml`, not changesets (see the
  [config & environment decision](../decisions/configuration-and-environment-model.decision.md)). `kafkaman::migrate(&pool)` converges
  the DB (at app startup, advisory-locked; run on every env); `Runtime::start()`
  boots the schedulers and the purge enforcer. Tables are **distinct per message type** in
  a dedicated `kafkaman` schema, generated from one template; plain tables stay
  inspectable for break-glass fixes. No SQL stored functions. Full rationale and
  alternatives:
  [schema-and-change-management.decision.md](../decisions/schema-and-change-management.decision.md).

## Proposed Shape

Shared infrastructure crates plus an optional worker binary:

- `kafkaman-core` — message envelope, state machine, traits, errors, retry
  policy, handler result model.
- `kafkaman-sqlx` — Postgres schema, repository, transaction helpers,
  `FOR UPDATE SKIP LOCKED` job claiming, migrations.
- `kafkaman-rdkafka` — Kafka producer/consumer backend.
- `kafkaman-axum` — request-path integration: a `CorrelationLayer`, a `Sender`
  extractor (opinionated `axum-sqlx-tx` pairing for atomic outbox enqueue),
  health/admin/DLQ routes, and the `serve().with_runtime()` shutdown helper.
- `kafkaman-observability` — tracing spans and metrics helpers (may start as a
  module if a separate crate is premature).
- `kafkaman-worker` — the relay/consumer scheduler subsystems, run **embedded**
  (next to Axum) or in a **worker-role host binary** (no HTTP). Not a standalone
  shipped daemon — handlers are compiled into the host; see the
  [runtime-composition decision](../decisions/runtime-composition-and-topology.decision.md).
- `kafkaman-test` — consumer-facing test toolkit (dev-dependency): the `Harness`
  (ephemeral schema + `migrate()` + capturing sender + `dispatch_once()` + a
  controllable `Clock`), `tower` `oneshot` handler tests, and the
  `#[kafkaman::test]` macro. See the
  [consumer test-tooling decision](../decisions/consumer-test-tooling.decision.md).
- `examples/` — runnable Axum + SQLx + Kafka service matching the local
  monorepo style.

## Design Requirements (from sources)

- **Durable schema:** outbox messages, inbox/processed messages, attempts,
  locks/leases, dead letters, scheduled retries, cleanup metadata.
- **Message envelope:** message ID, idempotency key, topic, partition key,
  payload, payload type/version, causation/correlation IDs, actor context,
  trace context, headers, timestamps.
- **Outgoing API:** insert message inside the caller's SQLx transaction, then
  dispatch asynchronously after commit.
- **Incoming API:** record receipt/processed ID, execute handler
  transactionally, commit/ack offset only after durable success.
- **Retry model:** retryable vs terminal errors, exponential backoff, max
  attempts, DLQ, poison-message classification.
- **Ordering model:** partition by aggregate/user key, preserve per-key order
  where required.
- **Handler model:** async traits, typed payload decode, validation hooks,
  middleware/interceptors, graceful shutdown.
- **Observability:** tracing spans, metrics, operator-readable status, stuck-job
  detection, lag/age metrics, DLQ inspection.
- **Testing:** local Docker/compose stack (Postgres + Kafka or Redpanda),
  failure-injection tests, replay/idempotency tests, `cargo check --workspace`.

## Open Questions

These are tracked and unresolved; several gate v1 scope:

1. ~~Standalone worker process, embedded-in-service, or both?~~ **Resolved**
   ([decision](../decisions/runtime-composition-and-topology.decision.md)):
   schedulers are spawnable units (`runtime.run(shutdown)` / `into_tasks()`); the
   host owns the process model — embedded or worker-role is a host choice via
   `.subsystems(...)`. No standalone daemon (handlers are compiled-in Rust).
2. ~~Depend directly on `rdkafka`, or define a transport trait?~~ **Resolved**
   ([decision](../decisions/messaging-scope-and-receive-model.decision.md)):
   Kafka-only in v1, implement `kafkaman-rdkafka` concretely, no transport-trait
   abstraction until a real second transport exists.
3. ~~Postgres polling first, or reserve schema compatibility for Debezium CDC
   from the start?~~ **Resolved**
   ([decision](../decisions/message-consumption-and-handler-model.decision.md)):
   Postgres polling (`FOR UPDATE SKIP LOCKED`) for the dispatch scheduler; the
   ingest scheduler uses the rdkafka consumer. CDC is **not** pursued for v1 (the
   single-table Debezium outbox router fights the per-type-table decision);
   `LISTEN/NOTIFY` is the latency escape hatch if dispatch polling lag ever bites.
4. ~~Are "commands over Kafka" in scope for v1?~~ **Resolved**
   ([decision](../decisions/messaging-scope-and-receive-model.decision.md)): No.
   v1 is durable **async** (outbox→Kafka, idempotent consumer, handler, retry).
   Commands/HTTP/synchronous outcomes are deferred; envelope carries
   `correlation_id`/`causation_id` to keep the outcome story open.
5. How much host-architecture-specific context (actor context, Keycloak user
   IDs, cache-invalidation conventions) belongs in the library vs the host?

## Promotion Target

On acceptance, promote the settled objective and crate boundaries to a
`spec` once a proof-of-concept validates them (see
[first-poc-outbox-publisher](../plans/first-poc-outbox-publisher.plan.md)).
