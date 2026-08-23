# Messaging Scope and Receive Model

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-20
- Amended: 2026-08-14
- Category: Architecture scope
- Scope: Originally fixed kafkaman's v1 scope around durable async execution.
  Amended 2026-08-14: kafkaman's product purview is compact entity-cache
  propagation only; non-entity work items, commands, generic jobs, emails,
  payments, analytics events, and direct transport are outside scope.
- Sources:
  - wiki/proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack-migration-evidence.md (committed verbatim excerpt of the load-bearing migrations)
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack/ (full project; local only, gitignored)
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md (the design discussion this decision records)
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/references/rust-kafka-outbox-ecosystem.reference.md
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Decision

1. **Compact entity-cache propagation is the v1 product scope.** The
   2026-08-14 entity-only purview amendment supersedes the original
   durable-execution-first scope. kafkaman's durable ledger, status state
   machine, `FOR UPDATE SKIP LOCKED` claiming, retry/backoff, and idempotent
   receive remain only as infrastructure for reliable entity propagation and
   cache coherence. kafkaman is not a general durable job queue.
2. **Kafka is the only transport in v1.** Implement `kafkaman-rdkafka`
   concretely. Do **not** build a transport-trait abstraction or a second
   transport until one is actually needed — avoid over-abstraction. The core
   stays transport-neutral in *design*, not in premature *interface*.
3. **HTTP / internal-call transport is deferred (possibly indefinitely).**
   Working thesis to validate: a sufficiently reliable Kafka **async** story may
   make non-Kafka inter-service messaging unnecessary — realizing the original
   "Kafka instead of REST" goal through *reliability*, not through synchronous
   request/response over Kafka.
4. **Commands-over-Kafka and non-entity work are out of v1 scope.** v1 =
   durable **async entity propagation**: outbox-to-Kafka publish of compact
   entity snapshots, idempotent consume, guarded cache write, and retry/DLQ for
   that pipeline. Messages such as "send welcome email", payments, commands,
   analytics events, and generic durable jobs belong outside kafkaman.
5. **Receive/outcome model for entity propagation:** v1 is **fire-and-forget
   with durable status** for entity-cache processing. The envelope carries
   `correlation_id` and `causation_id` as first-class fields so an adjacent
   outcome story can be built later **without changing the existing envelope
   fields**. The wait-for-outcome mechanism is **deferred**; under the
   2026-08-14 purview amendment, it remains speculative adjacent infrastructure
   rather than v1 scope.

## Why

The reference architecture (RepForge / cqrs-fullstack) already ran the original
durable-execution experiment:

- It built **commands over Kafka** (`commands_inbox`, `processed_commands`),
  then cut over and **dropped** them — `exercise-api` migration
  `010_drop_legacy_command_transport_tables.sql`: *"Remove Kafka command-path
  tables left over from the pre-cutover architecture."* (Verbatim excerpts of all
  cited migrations are committed at
  `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack-migration-evidence.md`.)
- It replaced them with `mutation_jobs` — a *"Durable internal HTTP mutation
  dispatch queue"* with a status machine (`Pending | Dispatching | Applied |
  Rejected | RetryableFailure | TerminalFailure`), `attempt_count`,
  `next_attempt_at`, `FOR UPDATE SKIP LOCKED` claiming, plus `outcome_waiters`
  (await-by-`correlation_id`) and `processed_mutations` (idempotent receive via
  `Idempotency-Key`).

The original 2026-06 conclusion was that the `mutation_jobs` engine validated
durable execution as kafkaman's core. The 2026-08-14 amendment narrows that
interpretation: `mutation_jobs` proves applications may need durable work
queues, but kafkaman should not own them as product scope. Its public promise is
distributed cache coherence for compact entity snapshots.

Outcome-awaiting is a **transport capability**, not a core concern: intrinsic
over HTTP (the response *is* the outcome), but opt-in and fragile over Kafka
(needs a reply topic + correlation — the exact thing that made commands-over-Kafka
painful). It therefore belongs *above* the durable core, delivered as primitives.

## Alternatives Considered

- **Kafka-command-first (Option A):** refuted by RepForge's own production
  cutover away from `commands_inbox`.
- **Propagation-only (Option B):** originally rejected as too narrow. Superseded
  2026-08-14; this is now accepted as the clearer kafkaman purview.
- **Await-outcome baked into the core:** couples the core to a request/response
  shape, heavy, and awkward precisely over Kafka.

## Consequences / Tradeoffs Accepted

- v1 ships **less**: no synchronous outcomes, no HTTP. But the envelope's
  `correlation_id`/`causation_id` keep the outcome and multi-transport doors open
  cheaply.
- We deliberately delay the transport-trait abstraction until a real second
  transport exists, accepting a future refactor in exchange for not
  over-generalizing now.
- v1 now ships less than the original durable-execution scope too: generic
  durable jobs and non-entity messages are excluded even if existing M1-M4 code
  still contains reusable machinery.
- M4 retry/backoff/DLQ is retained as entity-cache pipeline support, not as a
  durable job queue product promise.

## Revisit When

- A concrete need for synchronous service-to-service or human-facing outcomes
  appears (build the outcome primitives then).
- The Kafka-async thesis is falsified — async proves insufficient to replace
  inter-service HTTP (reconsider an HTTP transport).
- The envelope's correlation/causation fields prove inadequate for the outcome
  story (schema revision).
- A concrete v1 adopter need justifies reopening generic durable jobs as a
  kafkaman product scope rather than adjacent application infrastructure.
