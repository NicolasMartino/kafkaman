# Messaging Scope and Receive Model

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-20
- Category: Architecture scope
- Scope: Fixes kafkaman's v1 scope (transport, command vs event, receive/outcome model) and what is deliberately deferred.
- Sources:
  - wiki/proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack-migration-evidence.md (committed verbatim excerpt of the load-bearing migrations)
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/01-cqrs-fullstack/ (full project; local only, gitignored)
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md (the design discussion this decision records)
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/references/rust-kafka-outbox-ecosystem.reference.md
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Decision

1. **Durable-execution-first, transport-neutral core.** kafkaman's core is a
   durable execution engine — ledger, status state machine, `FOR UPDATE SKIP
   LOCKED` claiming, retry/backoff, idempotent receive. Transport is a backend
   under that core.
2. **Kafka is the only transport in v1.** Implement `kafkaman-rdkafka`
   concretely. Do **not** build a transport-trait abstraction or a second
   transport until one is actually needed — avoid over-abstraction. The core
   stays transport-neutral in *design*, not in premature *interface*.
3. **HTTP / internal-call transport is deferred (possibly indefinitely).**
   Working thesis to validate: a sufficiently reliable Kafka **async** story may
   make non-Kafka inter-service messaging unnecessary — realizing the original
   "Kafka instead of REST" goal through *reliability*, not through synchronous
   request/response over Kafka.
4. **Commands-over-Kafka (synchronous request/response) is out of v1 scope.**
   v1 = durable **async**: outbox→Kafka send, idempotent consumer, handler
   execution, retry/DLQ.
5. **Receive/outcome model:** v1 is **fire-and-forget with durable status**. The
   envelope carries `correlation_id` and `causation_id` as first-class fields so
   the outcome story can be built later **without changing the existing envelope
   fields** (the durable waiter store it needs will still add its own tables). The
   wait-for-outcome mechanism is **deferred**; when built, kafkaman provides only
   **primitives** — (a) emit a correlated outcome message, (b) durably
   await/subscribe by `correlation_id` — and leaves **policy** (timeouts, batch
   completion, synchronous client responses; i.e. the BFF `outcome_waiters` role)
   to the host application.

## Why

The reference architecture (RepForge / cqrs-fullstack) already ran this
experiment:

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

That `mutation_jobs` engine is the same durable-execution machinery as a Kafka
outbox with a different dispatch verb (HTTP POST vs Kafka produce). So the
**durable-execution model is the validated need; the transport is incidental.**

Outcome-awaiting is a **transport capability**, not a core concern: intrinsic
over HTTP (the response *is* the outcome), but opt-in and fragile over Kafka
(needs a reply topic + correlation — the exact thing that made commands-over-Kafka
painful). It therefore belongs *above* the durable core, delivered as primitives.

## Alternatives Considered

- **Kafka-command-first (Option A):** refuted by RepForge's own production
  cutover away from `commands_inbox`.
- **Propagation-only (Option B):** too narrow — leaves the handler/retry/durable
  engine (the most valuable, currently hand-rolled part) outside the library.
- **Await-outcome baked into the core:** couples the core to a request/response
  shape, heavy, and awkward precisely over Kafka.

## Consequences / Tradeoffs Accepted

- v1 ships **less**: no synchronous outcomes, no HTTP. But the envelope's
  `correlation_id`/`causation_id` keep the outcome and multi-transport doors open
  cheaply.
- We deliberately delay the transport-trait abstraction until a real second
  transport exists, accepting a future refactor in exchange for not
  over-generalizing now.

## Revisit When

- A concrete need for synchronous service-to-service or human-facing outcomes
  appears (build the outcome primitives then).
- The Kafka-async thesis is falsified — async proves insufficient to replace
  inter-service HTTP (reconsider an HTTP transport).
- The envelope's correlation/causation fields prove inadequate for the outcome
  story (schema revision).
