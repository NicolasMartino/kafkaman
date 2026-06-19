# Research Summary: kafkaman Objectives and Rust Microservice Integration

Research Question: What should kafkaman's project objectives be, what similar projects or patterns exist, and how should it integrate with the user's typical modern Rust fullstack microservice architecture?

Date: 2026-06-20
Status: Ready for ingest as proposal/reference material

## Scope

This bundle combines:

- The local architecture reference project at `/Users/nicolasmartino/Documents/workout/cqrs-fullstack`.
- Rust Kafka client and outbox crate metadata.
- Canonical outbox/inbox/idempotent consumer patterns.
- Comparable mature frameworks from JVM and .NET ecosystems.

## Key Findings

### 1. Project Objective Candidate

kafkaman should be defined as a Rust library and optional worker runtime for reliable Kafka-backed service communication using Postgres as the durable execution ledger.

The core promise:

- When a service intends to send a message, the intent is durably stored before broker dispatch.
- When a service receives a message, receipt and handler execution are tracked before side effects are considered complete.
- Retries, backoff, poison-message handling, idempotency, ordering, and operator visibility are built in.
- The library integrates with modern Rust service stacks instead of requiring a separate platform rewrite.

This is more specific than "Kafka wrapper" and more useful than "outbox table helper." The objective is the durable execution model around Kafka.

### 2. Fit With The Local Rust Fullstack Architecture

The copied RepForge source shows a modern Rust monorepo shaped roughly as:

- `code/frontend/app`, `code/frontend/shared-ui`, `code/frontend/gallery`
- `code/shared/contracts`, `code/shared/db`, `code/shared/messaging`, `code/shared/auth`, `code/shared/observability`
- service crates such as `user-api`, `workout-api`, and `exercise-api`
- workspace dependencies around Tokio, Axum, SQLx/Postgres, tracing, metrics, utoipa/OpenAPI, reqwest, Dioxus, and Kafka clients
- `just`-driven development, strict workspace compilation, and docs lanes for current ADRs/specs versus future proposals

kafkaman should fit this architecture as shared infrastructure crates plus an optional worker binary:

- `kafkaman-core`: message envelope, state machine, traits, errors, retry policy, handler result model.
- `kafkaman-sqlx`: Postgres schema, repository, transaction helpers, `FOR UPDATE SKIP LOCKED` job claiming, migrations.
- `kafkaman-rdkafka`: Kafka producer/consumer backend.
- `kafkaman-axum`: health/admin/debug routes and graceful shutdown integration.
- `kafkaman-observability`: tracing spans and metrics helpers, or a module if a separate crate is premature.
- `kafkaman-worker`: embedded or standalone relay/consumer scheduler.
- `examples/`: runnable Axum + SQLx + Kafka service example matching the local monorepo style.

### 3. Important Architecture Tension To Resolve

RepForge's current ADRs say authoritative mutation completion happens through a BFF durable mutation queue plus internal HTTP, while Kafka remains for propagation and cache coherence. kafkaman's initial project statement says services should communicate through Kafka instead of REST.

This is not a blocker, but it should be explicit. There are three viable positions:

1. Kafka-command-first: kafkaman intentionally explores service commands over Kafka, diverging from current RepForge practice.
2. Propagation-first: kafkaman supports the Kafka role RepForge currently accepts: events, cache invalidation, projection rebuilds, and cross-service propagation.
3. Durable-execution-first: kafkaman abstracts the durable ledger, retry, and handler model so the same core can support Kafka publication and, later, internal HTTP dispatch.

The third position is the most compatible with the local architecture while preserving the original Kafka goal.

### 4. Similar Projects And Ecosystem

Rust:

- `rdkafka` is the strongest production Kafka client candidate. It wraps `librdkafka`, supports Tokio integration, producer/consumer APIs, offset commits, transactions, metrics, and broker metadata.
- `rskafka` is a pure Rust Kafka client, but it is intentionally minimal and lacks consumer groups, offset tracking, and transactions.
- `outbox-pattern-processor` is a Rust outbox processor that targets database-to-SQS/SNS/HTTP dispatch. It is relevant but not a Kafka-first microservice framework.

Cross-language:

- Debezium Outbox Event Router is a CDC-based outbox publishing model.
- Eventuate Tram is a JVM transactional messaging framework with database-backed send/receive behavior, Kafka support, duplicate message detection, interceptors, and handler decorators.
- MassTransit and NServiceBus show how mature frameworks package consumer outbox, inbox dedupe, transaction boundaries, and API caveats.

Conclusion: kafkaman has a plausible niche if it focuses on Rust-native Kafka + Postgres durable messaging rather than trying to be a generic message broker abstraction.

### 5. Design Requirements Suggested By The Sources

kafkaman should probably include:

- Durable schema: outbox messages, inbox or processed messages, attempts, locks/leases, dead letters, scheduled retries, and cleanup metadata.
- Message envelope: message ID, idempotency key, topic, partition key, payload, payload type/version, causation/correlation IDs, actor context, trace context, headers, timestamps.
- Outgoing API: insert message inside caller's SQLx transaction, then dispatch asynchronously after commit.
- Incoming API: record receipt or processed ID, execute handler transactionally, then commit/ack offset only after durable success.
- Retry model: retryable versus terminal errors, exponential backoff, max attempts, DLQ, poison-message classification.
- Ordering model: partition by aggregate/user key and preserve per-key processing order where required.
- Handler model: async traits, typed payload decode, validation hooks, middleware/interceptors, graceful shutdown.
- Observability: tracing spans, metrics, operator-readable status, stuck job detection, lag/age metrics, DLQ inspection.
- Testing: local Docker/compose stack with Postgres and Kafka or Redpanda, failure injection tests, replay/idempotency tests, and `cargo check --workspace`.

## Open Questions

- Should kafkaman own a standalone worker process, embed in each service, or support both?
- Should the first release depend directly on `rdkafka`, or define a transport trait with only an `rdkafka` implementation?
- Should kafkaman implement polling from Postgres first, or reserve schema compatibility for Debezium CDC from the start?
- Should "commands over Kafka" be in scope for v1, or should v1 focus on outbox events and idempotent consumers?
- How much RepForge-specific context belongs in the library, such as actor context, Keycloak user identifiers, and cache invalidation conventions?

## Ingest Readiness

Ready for ingest as:

- A project objectives proposal.
- A reference page on similar projects and ecosystem.
- A decision candidate on Kafka command-first versus durable-execution-first scope.
- A plan for a first proof-of-concept: SQLx Postgres outbox + rdkafka publisher + Axum example service + integration test.
