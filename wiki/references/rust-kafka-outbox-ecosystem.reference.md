# Rust Kafka + Outbox Ecosystem

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-20
- Category: Ecosystem and prior art
- Scope: Survey of Rust Kafka clients, the nearest Rust outbox crate, and cross-language transactional-messaging comparables, to position kafkaman.
- Sources:
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/02-rdkafka-docs.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/03-rskafka-docs.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/04-outbox-pattern-processor.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/05-microservices-patterns.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/06-debezium-outbox-event-router.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/07-eventuate-tram.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/08-crates-io-outbox-search.json
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/09-crates-io-kafka-search.json
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/10-crates-io-rdkafka.json
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/11-crates-io-rskafka.json
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/sources/12-masstransit-nservicebus-outbox.md
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md

## Rust Kafka Clients

| Crate | Version | Total downloads | Role | Notes |
| --- | --- | --- | --- | --- |
| `rdkafka` | 0.39.0 | ~30.27M (5.65M recent) | Production client | Wraps `librdkafka`. Tokio integration, producer/consumer APIs, offset commits, transactions, metrics, broker metadata. |
| `rskafka` | 0.6.0 | ~334.6K | Minimal pure-Rust client | Intentionally minimal: **lacks consumer groups, offset tracking, and transactions.** Last updated 2025-03. |

Crate metadata retrieved 2026-06-20.

**Takeaway:** `rdkafka` is the strongest production Kafka client candidate for
kafkaman. `rskafka` is too minimal to back the durable-execution model (no
consumer groups / offsets / transactions).

## Nearest Rust Outbox Crate

**`outbox-pattern-processor`** (deroldo) — v0.4.0, updated 2026-06-15, ~14.5K
downloads.

- Dispatches outbox-pattern rows from a database to **SQS, SNS, or HTTP(S)**.
- Application writes to an `outbox` table; the processor relays it.
- **Not Kafka-first** and not a full microservice messaging framework.
- docs.rs/crates.io disagreed on recency (docs.rs showed 0.3.6, crates.io 0.4.0)
  — a caveat, not a blocker.

**Takeaway:** relevant prior art, but it points to a gap: this research did not
find a dominant Rust crate
combines Kafka + Postgres inbox/outbox + handler execution + retries + full
microservice integration. kafkaman has a plausible niche.

## Cross-Language Comparables

- **Debezium Outbox Event Router** — CDC-based outbox: reads the DB change log
  and routes outbox rows to Kafka topics. An alternative to polling; relevant to
  the "poll vs CDC" open question.
- **Eventuate Tram (JVM)** — mature transactional messaging: DB-backed
  send/receive, Kafka support, duplicate-message detection, interceptors,
  handler decorators. Closest conceptual sibling.
- **MassTransit / NServiceBus (.NET)** — show how mature frameworks package
  consumer outbox, inbox dedupe, transaction boundaries, and the API caveats
  that come with them.

## Canonical Patterns

- **Transactional outbox** (microservices.io) — write the message to an outbox
  table in the same local transaction as the business state change; relay
  asynchronously.
- **Idempotent consumer** (microservices.io) — track processed message IDs so
  redelivery is safe.

These two patterns are the backbone of kafkaman's durable send/receive promise.

## Net Conclusion

kafkaman's defensible niche: **Rust-native Kafka + Postgres durable messaging**
(outbox/inbox, retries, idempotency, observability) — not a generic message-broker
abstraction, and not a thin client wrapper. Back it with `rdkafka`; learn schema
and reliability shape from Eventuate Tram and the canonical patterns; keep
Debezium CDC in mind as a future ingestion path.
