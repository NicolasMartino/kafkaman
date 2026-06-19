# Research Manifest: kafkaman Objectives and Rust Microservice Integration

Research Question: What should kafkaman's project objectives be, what similar projects or patterns exist, and how should it integrate with the user's typical modern Rust fullstack microservice architecture?

Goal: Gather source material for later wiki ingest. This bundle should support a first project objective proposal, an ecosystem/reference page, and one or more architecture decision candidates.

Date: 2026-06-20
Source Modes: Path, web, crates.io metadata

## Source Inventory

1. `sources/01-cqrs-fullstack/`
   - Original path: `/Users/nicolasmartino/Documents/workout/cqrs-fullstack`
   - Acquisition method: local path copy with `rsync`
   - Exclusions: `.git`, `target`, `node_modules`, `dist`, `build`
   - Role: Local baseline for the user's typical Rust fullstack microservice architecture.

2. `sources/02-rdkafka-docs.md`
   - URL: https://docs.rs/rdkafka/latest/rdkafka/
   - Role: Rust Kafka client baseline.

3. `sources/03-rskafka-docs.md`
   - URL: https://docs.rs/rskafka/latest/rskafka/
   - Role: Pure Rust Kafka client alternative.

4. `sources/04-outbox-pattern-processor.md`
   - URLs:
     - https://docs.rs/outbox-pattern-processor/latest/outbox_pattern_processor/
     - https://github.com/deroldo/outbox-pattern-processor
   - Role: Closest Rust-specific outbox comparable found in this pass.

5. `sources/05-microservices-patterns.md`
   - URLs:
     - https://microservices.io/patterns/data/transactional-outbox.html
     - https://microservices.io/patterns/communication-style/idempotent-consumer.html
     - https://microservices.io/post/microservices/patterns/2020/10/16/idempotent-consumer.html
   - Role: Canonical pattern definitions for outbox and idempotent consumer.

6. `sources/06-debezium-outbox-event-router.md`
   - URL: https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
   - Role: CDC-based outbox alternative and Kafka routing reference.

7. `sources/07-eventuate-tram.md`
   - URL: https://eventuate.io/docs/manual/eventuate-tram/latest/getting-started-eventuate-tram.html
   - Role: Mature JVM transactional messaging comparable.

8. `sources/08-crates-io-outbox-search.json`
   - URL: https://crates.io/api/v1/crates?q=outbox
   - Role: crates.io search evidence for Rust outbox-related crates.

9. `sources/09-crates-io-kafka-search.json`
   - URL: https://crates.io/api/v1/crates?q=kafka
   - Role: crates.io search evidence for Kafka-related Rust crates.

10. `sources/10-crates-io-rdkafka.json`
    - URL: https://crates.io/api/v1/crates/rdkafka
    - Role: direct rdkafka metadata.

11. `sources/11-crates-io-rskafka.json`
    - URL: https://crates.io/api/v1/crates/rskafka
    - Role: direct rskafka metadata.

12. `sources/12-masstransit-nservicebus-outbox.md`
    - URLs:
      - https://masstransit.io/documentation/patterns/transactional-outbox
      - https://docs.particular.net/nservicebus/outbox/
    - Role: mature .NET outbox framework comparables.

## Selection Rationale

- The local `cqrs-fullstack` project was included because the user explicitly identified it as the typical architecture model.
- Web sources prioritize official project docs, docs.rs/crates.io metadata, and canonical pattern sources.
- Cross-language comparables were selected because the Rust ecosystem appears to have Kafka clients and at least one outbox processor, but no obvious dominant Rust framework combining Kafka, Postgres inbox/outbox, handler execution, retries, and full microservice integration.

## Gaps And Caveats

- This is research intake only. No wiki pages were updated.
- The copied `cqrs-fullstack` source was not exhaustively reviewed file by file.
- The research did not clone or build external comparable projects.
- The `outbox-pattern-processor` docs.rs and crates.io data disagree on visible version recency: crates.io metadata showed 0.4.0 updated 2026-06-15, while docs.rs showed generated docs for 0.3.6.
- The local RepForge architecture has an important tension with kafkaman's original project idea: RepForge current ADRs moved authoritative mutation completion away from Kafka command topics toward BFF durable mutation queue plus internal HTTP, while kafkaman's stated idea is service-to-service Kafka instead of REST. This needs an explicit project decision.
