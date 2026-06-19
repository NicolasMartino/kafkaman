# Source Note: outbox-pattern-processor

Docs.rs URL: https://docs.rs/outbox-pattern-processor/latest/outbox_pattern_processor/
Repository URL: https://github.com/deroldo/outbox-pattern-processor
Related metadata: `08-crates-io-outbox-search.json`
Retrieved: 2026-06-20
Mode: Web and crates.io metadata

## What This Source Is

`outbox-pattern-processor` is a Rust crate found through a crates.io `outbox` query.

Crates.io metadata saved in `08-crates-io-outbox-search.json` records:

- Default version: 0.4.0
- Updated: 2026-06-15
- Repository: https://github.com/deroldo/outbox-pattern-processor
- Downloads: 14,478 at retrieval time
- Recent downloads: 75 at retrieval time

Docs.rs for the latest generated documentation showed version 0.3.6 and 0 percent documented. The repository README describes the library as a way to dispatch outbox-pattern data from a database to SQS, SNS, or HTTP(S) gateways.

## Relevant Findings

- The application writes to an `outbox` table.
- The processor can run as a worker and dispatch to supported destinations.
- The README describes partition keys, idempotent keys, retry/failure limits, delay, query limits, execution interval, and cleanup settings.
- Optional dependencies include SQLx, Tokio, Axum-related worker code, AWS SDKs, reqwest, tracing, and uuid.
- The README notes an `x-idempotent-key` header/message attribute for downstream duplicate handling.

## Implication For kafkaman

This is the closest Rust-specific comparable located in the research pass, but it is not a Kafka-first microservice framework. It suggests there is room for a Rust library focused on Postgres-backed Kafka inbox/outbox execution with stronger service integration, typed envelopes, consumer handling, DLQ behavior, and observability.

## Caveats

The docs.rs page was sparse and showed a lower version than crates.io metadata. Before treating this as a design precedent, inspect the repository source and release history. It should be considered a comparable, not a direct foundation.
