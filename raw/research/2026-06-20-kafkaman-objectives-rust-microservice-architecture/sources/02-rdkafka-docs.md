# Source Note: rust-rdkafka

Source URL: https://docs.rs/rdkafka/latest/rdkafka/
Related metadata: `10-crates-io-rdkafka.json`
Retrieved: 2026-06-20
Mode: Web

## What This Source Is

`rdkafka` is the main Rust wrapper around `librdkafka`. The docs.rs page reviewed during research showed version 0.39.0 and describes it as a futures-enabled Apache Kafka client for Rust.

Crates.io metadata saved in `10-crates-io-rdkafka.json` records:

- Default version: 0.39.0
- Repository: https://github.com/fede1024/rust-rdkafka
- Category: API bindings
- Recent downloads: 5,647,482 at retrieval time

## Relevant Findings

- It supports high-level async producer and consumer APIs, including `FutureProducer` and `StreamConsumer`.
- It integrates with Tokio, matching the typical modern Rust service stack used by the sibling project.
- It exposes delivery semantics needed by kafkaman: manual/custom offset commits, idempotent and transactional producers, read-committed consumers, metrics, callbacks, and broker metadata access.
- Its installation model can be either static build of `librdkafka` or dynamic linking to system `librdkafka`.
- RepForge already has `rdkafka = "0.36"` with `dynamic-linking` in its workspace dependencies, so kafkaman should either preserve that deployment assumption or make the Kafka backend feature-gated.

## Implication For kafkaman

`rdkafka` is the strongest first backend for a Rust Kafka library that needs production-grade Kafka semantics. kafkaman should avoid hiding Kafka concepts entirely; it should expose enough configuration for producer acks, consumer groups, offset commits, partitions, retry topics, DLQs, and metrics.

## Caveats

`rdkafka` is a Kafka client, not an inbox/outbox framework. kafkaman's value would be the durable Postgres scheduler, handler lifecycle, idempotency, retry state, schema/version conventions, and service integration around this client.
