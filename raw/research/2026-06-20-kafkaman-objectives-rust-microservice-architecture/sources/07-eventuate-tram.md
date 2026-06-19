# Source Note: Eventuate Tram

Source URL: https://eventuate.io/docs/manual/eventuate-tram/latest/getting-started-eventuate-tram.html
Retrieved: 2026-06-20
Mode: Web

## What This Source Is

Eventuate Tram is a framework for transactional messaging in JVM microservices. It supports database-backed message production and consumption across message brokers including Kafka.

## Relevant Findings

- Eventuate Tram sends and receives messages as part of a database transaction.
- It supports transaction log tailing for MySQL and Postgres and polling for other SQL databases.
- It supports Kafka, ActiveMQ, RabbitMQ, and Redis as brokers.
- Its producer implementation uses JDBC.
- Its consumer side includes broker-specific dependencies and optional JDBC-based idempotency.
- It provides a duplicate message detector abstraction, including a SQL table based detector that records successfully processed message IDs.
- It has extension points around message interceptors and handler decorators.

## Implication For kafkaman

Eventuate Tram is a mature non-Rust comparable for kafkaman's likely shape:

- A producer API that writes durable messages within the service database transaction.
- A consumer runtime with duplicate detection and transaction management.
- Pluggable broker backends, even if Kafka is first.
- Handler/interceptor hooks for tracing, headers, actor context, validation, and domain-specific behavior.

## Caveats

Eventuate Tram is tied to Java frameworks and JDBC. kafkaman should not clone its API surface directly; the Rust equivalent should be trait and async-first, SQLx-compatible, and aligned with Tokio, Axum, tracing, and workspace crates.
