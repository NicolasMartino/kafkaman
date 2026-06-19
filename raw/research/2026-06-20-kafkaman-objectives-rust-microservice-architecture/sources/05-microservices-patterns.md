# Source Note: Transactional Outbox and Idempotent Consumer Patterns

Transactional outbox URL: https://microservices.io/patterns/data/transactional-outbox.html
Idempotent consumer URL: https://microservices.io/patterns/communication-style/idempotent-consumer.html
Related article URL: https://microservices.io/post/microservices/patterns/2020/10/16/idempotent-consumer.html
Retrieved: 2026-06-20
Mode: Web

## What These Sources Are

Microservices.io documents the canonical transactional outbox and idempotent consumer patterns. These are not Rust-specific, but they define the distributed systems problem kafkaman is meant to solve.

## Relevant Findings

- Transactional outbox stores messages in the same database transaction as application state, then a separate relay publishes them to the broker.
- The pattern avoids two-phase commit while preserving the guarantee that committed database work has a corresponding message to send.
- It requires preserving send order, usually by aggregate or partition key.
- The idempotent consumer pattern exists because brokers with at-least-once delivery can redeliver messages.
- A consumer can record processed message IDs in a database table, commonly keyed by subscriber and message ID, and use a uniqueness constraint to reject duplicates.

## Implication For kafkaman

kafkaman should make these two patterns first-class:

- Outgoing path: domain transaction inserts an outbox row; scheduler/relay claims rows and publishes to Kafka; delivery state is durable.
- Incoming path: consumer stores or checks an inbox/processed-message row before executing the handler; handler completion and message acknowledgment are coordinated.

The library should be explicit that Kafka exactly-once semantics do not automatically make external database writes exactly-once. Application-level idempotency remains necessary.

## Caveats

The pattern sources describe concepts, not Rust APIs. kafkaman still needs concrete integration decisions for SQLx transactions, Kafka offsets, concurrency, backoff, worker ownership, and schema migrations.
