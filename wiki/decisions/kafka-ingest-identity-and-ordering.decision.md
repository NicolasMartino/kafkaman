# Kafka Ingest Identity And Ordering

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-22
- Category: Durable receive
- Scope: Identity fields and ordering guarantee for the first Kafka ingest path.
- Sources:
  - wiki/plans/m3-durable-completion.plan.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - crates/kafkaman-rdkafka/src/lib.rs
  - tests/durable-send/tests/redpanda_full_loop/
- Related:
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md

## Decision

The first Kafka ingest path requires `kafkaman-idempotency-key` on the consumed
record and persists that value as the required received-table `idempotency_key`.
If present, `kafkaman-message-id`, `kafkaman-correlation-id`, and
`kafkaman-causation-id` are parsed as kafkaman metadata. User headers are copied
only when they are outside the reserved `kafkaman-` namespace.

The ingest operation performs:

1. consume record
2. deserialize payload
3. insert received row in Postgres with `ON CONFLICT (idempotency_key) DO NOTHING`
4. commit the database transaction
5. commit the Kafka offset

The API commits duplicate deliveries after the durable dedup row is observed,
because the row already exists and no message would be lost.

## Rationale

The ordering guarantee is the important durability boundary: the broker offset
is never committed before the received row transaction commits. Using the
existing kafkaman metadata headers preserves compatibility with the current
send-side `RdkafkaPublisher` wire format while keeping user headers out of the
reserved namespace.

## Evidence

- `full_loop_ingests_from_redpanda_and_dispatches_received_row`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop`
