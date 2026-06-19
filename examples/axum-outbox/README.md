# Axum Outbox Example

This example demonstrates the M1 durable-send path without `kafkaman-axum`:

1. run `kafkaman::migrate()` with a local changelog,
2. start the relay worker,
3. write business state and enqueue an outbox row in one SQL transaction.

## Requirements

- Postgres reachable through `DATABASE_URL`
- Kafka or Redpanda reachable through `KAFKA_BROKERS`

## Run

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/kafkaman \
KAFKA_BROKERS=localhost:9092 \
cargo run -p axum-outbox
```

Create an order:

```bash
curl -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -d '{"description":"first order"}'
```

The endpoint inserts into `orders` and enqueues `OrderCreated` in the same
transaction. The relay publishes the outbox row after commit and then marks it
`Published`.
