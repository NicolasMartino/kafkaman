# Receive Handler Surface Scope

Document Class: Decision
Status: Accepted
Date: 2026-06-22
Category: Durable receive
Scope: Minimal handler API needed for consume-then-produce atomicity in M3.
Sources:
- wiki/plans/m3-durable-completion.plan.md
- wiki/decisions/message-consumption-and-handler-model.decision.md
- crates/kafkaman-sqlx/src/lib.rs
- tests/durable-send/tests/durable_receive.rs
Related:
- wiki/decisions/message-consumption-and-handler-model.decision.md

## Decision

M3 keeps the explicit closure handler surface:

```rust
Fn(&mut PgConnection, ReceivedMeta, P) -> HandlerFuture
```

and adds `enqueue_on_connection` so handlers can enqueue an outbox message using
the same receive transaction connection.

The larger `FromMessage` / `Rx` / state / Tower `Service` and `Layer` model
remains deferred until a later ergonomics phase. It is not required to prove M3
consume-then-produce atomicity.

## Rationale

The existing closure surface already gives handlers the transaction-bound
connection and metadata. Adding a public connection-based enqueue helper is the
smallest API that proves the durability property without committing M3 to the
larger Tower handler model before the explicit APIs are validated.

## Evidence

- `handler_enqueues_outbox_atomically_with_receive_transaction`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
