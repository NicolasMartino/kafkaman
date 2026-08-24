# M5 Entity-First Outbox Supersede Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-14
- Category: Entity-first outbound ordering and schema compatibility
- Scope: Records public API, schema, and relay behavior changes from the M5
  per-entity outbox supersede implementation slice.
- Sources:
  - wiki/plans/entity-first-propagation.plan.md
  - wiki/decisions/entity-first-propagation-model.decision.md
  - crates/kafkaman-core/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - tests/durable-send/tests/entity_first_outbox_supersede.rs
- Related:
  - wiki/compatibility/m5-entity-first-cache-api.compat.md

## Public API Changes

- `kafkaman_core::OutboxStatus` adds `Superseded`.
- `kafkaman_core::OutboxRow` adds `entity_key: Option<String>`.
- `kafkaman_sqlx::AddOutboxEntityKey` is added as an upgrade changeset for
  outbox tables created before this slice.
- `kafkaman_sqlx::add_outbox_entity_key_sql` is added for the column upgrade.

## Schema Changes

- Fresh outbox tables now include nullable `entity_key TEXT`.
- Fresh outbox tables now include an entity-state index over
  `(entity_key, status, created_at)` for rows with a non-null `entity_key`.
- Existing outbox tables can be upgraded with `AddOutboxEntityKey`; the changeset
  adds the column and entity-state index idempotently.

## Runtime Behavior Changes

- Enqueue persists `entity_key` for every outbox row.
- Registered entity message types with a valid idempotency identity acquire a
  transaction-scoped advisory lock keyed by schema, message type, and entity key
  before supersede decisions.
- Under that lock, enqueue marks existing `Pending` rows for the same
  entity as `Superseded` before inserting the new pending row.
- Enqueue does not supersede rows that are already `Publishing`; those rows
  remain in flight.
- Relay claiming excludes `Superseded` rows by status and also blocks a newer
  `Pending` row while another row for the same entity is still `Publishing`.
- When an outbox row's Kafka partition key is not the entity key, kafkaman
  stores the internal `kafkaman-entity-key` header in the outbox row. User-supplied
  reserved `kafkaman-*` headers remain rejected.

## Verified

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_outbox_supersede -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

## Deferred

- ~~Boot-time broker topic validation that entity topics use
  `cleanup.policy=compact` alone.~~ **Delivered 2026-08-25**, outside M5, by
  [topic-convergence-api.compat.md](topic-convergence-api.compat.md): checked at
  boot under `[topics] mode`, before any loop is spawned. Struck here rather
  than deleted because this note is a release record.
- A positive state-sourced republish API. `Replay::outbox` is now rejected for
  all message types; the replacement resync surface is not implemented.
- Broker topic-lifecycle invalidation and forced re-bootstrap hooks. A received
  row whose cache origin differs now fails as `CacheOriginMismatch`, but kafkaman
  does not yet detect topic recreation or repartitioning proactively.
