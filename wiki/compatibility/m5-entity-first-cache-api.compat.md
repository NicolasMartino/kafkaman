# M5 Entity-First Cache API Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-13
- Category: Entity-first cache API and behavior compatibility
- Scope: Records public API and runtime behavior changes from the first M5 entity-first propagation implementation slice.
- Sources:
  - wiki/plans/entity-first-propagation.plan.md
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - crates/kafkaman-core/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - crates/kafkaman-test/src/lib.rs
  - tests/durable-send/tests/entity_first_propagation.rs

## Public API Changes

- `kafkaman_core::RetentionClass` is added with `Compact` and `Delete`.
- `KafkaMessage` gains defaulted methods:
  - `entity_key(&self, message_id: Uuid) -> String`, defaulting to
    `message_id.to_string()`.
  - `retention_class() -> RetentionClass`, defaulting to `Delete`.
- `MessageDescriptor` now stores `retention_class`. Constructor-style use through
  `MessageDescriptor::new` and `KafkaMessage::descriptor` remains compatible and
  defaults to `Delete`; direct struct literals must provide the new field.
- `kafkaman_sqlx::CacheTable`, `CreateCacheTable`, and `create_cache_table_sql`
  are added for compact entity cache storage.

## Runtime Behavior Changes

- Receive dispatch now applies a guarded cache upsert for `Compact` message
  types before marking the received row `Processed`.
- The upsert key is chosen from `kafkaman-entity-key` when present, then the
  Kafka record key when it is valid UTF-8, then `message_id` as the default.
- The cache guard updates only when the incoming record has the same topic and
  partition as the stored row and a higher offset. Older retry/redrive rows can
  still be processed, but they do not regress cache state.
- `kafkaman-test::Harness` creates a cache table when a compact received type is
  registered, so M5 convergence tests can assert cache state through the normal
  harness path.

## Verified

- `rtk cargo check --workspace --all-features`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

## Deferred

- Boot-time broker topic validation from retention class to `cleanup.policy`.
- Topic/partition mismatch invalidation path.
- State-sourced `Replay::outbox` rejection for compact entity types.
- Key-serialized outbound supersede and `Superseded` outbox status are covered
  by [m5-entity-first-outbox-supersede.compat.md](m5-entity-first-outbox-supersede.compat.md).
