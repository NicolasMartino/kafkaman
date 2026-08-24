# M5 Entity-First Cache API Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-13
- Revised: 2026-08-14
- Category: Entity-first cache API and behavior compatibility
- Scope: Records public API and runtime behavior changes from the first M5
  entity-first propagation implementation slice. Revised 2026-08-14: records the
  removal of the superseded retention-class API after kafkaman's purview
  narrowed to compact entity-cache propagation only.
- Sources:
  - wiki/plans/entity-first-propagation.plan.md
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - crates/kafkaman-core/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - crates/kafkaman-test/src/lib.rs
  - tests/durable-send/tests/entity_first_propagation.rs

## Public API Changes

- `KafkaMessage` gains a required `entity_key(&self) -> String` method. The
  previous default to `message_id` is removed so every message declares a real
  entity identity.
- `kafkaman_core::RetentionClass` is removed.
- `KafkaMessage::retention_class` is removed.
- `MessageDescriptor.retention_class` is removed.
- `kafkaman_sqlx::CacheTable`, `CreateCacheTable`, and `create_cache_table_sql`
  are added for entity cache storage.

## 2026-08-14 Scope Amendment

Proposal 12 now selects compact entity-cache purview only. The same-day
implementation removes the public retention-class split instead of carrying a
`delete` compatibility path forward.

## Runtime Behavior Changes

- Receive dispatch now applies a guarded cache upsert for registered message
  types before marking the received row `Processed`.
- The upsert key is chosen from the received row's `entity_key` column, written
  from the typed payload at ingest. Rows written before that column existed fall
  back to the Kafka record key when it is valid UTF-8.
- The upsert deliberately does not read `kafkaman-entity-key` from received-row
  headers and does not fall back to `message_id`. Header-only or keyless legacy
  rows fail with `Error::MissingEntityKey` instead of being cached under a
  fabricated identity.
- The cache guard updates only when the incoming record has the same topic and
  partition as the stored row and a higher offset. Older retry/redrive rows can
  still be processed, but they do not regress cache state.
- A topic or partition mismatch now fails as `Error::CacheOriginMismatch`
  instead of being silently ignored, because the stored offset is not comparable
  with the incoming origin.
- `kafkaman-test::Harness` creates a cache table when a received type is
  registered, so M5 convergence tests can assert cache state through the normal
  harness path.

## Verified

- `rtk cargo check --workspace --all-features`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
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
- Key-serialized outbound supersede and `Superseded` outbox status are covered
  by [m5-entity-first-outbox-supersede.compat.md](m5-entity-first-outbox-supersede.compat.md).
