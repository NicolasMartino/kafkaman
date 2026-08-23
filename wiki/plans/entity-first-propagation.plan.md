# Entity-First Propagation Implementation Plan

- Document Class: Plan
- Status: Completed
- Date: 2026-08-13
- Category: Delivery execution
- Scope: Tactical implementation sequence for kafkaman's compact entity-cache
  purview — per-type cache tables, the offset-guarded upsert,
  entity-key-serialized outbox supersede, state-sourced republish, topic
  lifecycle detection, and removal of the superseded delete-retention work-item
  surface.
- Sources:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - wiki/specs/m3-durable-receive.spec.md
  - wiki/specs/m4-retry-backoff-dlq.spec.md
  - crates/kafkaman-sqlx/src/lib.rs
- Related:
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md
  - wiki/proposals/05-deep-durability-testing.proposal.md
  - wiki/decisions/ingest-poison-quarantine-policy.decision.md

## Deliverable

kafkaman owns the distributed cache for compact domain entities: per-type cache
tables, a guarded upsert keyed on the Kafka offset, and the send-side supersede
that makes that offset trustworthy. The validated M5 closeout is promoted to
[entity-first-propagation.spec.md](../specs/entity-first-propagation.spec.md).

Closeout 2026-08-24: the implementation validates retry/redrive/concurrent
dispatch convergence, received-row entity identity, per-entity outbound
supersede and claim-time pending collapse, unsafe outbox replay rejection,
jittered retry backoff, and opt-in outbox retention. Bootstrap/readiness,
boot-time broker topic validation, advisory origin intent, a positive
state-sourced republish API, proactive topic-lifecycle invalidation, and a typed
soft-delete workflow are explicitly deferred.

## In Scope

- Entity message surface: `entity_key`, advisory origin intent.
- Per-type cache table generation and changesets for in-purview compact entity
  types.
- Boot-time topic validation that in-purview entity topics use
  `cleanup.policy=compact` alone, when broker metadata is available.
- Offset-guarded cache upsert owned by kafkaman.
- `Superseded` outbox status and per-entity supersede on enqueue.
- State-sourced republish, and the `Replay::outbox` constraint for in-purview
  entity types.
- Topic-lifecycle detection and the forced re-bootstrap hook.
- Soft-delete-first as entity state.

## Out of Scope

- Cache bootstrap and readiness typestate (proposal 10) beyond the hook this
  plan calls.
- Reclamation of soft-deleted cache rows.
- Real-tombstone ingestion for foreign producers (proposal 07 residual).
- Non-entity work items, commands, generic jobs, emails, payments, analytics
  events, mutation dispatch queues, and direct transport.
- Schema separation and restore policy (deferred; see
  `raw/design/2026-08-12-restore-policy-and-schema-separation-discussion.md`).
- Proposal 08 LISTEN/NOTIFY polling cost.

## Prerequisite

Satisfied 2026-08-13: the M3/M4 branch merged to `main`. Every phase below
builds on shipped receive surface — `source_offset` columns, dispatch claiming,
retry accounting, `Replay::received`.

## Phase 0 — Gap-revealing tests

Tests lead, per the roadmap's outside-in method. These must **fail** against
current code, because current code has no cache at all; they encode the
convergence contract before any of it exists.

1. `retry_after_newer_applied_does_not_regress_cache` — older row fails, backs
   off, newer row applies, older row retries and must lose. This is the M4 case
   that motivates the whole guard.
2. `concurrent_dispatch_of_two_states_converges_to_newer` — two dispatchers,
   unequal handler durations, no failures.
3. `redrive_after_newer_applied_does_not_regress_cache` — `Replay::received`
   on a terminal row whose entity has since advanced.
4. `redelivery_after_received_purge_does_not_regress_cache` — the dedup-memory
   gap.
5. `bootstrap_replay_from_zero_converges_to_current_state` — same guard, no
   special-casing.

Progress 2026-08-13: `retry_after_newer_applied_does_not_regress_cache`,
`concurrent_dispatch_of_two_states_converges_to_newer`, and
`redrive_after_newer_applied_does_not_regress_cache` are implemented in
`tests/durable-send/tests/entity_first_propagation.rs` and pass against the first
cache-upsert slice. Redelivery-after-purge and bootstrap replay remain pending.

## Phase 1 — Entity Surface And Cache Tables

- Entity message trait surface: `entity_key()` and `origin_intent()`.
- `#[non_exhaustive]` intent enum with a catch-all wire variant. The catch-all is
  a hard requirement: without it one added variant trips the ingest
  consecutive-skip circuit breaker topic-wide on un-redeployed consumers.
- `CacheTable` mirroring `ReceivedTable`/`OutboxTable` construction, schema-
  qualified, generated for in-purview entity descriptors.
- `CreateCacheTable` changeset for entity types. Columns: `entity_key`
  PRIMARY KEY, payload, `applied_topic`, `applied_partition`, `applied_offset`,
  `deleted`, `updated_at`.
- Boot validation checks that entity topics use `cleanup.policy=compact` alone
  when broker metadata is available.
- `kafkaman-entity-key` reserved header where the entity key is not the record
  key.

Progress 2026-08-13: `RetentionClass`, defaulted `KafkaMessage::entity_key`,
defaulted `KafkaMessage::retention_class`, `CacheTable`, `CreateCacheTable`, and
compact-type cache generation in `kafkaman-test::Harness` were implemented as
the first slice. Boot-time broker topic validation remains deferred after M5.

Progress 2026-08-14: proposal 12 narrowed kafkaman's purview to compact
entity-cache propagation only. The follow-up removes `RetentionClass`,
`KafkaMessage::retention_class`, `MessageDescriptor.retention_class`, and the
default `entity_key = message_id`; cache tables, cache upsert, entity headers,
and outbound supersede now apply to every registered kafkaman message type.

Closeout 2026-08-24: a full-workspace audit corrected entity-key persistence:
kafkaman's own receive path resolves entity identity from the typed payload at
ingest and stores it in the received row's `entity_key` column. Legacy rows fall
back to the Kafka record key only. There is no header tier and no `message_id`
fallback; unresolvable rows fail with `MissingEntityKey`.

## Phase 2 — The guarded upsert

- kafkaman performs the upsert; the handler does not.
- Guard: `WHERE incoming.source_offset > current.applied_offset`, applied only
  when `applied_topic` and `applied_partition` match the incoming record.
- Mismatch on topic or partition is **not** silently accepted — it raises the
  invalidation path in Phase 5.
- Runs inside the dispatch transaction, so a cache write and the received-row
  status update commit together.
- Phase 0 tests turn green here.

Progress 2026-08-13: receive dispatch applies the compact cache upsert before
marking a row `Processed`. The guard prevents older retry/redrive rows from
regressing cache state.

Closeout 2026-08-24: topic/partition mismatch now fails the affected row as
`CacheOriginMismatch` instead of freezing the cache silently. The broader broker
topic-lifecycle detection and forced re-bootstrap hook remain deferred.

## Phase 3 — Per-entity supersede

- `Superseded` outbox status, added to the status CHECK list and excluded from
  relay claiming.
- Enqueue takes a transaction-scoped lock keyed by `(message_type, entity_key)`
  before inspecting outbox state, using either a PostgreSQL advisory lock or a
  dedicated entity enqueue lock/control row.
- After acquiring the key lock, enqueue re-reads the latest outbox row for that
  entity and only then decides whether to supersede or insert.
- Enqueue for an entity with a pending row for the same entity marks that row
  `Superseded` and inserts the new row, in one transaction.
- Enqueue while a row for that entity is claimed for send inserts a fresh row
  without superseding.
- Gate: `first_concurrent_enqueues_for_entity_serialize` — two writers race for
  an entity with no existing outbox row and finish with one pending winner.
- Gate: `waiting_writer_rereads_latest_outbox_before_deciding` — a writer
  blocked behind another writer observes the row inserted while it waited.
- Gate: `supersede_races_in_flight_send_preserves_order` — the case where the
  relay claims a row between the supersede check and the insert.
- Gate: `supersede_collapses_queued_updates` — N enqueues, one publish.

Correctness note for reviewers: supersede is what licenses offset-as-ordinal. It
is not an optimization that can be dropped later without reopening the ordinal
choice. Locking only the latest existing outbox row is insufficient: the first
concurrent writes have no row to lock, and a waiting writer must re-read latest
state under the entity-key lock before deciding.

Progress 2026-08-14: `Superseded`, nullable outbox `entity_key`,
`AddOutboxEntityKey`, compact-type key-level advisory enqueue locking, pending
row supersede, and relay claim blocking behind same-entity `Publishing` rows are
implemented. Verified gates:
`first_concurrent_enqueues_for_entity_serialize`,
`supersede_collapses_queued_updates`, and
`publishing_entity_blocks_newer_pending_claim_until_published`. The first gate
also covers the waiting-writer re-read case because the second writer waits
behind the first writer's uncommitted insert, then supersedes it only after the
first transaction commits.

## Phase 4 — State-sourced republish

- `Replay::outbox` rejects all in-purview entity types at construction, with an
  error naming the state-sourced alternative.
- A resync surface that re-reads current entity state and enqueues it through the
  normal path, so supersede applies.
- Gate: `resync_racing_live_update_loses_to_live_update` — the drift-repair
  corruption case, now expected to resolve in favor of the live update.

Closeout 2026-08-24: the negative half shipped: `Replay::outbox` is now always
rejected as `UnsafeOutboxReplay`. The positive state-sourced resync API and race
gate remain deferred.

## Phase 5 — Topic-lifecycle invalidation

- Verify whether rdkafka exposes the Kafka topic ID. If it does, persist it
  alongside `applied_topic` and treat a change as invalidation.
- Regardless: consecutive-regression circuit breaker, structurally mirroring
  `ConsecutiveSkipLimitExceeded`. One below-watermark message is normal; N
  consecutive distinct entities regressing is the signature.
- On trip: halt ingest for that type, surface a typed error, require explicit
  re-bootstrap. No self-healing — a false positive would wipe a healthy cache.
- Gate: `topic_recreation_trips_circuit_breaker_without_wiping_cache`.
- Gate: `single_stale_redelivery_does_not_trip_breaker`.

Closeout 2026-08-24: proactive topic-ID validation and the global
consecutive-regression breaker are not implemented. The shipped protection is
row-level `CacheOriginMismatch`, which fails the affected row terminally without
halting unrelated entities.

## Phase 6 — Soft-delete-first

- Deletion carried as entity state through the same guard, same upsert, same
  ordering — no separate message type and no real tombstone on emission.
- Gate: `delete_followed_by_pre_delete_redelivery_stays_deleted`.

Closeout 2026-08-24: cache tables include the `deleted` column, but M5 does not
validate a typed soft-delete emission/ingestion workflow or reclamation batch.
Those remain future work.

## Phase 7 — Promotion

- Compatibility note covering the `Superseded` status, cache tables and
  columns, the `Replay::outbox` constraint, the reserved entity-key header, and
  the 2026-08-14 removal of the delete-retention work-item surface.
- Promote validated behavior to `wiki/specs/entity-first-propagation.spec.md`.
- Confirm the V1 roadmap still reflects reality; this work is slotted as M5,
  ahead of observability, with hardening at M7.

Completed 2026-08-24. Validated behavior is promoted to
[entity-first-propagation.spec.md](../specs/entity-first-propagation.spec.md);
the roadmap and index mark M5 completed and leave the two-service example active
as a parallel wiring proof.

## Verification Gates

- `rtk cargo test --workspace --all-features`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- Clippy clean at the workspace's existing strictness.

Closeout 2026-08-24:

- `rtk cargo test --workspace --all-features` - 141 passed, 3 ignored.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests` - 69
  passed, 3 ignored.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features
  redpanda --test redpanda_full_loop -- --test-threads=1` - 11 passed.
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings` -
  no issues found.
- `rtk cargo fmt --all -- --check`

## Known Risks

- **Cross-database writers remain forbidden.** The key-level enqueue lock solves
  horizontally scaled instances sharing one database. It does not coordinate two
  services, or two databases, writing the same entity type; that remains an
  entity-ownership violation rather than a supported topology.
- **Phase 5 depends on an unverified rdkafka capability.** The circuit breaker
  is the fallback and does not depend on it, so Phase 5 can land either way.
- **Cache ownership is a scope expansion.** kafkaman generating and writing
  per-type state tables is what makes it a distributed cache library rather than
  a message library; proposal 09 open question 1 records that this was a
  deliberate choice, and it is the largest single commitment in this plan.
- **Public-surface removal is intentionally breaking pre-v1.** The
  `RetentionClass` split and default entity-key fallback are removed instead of
  deprecated so kafkaman does not advertise generic durable jobs.
