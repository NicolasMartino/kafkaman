# M5 Code Audit Remediation Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-24
- Category: Public API, schema, and runtime behavior compatibility
- Scope: Records the public surface, schema, and behavior changes made while
  remediating the full-workspace code audit on
  `implementation/m5-entity-first-propagation`. Every change here closes a gap
  between a documented invariant and its implementation.
- Sources:
  - review.md
  - wiki/decisions/entity-first-propagation-model.decision.md
- Related:
  - wiki/compatibility/m5-entity-first-cache-api.compat.md
  - wiki/compatibility/m5-entity-first-outbox-supersede.compat.md

## Behavior Changes (Silent Failures Closed)

### Entity key no longer depends on a header surviving ingest

Enqueue writes `kafkaman-entity-key` into stored headers when the partition key
differs from the entity key, and the publisher emits it. Ingest, however, strips
the entire `kafkaman-` namespace from user headers, so the consumer never saw it
and `received_entity_key` fell back to the Kafka record key — or, absent one, to
`message_id`. For every type whose partition key is not its entity key, the
cache was keyed on the wrong value and could not converge.

The entity key is now resolved from the typed payload at ingest and persisted in
a new `entity_key` column on the received table. The header is still published
for foreign consumers, but kafkaman's own path no longer depends on it.

Resolution order is now: the `entity_key` column, then the Kafka record key for
rows written before that column existed. There is deliberately **no** header
tier — it could never fire, because ingest strips the reserved namespace and
`insert_received_with_outcome` rejects any envelope carrying a reserved header.
A row with neither fails with `Error::MissingEntityKey`.

### An unresolvable entity key is an error, not a fabricated key

`received_entity_key` no longer falls back to `message_id`. That fallback gave
every message its own cache row, so the cache grew without bound and never
converged, while appearing healthy until read. Unresolvable rows now fail with
`Error::MissingEntityKey` and go through normal retry/DLQ handling.

### A partition change halts instead of freezing the cache

The guarded cache upsert requires `applied_topic` and `applied_partition` to
match. If an entity moved partition the predicate could never be true again and
the row froze permanently, with no error and no metric. The upsert now inspects
its result and distinguishes the two indistinguishable outcomes:

- `CacheApplyOutcome::Ignored` — a stale record correctly dropped;
- `Error::CacheOriginMismatch` — topic or partition changed, which fails the row
  and names the required re-bootstrap, per decision point 4.

`CacheOriginMismatch` and `MissingEntityKey` are **terminal on the first
attempt**: the condition is a property of the stored row, so retrying re-reads it
and fails identically, spending the whole retry budget and delaying the operator
signal by exactly that long. Decision point 4's consecutive-regression breaker is
deliberately not implemented — a breaker halts the pipeline, and one entity's
repartition must not stop dispatch for every other entity. Failing the affected
row immediately achieves the same purpose with a blast radius of one row.

### Keyless entity types publish under their entity key

The Kafka record key came only from `partition_key()`. A type declaring none
produced keyless records, which Kafka routes round-robin — scattering one
entity's snapshots across partitions where the convergence guard can never
reconcile them. The key now falls back to `entity_key` via the new
`OutboxRow::record_key()`.

### Producer occurrence time and idempotency source cross the wire

`occurred_at` was re-stamped at ingest, so every received row recorded consumer
arrival time rather than event time, and `idempotency_source` — modelled as a
column on both tables — was always `NULL` for ingested rows. Both now travel as
`kafkaman-occurred-at` (RFC 9557) and `kafkaman-idempotency-source`.

The two are read back with deliberately different strictness. A malformed
`kafkaman-occurred-at` is a hard `InvalidHeader` ingest failure, because silently
falling back would record consumer arrival time as event time. A malformed
`kafkaman-idempotency-source` is **dropped to `NULL` and ingest proceeds**: dedupe
compares the digest, which is carried separately and validated, so quarantining
the record would cost real delivery to preserve a triage aid. A foreign producer
that encodes its source differently is therefore ingested, not rejected.

### Retry backoff carries jitter

Backoff was purely exponential, so a batch failed by one outage retried in
lockstep against the recovering dependency. Delays now use equal jitter and land
in `[base/2, base]`. **Tests asserting an exact `next_attempt_at` must assert a
window instead.**

### Producer is idempotent

`RdkafkaPublisher::from_brokers` now sets `enable.idempotence=true` and
`acks=all`. An outbox relay republishes on every uncertain outcome, so without
broker-side dedupe a retried send after a lost ack writes the snapshot twice at
two offsets.

## Breaking API Changes

| Removed / changed | Replacement | Reason |
| --- | --- | --- |
| `Replay::outbox::<T>()` now always returns `Err(Error::UnsafeOutboxReplay)` | `Replay::received`, or state-sourced republish | Row-sourced replay emits stale state at a fresh, higher offset; every consumer cache then treats it as newest. Decision point 9; the README already promised this. |
| `Envelope::with_idempotency_key` (panicking) | `Envelope::with_idempotency_identity` (infallible), `try_with_idempotency_key` (fallible), or `kafkaman_test::EnvelopeTestExt` in tests | A library builder must not abort the caller's process on bad input. All 72 call sites were tests. |
| `RelayConfig::validate() -> Result<(), String>` | `Result<(), kafkaman_core::Error>` | Stringly-typed error in a `thiserror` crate. |
| `kafkaman_worker::Error::InvalidConfig(String)` | `Error::Core(kafkaman_core::Error)`, `Error::InvalidDispatcherConfig` | Same. |
| `ClaimedOutboxRow.claim_id` is per batch, not per row | — | `claim_batch` is now one statement. The id identifies a claim *generation*; `(message_id, claim_id)` matching is unchanged, so stale-claim rejection still holds. |
| `ReplayTarget` and the outbox replay SQL builders | — | Unreachable once outbox replay was rejected. |

## Additive API

- `AddReceivedEntityKey` changeset — upgrades a received table created before
  the `entity_key` column. Fresh tables get it from `create_received_table_sql`.
- `ReceivedRow.entity_key` / `ReceivedMeta.entity_key`.
- `OutboxRow::record_key()`.
- `CacheApplyOutcome`.
- `ResolvedConfig::try_with_message` — rejects a `message_type` re-registered
  under a *different* topic. `with_message` still keeps the first registration.
- `kafkaman` facade gained an `rdkafka` feature re-exporting the transport, so an
  application needs one kafkaman dependency rather than two.

## Migration

1. Add `AddReceivedEntityKey` to the changelog for every existing received
   table. Rows written before it keep `entity_key IS NULL` and fall back to the
   Kafka record key; a legacy row with no record key fails with
   `Error::MissingEntityKey` instead of being cached under a fabricated identity.
2. Replace any `Replay::outbox` usage with state-sourced republish.
3. Replace `Envelope::with_idempotency_key`; in tests, import
   `kafkaman_test::EnvelopeTestExt`.
4. Widen exact `next_attempt_at` assertions to the jitter window.
5. Expect `Error::CacheOriginMismatch` to surface after a repartition, and treat
   it as the signal to re-bootstrap that cache.
