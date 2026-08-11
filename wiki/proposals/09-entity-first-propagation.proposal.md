# Entity-First Propagation

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-12
- Category: Propagation model
- Scope: Proposes entity-first reference propagation as kafkaman's headline use case, with entity identity and versioning, a per-entity replica table behind the existing received inbox, advisory origin intent, and soft-delete-first deletion.
- Sources:
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - workout2 `wiki/decisions/architecture-baseline.decision.md` (prior art, local only)
- Related:
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
  - wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md
  - wiki/proposals/03-direct-transport-mode.proposal.md
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/decisions/kafka-ingest-identity-and-ordering.decision.md
- Promotion Target: wiki/decisions/entity-first-propagation-model.decision.md

## Context

kafkaman's primary purpose is reference propagation: replicating one domain's
entities into another domain's local store so services avoid synchronous
inter-service calls. User-facing interaction stays HTTP-driven.

This is already the validated direction rather than a new one.
`wiki/decisions/messaging-scope-and-receive-model.decision.md` put
commands-over-Kafka out of v1 scope on the evidence that the reference project
built `commands_inbox` and then dropped it. The workout2 project independently
records "Kafka For Propagation, Not Public Write Authority", with live
`user.sync`, `exercise.index`, and `invalidations.v1` topics and no active
`commands.*` or `outcomes.*` topics.

What has not been settled is what the library owes that use case. Today kafkaman
delivers messages durably and stops there; the replica itself is entirely the
host's problem, and the pieces that make a replica correct are missing.

### Doctrine, not scope

The messaging-scope decision explicitly refuted propagation-*only* as a library
scope, because it "leaves the handler/retry/durable engine outside the library".
That refutation still holds and this proposal does not reopen it.

The formulation here is two-layer:

- **Mechanism**: durable execution — outbox, ledger, claim-lease, retry, DLQ,
  idempotent receive. Unchanged.
- **Headline use case**: reference propagation and cache coherence.

Entity-first is a positioning and optimization claim about what kafkaman leads
with and makes easy. It removes nothing.

### The gap: dedup is not convergence

kafkaman's identity model answers *"have I executed this work item before?"* —
`IdempotencyKey`, being reworked into a typed SHA-256 digest under proposal 06.
A replica needs a different question answered: *"is this newer than what I
currently hold for this entity?"*

These guards are not interchangeable. A stale message carries a genuinely
distinct payload, so content-digest dedup does not catch it.

Verified against the code:

- `KafkaMessage::partition_key()` exists
  (`crates/kafkaman-core/src/lib.rs:173`).
- `message_version` (`crates/kafkaman-core/src/lib.rs:617`) is the **schema**
  version, not an entity version.
- No per-entity ordering or convergence guard exists anywhere.

Diff-and-upsert does not close the gap. Diff answers "is this different?", not
"is this newer?", and those come apart whenever application order stops matching
production order — which **kafkaman's own M4 machinery causes by design**:

- A message for entity X (state A) fails and enters retry with backoff. During
  the backoff, state B arrives and applies. A's retry then succeeds, diff
  reports a difference, upsert replaces, and the replica is permanently A.
  Backoff *is* a mechanism for reordering application.
- `Replay::received` redrive re-dispatches an older row after a newer one has
  applied.
- At-least-once redelivery after a consumer rebalance.
- Bootstrap replay racing live tailing (see proposal 10).

The failure is silent, permanent, and typically undetected until someone reads
stale data.

## Options

### Entity identity ordinal

1. **No ordinal; diff-and-upsert on arrival.** Simplest, requires no schema
   change. Rejected: last-writer-wins by *arrival* order is wrong under retry,
   redrive, redelivery, and bootstrap, as above.
2. **Kafka offset as the version.** Free and monotonic per partition, and since
   the entity key is the partition key all states of an entity share a
   partition. Rejected as the default: concurrent `SKIP LOCKED` relay workers
   can publish rows out of insert order, so offset order is not truth order; it
   also does not exist in direct transport mode (proposal 03).
3. **Outbox sequence by default, caller-overridable by a domain version.**
   Selected.

Selected option: **3**.

The outbox sequence is a kafkaman artifact rather than a domain fact, which is
why the override is required rather than cosmetic:

- **Proposal 03 (direct transport mode) has no outbox**, so the default cannot
  exist there and the version must come from the type.
- **Periodic full-republish drift repair corrupts the replica under
  outbox-sequence versioning.** Republish reads state A; concurrently a live
  update writes B and publishes at seq 100; republish then publishes A at seq
  101, so A wins and the repair mechanism corrupts what it was meant to repair.
  With a domain version, A carries v5, B carries v6, and B wins correctly.
- The outbox sequence is a lossy echo of the truth under coalesced writes,
  commit-order skew, and entities written by more than one producer.

### Message shape

1. **Deltas / action messages.** Rejected. **Deltas and log compaction are
   fundamentally incompatible**: compaction drops intermediate records per key,
   so a compacted topic of partial updates hands a late consumer a torn,
   unreconstructable state.
2. **Two message classes** — state messages and work messages, with separate
   table shapes. Rejected as an unnecessary split; see below.
3. **Entity messages only: whole entities, one message type.** Selected.

Selected option: **3**.

A compacted topic of full snapshots is self-healing by construction, and
applying the same full state twice is a no-op, so application is idempotent
without extra machinery. Given that compaction and deletion are both wanted,
entity-first is a requirement rather than a preference.

Accepted costs: message size grows with entity size — fine for reference data
such as a user profile or exercise catalog, painful for large aggregates such as
a full workout session — and origin intent is lost from the payload, addressed
separately below.

## Proposal

### Entity identity

A propagating message type declares an entity identity in addition to its
existing message identity. Three axes coexist, each answering a different
question:

| Axis | Question answered | Status |
| --- | --- | --- |
| `message_id` | which physical record is this | exists |
| `idempotency_key` | have I executed this work item | exists (proposal 06) |
| `(entity_key, entity_version)` | is this the newest state I hold for this entity | new |

`entity_key` is the compaction and partition key. `entity_version` is a
monotonic ordinal within that key.

### Version source

The version source is declared per message type: the outbox sequence by
default, or a caller-supplied domain version as an override.

Boot-time validation must fail fast when a type relies on the default in a mode
that cannot supply one, matching the M2 fail-fast resolved-config idiom rather
than discovering it at runtime.

Compile-time enforcement was considered and is **not** proposed for this check.
A marker-trait bound — `type VersionSource = OutboxSequence | DomainVersion`,
with direct-mode send requiring `P::VersionSource: SelfContained` — would work
at static call sites but cannot replace the boot check, because the mode is
chosen at runtime from `kafkaman.toml` and the compiler never sees the config.
It would add generic machinery for partial coverage. (Proposal 10 identifies a
better target for compile-time state.)

### Wire format

The version travels as a reserved header, `kafkaman-entity-version`, since the
consumer cannot see the producer's outbox. The entity key travels as
`kafkaman-entity-key` where it is not already the record key. Both fit the
existing reserved `kafkaman-*` namespace and its rejection rule for user headers.

### One message type, two tables

The received/inbox table and the replica table are two tables for one message,
not two message classes:

- **Received/inbox** — exists today. Per-message, transient, carrying status,
  attempts, error history, retry scheduling, and DLQ state.
- **Replica** — new. Per-entity, `PRIMARY KEY (entity_key)`, holding current
  state plus `entity_version`.

The message flows through both: the inbox provides durability and retry, the
handler upserts into the replica, and the replica provides convergence. The
upsert is guarded — `WHERE incoming.entity_version > current.entity_version` —
which makes duplicate and out-of-order application safe by construction.

kafkaman should generate and own the replica table per type alongside the
received table, and perform the guarded upsert itself. That ownership is what
makes kafkaman a distributed cache library rather than a message library to
build a cache on.

### Outbox monotonicity constraint

The send side enforces per-entity monotonic versions at insert: an insert whose
version is not greater than the last recorded version for that entity is
invalid work. This follows the accepted error-row symmetry exactly — record the
error row transactionally, return an error, let the caller roll back or commit
for audit, and have workers claim only non-error rows.

Two constraints on the implementation:

- **It does not guarantee publish order.** Concurrent `SKIP LOCKED` relay
  workers can publish rows 5 and 6 in either order. This is defense-in-depth at
  the source; the consumer-side guard remains mandatory.
- **The high-water mark cannot live in the outbox rows**, which are pruned after
  publish; once pruned, a stale insert would be accepted again. A separate
  per-entity high-water table that outlives pruning is required. A cheaper tier
  — a unique constraint on `(entity_key, entity_version)` — catches duplicate
  versions but not regressions.

### Advisory origin intent

Entity messages carry an enum describing the action that originated them, so
consumers that need to react to a transition are not forced to diff.

The governing rule: **state is authoritative and complete; intent is advisory.**

On a compacted topic intents are lossy. Compaction keeps only the last record
per key, so `Created` followed later by `Updated` compacts to just `Updated`. A
consumer firing side effects on `Created` never observes it for any entity that
was subsequently edited — and this works in development and silently stops
working once compaction runs. Intent is therefore reliable for live tailing and
never for bootstrap. Any consumer whose correctness depends on observing every
intent has rebuilt the delta stream this model exists to avoid.

Domain-specific intents (`EmailChanged`, `SubscriptionCancelled`) carry more
than generic CRUD but become a versioned part of the schema. Two requirements
follow, and neither is optional:

- **`#[non_exhaustive]` on the intent enum.** A single-version fleet with
  coordinated releases survives exhaustive matching; kafkaman as a library
  cannot assume one.
- **A catch-all wire variant** (serde `other` / `Unknown(String)`). Today an
  unknown variant fails deserialization, and ingest treats deserialization
  failure as poison — quarantine plus the consecutive-skip circuit breaker from
  `wiki/decisions/ingest-poison-quarantine-policy.decision.md`. Without a
  catch-all, **adding one intent variant can trip the circuit breaker
  topic-wide** on any consumer not yet redeployed, turning a routine schema
  addition into an outage.

Where the shared type lives is a host convention, not a library requirement:
kafkaman needs only `P: KafkaMessage + EntityMessage`. Sharing the type through
a separate crate, as the reference project does, is the expected pattern.

### Soft-delete-first deletion

Deletion is carried as state: the entity flows in full with a deleted status
and/or a delete intent, and rows are reclaimed by a later batch. This
**reverses the selected option in proposal 07**, which chose per-message-type
opt-in real tombstones.

The justification is a correctness hazard proposal 07 did not weigh: **real
tombstones are reclaimed after `delete.retention.ms`**, so a replica offline
longer than that window misses the delete and holds a ghost entity forever. A
soft-delete record is a normal record with a value, so it is always the last
record for its key and compaction retains it indefinitely — bootstrap always
observes the delete and the ghost-entity trap cannot occur.

Soft delete additionally removes the `MissingPayload` special case from the
emission path and gives consumers the full entity along with the delete rather
than a bare key.

The cost is storage, and it is real: nothing reclaims, so deleted entities
persist in the topic and in every replica until each consumer's batch runs. For
high-churn types that is unbounded growth. Staging:

1. **Soft delete as the default**, now.
2. **Defer reclamation.** When topic size actually hurts, emit a real null
   tombstone *after* a window comfortably longer than any consumer's maximum
   offline time. Correctness now, bounded storage later, with the retention
   hazard never load-bearing.

Proposal 07's remaining valid concern is preserved: real-tombstone **ingestion**
is still required for foreign producers such as Debezium, independently of what
kafkaman itself emits. That is now the residual scope of proposal 07.

## Consequences

Positive:

- Replicas converge correctly under retry, redrive, redelivery, and replay
  rather than silently regressing.
- Compaction becomes usable, which makes bootstrap-by-replay possible at all
  (proposal 10).
- Deletion becomes representable without the tombstone retention hazard.
- The drift-repair republish escape hatch becomes safe under a domain version.

Costs and risks:

- New per-type replica tables and a per-entity high-water table, each requiring
  changesets; per-type tables mean the migration count scales with type count.
- New reserved headers and new columns on outbox and received tables.
- **Switching version source is breaking.** Starting on outbox sequence at 5000
  and later switching to domain version 7 freezes every replica forever, because
  nothing exceeds 5000 again. Changing the source requires a replica rebuild,
  and this must be documented as a compatibility rule rather than discovered.
- Enforcing per-entity monotonicity serializes inserts for the same entity;
  acceptable for reference data, a throughput ceiling for hot entities.
- Tests must cover the destructive and regressive directions specifically:
  retry-after-newer-applied, redrive-after-newer-applied, duplicate version
  insert, delete followed by redelivery of a pre-delete state, and an unknown
  intent variant arriving at an un-redeployed consumer.

## Open Questions

1. Does kafkaman own the replica upsert, or expose the guard as a primitive the
   handler calls? Ownership is proposed, but it puts kafkaman in the business of
   generating per-type state tables.
2. Should `entity_key` be required to equal the Kafka record key, or may they
   differ with the key carried in a header?
3. How does an entity type that is not propagating opt out cleanly, so
   non-replicated work types pay none of this cost?
4. Should the soft-delete status live in kafkaman's row metadata, in the domain
   payload, or both — and who owns the reclamation batch?
5. Does the per-entity high-water table need to survive replica rebuild, or is
   it purely send-side state?

## Promotion Target

Promote into `wiki/decisions/entity-first-propagation-model.decision.md`, fixing
the doctrine layering, the three identity axes, the version-source default and
override, the inbox/replica split with a guarded upsert, the advisory-intent
rule with its two enum requirements, and soft-delete-first deletion.

Implementation planning is deliberately deferred until M3/M4 merges and the
proposal 06 fix plan lands, because entity identity sits directly beside
idempotency identity.
