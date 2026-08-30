# Entity-First Propagation

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-12
- Revised: 2026-08-13
- Category: Propagation model
- Scope: Proposes entity-first reference propagation as kafkaman's headline use case, with entity identity and versioning, a per-entity cache table behind the existing received inbox, advisory origin intent, and soft-delete-first deletion. Revised 2026-08-13: the convergence ordinal is the Kafka offset rather than a producer-stamped version, backed by `(message_type, entity_key)` enqueue serialization.
- Sources:
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
  - raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - workout2 architecture-baseline prior art (local only; not present in this wiki)
- Related:
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
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
delivers messages durably and stops there; the cache itself is entirely the
host's problem, and the pieces that make a cache correct are missing.

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
A cache needs a different question answered: *"is this newer than what I
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
  reports a difference, upsert replaces, and the cache is permanently A.
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
2. **Kafka offset as the ordinal, paired with per-entity supersede in the
   outbox.** Free and monotonic per partition, and since the entity key is the
   partition key all states of an entity share a partition. **Selected
   2026-08-13.**
3. **Outbox sequence by default, caller-overridable by a domain version.**
   Originally selected 2026-08-12; **superseded 2026-08-13.**

Selected option: **2**.

Option 2 was originally rejected on the grounds that concurrent `SKIP LOCKED`
relay workers can publish rows out of insert order, so offset order is not truth
order. That objection is answered rather than ignored: **per-entity supersede in
the outbox is what makes log order equal truth order.** When a row for an entity
is pending relay and a second message for the same entity is written, the
pending row is marked superseded and the new row inserted; when the relay is
already mid-send, the new write inserts a fresh row instead. Only one message
per entity is ever in flight, so the relay cannot race a single entity's states
into the log out of order.

The two halves are a pair, not alternatives. Offset-as-ordinal is valid *only*
because supersede holds; without it the offsets faithfully record the wrong
order.

The second original objection — that offsets do not exist in direct transport
mode — does not hold. Direct mode still produces to Kafka, the record still
receives an offset, and the consumer still reads it. The objection assumed the
producer must *stamp* the ordinal; under this option it never does.

Why this beats a producer-stamped version:

- **No producer-side ordinal state exists**, so no database restore can rewind
  it. The entire class of rewound-sequence hazards disappears rather than being
  mitigated by runbook.
- **Retry self-heals.** An older offset simply loses the guard, so a poison
  message parks in the DLQ without freezing its entity behind a per-entity
  serialization lock.
- **Bootstrap and live tailing share one code path**, because both read real
  offsets.
- It is **already persisted**: `source_topic`, `source_partition` and
  `source_offset` are columns on every received row in shipped M3 code.

The cost is a relocated fragility, accepted deliberately — see *Ordinal validity
boundaries* below.

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
| `(entity_key, source_offset)` | is this the newest state I hold for this entity | new |

`entity_key` is the compaction and partition key. The convergence ordinal is the
**Kafka offset of the record that carried the state**, which is monotonic within
a partition and therefore monotonic within an entity.

### Ordinal source

There is no producer-side version and no version source to declare. The ingest
writer already records `source_topic`, `source_partition` and `source_offset` on
every received row; convergence reads those.

This deletes, rather than configures, a set of previously proposed machinery:
the per-entity high-water table, the outbox monotonicity constraint, the
per-type version-source declaration and its boot-time fail-fast, and the
compile-time marker-trait question. None of them have anything left to guard.

### Ordinal validity boundaries

Offset comparison is only meaningful within one topic and one partition, so the
cache stores `applied_topic` / `applied_partition` / `applied_offset` and
compares offsets only when topic and partition match. Three events invalidate a
stored offset:

- **Topic recreation** — offsets reset to 0, so every cache is ahead and
  rejects everything.
- **Repartitioning** — keys remap and cross-partition offsets are not
  comparable.
- **Cross-cluster mirroring** — MirrorMaker does not preserve offsets.

All three are topic-lifecycle events: rare, deliberate, planned and visible.
That is a deliberately chosen home for the fragility, in contrast to database
restore, which is unplanned and happens mid-incident.

Detection must not key off observing offset 0. The dangerous case produces no
offset 0 at all: consumer-group offsets live in Kafka separately from the topic,
so if the group holds offset 5000 and the recreated topic has grown past 5000,
the fetch is in range and the consumer resumes there, reading entirely different
messages with no error. Preferred signal is Kafka's **topic ID**, a UUID that
changes on recreation even when the name is reused — pending verification that
rdkafka exposes it. Fallback, reusing the existing idiom, is a
**consecutive-regression circuit breaker** structurally identical to
`ConsecutiveSkipLimitExceeded`: one below-watermark message is normal, many
entities regressing in a row is a signature nothing else produces.

Recovery is cache-side — reset applied offsets and re-bootstrap via proposal
10 — and never inbox-side. Clearing the inbox re-fires every irreversible side
effect whose message is still on the topic. Detection halts and demands an
explicit re-bootstrap rather than self-healing, because a false positive would
auto-wipe a healthy cache.

### Wire format

Nothing carries the ordinal; the consumer reads it from record metadata. The
entity key travels as `kafkaman-entity-key` where it is not already the record
key, fitting the existing reserved `kafkaman-*` namespace and its rejection rule
for user headers.

### One message type, two tables

The received/inbox table and the cache table are two tables for one message,
not two message classes:

- **Received/inbox** — exists today. Per-message, transient, carrying status,
  attempts, error history, retry scheduling, and DLQ state.
- **Cache** — new. Per-entity, `PRIMARY KEY (entity_key)`, holding current
  state plus `applied_topic` / `applied_partition` / `applied_offset`.

The message flows through both: the inbox provides durability and retry, the
handler upserts into the cache, and the cache provides convergence. The
upsert is guarded — `WHERE incoming.source_offset > current.applied_offset`,
after checking that topic and partition match — which makes duplicate and
out-of-order application safe by construction.

kafkaman should generate and own the cache table per type alongside the
received table, and perform the guarded upsert itself. That ownership is what
makes kafkaman a distributed cache library rather than a message library to
build a cache on.

### Per-entity supersede in the outbox

The send side keeps at most one in-flight message per entity, which is what
makes log order equal truth order and therefore what licenses using the offset
as the ordinal.

- Writing a message for an entity that already has a **pending** row for that
  entity marks the pending row `Superseded` and inserts the new row. Only the
  newest state is published.
- Writing while a row for that entity is **already claimed for send** does not
  supersede; it inserts a fresh row, which publishes after the in-flight one
  completes.

This decision is made under an enqueue lock keyed by
`(message_type, entity_key)`, followed by a re-read of the latest outbox row in
the same transaction. Locking only the most recent existing outbox row is not
enough: first concurrent writes may have no row to lock, and a writer that
blocked on an older row can otherwise decide from stale latest state. The
implementation may use a transaction-scoped PostgreSQL advisory lock or a
dedicated entity enqueue lock/control row; the invariant is key-level
serialization before the supersede-or-insert decision.

This replaces the previously proposed outbox monotonicity constraint, which
enforced a producer-side version that no longer exists. It is also a genuine
optimization independent of ordering: it collapses N queued updates for one
entity into a single publish, cutting topic traffic and compaction pressure for
exactly the high-churn reference types this proposal targets.

Cost: per-entity serialization of in-flight sends. Acceptable for reference
data, a throughput ceiling for hot entities.

### Republish is state-sourced, never row-sourced

Under offset-as-ordinal, **re-emitting a stored outbox row is unsafe**: the
republished record receives a new, higher offset, so stale state wins at every
cache. This affects M2's `Replay::outbox`, periodic full-republish drift
repair, and any recovery that replays retained rows.

The rule that resolves all three: for propagating entity types, republish
**re-reads the entity's current state** and enqueues it through the normal
outbox path. Never re-emit a stored row.

This is safe precisely because supersede applies. Repair reads state A and
enqueues it; a concurrent live update writes B and **supersedes the still-
pending repair row**, so B wins. The drift-repair corruption that motivated the
domain-version override in the original proposal is therefore fixed by the
send-side mechanism rather than by a second ordinal — which is why the override
is dropped entirely rather than narrowed.

`Replay::received` is unaffected. It redrives rows in the inbound table, which
retain their original `source_offset`, so a redriven row that lost the guard
once loses it again.

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
tombstones are reclaimed after `delete.retention.ms`**, so a cache offline
longer than that window misses the delete and holds a ghost entity forever. A
soft-delete record is a normal record with a value, so it is always the last
record for its key and compaction retains it indefinitely — bootstrap always
observes the delete and the ghost-entity trap cannot occur.

Soft delete additionally removes the `MissingPayload` special case from the
emission path and gives consumers the full entity along with the delete rather
than a bare key.

The cost is storage, and it is real: nothing reclaims, so deleted entities
persist in the topic and in every cache until each consumer's batch runs. For
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

- Caches converge correctly under retry, redrive, redelivery, and bootstrap
  rather than silently regressing.
- Compaction becomes usable, which makes bootstrap-by-replay possible at all
  (proposal 10).
- Deletion becomes representable without the tombstone retention hazard.
- **No producer-side ordinal state exists**, so no database restore can rewind
  convergence. The rewound-sequence hazard class is removed rather than
  mitigated.
- **Retry self-heals.** A stale message loses the guard and parks in the DLQ
  without freezing its entity behind a serialization lock.
- Supersede reduces published volume for high-churn entities independently of
  its ordering role.

Costs and risks:

- New per-type cache tables requiring changesets; per-type tables mean the
  migration count scales with type count.
- New `applied_topic` / `applied_partition` / `applied_offset` columns on
  cache tables; the received-side columns already exist.
- **Send-side replay of stored rows becomes unsafe for entity types**, because a
  republished row receives a new higher offset. Republish must be state-sourced.
  This is a behavior constraint on M2's `Replay::outbox` and must be documented
  as a compatibility rule rather than discovered.
- **Topic-lifecycle events invalidate stored offsets** — recreation,
  repartitioning, mirroring — each requiring detection and a forced
  re-bootstrap. This is the accepted relocation of fragility away from database
  restore.
- Per-entity supersede serializes in-flight sends for the same entity;
  acceptable for reference data, a throughput ceiling for hot entities.
- Tests must cover the destructive and regressive directions specifically:
  retry-after-newer-applied, redrive-after-newer-applied, concurrent dispatch of
  two states of one entity, delete followed by redelivery of a pre-delete state,
  first concurrent enqueues for one entity, a waiting writer re-reading latest
  outbox state before deciding, supersede racing an in-flight send, and an
  unknown intent variant arriving at an un-redeployed consumer.

## Open Questions

1. Does kafkaman own the cache upsert, or expose the guard as a primitive the
   handler calls? Ownership is proposed, but it puts kafkaman in the business of
   generating per-type state tables.
2. Should `entity_key` be required to equal the Kafka record key, or may they
   differ with the key carried in a header?
3. ~~How does an entity type that is not propagating opt out cleanly, so
   non-replicated work types pay none of this cost?~~ **Resolved 2026-08-13 by
   [proposal 12](12-entity-only-message-model.proposal.md).** A type declares a
   retention class: a non-propagating work type declares `delete`, gets no cache
   table and no bootstrap offer, and is charged for nothing it does not use.
4. Should the soft-delete status live in kafkaman's row metadata, in the domain
   payload, or both — and who owns the reclamation batch?
5. Does rdkafka expose Kafka's topic ID, or must topic-recreation detection rely
   solely on the consecutive-regression circuit breaker?
6. ~~Does a propagating entity with more than one writing producer have any safe
   story?~~ **Resolved 2026-08-13; amended same day.** Enqueue serializes on
   `(message_type, entity_key)` before checking send status, using a
   transaction-scoped advisory lock or a dedicated entity enqueue lock/control
   row, then re-reads latest outbox state and performs the supersede-or-insert
   decision in the same transaction. This serializes every instance of the
   owning service - the case that actually occurs, since all instances share one
   database. Two *different services* writing one type remains unsafe and is
   forbidden by entity ownership rather than solved.

## Promotion Target

Promoted into `wiki/decisions/entity-first-propagation-model.decision.md`, which
was amended 2026-08-13 to match this revision: the offset ordinal, per-entity
supersede, state-sourced republish, and the removal of the high-water table,
monotonicity constraint, entity-version header, and domain-version override.

Execution planning is now tracked in
`wiki/plans/entity-first-propagation.plan.md`.
