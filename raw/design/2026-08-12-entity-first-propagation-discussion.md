# Design Discussion: Entity-First Propagation (2026-08-12)

Curated record of the human + agent design conversation on 2026-08-12 that
produced the entity-first propagation model, the entity identity/version rules,
the advisory-intent rule, and the soft-delete-first deletion reversal. Captured
as a `raw/` source so the resulting proposals and decision have durable
provenance for their rationale. This is a synthesis, not a verbatim transcript.
Treated as **append-only provenance**.

Context at time of writing: branch `implementation/m3-durable-receive`, with
M3/M4 unmerged and two open high-severity idempotency findings in
`wiki/reviews/m3-m4-pre-merge-branch-review.reference.md`.

## Framing: what kafkaman is for

The human framing that opened the discussion:

- kafkaman's objective is **not** to move all RPC onto Kafka. It is primarily a
  **reference propagation system** — a distributed cache/replication mechanism —
  so services avoid inter-service calls as much as possible.
- **User interaction should stay HTTP-driven.**

This was found to be already-validated rather than new:

- `wiki/decisions/messaging-scope-and-receive-model.decision.md` already put
  commands-over-Kafka out of v1 scope, on the evidence that the reference
  project built `commands_inbox` and then dropped it
  (`010_drop_legacy_command_transport_tables.sql`).
- The workout2 project's `wiki/decisions/architecture-baseline.decision.md`
  independently records "Kafka For Propagation, Not Public Write Authority",
  with live `user.sync`, `exercise.index`, `invalidations.v1` topics and **no**
  active `commands.*` / `outcomes.*` topics.

### Doctrine, not scope

Important correction agreed in discussion: the messaging-scope decision
explicitly **refuted** "propagation-only" as a library scope ("too narrow —
leaves the handler/retry/durable engine outside the library"), and that
refutation still holds.

The coherent formulation is two-layer:

- **Mechanism**: durable execution (outbox, ledger, claim-lease, retry, DLQ,
  idempotent receive). Unchanged.
- **Headline use case / doctrine**: reference propagation and cache coherence.

Propagation-first is a positioning and optimization claim, not a deletion of the
durable-execution engine.

## Dedup is not convergence

The central technical finding of the discussion.

kafkaman's identity model answers *"have I executed this work item before?"*
(`IdempotencyKey`, being reworked into a typed SHA-256 digest under proposal
06). A replica needs a different question answered: *"is this newer than what I
currently hold for this entity?"*

These are different guards. A stale message is a genuinely distinct payload, so
content-digest dedup does not catch it.

Verified against the code at discussion time:

- `KafkaMessage::partition_key()` exists (`crates/kafkaman-core/src/lib.rs:173`).
- `message_version` (`crates/kafkaman-core/src/lib.rs:617`) is the **schema**
  version, not an entity version.
- No per-entity ordering or convergence guard exists anywhere.

### Why a version is required even with diff-and-upsert

The human proposed that a version may be unnecessary: on receive, look up the
entity by id, diff it, and upsert if different.

This was rejected, because diff answers "is this different?" and cannot answer
"is this newer?". Those come apart whenever application order stops matching
production order — which **kafkaman's own M4 machinery causes by design**:

- Message for entity X (state A) fails and enters retry with backoff. During the
  backoff, state B arrives and applies. A's retry then succeeds, diff reports a
  difference, upsert replaces, and the replica is permanently A. Backoff *is* a
  mechanism for reordering application.
- `Replay::received` redrive re-dispatches an older row after a newer one has
  applied.
- At-least-once redelivery after a rebalance.
- Bootstrap replay racing live tailing.

The failure is silent, permanent, and typically undetected until someone reads
stale data. The fix is one column and one `WHERE incoming.version >
current.version` on the upsert.

### Where the version comes from

Discussed candidates:

1. **Kafka offset.** Entity key is the partition key, so all states of an entity
   share a partition and offsets are monotonic. Requires no producer-side
   change. **Fails** if the relay claims outbox rows with `SKIP LOCKED` across
   concurrent workers, because publish order can then invert insert order.
2. **Outbox sequence.** The outbox row already carries a monotonic id.
3. **Domain version.** The entity's own optimistic-concurrency `version` /
   `updated_at`.

Agreed resolution: **outbox sequence by default, user-overridable.**

The human's stated motivation was possible future use of kafkaman without an
outbox table while keeping the same API, self-assessed as "probably pointless".
This was found to be better-grounded than stated, and two stronger reasons were
identified:

- **Proposal 03 (direct transport mode) is the no-outbox case.** Direct mode has
  no outbox sequence, so the version must come from elsewhere there.
- **Periodic full-republish drift repair corrupts the replica under
  outbox-sequence versioning.** Republish reads state A; concurrently a live
  update writes B and publishes at seq 100; republish then publishes A at seq
  101, so A wins. With a domain version, A carries v5 and B carries v6 and B
  wins correctly. The repair mechanism would otherwise corrupt what it repairs.

Additional cases where the outbox sequence is a lossy echo of the truth: coalesced
writes, commit-order skew, and entities written by more than one producer.

Consequences recorded:

- The version must travel on the wire as a reserved header
  (`kafkaman-entity-version`), since the consumer cannot see the producer's
  outbox. Fits the existing reserved `kafkaman-*` namespace.
- **Switching version source is breaking.** Starting on outbox sequence at 5000
  and later switching to domain version 7 freezes every replica forever, because
  nothing exceeds 5000 again. Changing the source requires a replica rebuild.
- Fail fast at boot if a type relies on the default in a mode that cannot supply
  one, matching the M2 "fail-fast resolved config" idiom.

### Compile-time enforcement (typestate)

The human asked whether typestate could make the version-source check
compile-time.

Assessed as **partly, and not worth it for that target**:

- The construct is a marker-trait / sealed-capability bound, not typestate
  proper (typestate encodes state *transitions* and consumes `self`).
- Shape: `type VersionSource = OutboxSequence | DomainVersion`, with direct-mode
  send requiring `P::VersionSource: SelfContained`.
- It **cannot replace the boot check**, because the mode is chosen at runtime
  from `kafkaman.toml` and the compiler never sees the config. It would add
  generic machinery for partial coverage.

Identified as a better target for typestate proper: the **replica bootstrap
lifecycle**, `Replica<Bootstrapping>` → `Replica<Ready>` with `get()` only on
`Ready`. This turns "do not serve reads from a cold cache" into a compile error
rather than an `is_ready()` call that callers forget.

### Outbox-side monotonicity constraint

Human proposal: the outbox should keep the version and forbid any insert smaller
than the last one for that entity.

Accepted as defense-in-depth, with two caveats:

- It fits the accepted error-row symmetry from proposal 06 exactly: an
  out-of-order insert is invalid work, recorded as an error row, returning an
  error and allowing rollback. It catches producer bugs transactionally, in the
  same transaction as the domain write, before anything reaches the wire.
- **It does not guarantee publish order.** Concurrent `SKIP LOCKED` relay
  workers can publish rows 5 and 6 in either order. The consumer-side version
  guard remains mandatory.
- **The high-water mark cannot live in the outbox rows**, which are pruned after
  publish; once pruned, a stale insert would be accepted again. A separate
  per-entity high-water table that outlives pruning is required. A cheaper tier
  — a unique constraint on `(entity_key, entity_version)` — catches duplicate
  versions but not regressions.

## Entity-first: whole entities, not actions or parts

Human position: the system should be entity-first — entire entities flow, not
actions or deltas.

Agreed. The decisive technical argument is not ergonomics:

**Deltas and log compaction are fundamentally incompatible.** Compaction drops
intermediate records per key, so a compacted topic of partial updates hands a
late consumer a torn, unreconstructable state. A compacted topic of full
snapshots is self-healing by construction. Given that compaction and tombstones
are wanted, entity-first is a requirement rather than a preference.

Secondary benefit: applying the same full state twice is a no-op, so application
is idempotent by construction.

Costs accepted:

- Message size grows with entity size. Acceptable for reference data (user
  profile, exercise catalog); painful for large aggregates such as a full
  workout session.
- Intent is lost: "user changed email" becomes "user now has email X".

### One message type, two tables

An initial agent framing of "two message classes" (state messages vs work
messages) was **withdrawn as wrong** after human pushback. The human wanted
entity messages only.

Correct framing: **one message type, two tables.**

- The **received/inbox** table already exists — per-message, transient,
  status/attempts/error history, retry, DLQ.
- A **replica** table is new — per-entity, `PRIMARY KEY (entity_key)`, holding
  current state plus version.

The same message flows through both. The inbox provides durability and retry;
the handler upserts into the replica; the replica provides convergence. No
second message class is needed.

Follow-on: if entity-first is doctrine, kafkaman should probably **own** the
replica table too — generating it per type alongside the received table and
performing the guarded upsert. That is what makes it a distributed cache library
rather than a message library to build a cache on.

## Intent as an advisory enum

Human proposal: recover lost intent with an enum representing the action that
originated the message, carried inside the message type, with the type shared
through a separate crate (as done in cqrs/workout2).

Agreed, with one rule that governs how it may be used:

**On a compacted topic, intents are lossy.** Compaction keeps only the last
record per key, so `Created` followed later by `Updated` compacts to just
`Updated`. A consumer firing side effects on `Created` never sees it for any
entity that was subsequently edited — and this works in development and silently
stops working once compaction runs.

Therefore: **state is authoritative and complete; intent is advisory.** It is
reliable for live tailing and never for bootstrap. Any consumer whose
correctness depends on observing every intent has rebuilt the delta stream the
model exists to avoid.

Domain-specific intents (`EmailChanged`, `SubscriptionCancelled`) carry more
than generic CRUD but become a versioned part of the schema. Accepted knowingly.

### Shared-crate requirements

The shared types crate stays a host convention; kafkaman requires only
`P: KafkaMessage + EntityMessage` and does not care where the type lives.

Two requirements are not optional:

- **`#[non_exhaustive]` on the intent enum.** workout2 runs a single-version
  fleet with coordinated releases, so exhaustive matching survives there;
  kafkaman as a library cannot assume that.
- **A catch-all wire variant** (serde `other` / `Unknown(String)`). Today an
  unknown variant fails deserialization, and ingest treats deserialization
  failure as poison — quarantine plus the consecutive-skip circuit breaker from
  `ingest-poison-quarantine-policy`. **Adding one intent variant could therefore
  trip the circuit breaker topic-wide** on any consumer not yet redeployed,
  turning a routine schema addition into an outage.

## Deletion: soft-delete-first reverses proposal 07

Human proposal: rather than Kafka tombstones, carry a `deleted` status inside the
entity and batch-delete later — so consumers still receive a full entity, with
intent `delete` and/or status `deleted`.

Agreed, and this **reverses the selected option in proposal 07**, which had
chosen per-message-type opt-in real tombstones and had rejected typed-payload
deletion (its Option 1) largely on unbounded topic growth.

The reversal is justified by a correctness hazard identified in this discussion
and not weighed in proposal 07:

**Real tombstones are reclaimed after `delete.retention.ms`.** A replica offline
longer than that window misses the delete and holds a ghost entity forever. A
soft-delete record is a normal record with a value, so it is always the last
record for its key and compaction retains it indefinitely. Bootstrap always sees
the delete. The ghost-entity trap cannot occur.

Soft delete additionally removes the `MissingPayload` special case on the
emission path and gives consumers the full entity along with the delete, rather
than a bare key.

The cost is real and is storage: nothing reclaims. Deleted entities persist in
the topic and in every replica until each consumer's batch runs. For high-churn
types that is unbounded growth.

Agreed staging:

1. **Soft delete as the default**, now.
2. **Defer reclamation.** When topic size actually hurts, emit a real null
   tombstone *after* a window comfortably longer than any consumer's maximum
   offline time. Correctness now, bounded storage later, with the retention
   hazard never load-bearing.

Refinement preserved from proposal 07: its Option 1 critique that typed-payload
deletion "cannot interoperate with external producers such as Debezium that emit
real tombstones" **remains valid**. Soft delete is the *emission* doctrine;
real-tombstone *ingestion* is still required for foreign producers.

## Catch-up / bootstrap

Human question: can the catch-up story be implemented in kafkaman?

Yes, and it becomes nearly free given entity-first plus soft-delete plus
versioning: replaying a compacted topic from offset 0 **is** the catch-up
protocol, because every message is a full entity snapshot. No request/response,
no snapshot topic, and no reintroduction of the commands-over-Kafka shape.

What kafkaman would own:

1. A bootstrap consumer mode that seeks to beginning and ingests through the LWW
   guard, so duplicate and out-of-order application is safe by construction.
2. A `replica_state` table tracking per-partition `(bootstrap_high_watermark,
   current_offset, status)`.
3. **A readiness signal** — the primitive with the most value. "Is my local cache
   warm enough to serve reads?" is routinely hand-rolled and wrong, and is the
   difference between a replica and a cache that silently serves empty results
   after a deploy.

Two design traps recorded:

- **Replica consumers are not load-balanced consumers.** Every instance needs a
  full copy, so each process needs its own consumer group reading *all*
  partitions, not a shared group with partition assignment (the Kafka Streams
  GlobalKTable distinction). This is a genuinely different consumer mode from the
  work-queue dispatcher and it bites at deploy time when a second pod comes up
  holding half the data.
- **Tombstone retention** — the ghost-entity trap above. Under soft-delete-first
  this is neutralized; it returns if and when reclamation is added, and the rule
  is that a resume gap exceeding the retention window requires dropping the
  replica table and rebuilding rather than resuming incrementally.

An operational escape hatch worth keeping: producer-side periodic full
republish. Crude, but it repairs drift without a new protocol — subject to the
versioning caveat above.

## Transport trait

Reaffirmed: **do not build the transport trait now.**

The reference project's `mutation_jobs` is kafkaman's engine with `POST`
substituted for `produce`, which is exactly the trigger condition recorded in
decision item 2 ("do not build a transport-trait abstraction until one is
actually needed"). Agreed to let workout2 be the forcing function that proves the
seam is real, then extract it, rather than building the abstraction
speculatively.

## Sequencing agreed

- **Proposal**: ready now, split in two — (1) the entity-first core, (2)
  bootstrap and readiness. Splitting keeps the core shippable.
- **Proposal 07 must be revised either way**, since it currently proposes an
  option this discussion superseded; leaving a contradicting `Proposed` page is a
  lint failure.
- **Decision**: the core is decision-ready; nothing substantive remained open at
  the end of the discussion.
- **Plan**: deferred. M3/M4 is unmerged with two open high-severity idempotency
  findings, and proposal 06's compatibility note still has Docker verification
  pending. Entity identity sits directly beside idempotency identity, so planning
  on top of an unverified identity model means replanning it.

Explicitly rejected as premature: pre-adding nullable `entity_key` /
`entity_version` columns during the M3/M4 merge to "save a migration". The change
engine exists for exactly this, and speculative unused columns cut against the
project's anti-over-abstraction discipline.
