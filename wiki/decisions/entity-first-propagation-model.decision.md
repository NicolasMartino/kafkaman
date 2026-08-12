# Entity-First Propagation Model

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-12
- Amended: 2026-08-13
- Category: Propagation model
- Scope: Fixes entity-first propagation as kafkaman's headline use case and defines entity identity, the inbox/cache split, advisory intent, retention class, and soft-delete-first deletion. Amended 2026-08-13: the convergence ordinal is the Kafka offset, every type is an entity with a declared retention class, and outbound entity enqueue serializes on `(message_type, entity_key)`.
- Sources:
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
  - raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
- Related:
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md
  - wiki/proposals/12-entity-only-message-model.proposal.md (accepted amendment)
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/decisions/ingest-poison-quarantine-policy.decision.md

## Decision

1. **Propagation is doctrine, not scope.** kafkaman's mechanism remains durable
   execution — outbox, ledger, claim-lease, retry, DLQ, idempotent receive. Its
   headline use case is reference propagation and cache coherence, so services
   avoid synchronous inter-service calls. User-facing interaction stays
   HTTP-driven. This does not reopen the messaging-scope decision's refutation of
   propagation-*only* as a library scope; nothing is removed. The accepted
   proposal 12 amendment models work items as entities too, rather than excluding
   them from kafkaman.

2. **Three identity axes coexist.** `message_id` is physical record identity.
   `idempotency_key` is the work-item dedupe identity from the typed-idempotency
   decision. `(entity_key, source_offset)` is the convergence identity.
   Dedup and convergence answer different questions and neither substitutes for
   the other. Every type has an `entity_key`; for existing M1-M4 work-item types
   it defaults to `message_id`.

3. **A convergence ordinal is required for propagating types**, because
   kafkaman's own retry backoff, `Replay::received` redrive, at-least-once
   redelivery, concurrent dispatch, and bootstrap replay all reorder application
   relative to production. Diff-and-upsert is insufficient: it answers "is this
   different?", not "is this newer?".

4. **The ordinal is the Kafka offset, not a producer-stamped version.**
   (Amended 2026-08-13; previously the outbox sequence with a domain-version
   override.) The ingest writer already records `source_topic`,
   `source_partition` and `source_offset` on every received row. There is no
   producer-side ordinal, no high-water table, no version-source declaration,
   and no domain-version override.

   The ordinal is valid **only because of per-entity supersede** (point 8): the
   offset records log order, and supersede is what makes log order equal truth
   order. The two are a pair.

   Offset comparison is scoped to one topic and partition. Topic recreation,
   repartitioning, and cross-cluster mirroring invalidate stored offsets and
   require a forced cache re-bootstrap. Detection must not key off observing
   offset 0, because the dangerous case produces none: if the consumer group's
   committed offset is still in range on the recreated topic, the consumer
   resumes there and reads different messages with no error. Preferred signal is
   Kafka's topic ID; fallback is a consecutive-regression circuit breaker
   mirroring `ConsecutiveSkipLimitExceeded`. Detection halts and demands an
   explicit re-bootstrap rather than self-healing.

5. **The ordinal does not travel on the wire.** The consumer reads it from
   record metadata. `kafkaman-entity-key` is still carried where the entity key
   is not already the record key.

6. **Every message type is an entity with a declared retention class.** The
   accepted proposal 12 amendment replaces the implicit entity/non-entity split
   with an explicit `compact` / `delete` retention class. `compact` means bounded
   key space, compacted topic, cache table, bootstrap eligibility, and a
   meaningful convergence guard. `delete` means unbounded key space,
   delete-by-age topic, no cache table, no bootstrap offer, and a guard that is
   structurally a no-op because the default `entity_key` is `message_id`.

   Deltas remain rejected for `compact` cache types because deltas and log
   compaction are fundamentally incompatible: compaction drops intermediate
   records, leaving a late consumer with a torn, unreconstructable state.

7. **One envelope, retention-class storage.** The existing received/inbox table
   stays per-message and transient for every type, carrying status, attempts,
   error history, retry, and DLQ. `compact` types additionally get a per-entity
   cache table keyed on `entity_key`, holding current state plus
   `applied_topic` / `applied_partition` / `applied_offset`. kafkaman owns the
   cache table and performs a guarded upsert — apply only when
   `incoming.source_offset > current.applied_offset`, after checking that topic
   and partition match.

8. **The outbox supersedes pending rows per entity.** Writing a message for an
   entity that already has a pending row marks that row `Superseded` and inserts
   the new one; writing while a row is already claimed for send inserts a fresh
   row instead. At most one message per entity is in flight, so the relay cannot
   race one entity's states into the log out of order.

   This replaces the previously decided outbox monotonicity constraint and its
   per-entity high-water table, both of which guarded a producer-side version
   that no longer exists. Supersede is independently valuable: it collapses N
   queued updates for one entity into a single publish.

   **Mechanism (amended 2026-08-13):** enqueue serializes on
   `(message_type, entity_key)` before inspecting the outbox. The lock is keyed
   to the entity, not to the most recent existing outbox row: first concurrent
   writes may have no row to lock, and a writer that waited on an older row must
   re-read latest state before deciding. Implementation may use a
   transaction-scoped PostgreSQL advisory lock or a dedicated entity enqueue
   lock/control row, but after acquiring it must re-read the latest outbox row
   and perform the supersede-or-insert decision in the same transaction. This
   serializes every instance of the owning service, which is the multi-writer
   case that actually occurs - horizontal scaling - because all instances share
   one database.

   **Cross-database writers are forbidden, not solved.** Two *different services*
   writing the same entity type share no lock, so they can still race states into
   the log, and no consumer-side guard can distinguish that from correct
   staleness. This is an entity-ownership violation rather than a case to
   support: one domain owns a type and others consume it.

9. **Republish is state-sourced, never row-sourced.** Re-emitting a stored
   outbox row is unsafe under an offset ordinal, because the republished record
   receives a new higher offset and stale state wins at every cache. For
   propagating entity types, replay and drift repair re-read the entity's
   current state and enqueue it normally, so a concurrent live update supersedes
   the still-pending repair row and wins correctly. This constrains M2's
   `Replay::outbox` for entity types and is the reason the domain-version
   override could be dropped rather than narrowed. `Replay::received` is
   unaffected: redriven inbound rows retain their original `source_offset`.

10. **Origin intent is advisory.** Entity messages carry an enum describing the
   originating action, but state is authoritative and complete. Intent is
   reliable for live tailing and never for bootstrap, because compaction keeps
   only the last record per key and therefore loses intermediate intents. The
   enum must be `#[non_exhaustive]` and must have a catch-all wire variant.

11. **Deletion is soft-delete-first.** The entity flows in full with a deleted
    status and/or delete intent; reclamation is deferred. Real-tombstone
    *ingestion* remains required for foreign producers and stays in proposal 07.

## Rationale

Two projects independently converged on Kafka-for-propagation: kafkaman's
messaging-scope decision, on the evidence that the reference project built and
then dropped `commands_inbox`; and workout2's architecture baseline, which runs
`user.sync`, `exercise.index`, and `invalidations.v1` with no active `commands.*`
or `outcomes.*` topics. Naming propagation as the headline use case makes
kafkaman's obligations to that case explicit.

The convergence guard is the load-bearing addition. kafkaman already ships
machinery that reorders application by design — retry backoff exists precisely to
defer a message behind later ones — so a cache built on arrival-order upsert
regresses silently and permanently. The fix is one comparison, set against a
failure that is invisible until someone reads stale data.

The 2026-08-13 amendment changes where the ordinal comes from, not what it does.
A producer-stamped version has to live in durable producer-side state, and that
state can be rewound by a database restore while consumers keep running — after
which the producer stamps genuinely new state with numbers consumers have already
passed, and every such update is silently and permanently dropped. Sourcing the
ordinal from the Kafka offset removes the state that could be rewound, rather
than protecting it by runbook. What remains is a fragility tied to topic
lifecycle — recreation, repartitioning, mirroring — which is rare, deliberate and
visible, where a restore is unplanned and happens mid-incident.

The offset is only trustworthy because supersede keeps one message per entity in
flight. Without it a racing relay writes the wrong order into the log and the
offsets faithfully record it, which is exactly why the offset was rejected as an
ordinal in the original proposal.

Entity-first follows from wanting compaction at all. Full snapshots make a
compacted topic self-healing and make repeated application a no-op; deltas make
it unreconstructable. The cost, message size proportional to entity size, is
acceptable for reference data and is the case kafkaman is optimizing for.

Soft-delete-first trades storage for correctness. A real tombstone is reclaimed
after `delete.retention.ms`, so a cache offline longer than that window holds a
ghost entity forever; a soft-delete record is always the last record for its key
and is retained indefinitely, so bootstrap always observes the delete. Unbounded
growth is a visible, measurable cost that can be addressed later by a reclamation
phase; a missed delete is a silent one that cannot.

Keeping intent advisory prevents the model from quietly turning back into the
delta stream it exists to avoid.

## Consequences

- New per-type cache tables needing changesets; per-type tables make migration
  count scale with type count.
- New `applied_topic` / `applied_partition` / `applied_offset` cache columns
  and a `Superseded` outbox status, requiring a compatibility note when
  implemented. The received-side offset columns already exist.
- **Send-side replay of stored rows is unsafe for entity types.** A republished
  row receives a new higher offset and overwrites newer state at every cache.
  Republish must be state-sourced; this is a documented constraint on
  `Replay::outbox`, not a discovered one.
- **Topic-lifecycle events invalidate stored offsets.** Recreation,
  repartitioning and mirroring each require detection and a forced cache
  re-bootstrap. Detection cannot rely on observing offset 0.
- Per-entity supersede serializes in-flight sends for the same entity:
  acceptable for reference data, a throughput ceiling for hot entities.
- Without a catch-all intent variant, adding one enum variant can trip the
  ingest consecutive-skip circuit breaker topic-wide on consumers that have not
  been redeployed. The catch-all is therefore a hard requirement, not a style
  preference.
- Deleted entities persist in the topic and in every cache until each
  consumer's reclamation batch runs.
- Two deletion representations coexist once foreign tombstone ingestion lands,
  and both must converge to the same cache state.
- Test obligations grow in the regressive direction specifically:
  retry-after-newer-applied, redrive-after-newer-applied, concurrent dispatch of
  two states of one entity, supersede racing an in-flight send, delete followed
  by redelivery of a pre-delete state, and an unknown intent variant at an
  un-redeployed consumer.

## Implementation Sequencing

Planning was deferred until the typed-idempotency fix plan closed, because entity
identity sits directly beside idempotency identity. That plan is now Completed
and all five pre-merge review findings are closed, so execution planning is
tracked in [entity-first-propagation.plan.md](../plans/entity-first-propagation.plan.md).
The M3/M4 branch merged to `main` on 2026-08-13. Every M5 phase builds on that
shipped receive surface.

Pre-adding nullable `entity_key` and cache columns during the M3/M4 merge to
save a migration is explicitly rejected: the change engine exists for exactly
this, and speculative unused columns cut against the project's
anti-over-abstraction discipline. The 2026-08-13 amendment strengthens this —
the received-side offset columns the ordinal now uses already exist, so the
speculative columns would have been wasted.

## Revisit When

- The accepted retention-class split proves insufficient, for example because a
  type declared `compact` has unbounded key cardinality in practice, or because a
  `delete` type needs a cache table despite having no compaction backstop.
- Message size under full-entity propagation becomes a measured problem for a
  real type, which would reopen the deltas-versus-snapshots choice for that type.
- Topic growth from unreclaimed soft deletes becomes a measured problem, which
  triggers the deferred reclamation phase and reintroduces the tombstone
  retention hazard as a live concern.
- A genuine need appears for two *different services* to write one entity type,
  which the ownership rule currently forbids. There is no mechanism that would
  make it safe, so this would require reopening entity ownership itself rather
  than adding a guard.
- A propagating type needs a domain-supplied ordinal for a reason state-sourced
  republish does not cover, which would reopen the dropped override.
- Topic recreation or repartitioning is observed in practice, which would move
  the detection mechanism from anticipatory to load-bearing.
- Consumers appear that genuinely require every intent transition, which would
  indicate the advisory-intent rule is not serving a real need.
