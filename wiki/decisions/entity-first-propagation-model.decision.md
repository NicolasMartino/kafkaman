# Entity-First Propagation Model

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-12
- Category: Propagation model
- Scope: Fixes entity-first propagation as kafkaman's headline use case and defines entity identity, the inbox/replica split, advisory intent, and soft-delete-first deletion.
- Sources:
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
- Related:
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
  - wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/decisions/ingest-poison-quarantine-policy.decision.md

## Decision

1. **Propagation is doctrine, not scope.** kafkaman's mechanism remains durable
   execution — outbox, ledger, claim-lease, retry, DLQ, idempotent receive. Its
   headline use case is reference propagation and cache coherence, so services
   avoid synchronous inter-service calls. User-facing interaction stays
   HTTP-driven. This does not reopen the messaging-scope decision's refutation of
   propagation-*only* as a library scope; nothing is removed.

2. **Three identity axes coexist.** `message_id` is physical record identity.
   `idempotency_key` is the work-item dedupe identity from the typed-idempotency
   decision. `(entity_key, entity_version)` is the convergence identity.
   Dedup and convergence answer different questions and neither substitutes for
   the other.

3. **`entity_version` is required for propagating types**, because kafkaman's
   own retry backoff, `Replay::received` redrive, at-least-once redelivery, and
   bootstrap replay all reorder application relative to production. Diff-and-
   upsert is insufficient: it answers "is this different?", not "is this newer?".

4. **The version source is the outbox sequence by default, overridable by a
   caller-supplied domain version.** The override is required, not cosmetic:
   direct transport mode has no outbox sequence, and periodic full-republish
   drift repair corrupts the replica under outbox-sequence versioning. Boot-time
   validation fails fast when a type relies on the default in a mode that cannot
   supply one.

5. **The version travels on the wire** as the reserved header
   `kafkaman-entity-version`, with `kafkaman-entity-key` where the entity key is
   not already the record key.

6. **Entity messages only — whole entities, one message type.** Deltas are
   rejected because deltas and log compaction are fundamentally incompatible:
   compaction drops intermediate records, leaving a late consumer with a torn,
   unreconstructable state.

7. **One message type, two tables.** The existing received/inbox table stays
   per-message and transient, carrying status, attempts, error history, retry,
   and DLQ. A new replica table is per-entity, keyed on `entity_key`, holding
   current state plus version. kafkaman owns the replica table and performs a
   guarded upsert — apply only when `incoming.entity_version >
   current.entity_version`.

8. **The outbox rejects non-monotonic inserts per entity**, recorded through the
   accepted error-row rule: record the error row transactionally, return an
   error, let the caller roll back or commit for audit, and have workers claim
   only non-error rows. The per-entity high-water mark lives in its own table
   that outlives outbox pruning. This is defense-in-depth only — concurrent
   `SKIP LOCKED` relay workers can still publish out of order, so the
   consumer-side guard remains mandatory.

9. **Origin intent is advisory.** Entity messages carry an enum describing the
   originating action, but state is authoritative and complete. Intent is
   reliable for live tailing and never for bootstrap, because compaction keeps
   only the last record per key and therefore loses intermediate intents. The
   enum must be `#[non_exhaustive]` and must have a catch-all wire variant.

10. **Deletion is soft-delete-first.** The entity flows in full with a deleted
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
defer a message behind later ones — so a replica built on arrival-order upsert
regresses silently and permanently. The fix is one column and one comparison, set
against a failure that is invisible until someone reads stale data.

Entity-first follows from wanting compaction at all. Full snapshots make a
compacted topic self-healing and make repeated application a no-op; deltas make
it unreconstructable. The cost, message size proportional to entity size, is
acceptable for reference data and is the case kafkaman is optimizing for.

Soft-delete-first trades storage for correctness. A real tombstone is reclaimed
after `delete.retention.ms`, so a replica offline longer than that window holds a
ghost entity forever; a soft-delete record is always the last record for its key
and is retained indefinitely, so bootstrap always observes the delete. Unbounded
growth is a visible, measurable cost that can be addressed later by a reclamation
phase; a missed delete is a silent one that cannot.

Keeping intent advisory prevents the model from quietly turning back into the
delta stream it exists to avoid.

## Consequences

- New per-type replica tables and a per-entity high-water table, each needing
  changesets; per-type tables make migration count scale with type count.
- New reserved headers and new outbox and received columns, requiring a
  compatibility note when implemented.
- **Switching version source is breaking.** Starting on outbox sequence at 5000
  and later switching to domain version 7 freezes every replica forever, because
  nothing exceeds 5000 again. Changing the source requires a replica rebuild.
- Per-entity monotonicity serializes inserts for the same entity: acceptable for
  reference data, a throughput ceiling for hot entities.
- Without a catch-all intent variant, adding one enum variant can trip the
  ingest consecutive-skip circuit breaker topic-wide on consumers that have not
  been redeployed. The catch-all is therefore a hard requirement, not a style
  preference.
- Deleted entities persist in the topic and in every replica until each
  consumer's reclamation batch runs.
- Two deletion representations coexist once foreign tombstone ingestion lands,
  and both must converge to the same replica state.
- Test obligations grow in the regressive direction specifically:
  retry-after-newer-applied, redrive-after-newer-applied, duplicate version
  insert, delete followed by redelivery of a pre-delete state, and an unknown
  intent variant at an un-redeployed consumer.

## Implementation Sequencing

Implementation planning is deliberately deferred. M3/M4 is unmerged with two open
high-severity idempotency findings in the pre-merge branch review, and the
typed-idempotency fix plan has Docker-backed verification pending. Entity
identity sits directly beside idempotency identity, so planning before that
settles means replanning.

Pre-adding nullable `entity_key` / `entity_version` columns during the M3/M4
merge to save a migration is explicitly rejected: the change engine exists for
exactly this, and speculative unused columns cut against the project's
anti-over-abstraction discipline.

## Revisit When

- Message size under full-entity propagation becomes a measured problem for a
  real type, which would reopen the deltas-versus-snapshots choice for that type.
- Topic growth from unreclaimed soft deletes becomes a measured problem, which
  triggers the deferred reclamation phase and reintroduces the tombstone
  retention hazard as a live concern.
- A propagating entity acquires more than one writing producer, which makes any
  single producer's sequence insufficient as a version.
- Direct transport mode ships, which makes the version-source override load
  bearing rather than anticipatory.
- Consumers appear that genuinely require every intent transition, which would
  indicate the advisory-intent rule is not serving a real need.
