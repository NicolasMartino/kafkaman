# Restore, Retention, and Schema Boundaries

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-13
- Category: Operational model
- Scope: Names the operational invariants kafkaman's durability guarantees silently depend on — topic retention configuration, database restore policy, and the schema boundary between messaging state and business state — and proposes which kafkaman enforces versus documents.
- Sources:
  - raw/design/2026-08-12-restore-policy-and-schema-separation-discussion.md
  - raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md
  - crates/kafkaman-sqlx/src/lib.rs
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md
  - wiki/proposals/12-entity-only-message-model.proposal.md
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
- Promotion Target: a restore-and-retention policy decision, plus a compatibility note if the schema split is accepted.

## Context

Every concern examined in the 2026-08-12 design discussion shared one shape.
None was a bug in the mechanism. Each was a place where the design's correctness
**rests on an operational assumption that had not been named** — a topic
configured a particular way, a backup taken a particular way, a restore scoped a
particular way. The corrective action is the same one the project already
applies to configuration: state the invariant explicitly and fail fast where
possible, so it moves from lurking hazard to known boundary.

kafkaman is a library. It cannot run the operator's backup tooling. So the
question this proposal answers is not "how do we guarantee these," but **which
of them kafkaman enforces in code, which it validates at boot, and which it can
only document.**

### What the offset ordinal already dissolved

The 2026-08-13 revision of proposal 09 removed producer-side ordinal state
entirely. Three of the original discussion's nine invariants died with it and
are recorded here only so they are not re-derived:

- *"The version source is immutable and boot-validated"* — there is no version
  source.
- *"Producer durable state must never be restored behind what consumers have
  observed"* — the ordinal no longer lives in producer durable state.
- The rewound-sequence rationale for never restoring the outbox.

One invariant survived with its **rationale replaced**, which is the subtle part:
the outbox still must not be restored-and-republished, but now because a
republished row receives a *new, higher* offset and therefore overwrites newer
state at every cache. That half is already captured as state-sourced republish
in the entity-first decision. The backup-set half is proposed below.

## Options

### Where kafkaman's tables live

1. **One shared `kafkaman` schema.** Status quo — `database.schema`, default
   `kafkaman`, with `OutboxTable` and `ReceivedTable` both schema-qualifying into
   it. Rejected: it makes "exclude the outbox from backups" a table-name glob
   (`-T 'kafkaman.*_outbox'`) that silently stops being correct the first time a
   table is named differently.
2. **Split inbound from outbound.** Rejected as the cut, though not as the idea:
   it puts the received table (irreplaceable) and the cache table (rebuildable
   by replay) in the same schema, which is the one grouping that has no
   operational meaning.
3. **Split by reconstructibility — three schemas.** Selected.

Selected option: **3**.

| Schema | Holds | Directive |
|---|---|---|
| `kafkaman_outbound` | outbox tables | **drop** — exclude from backups; rebuild empty |
| `kafkaman_inbound` | received tables | **protect** — back up harder than business data |
| `kafkaman_cache` | cache tables, `cache_state` | **rebuild** — restore for speed only; replay is authoritative |

`changelog_history` and the migration advisory lock stay in a single control
schema. Migration is one logical operation across all of kafkaman's tables;
splitting it into three domains would mean three lock keys and three ledgers for
no operational gain.

Each schema carries exactly one verb, which is the property the inbound/outbound
cut lacked.

**The `rebuild` verb is conditional, and this proposal originally stated it too
broadly.** "Replay is authoritative" holds only for a compacted topic. A topic on
delete-by-age has holes by design, so a cache rebuilt from it is not equivalent to
a restored one, and any type whose topic ages records out is not reconstructible
from the log at all. Under entity-first this was implicit — non-entity types had
no cache table, so the question did not arise. It becomes explicit under
accepted [proposal 12](12-entity-only-message-model.proposal.md), which resolves it by
tying cache-table generation and bootstrap eligibility to a declared retention
class: only `compact` types get a cache, so the `rebuild` directive holds for
every type that has one. Either way the directive should be read as *"replay is
authoritative for compacted types"*, not unconditionally.

### What the split actually buys

Stated precisely, because the original discussion overclaimed it:

- **Backup-set composition.** `pg_dump --exclude-schema=kafkaman_outbound` is
  robust where a table-name pattern is not.
- **Distinct grants** per side.
- **Legible intent** — the namespace states the directive.

It does **not** buy independently exercised restore policies. PITR and physical
replication are cluster-wide; a single schema cannot be rewound to a different
point in time. Delivering that would require restoring to a scratch cluster and
selectively dumping one schema back into the live one, which is a substantially
larger operational commitment than the split itself and is **not** proposed here.

### The constraint the split must respect

`enqueue_on_connection` takes the handler's transaction-bound `PgConnection`, so
a received-row status update and an outbox insert **commit in the same
transaction** — test-pinned as
`handler_enqueues_outbox_atomically_with_receive_transaction`. Inbound and
outbound are therefore transactionally coupled by design in the one feature M3
shipped to make consume-then-produce atomic.

Restoring the two schemas to different points would tear committed transactions
apart. This is the strongest argument that the split is a **backup-set** tool and
not a restore-independence tool, and any future proposal to exercise the schemas'
restore policies independently must answer it.

### Topic retention invariants

A topic can enable `compact` and `delete` together, but they do not combine the
way intuition suggests. There is no "compact after N days." Enabling both gives
compaction **plus** unconditional delete-by-age, and delete-by-age can remove the
**last surviving record for a key** if that key has not been updated recently — a
quiet reference entity silently vanishes from the log and a fresh bootstrap never
learns it existed.

Proposed invariants:

- **Entity topics use compaction alone.** Never combined with delete-by-age.
- Where recent history must linger before collapsing, the lever is
  `min.compaction.lag.ms`, which delays compaction of a record until it reaches a
  given age.
- kafkaman should **validate this at boot** where the broker exposes topic
  configuration, matching the M2 fail-fast idiom, rather than documenting it and
  hoping. This is the one item here that is plausibly enforceable in code.

### Restore policy

- **The outbox is excluded from the backup set** and starts empty on recovery.
  Rows pending at incident time are lost; this is acceptable because sends are
  asynchronous and non-blocking, and because entity messages are whole snapshots
  that the next write supersedes.
- **Non-snapshot message types make this a per-type property.** A
  `PaymentRequested`-style event has no successor snapshot, so dropping it is
  permanent loss. Those types need an **audit of what was dropped** so a human can
  reconcile; they cannot self-heal.

  *Qualified by accepted
  [proposal 12](12-entity-only-message-model.proposal.md).* The per-type audit
  disappears only once a resync sweep can identify non-terminal work items, not
  merely current entity state. Until such a sweep exists, this bullet stands as
  the operational recovery requirement.
- **The inbound ledger is never rewound past its processed-markers.** It is the
  only durable record of *what was done*, reconstructible from nothing, and
  rewinding it re-fires irreversible side effects.
- **A business-data restore requires a deliberate resync sweep.** Rolling back
  business state does not propagate: the producer holds old state, consumers hold
  new state, and nothing re-emits an entity unless it is written again. The sweep
  re-reads current state and enqueues it through the normal path, so per-entity
  supersede applies.

  The sweep carries more weight than its placement here suggests. It is also what
  makes the dropped outbox recoverable at all, and
  [proposal 12](12-entity-only-message-model.proposal.md)'s claim that restore
  policy stops being per-type rests on it entirely. Its scope must therefore cover
  **non-terminal work items**, not just current entity state — a sweep that
  re-emits every live entity but cannot enumerate in-flight work leaves the exact
  gap the per-type audit above exists to cover.

  Ownership splits along the same line. For a propagating entity type kafkaman
  owns the cache table and can enumerate it, so the sweep is library code. For a
  work-item type, "non-terminal" is a predicate over the application's own status
  machine in its own table, which kafkaman cannot know — the best available shape
  is a per-type hook the adopter implements. Any promotion of this proposal should
  say which half it is committing to build.

### Layered idempotency

Schema separation makes the correct restore policy expressible; it does not
enforce it, and a careless whole-database restore still takes everything down
together. Three layers, each covering what the one below cannot:

1. **kafkaman's processed-marker.** Stops redelivery in normal running, but lives
   in a schema that could be rolled back.
2. **Schema separation.** Lets business data be restored without rewinding the
   messaging ledger.
3. **A business-owned idempotency record at the point of the irreversible
   action**, checked by the handler regardless of what kafkaman believes.

Layer 3 only works if it lives in a **genuinely independent restore domain**. A
"dedicated ledger" in the business schema is erased by the same restore it exists
to survive. The strongest form is the *downstream* system deduplicating on an
idempotency key the handler passes it, rather than the handler remembering
locally.

Applied selectively. Most inbound messages are naturally idempotent — refreshing
a cached user costs nothing to replay — and guarding everything would be the
over-abstraction this project consistently resists. Layer 3 belongs precisely
where a double-fire is irreversible and expensive.

### Operational growth of kafkaman's own tables

**Added 2026-08-24.** Open Question 1 names storage growth of the *topic's* key
space. The equivalent problem in Postgres was never named, and is worse because it
has no compaction analogue: **nothing purges any kafkaman table.** There is no
`DELETE` anywhere in `crates/` or `apps/`, so `Published` outbox rows, `Processed`
received rows, and quarantine rows all accumulate for the life of the application.

The three-way split above already determines the answer per table, which is the
point worth recording — this needs no new policy, only the existing directive
applied continuously rather than only at recovery:

| Schema | Verb | Retention consequence |
|---|---|---|
| `kafkaman_outbound` | drop | Safe to purge. The outbox is already excluded from the backup set and rebuilt empty on recovery, so nothing is permitted to depend on a historical row. `Replay::outbox` is now rejected as unsafe, so no read path over old rows exists at all. |
| `kafkaman_inbound` | protect | **Do not purge.** It is the only durable record of *what was done*. Purging `Processed` rows additionally shrinks the ingest dedupe window — the unique `idempotency_key` index is what makes redelivery a no-op — so a purge shorter than the redelivery window silently re-runs handlers. |
| `kafkaman_cache` | rebuild | **Do not purge.** It *is* the state. Reclaiming soft-deleted rows is Open Question 2 and gates on proposal 10's `cache_state`, not on a retention window. |

Only the outbox is in scope, and its scope is inherited from the verb rather than
argued fresh. `Failed` outbox rows are the invalid-send audit trail from the
error-row-symmetry decision and are the one class within `kafkaman_outbound` that
retention should keep by default.

### Index-adding changesets block writes

**Added 2026-08-24.** `run_migrations` applies each changeset inside a transaction
(`conn.begin()` per changeset), and `CREATE INDEX CONCURRENTLY` cannot run in a
transaction block. Every index kafkaman's schema builds therefore takes a `SHARE`
lock and blocks writes for the duration of the build.

On a small table that is milliseconds. On a table that has grown without bound —
which is the state the previous section describes as the default — it is a write
outage, and because `enqueue` runs *inside the caller's business transaction* the
outage propagates into application requests rather than staying inside kafkaman.

This is an invariant of exactly the kind this proposal exists to name: the
migration engine's correctness is unaffected, but its *operational* safety silently
depends on tables being small, which nothing currently ensures. Two consequences:

- Retention is close to a prerequisite for adding any index to an existing
  deployment's outbox, so the two changes are ordered, not independent.
- Any compatibility note introducing an index should say so, because the adopter
  cannot infer it from the changeset.

## Consequences

Positive:

- The operational assumptions become checkable rather than tacit, and one of them
  (topic retention) becomes boot-validated.
- Backup exclusion stops depending on table-naming conventions holding forever.
- The per-type distinction between self-healing and audit-only message types is
  named before someone discovers it during an incident.

Costs and risks:

- **The schema split breaks M2's validated single-schema surface.** The migration
  advisory lock is keyed on `cfg.schema.as_str()`, and bootstrap owns creating
  the schema and `changelog_history`. `ResolvedConfig` grows from one schema to a
  control schema plus three data schemas, threading through `OutboxTable`,
  `ReceivedTable`, and every qualified name. This needs its own compatibility
  note and is the largest single cost here.
- Three schemas mean three sets of grants to get right; a misconfigured
  `search_path` or grant fails at runtime rather than at boot unless validated.
- The resync sweep is new surface with no current home — it is the same mechanism
  state-sourced republish needs, so the two should land together.
- Boot-time topic-config validation requires broker metadata access the library
  does not currently use, and degrades to documentation where the broker or ACLs
  do not expose it.

## Open Questions

1. **Soft-delete-first means the topic's key space never shrinks.** Compaction
   retains at least one record per key forever, so topic size and bootstrap time
   scale with all-time entity count, not live count. Storage cost is real and
   unresolved.

   The **erasure** half is resolved (2026-08-13): publish the entity with every
   field but the ID stripped and a deleted status. That record becomes the last
   for its key, so compaction reclaims the earlier PII-bearing records, and
   every cache converges to the redacted version — erasure propagates without a
   topic rebuild and without a real tombstone. Two conditions apply:

   - **The entity ID must be an opaque surrogate.** It survives forever by
     design, so an email or any linkable natural key is still personal data and
     redaction does not help.
   - **Compaction timing becomes a compliance parameter.**
     `min.cleanable.dirty.ratio` and `min.compaction.lag.ms` govern how long
     superseded PII lingers, and "without undue delay" means they must be tuned
     and demonstrable — which sits in direct tension with this proposal's
     suggestion to raise `min.compaction.lag.ms` in order to *retain* recent
     history. Types carrying personal data cannot have both.
2. Who owns the reclamation batch for soft-deleted cache rows — kafkaman or the
   consuming application? If kafkaman, it must gate on the cache's own
   continuity via proposal 10's `cache_state`, never on a guessed global
   offline window.
3. Should the audit of outbox rows dropped at recovery be a kafkaman surface, or
   is "what was in the outbox at backup time" the operator's problem?
4. Proposal 08 observes that idle polling cost scales with type count × cache
   count. The 2026-08-12 discussion raised it and then addressed only table
   count, not polling. It remains unanswered and belongs to proposal 08.
5. Does the control schema hold `changelog_history` alone, or should replay and
   other operational changesets live there too?

## Promotion Target

Promote the retention invariants, the restore policy, and layered idempotency
into a restore-and-retention policy decision. The schema split should promote
separately and later — it is a breaking change to M2's validated surface, it
needs a compatibility note, and unlike the rest of this proposal it is a code
change rather than a policy statement.
