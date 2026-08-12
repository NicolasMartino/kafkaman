# Design Discussion: Offset as Convergence Ordinal (2026-08-13)

Curated record of the human + agent design conversation on 2026-08-13 that
reviewed the 2026-08-12 restore-policy discussion and, in doing so, dismantled
and rebuilt the entity-first version model. The conclusion: **there is no
producer-side entity version. The convergence ordinal is the Kafka offset,
already persisted on every received row, made trustworthy by per-entity
supersede in the outbox.**

Captured as a `raw/` source so the resulting proposal revision and decision
amendment have durable provenance. This is a synthesis, not a verbatim
transcript. Treated as **append-only provenance**.

Context at time of writing: branch `implementation/m3-durable-receive` is 18
commits ahead of `main`, unmerged. M3 and M4 are Completed with specs promoted.
Entity-first propagation is Accepted but has no execution plan.

## Starting point: reviewing the restore discussion

The agent's review of the 2026-08-12 restore-policy discussion accepted its
core finding (the producer-restore rewind hazard is real and
correctness-grade) but challenged four of its arguments:

1. **"Don't restore the outbox" does not close the crack structurally.** The
   domain-override path draws its number from the business schema, which *is*
   restored. Excluding the outbox does nothing there.
2. **Excluding the outbox fixes version collision but not rollback
   propagation.** After a business restore the producer holds old state,
   consumers hold new state, and nothing re-emits the entity unless it is
   written again. A deliberate re-publication sweep is the missing half of the
   runbook.
3. **Separate schema does not give separate restore policy.** `pg_dump`
   supports `--exclude-schema`, but PITR and physical replication are
   cluster-wide. Most production restores are PITR.
4. **The inbound/outbound split fails the discussion's own test.** Invariants
   5, 6 and 7 all say "never rewind," so the two sides do not have policies
   exercised independently. The justification that *does* work is
   disposable-vs-irreplaceable.

Two gaps were also identified: soft-delete-only means the topic's key space
never shrinks (making GDPR erasure a topic-rebuild operation), and the
per-type table concern was defused for table count but not for proposal 08's
polling cost.

Grounding check: kafkaman already has one configurable schema
(`database.schema`, default `kafkaman`), and both `OutboxTable` and
`ReceivedTable` already schema-qualify every name. The "own schema" half of
the discussion's invariant 8 is already shipped; only the split is new work,
and it lands on M2's validated surface (the migration advisory lock is keyed
on the single schema, and bootstrap owns `changelog_history`).

## The human's counter: outbound is droppable by design

The human rejected the agent's proposed "restore the rows but exclude the
sequence" as more complex without being better, and argued that removing the
outbox entirely is safe by design, because outbound sends are asynchronous and
non-blocking, with audit for what was dropped.

The agent conceded. The supporting facts:

- Outbox rows are **already pruned after publish** (proposal 09), so the
  outbox is transient by construction, not by assertion.
- The per-entity high-water mark is a **cache** of a fact the compacted topic
  already holds authoritatively. Restoring a cache from a stale backup is the
  wrong operation, full stop.
- Entity messages are **whole snapshots**, so a lost message is superseded by
  the next write. Staleness, not divergence.

Boundaries named: quiet entities never get superseded (needs the resync
sweep), and non-snapshot message types have no next snapshot, so dropping the
outbox is real loss there — making this a per-type property rather than a
global invariant.

## The human's challenge: why a version at all?

The human then pressed on whether a version is needed given that Kafka orders
messages within a partition, and proposed two producer-side mechanisms:

- If a row for an entity is pending relay and a second message for the same
  entity is written, mark the first **superseded** and insert the new one.
- If the relay is mid-send (`sending` status / row locked), do not supersede;
  insert a fresh row.

The agent's position was that this fixes the relay race — proposal 09's stated
reason for rejecting offset-as-version — but not reordering that happens
elsewhere, and asked to be shown the inbound case.

### The inbound disorder mechanism

Two mechanisms were established. The second is decisive because it requires no
concurrency:

1. **Concurrent dispatchers.** Two received rows for one entity are claimed by
   two workers under `FOR UPDATE SKIP LOCKED`. The handler for the older row is
   slower, so it commits last and the replica ends up stale. No failure
   required — only unequal handler durations.

2. **A single dispatcher plus M4 retry.** The dispatcher claims the older row,
   the handler fails transiently, M4 sets `next_attempt_at = now + 30s` and
   parks it. The newer row is claimed and succeeds. Thirty seconds later the
   older row retries and succeeds, overwriting the replica with stale state
   permanently. **Retry is definitionally reordering** — backoff means
   deferring a message past its successors.

Dedup does not catch this: the two rows are different work items with
different idempotency keys, both legitimately unprocessed.

`Replay::received` is the same case with an operator pressing the button. It
targets terminal Failed rows in the *inbound* table (`ReplayTarget::Received`),
which per-entity outbox supersede never sees.

Fixing this with locks alone requires per-entity serialized dispatch plus
failed rows blocking their entity, which freezes an entity on a single poison
message until an operator intervenes.

## The synthesis: offset as the ordinal

The human then observed that the inbound writer already knows the Kafka order,
so it can stamp the row with the offset — giving a version that exists only on
the inbound side.

This is already true in shipped code. The received-table DDL carries:

```
source_topic     TEXT     NOT NULL,
source_partition INTEGER  NOT NULL,
source_offset    BIGINT   NOT NULL,
```

The ingest quarantine table even keys on
`(source_topic, source_partition, source_offset)`.

**The two halves are a pair, not alternatives.** Offset-as-ordinal is valid
only because per-entity supersede makes log order equal truth order. Without
it, a racing relay writes the wrong order into the log and the offsets
faithfully record it — which was proposal 09's original reason for rejecting
offsets.

Resulting design:

- Replica stores `applied_topic` / `applied_partition` / `applied_offset`.
- Guard: `incoming.source_offset > current.applied_offset`, after checking
  topic and partition match.
- Nothing on the wire; the consumer reads the offset from record metadata.
- No high-water table, no clock, no producer-side ordinal state of any kind.

### What this removes

- the per-entity high-water table
- the outbox monotonicity constraint
- the `kafkaman-entity-version` reserved header
- per-type version-source declaration and its boot-time fail-fast
- "switching version source is breaking"
- the restore discussion's invariant 4, and the version-rewind rationale
  behind invariants 5 and 6

### Properties gained

- Retry self-heals rather than requiring per-entity blocking: an older offset
  simply loses, so a poison message parks in the DLQ without freezing its
  entity.
- Bootstrap-from-zero and live tailing use the identical guard with no
  special-casing, because both read real offsets.
- The ordinal never lives in the producer's database, so no database restore
  can rewind it.

### The relocated fragility

Offset comparison is invalidated by **topic recreation** (offsets reset to 0),
**repartitioning** (keys remap, cross-partition offsets are not comparable),
and **cross-cluster mirroring** (MirrorMaker does not preserve offsets). All
three are topic-lifecycle events: rare, deliberate, planned, visible — a
better home for fragility than database restore, which is unplanned and
happens mid-incident.

### An earlier agent claim, corrected

Considered before the clock idea was dropped: sourcing the version from a
clock (`max(last_issued + 1, now_micros)`) would survive erasure by
construction. It was superseded by offset-as-ordinal, which needs no producer
state at all. Recorded because it explains why the conversation moved from
"re-derive the high-water" to "do not have a high-water."

## Topic-recreation detection

The human proposed detecting recreation by observing offset 0.

Rejected as the primary signal:

- **The dangerous case produces no offset 0.** Consumer group offsets live in
  Kafka separately from the topic. If the group holds offset 5000 and the
  recreated topic has grown past 5000, the fetch is in range and the consumer
  resumes at 5000 — reading entirely different messages with no error.
- Offset 0 is also the first message of a genuinely new topic, though that is
  resolvable by checking whether the replica already holds an applied offset.
- Retention can delete early segments, so the lowest observed offset is not
  necessarily 0.

Preferred: **Kafka's topic ID** (a UUID that changes on recreation even when
the name is reused) — pending verification that rdkafka exposes it. Fallback,
reusing an existing idiom: a **consecutive-regression circuit breaker**,
structurally the same as `ConsecutiveSkipLimitExceeded`. A single
below-watermark message is normal; many entities regressing in a row is a
signature nothing else produces.

The human's proposed recovery (empty the inbound table, or mark rows ignored)
points at the wrong table. The stale state is the replica's `applied_offset`,
not the inbox. Emptying the inbox is precisely what the restore discussion's
invariant 7 forbids, because it re-fires every irreversible side effect whose
message is still on the topic. Recovery is replica-side: reset applied offsets
and re-bootstrap using proposal 10's machinery.

Auto-recovery rejected: a false positive would auto-wipe a healthy replica.
Halt and require explicit re-bootstrap.

## Open question raised during writeup

**Send-side replay is unsafe for entity types under offset-as-ordinal.**
Republishing a stored outbox row gives it a *new, higher* offset, so stale
state wins at every replica. This inverts an earlier claim made during the
conversation that replay is harmless under a version guard — true for a
producer-stamped version, false for an offset.

The same shape affects periodic full-republish drift repair and any restore
that republishes old rows.

Candidate resolution, not yet ratified: for entity types, republish is always
**state-sourced, never row-sourced** — re-read current entity state and enqueue
it normally, so per-entity supersede applies and a concurrent live update wins.
If that holds, it also removes the last surviving justification for the domain
version override.

## Follow-ups

- Revise proposal 09's version-source, wire-format, and outbox-monotonicity
  sections; amend the entity-first decision to match.
- Ratify the state-sourced republish rule and settle whether the domain
  override survives it.
- Verify whether rdkafka exposes the Kafka topic ID.
- Carry forward the restore discussion's surviving material: inbound-ledger
  protection, layered idempotency, and the schema split on a
  disposable/irreplaceable/derived cut.
- Unresolved from the prior discussion: topic key-space growth under
  soft-delete-only (GDPR erasure), and proposal 08's polling cost.
