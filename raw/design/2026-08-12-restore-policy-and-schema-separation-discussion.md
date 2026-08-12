# Design Discussion: Restore Policy, Schema Separation, and Layered Idempotency (2026-08-12)

Curated record of the human + agent design conversation on 2026-08-12 that
followed the entity-first propagation decision. It examined three suspected
shortcomings in the accepted design, resolved two of them, and produced a new
structural conclusion: **kafkaman's tables belong in their own schemas, split
inbound from outbound, because inbound and outbound have different blast radii
under database restore.**

Captured as a `raw/` source so the resulting proposal and decision have durable
provenance. This is a synthesis, not a verbatim transcript. Treated as
**append-only provenance**.

Context at time of writing: M3/M4 pre-merge fixes complete, entity-first
propagation model accepted, proposals 07/08/10 open.

## Framing: the through-line

All three concerns examined turned out to share one shape. None of them was a
bug in the mechanism. Each was a place where the design's correctness **rests on
an operational assumption that had not been named**. The corrective action in
every case is to state the invariant explicitly, the way boot-time fail-fast
validation was stated, so it moves from lurking hazard to known boundary.

---

## 1. Soft-delete reclamation — resolved, not a hazard

### The concern as raised

The entity-first decision defers physical reclamation of soft-deleted entities,
with the stated intent to emit a real tombstone later, "after a window
comfortably longer than any consumer's maximum offline time."

That phrase is not a knowable quantity. It is a hope dressed as a threshold. A
pod scaled to zero for a quarter, a stopped staging environment, a DR instance
returning next quarter — none of these are bounded. Worse, it is the wrong
*shape* of guarantee: a time window is an **open-loop bet**, emitted with no
feedback confirming that anyone consumed the soft-delete first. That is exactly
the ghost-entity trap that real tombstones fall into, rescheduled rather than
eliminated.

### The resolution

The human's actual model dissolves the problem. Reclamation is **not** kafkaman
emitting a real tombstone later. It is each consumer physically removing a
soft-deleted row from **its own replica**, locally.

This has a completely different risk profile:

- A consumer reclaiming its own row **cannot** cause another consumer to miss a
  delete, because the delete still lives in the topic as a normal valued record.
- Therefore no global "longer than any offline time" window has to be guessed by
  anyone. The question becomes local and answerable.

### Why this also wins on complexity

The stronger argument, and the human's primary motivation: keeping deletion as
**the entity message type with a status field** rather than a separate tombstone
message type is a real complexity reduction. Every additional message type is
another wire contract, another table shape, another deserialization path that
can poison ingest.

Folding delete into "the entity, now with status inactive" means deletion rides
the **same convergence guard, the same version ordering, the same replica
upsert** as every other state change. No special case anywhere.

### Compaction — the mechanism this rests on

Established during discussion, recorded here because the whole model leans on it:

- Kafka has two cleanup policies. The default expires records by **age or size**.
  **Log compaction** instead works **by key**: for each distinct message key,
  Kafka guarantees it retains at least the most recent record. Superseded records
  for that key become eligible for cleaning.
- This is precisely why **entity-first is a requirement, not a taste**. Under
  deltas, compaction silently drops intermediate records and a consumer reading
  from offset zero reconstructs a torn state. Under whole-entity snapshots, the
  single record compaction retains per key **is** the complete current state, so
  replay-from-zero yields exactly the current world. Bootstrap works because of
  this and only because of this.
- A real tombstone (null value) is the one record type compaction may eventually
  reclaim **completely** — which is the origin of the `delete.retention.ms`
  hazard. Avoiding real tombstones sidesteps the one compaction parameter that is
  genuinely dangerous to get wrong.

### Compaction + delete: a trap to avoid

A topic *can* enable both policies at once, but they do not combine the way
intuition suggests. There is no "compact after N days" setting. Enabling both
gives you compaction **plus** unconditional delete-by-age — and delete-by-age can
remove the **last surviving record for a key** if that key has not been updated
recently.

That is the ghost-entity problem again: a quiet reference entity nobody has
touched simply vanishes from the log, and a fresh bootstrap never learns it
existed.

**Invariant: entity topics use compaction alone.** If recent history should
linger before collapsing, the correct lever is `min.compaction.lag.ms`, which
delays compaction of a record until it reaches a given age. Never delete-by-age.

### Open question

Whether kafkaman itself should own the reclamation batch. There is a consistency
argument that it should: kafkaman already owns the replica table and performs the
guarded upsert, so owning the table's full lifecycle is the other half of that.
If it does, reclamation must be gated on the replica's **own continuity** —
`replica_state` from proposal 10 already tracks `current_offset` against the
bootstrap high watermark — and never on a guessed global constant.

---

## 2. Per-type table scaling — largely defused

### The concern as raised

Schema size scales with message type count, and the propagation use case
explicitly wants **many low-traffic reference types**. So the workload the
feature exists to serve is the workload that multiplies table count fastest. It
compounds with proposal 08's observation that idle polling cost scales with type
count × replica count.

### Why it is smaller than framed

Correction from the human: it is one table per type **per direction that service
actually participates in**, not a fixed multiplier. A service that produces type
A needs an outbox for A and never an inbox; a service that consumes type B needs
an inbox for B and never an outbox. Both halves only exist when one service
genuinely does both directions for the same type, which is uncommon. Tables are
generated mechanically from registered types, not hand-written.

### The residual increment

The entity-first replica table is the one genuine addition: a **consumer** of an
entity type now holds its inbox **plus** the replica table for that type. That is
the real increment worth naming — consuming side goes from one table to two — not
the base inbox/outbox count.

Verdict: real but modest. Not a structural objection.

---

## 3. Version-source and rewound-sequence hazard — the sharp one

### The failure signature

`entity_version` is the **sole** input to the convergence guard: the replica
applies an incoming entity only when its version strictly exceeds the stored
version. That guard assumes every version for a given key is drawn from **one
coherent, monotonic sequence**. The entire correctness of convergence rests on
that single assumption.

When the assumption breaks, the failure is **silent, permanent, and
correctness-not-liveness**: the guard's comparison simply returns false, the
producer emits happily, and the replica freezes. Nothing errors. It surfaces
weeks later as "why is this one replica stale."

### Where the producer-side insert guard does and does not help

The accepted design has the outbox rejecting non-monotonic inserts per entity via
the error-row rule, using a high-water mark in kafkaman's own table. The human's
instinct — "make the insert error when the value is too small" — is therefore
already in the design.

**Within a single outbox's lifetime, that guard is airtight.** You cannot insert
a version that breaks monotonicity. That path is closed.

It does **not** close two other paths:

1. **The override bypasses it.** A caller supplying a domain version is not drawing
   from the outbox sequence at all — that is the entire reason override exists.
   Where the caller owns the number, the outbox's guarantee does not cover it.
2. **A rewound or fresh high-water does not know it is repeating.** The guard
   compares against *local* state. After a reset the outbox genuinely does not
   know it has issued those numbers before. The insert is legal locally. This is
   not a bad insert — it is a **forgotten history**.

The consumer-side guard cannot compensate, because to the replica a version of 1
arriving after 900 is **indistinguishable from a legitimately stale redelivery it
should ignore**. That indistinguishability is the guard's whole purpose. It can
never tell "old duplicate, correctly dropped" from "migration disaster, silently
frozen."

### The version-source switch

A type shipped on outbox-sequence versioning reaches, say, version 900. A
refactor moves it to a caller-supplied domain version, which starts small — 1, or
an aggregate revision. Every subsequent update now looks stale. Silent permanent
freeze.

**Invariant: the version source is a pinned, boot-validated, immutable property of
the type.** It may not be switched on a live key without an explicit re-sequencing
step. Existing boot-time fail-fast covers a *missing* source; it must also cover a
*changed* one.

### The restore case — the one that survives

The two numbers being compared live in **two different databases**: the producer's
high-water in the producer's outbox schema, the stored version in the consumer's
replica table in another service entirely. The guard works only while both track
the same underlying sequence.

Restore the producer to an earlier point while consumers keep running, and they
decouple. The human's own statement of the consequence:

> If we restore a replica of our database into production, then because it
> contains the outboxes, the consumers won't get the messages until the
> monotonically increasing ID reaches the old ID.

Exactly right — and the producer goes **silent from the consumers' point of view**,
not erroring. Every message in that gap is delivered, accepted, and dropped as
stale.

**The nastier wrinkle:** while rewound, the producer does not merely replay old
updates. It processes **new business events and stamps them with numbers from the
rewound sequence**. Those are genuinely new state wearing old version numbers.
They are dropped too — and remain dropped after the producer climbs back past the
old high-water. This is not a delay. It is **permanent data divergence**.

### The fix: do not restore the outbox

The human's conclusion, and it attacks the root rather than the symptom:

> The fix is: don't copy the outbox tables.

The reasoning that supports it: **the outbox is transient work state, not a source
of truth.** Its job is "get these messages onto the topic," and once published
that job is complete. The durable truth of "what version is entity X at" lives in
the topic, kept alive by compaction. Restoring the outbox from an old backup
rewinds a work queue and treats it as history.

The fix has two halves:

1. **Exclude the outbox from the backup/restore set entirely.** A recovered
   producer starts with an empty outbox.
2. **On recovery, re-derive the high-water from the compacted topic**, not from
   local state — the producer reads its own entities back to learn "X is at 900"
   and resumes above it. This is a small bootstrap-the-producer step, and
   proposal 10's replay machinery already supplies the mechanism.

Do both and the crack closes structurally: there is no rewound sequence left to
collide with.

---

## 4. Schema separation — the structural conclusion

### Business schema vs. messaging schema

Placing kafkaman's tables in their **own Postgres schema**, separate from the
application's business tables, makes the two sides expressible as having
**different backup and restore policies**. When business data is corrupted and
rolled back, only the application schema rewinds; the durable-execution ledger
stays at present time, so the high-water never rewinds and the silence problem
never fires.

This is the standard separation: **the messaging ledger is infrastructure state,
not business state**, and conflating their lifecycles is what created the hazard
in the first place.

Mechanics:

- **sqlx handles this fine.** Postgres schemas are namespaces — either
  schema-qualify table names, or set `search_path` on the connection so
  kafkaman's queries resolve into its own schema.
- **Separate schema, yes. Separate database, no.** The outbox insert is
  transactional with the business write; that atomicity *is* the durability
  guarantee. Cross-schema in one database preserves it. Cross-database destroys
  it.

### Inbound vs. outbound: separate schemas too

The test for any further split is: **do the two sides have restore policies you
would ever exercise independently?** Inbound and outbound pass this test, because
they fail *differently* under rollback:

| | Rewind hazard | Blast radius |
|---|---|---|
| **Outbound ledger** | Producer re-emits versions consumers already passed | Silent staleness; permanent divergence for updates made during the rewound window |
| **Inbound ledger** | Already-processed messages are redelivered | **Duplicate side effects** — re-firing actions that may be irreversible |

The inbound case is the more severe one. An inbound message may have triggered a
call to another service, a payment, a transfer, an order. Restoring the inbound
schema to a point before that message was marked processed causes kafkaman to
replay it and the side-effecting action to **fire again**.

Consequently the inbound ledger arguably deserves the **strongest restore
protection in the system — stronger than the business data**. Business data being
wrong costs a correction. The inbound ledger being rewound costs a duplicated
irreversible act.

### Possible refinement (open)

The honest cut may not be exactly "inbound vs. outbound" but
**reconstructible-from-topic vs. not**: the inbound *replica* table can be
rebuilt by replay, whereas the inbound *received/processing* state cannot. Worth
deciding whether that finer distinction is worth living with operationally.

---

## 5. Layered idempotency — defense in depth

Schema separation makes the correct restore policy **expressible**. It does not
**enforce** it — a careless whole-database restore still takes everything down
together. So the separation must be paired with operational discipline **and**
with an independent guard at the point of any dangerous action.

Three layers, each covering a failure the layer below cannot:

1. **kafkaman's processed-marker** (inbound ledger). Stops redelivery in normal
   running. But it lives in the schema that might be rolled back — so it protects
   against redelivery, not against restore.
2. **Schema separation.** Lets business data be restored without rewinding the
   messaging ledger, so layer 1's marker survives a business-side rollback.
3. **A business-owned idempotency record at the point of the irreversible
   action.** Owned by the action's own system — the payment provider, or a
   dedicated ledger for that effect — not by kafkaman. When kafkaman hands over
   "process message X," the handler checks whether it has already executed the
   effect for X and no-ops if so, **regardless of what kafkaman believes**.

Layer 3 is what survives a rollback of layer 1: two independent memories of
"done," in two systems with two restore policies, so no single restore erases
both.

**Applied selectively.** Most inbound messages are naturally idempotent —
refreshing a cached user costs nothing to replay — and guarding everything would
be the over-abstraction this project consistently resists. Layer 3 belongs
precisely where a double-fire is irreversible and expensive.

---

## Invariants produced

1. Entity topics use **compaction alone**; never combined with delete-by-age. Use
   `min.compaction.lag.ms` if recent history must linger.
2. Deletion is expressed as **entity state**, never as a separate message type or
   a real Kafka tombstone (emission side).
3. Physical reclamation is a **local** decision gated on the replica's own
   continuity, never on a guessed global offline window.
4. A type's **version source is immutable** — pinned and boot-validated; a change
   of source is as invalid as a missing one.
5. **The outbox is transient work state and must never be restored.** Exclude it
   from backup sets; re-derive the high-water from the compacted topic on
   recovery.
6. **Producer durable state must never be restored to a point behind what
   consumers have already observed.** Restore producer and consumers as a set, or
   rebuild the producer from the topic.
7. **The inbound ledger must never be rewound past its processed-markers.**
8. kafkaman's tables live in **their own schemas, split inbound from outbound** —
   same database as the business tables (transactional outbox requires it),
   different schema and different restore policy.
9. **Irreversible actions carry their own idempotency record**, outside kafkaman,
   so no single restore can erase both memories of "done."

## Follow-ups

- Promote invariants 5–9 into a proposal, then a decision. The restore-policy
  material is decision-grade; the inbound/outbound schema split is a structural
  change touching table generation and `search_path` handling.
- Decide ownership of the reclamation batch (kafkaman vs. consumer application),
  and if kafkaman, its gating on `replica_state` continuity.
- Decide whether the inbound split is `inbound`/`outbound` or
  `reconstructible`/`non-reconstructible`.
- Extend boot-time fail-fast to detect a **changed** version source, not only a
  missing one.
- Proposal 07's residual (real-tombstone **ingestion** for foreign producers) is
  unaffected by this discussion and remains open.
