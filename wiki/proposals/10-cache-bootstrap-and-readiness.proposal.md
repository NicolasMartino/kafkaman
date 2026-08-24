# Cache Bootstrap and Readiness

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-12
- Category: Propagation model
- Scope: Proposes how a kafkaman cache catches up on a compacted entity topic from cold, tracks its own warmth, and exposes readiness so hosts never serve reads from an incomplete cache.
- Sources:
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
- Related:
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - wiki/proposals/05-deep-durability-testing.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/entity-first-propagation-model.decision.md
- Promotion Target: a decision fixing the bootstrap consumer mode, the cache state table, and the readiness surface.
- Revised: 2026-08-26 (the per-process cache assumption is wrong for kafkaman; see below)

## Revision 2026-08-26: this proposal was written for a per-process cache

**The consumer-group half of this proposal is superseded. The readiness half
survives, with one change to what it gates.**

This page assumes the cache lives *in the process* — it reaches for the Kafka
Streams GlobalKTable distinction and warns that "a second pod comes up holding
half the data and serving confidently". kafkaman's cache is not per-process. It
is a Postgres table that every replica of a service queries, which
`wiki/decisions/entity-first-propagation-model.decision.md:111` states as a
load-bearing fact: the multi-writer case "actually occurs - horizontal scaling -
because all instances share one database".

Three consequences, and they are not small:

1. **The shared consumer group already fills the cache.** Whichever replica
   consumes partition 3, the row lands in the one shared
   `cache_{message_type}` table. The failure this proposal is built around —
   pod 2 holding half the data — cannot happen, because pod 2 holds no data. It
   queries the table pod 1 filled.
2. **A cold start already replays from zero.** `RdkafkaConsumer::from_brokers`
   sets `auto.offset.reset = earliest`
   (`crates/kafkaman-rdkafka/src/consumer.rs:102`), so a fresh consumer group
   reads the whole compacted topic unaided. The replay was never the missing
   piece.
3. **What is actually missing is three things, none of them a consumer mode:**
   the *target* (each partition's high watermark), the *accounting* (has
   dispatch drained everything below it), and the *gate*.

### The hazard this proposal misses, which is worse than the one it describes

**A fresh database with a surviving consumer group.** A restored or recreated
database booting under the same `consumer_group` starts at the group's committed
offset, and every entity below that offset is missing from the cache
**permanently and silently**. Nothing recovers from it: the records were already
consumed, so no amount of waiting brings them back, and the cache simply has
holes that look like entities that never existed.

That is the case a replay consumer genuinely earns its place for — and it is
triggered by what the *database* says, not by where the group's offsets are.

### What replaces the per-instance group model

An **assign-based, non-committing replay**. It uses `assign()` rather than
`subscribe()`, joins no group, and commits no offset, so the broker retains no
state for it and there is nothing left behind to reap.

This **dissolves open question 1** rather than answering it: there are no
per-instance group ids to derive and no groups to clean up. It also deletes the
first entry under *Costs and risks* — the one about multiplying consumer-group
count by instance count.

`### Cache consumers are not load-balanced consumers` below is retained as the
record of what was considered, and is **superseded in full**.

### Readiness: the typestate survives, what it gates changes

`Cache<Bootstrapping>` → `Cache<Ready>` is right, and the argument for it — no
per-key fallback, so refill is necessarily bulk replay — is untouched.

But there is no `get()` to gate. kafkaman deliberately ships no typed key-value
getter, and `examples/order/src/lib.rs:40-42` gives the reason: "kafkaman
deliberately ships no typed key-value getter for a cache: the table is the API,
because the interesting reads are joins and aggregates that a `get(key)` shape
would turn into N+1 round trips."

So gate the **qualified table name** instead. It is the narrowest thing that can
be withheld and, unlike inventing a getter, it does not distort the read model.
Look at what a host actually holds: `AppState::new` resolves
`CacheTable::for_message::<ProductSnapshot>(&cfg)?.qualified_name()` once at boot
and stores an `Arc<str>` (`examples/order/src/lib.rs:52-59`) that every cache
query interpolates. *That string is the capability* — for a host that gets it
from the runtime, no table name means no query.

That qualifier is load-bearing, because the gate is **soft**: `CacheTable::for_message` stays public
under clause 11 of the runtime-builder decision, so a host can always get the
name the old way. The typestate makes the warm path the default path and the
cold path a visible act — the same class of guarantee
`tests/distributed-cache/tests/boot_surface.rs` already provides for the builder.

### Blocking prerequisite: `RuntimeTasks::wait()` kills the service

`crates/kafkaman/src/runtime/tasks.rs:98-107` resolves on the **first** loop to
exit, and line 104 — `Some(Ok((_, Ok(())))) | None => Ok(())` — treats a clean
`Ok(())` exactly like a failure signal. `Runtime::run` then drains everything.

That is correct today, because every kafkaman loop runs until cancelled. A
bootstrap loop would be **the first loop in kafkaman that finishes successfully
on purpose**. Spawn it into the existing `JoinSet` and a successful bootstrap
shuts the service down seconds after boot, presenting as a broker fault.

The fix is a second `JoinSet` for completing tasks, and it must land *before*
anything is spawned into it. See
`wiki/proposals/05-deep-durability-testing.proposal.md`.

### The open questions, answered

- **1 — group ids and cleanup.** Dissolved. No groups.
- **2 — write through the inbox, or bypass?** *Write through.*
  `insert_received_with_outcome` dedupes on the unique idempotency-key index,
  which is what lets a replay race live tailing safely. And the guarded upsert
  lives inside the dispatch transaction alongside the handlers, so bypassing the
  inbox bypasses `handle::<T>` — a type with a deriving handler would silently
  derive nothing during bootstrap. That is a correctness hole, not an
  optimisation. The inbox-inflation worry is real but smaller than stated: with
  `cache_state` the replay runs **once per database, not once per process**.
- **3 — per type or aggregate?** *Per type.* Aggregate readiness lets one slow
  type hold every other type's reads hostage. Ship an aggregate *report* for a
  `/readyz` body; never gate on it.
- **4 — stale reads with a staleness signal?** *No.* Without a per-key fallback —
  this proposal's own point — "stale during bootstrap" is indistinguishable from
  "absent", and the example already has the right answer for absent: 409,
  retryable. A staleness signal is a second way to say the same thing.
- **5 — can a DLQ'd row block readiness?** *It must not.* `Failed` counts as
  drained. Blocking would let one poison record hold a whole service out of
  rotation forever, which is strictly worse than a cache missing one entity — and
  the failure stays visible through `received_failed_rows`.

### Residual cost, recorded honestly

A fresh database's received table grows to the size of the compacted topic and
stays there, because `[retention]` is outbox-only by decision. Bounded by "once
per database" rather than "once per process", but real. Received-table retention
is separate, already-identified work and should not be solved here — it is also
the strongest argument someone will make for bypassing the inbox, which open
question 2 rules out.

## Context

A service holding a cache of another domain's entities has a cold-start
problem: on first deploy, on a new instance, and after a rebuild, the local
store is empty or stale, and the host must not serve reads from it until it is
warm.

This is the catch-up story left open by
[09-entity-first-propagation](09-entity-first-propagation.proposal.md). It is
separated because it depends on that model being settled, introduces a second
consumer mode, and is later work than the core.

kafkaman today has no notion of a cache, no notion of warmth, and no consumer
mode other than the work-queue dispatcher.

**Amended 2026-08-26.** "On a new instance" does not belong in that list for
kafkaman. A new instance inherits a warm cache, because the cache is the shared
table its peers fill. The cold-start problem is real but its unit is the
*database*, not the process — see the revision above.

## Options

1. **Snapshot request/response.** A consumer asks the owning service to re-emit
   its entities over a request topic. Rejected: it reintroduces the
   commands-over-Kafka request/response shape that
   `wiki/decisions/messaging-scope-and-receive-model.decision.md` deliberately
   put out of scope, and it couples every consumer's cold start to the
   producer's availability.
2. **Compacted-topic replay from offset 0.** The consumer seeks to the
   beginning and applies every record through the guarded upsert. Selected.
3. **Producer-side periodic full republish.** The owning service re-emits all
   entities on a schedule. Kept as an operational escape hatch rather than the
   primary mechanism.

Selected option: **2**, with **3** retained for drift repair.

Replay is nearly free given entity-first: because every message is a full entity
snapshot on a compacted topic, replaying from offset 0 yields exactly the current
state of every live entity plus the soft-delete records for deleted ones.
Replaying *is* the catch-up protocol — no new protocol, no reply topic, and no
producer involvement.

Option 3 remains useful because it repairs drift from any cause without a new
mechanism, but proposal 09's 2026-08-13 revision constrains how it may be built.
Under an offset ordinal *any* republished record wins, because it receives a new
higher offset — so republish must be **state-sourced, never row-sourced**: it
re-reads current entity state and enqueues it normally, letting per-entity
supersede hand the win to a concurrent live update. Re-emitting stored outbox
rows is unsafe for entity types regardless of which type it is.

## Proposal

### Bootstrap consumer mode

A second consumer mode alongside the work-queue dispatcher. It seeks to the
beginning of the assigned partitions and applies records through the same
guarded upsert used in steady state, so duplicate and out-of-order application
during bootstrap is safe by construction and needs no special casing.

Bootstrap and live tailing are the same code path with a different starting
offset. The offset guard is what makes a bootstrap replay racing live traffic
converge rather than corrupt.

**Amended 2026-08-26.** Two refinements from the revision above. The mode uses
`assign()` rather than `subscribe()` and commits no offset, so it joins no group
and leaves no broker state behind. And it is not entered on every cold start —
`auto.offset.reset = earliest` already replays a fresh consumer group unaided
(`crates/kafkaman-rdkafka/src/consumer.rs:102`). It is entered when the
*database* is fresh under a surviving group, which is the only case nothing else
recovers from.

### Cache consumers are not load-balanced consumers

**Superseded 2026-08-26 — retained as the record of what was considered.**
kafkaman's cache is a shared Postgres table, so there is no per-process copy to
keep whole and no per-instance group to create. See the revision above.

The trap that must be designed for up front: every instance needs a **full**
copy of the entity set, so each process needs its own consumer group reading
**all** partitions — not a shared group with partition assignment across
instances. This is the Kafka Streams GlobalKTable distinction.

This is a genuinely different consumer model from the work-queue dispatcher,
where shared-group partition assignment is exactly what is wanted. Getting it
wrong is silent at one instance and bites on deploy, when a second pod comes up
holding half the data and serving confidently.

### Cache state table

A `cache_state` table tracks, per type and partition:

- `bootstrap_high_watermark` — the partition high watermark captured at
  bootstrap start, which is the target the replay must reach
- `current_offset` — how far application has progressed
- `status` — bootstrapping, ready, or rebuilding

A partition is caught up when `current_offset` reaches the high watermark
captured at start; the cache is ready when every assigned partition is.

### Readiness surface

The primitive with the most value in this proposal. "Is my local cache warm
enough to serve reads?" is routinely hand-rolled and routinely wrong, and it is
the difference between a cache and a cache that silently serves empty results
after a deploy.

This cache does have an origin to fall back to — the compacted topic holds the
authoritative current state of every key, which is exactly what makes option 2
work. What it lacks is a **per-key** fallback: Kafka has no key-based random
read, so a miss cannot be faulted in on demand the way an ordinary cache queries
its backing store. Refill is necessarily bulk replay of the partition.

That asymmetry is the reason readiness has to be a typestate rather than
lazy-loading. A conventional cache can serve a miss by fetching one row; this one
must already be warm, because the cheapest possible answer to "I don't have that
entity" is to replay the whole partition.

Readiness should be exposed both as a runtime query and, preferably, in the type
system. This is the case where typestate proper earns its cost:
`Cache<Bootstrapping>` → `Cache<Ready>`, with the read capability available only
on `Ready`. That turns "do not serve reads from a cold cache" into a compile
error rather than an `is_ready()` call that callers forget.

**Amended 2026-08-26.** The transition survives; the ~~`get()`~~ this section
originally named as the gated method does not. kafkaman ships no typed key-value
getter, so there is no such method to withhold — what is gated is the qualified
table name. *Readiness: the typestate survives, what it gates changes* above is
authoritative on which capability that is. Nothing else in this section depends
on the answer.

Unlike configuration-dependent checks that cannot be fully compile-time because
the mode comes from `kafkaman.toml`, the bootstrap lifecycle is a genuine state
transition on a value the host already holds, so consuming `self` on the
transition is natural.

Proposal 09's 2026-08-13 revision adds a second caller for this machinery: topic
recreation, repartitioning and mirroring invalidate a cache's stored offsets
and require a forced re-bootstrap, which is exactly the `Ready` → `Bootstrapping`
transition this proposal defines.

### Rebuild rule

Under soft-delete-first (proposal 09) a delete record is never reclaimed, so a
cache can always resume incrementally without missing deletes.

If and when real-tombstone reclamation is added, that stops being true:
`delete.retention.ms` bounds how long a tombstone is observable, and a cache
offline longer than that can miss a delete and hold a ghost entity forever. The
rule to enforce then is that a resume gap exceeding the tombstone retention
window requires dropping the cache table and rebuilding from scratch rather
than resuming incrementally.

## Consequences

Positive:

- ~~Cold start, new instances, and rebuilds are all one mechanism.~~ **Amended by
  the 2026-08-26 revision.** Cold start on a fresh database and rebuilds are one
  mechanism. A new *instance* is not in that list: it needs no mechanism at all,
  because it queries the shared table its peers already filled.
- No request/response path is reintroduced.
- Hosts get a first-class answer to cache warmth instead of guessing.

Costs and risks — **partly superseded by the 2026-08-26 revision**; see
*Residual cost, recorded honestly* above for the current list:

- ~~A second consumer mode with per-instance consumer groups, which multiplies
  consumer-group count by instance count and needs a naming and cleanup policy.~~
  **Deleted.** The replay is assign-based and joins no group, so there is no
  group count to multiply and nothing to reap.
- ~~Bootstrap cost scales with the compacted topic size, and a large entity set
  makes deploys slower; a rebuild storm across many instances hits the broker at
  once.~~ **Halved.** The cost is real but paid once per *database*, not once per
  process, so there is no rebuild storm across instances.
- A `cache_state` table and its changeset. **Still stands.**
- Typestate on the cache handle changes the host-facing API shape and needs a
  non-typestate escape hatch for dynamic hosts. **Still stands**, and the escape
  hatch already exists: `CacheTable::for_message` stays public.
- **Added by the revision:** a fresh database's received table grows to the size
  of the compacted topic and stays there, because `[retention]` is outbox-only.

## Open Questions

**None of the five below are open. Question 1 was dissolved and 2-5 were
answered on 2026-08-26 — see *The open questions, answered* above, which is
authoritative. They are kept here as the record of what was asked.**

1. ~~How are per-instance consumer group ids derived, and who cleans up groups
   left behind by terminated instances?~~ **Dissolved** — no groups.
2. ~~Does bootstrap write through the received/inbox table as normal, or bypass it
   and write straight to the cache?~~ **Answered: write through.**
3. ~~Should readiness be per type or aggregate across all replicated types the
   host consumes?~~ **Answered: per type**, with an aggregate report that is
   never gated on.
4. ~~Should a cache serve stale reads with an explicit staleness signal while
   bootstrapping, rather than refusing reads outright?~~ **Answered: no.**
5. ~~How does this interact with the dispatcher's retry and DLQ state — can a row
   that lands in DLQ during bootstrap block readiness indefinitely, and should
   it?~~ **Answered: it must not**; `Failed` counts as drained.

Genuinely open, raised by the revision:

6. What triggers the replay? The hazard is a *fresh database*, so the trigger has
   to be a database-side signal — an empty or absent `cache_state` row — rather
   than anything the broker knows.
7. `RuntimeTasks::wait()` must stop treating a cleanly completing loop as a
   shutdown signal before the replay can be spawned at all. Tracked as Q1 in
   [05-deep-durability-testing](05-deep-durability-testing.proposal.md).

## Promotion Target

Promote into a decision fixing the bootstrap consumer mode and its consumer
group model, the `cache_state` table shape, the readiness surface including
whether typestate is mandatory or optional, and the rebuild rule for the
reclamation era.

Sequenced after [09-entity-first-propagation](09-entity-first-propagation.proposal.md)
is implemented, since bootstrap depends on the guarded upsert and soft-delete
semantics that proposal establishes.
