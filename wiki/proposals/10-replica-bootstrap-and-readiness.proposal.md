# Replica Bootstrap and Readiness

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-12
- Category: Propagation model
- Scope: Proposes how a kafkaman replica catches up on a compacted entity topic from cold, tracks its own warmth, and exposes readiness so hosts never serve reads from an incomplete cache.
- Sources:
  - raw/design/2026-08-12-entity-first-propagation-discussion.md
- Related:
  - wiki/proposals/09-entity-first-propagation.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
- Promotion Target: a decision fixing the bootstrap consumer mode, the replica state table, and the readiness surface.

## Context

A service holding a replica of another domain's entities has a cold-start
problem: on first deploy, on a new instance, and after a rebuild, the local
store is empty or stale, and the host must not serve reads from it until it is
warm.

This is the catch-up story left open by
[09-entity-first-propagation](09-entity-first-propagation.proposal.md). It is
separated because it depends on that model being settled, introduces a second
consumer mode, and is later work than the core.

kafkaman today has no notion of a replica, no notion of warmth, and no consumer
mode other than the work-queue dispatcher.

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
mechanism, but it is subject to the versioning caveat in proposal 09: under
outbox-sequence versioning a republish can publish stale state at a higher
sequence and win. It is only safe for types using a domain version.

## Proposal

### Bootstrap consumer mode

A second consumer mode alongside the work-queue dispatcher. It seeks to the
beginning of the assigned partitions and applies records through the same
guarded upsert used in steady state, so duplicate and out-of-order application
during bootstrap is safe by construction and needs no special casing.

Bootstrap and live tailing are the same code path with a different starting
offset. The version guard is what makes a bootstrap replay racing live traffic
converge rather than corrupt.

### Replica consumers are not load-balanced consumers

The trap that must be designed for up front: every instance needs a **full**
copy of the entity set, so each process needs its own consumer group reading
**all** partitions — not a shared group with partition assignment across
instances. This is the Kafka Streams GlobalKTable distinction.

This is a genuinely different consumer model from the work-queue dispatcher,
where shared-group partition assignment is exactly what is wanted. Getting it
wrong is silent at one instance and bites on deploy, when a second pod comes up
holding half the data and serving confidently.

### Replica state table

A `replica_state` table tracks, per type and partition:

- `bootstrap_high_watermark` — the partition high watermark captured at
  bootstrap start, which is the target the replay must reach
- `current_offset` — how far application has progressed
- `status` — bootstrapping, ready, or rebuilding

A partition is caught up when `current_offset` reaches the high watermark
captured at start; the replica is ready when every assigned partition is.

### Readiness surface

The primitive with the most value in this proposal. "Is my local cache warm
enough to serve reads?" is routinely hand-rolled and routinely wrong, and it is
the difference between a replica and a cache that silently serves empty results
after a deploy.

Readiness should be exposed both as a runtime query and, preferably, in the type
system. This is the case where typestate proper earns its cost:
`Replica<Bootstrapping>` → `Replica<Ready>`, with `get()` available only on
`Ready`. That turns "do not serve reads from a cold cache" into a compile error
rather than an `is_ready()` call that callers forget.

Unlike the version-source check in proposal 09 — which cannot be fully
compile-time because the mode comes from `kafkaman.toml` — the bootstrap
lifecycle is a genuine state transition on a value the host already holds, so
consuming `self` on the transition is natural.

### Rebuild rule

Under soft-delete-first (proposal 09) a delete record is never reclaimed, so a
replica can always resume incrementally without missing deletes.

If and when real-tombstone reclamation is added, that stops being true:
`delete.retention.ms` bounds how long a tombstone is observable, and a replica
offline longer than that can miss a delete and hold a ghost entity forever. The
rule to enforce then is that a resume gap exceeding the tombstone retention
window requires dropping the replica table and rebuilding from scratch rather
than resuming incrementally.

## Consequences

Positive:

- Cold start, new instances, and rebuilds are all one mechanism.
- No request/response path is reintroduced.
- Hosts get a first-class answer to cache warmth instead of guessing.

Costs and risks:

- A second consumer mode with per-instance consumer groups, which multiplies
  consumer-group count by instance count and needs a naming and cleanup policy.
- Bootstrap cost scales with the compacted topic size, and a large entity set
  makes deploys slower; a rebuild storm across many instances hits the broker at
  once.
- A `replica_state` table and its changeset.
- Typestate on the replica handle changes the host-facing API shape and needs a
  non-typestate escape hatch for dynamic hosts.

## Open Questions

1. How are per-instance consumer group ids derived, and who cleans up groups
   left behind by terminated instances?
2. Does bootstrap write through the received/inbox table as normal, or bypass it
   and write straight to the replica? Writing through gives uniform durability
   and error handling but inflates the inbox by the entire topic size on every
   cold start.
3. Should readiness be per type or aggregate across all replicated types the
   host consumes?
4. Should a replica serve stale reads with an explicit staleness signal while
   bootstrapping, rather than refusing reads outright?
5. How does this interact with the dispatcher's retry and DLQ state — can a row
   that lands in DLQ during bootstrap block readiness indefinitely, and should
   it?

## Promotion Target

Promote into a decision fixing the bootstrap consumer mode and its consumer
group model, the `replica_state` table shape, the readiness surface including
whether typestate is mandatory or optional, and the rebuild rule for the
reclamation era.

Sequenced after [09-entity-first-propagation](09-entity-first-propagation.proposal.md)
is implemented, since bootstrap depends on the guarded upsert and soft-delete
semantics that proposal establishes.
