# Listen/Notify Scheduler Wakeup

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-12
- Category: Runtime scheduling
- Scope: Proposes Postgres LISTEN/NOTIFY as an accelerator for the relay and dispatch schedulers, with the existing interval sweep retained as the correctness path.
- Sources:
  - User design discussion, 2026-08-12
  - Prior-art review of `cqrs-fullstack/code/shared/messaging/src/outbox_loop.rs` (workout2 project snapshot)
  - wiki/specs/m1-durable-send.spec.md
  - wiki/specs/m3-durable-receive.spec.md
- Related:
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
  - wiki/proposals/15-dispatch-concurrency-and-middleware.proposal.md
  - wiki/decisions/runtime-composition-and-topology.decision.md

## Note 2026-08-26: wanted for the next iteration, and it pairs with concurrency

Selected *for the next planning round*, not accepted. Status stays `Proposed`:
the defaults, the config surface and the fallback behaviour below are all still
open, and nothing here promotes them. What changed is only that this page is now
in scope for the developer-UX increment following the runtime-builder
workstream. Two things are worth recording without disturbing what is below.

**It pairs with `max_in_flight`.**
[15-dispatch-concurrency-and-middleware](15-dispatch-concurrency-and-middleware.proposal.md)
proposes opt-in concurrent dispatch, and the two are complementary rather than
alternatives: `NOTIFY` decides *when* a dispatcher wakes, `max_in_flight` decides
*how many* rows are in flight once it has. Both change `run_dispatcher`, so they
should be sequenced together rather than colliding.

**The framing below should survive implementation.** This page calls itself "an
efficiency proposal, not a latency proposal, and it should be ranked
accordingly", and that self-demotion is easy to quietly drop once someone is
building it. It should not be. The measurable win is idle database load that
currently scales with `types × replicas × poll frequency`; the latency
improvement is real but bounded by the sweep, which remains the correctness path
— a row becoming *due* for retry generates no `NOTIFY` at all.

## Context

Both kafkaman schedulers are interval pollers. `relay_once` claims due outbox
rows on a timer, and `run_dispatcher` drains claimed received rows and then
sleeps for a configured interval after an empty cycle. There is no LISTEN/NOTIFY
anywhere in the crates.

The consequence is a fixed idle cost. Each running worker issues a claim query
per table per interval whether or not work exists, and kafkaman uses per-type
tables, so that cost scales with the product of message types, worker replicas,
and polling frequency. A deployment with many low-traffic reference types pays
close to its full polling cost while doing almost nothing.

The `cqrs-fullstack` snapshot addresses this with a hybrid: it maintains a
`PgListener` on an `outbox_events_pending` channel and selects between the
listener and a timed sweep, falling back to sweep-only when the listener cannot
be established (`outbox_loop.rs:118-147`).

One framing correction is worth recording, because it changes how this proposal
should be prioritized. An earlier version of this argument leaned on end-to-end
latency for callers awaiting completion. Under the reference-replication use
case, that argument is weak: propagation is explicitly eventual, and a
one-to-two second sweep interval is acceptable. The real drivers are idle
database load and the ability to lengthen the sweep interval without making
low-traffic types feel unresponsive. This is an efficiency proposal, not a
latency proposal, and it should be ranked accordingly.

## Options

1. **Keep polling only.**
   No new failure modes and no held connections. Idle cost stays proportional to
   type count times replica count.
2. **Notify from application code after enqueue.**
   kafkaman already owns `enqueue`, so the notify is easy to place, and because
   Postgres `NOTIFY` is transactional it is delivered only if the caller's
   transaction commits — which matches outbox semantics exactly. It misses rows
   that appear by other means, such as operator SQL or a replay changeset.
3. **Notify from a database trigger on insert.**
   Fires regardless of who wrote the row. Costs a trigger invocation per insert
   and adds DDL surface to every generated table.
4. **Hybrid: notify as accelerator, interval sweep always retained.**
   Treats notification as best-effort and keeps the sweep as the mechanism that
   makes progress guaranteed.

Selected option: **4**, with **2** as the notification source.

Transactional delivery makes application-side notify correct for the common path
at no DDL cost, and the retained sweep already covers exactly the cases that
option 2 misses.

## Proposal

Add opt-in notification-driven wakeup to both schedulers.

### Correctness rule

The interval sweep remains the correctness path and must never be removed or made
conditional. Notification is best-effort by construction: listener connections
drop, and Postgres drops notification payloads when its queue overflows. No
kafkaman guarantee may depend on a notification arriving. A worker that fails to
establish a listener must degrade to sweep-only and continue, as the prior art
does.

### Scheduled retries still need the timer

A row parked with a future `next_attempt_at` becomes due through the passage of
time, not through an insert, so no notification will announce it. The sweep
interval therefore also bounds retry punctuality. This is the reason the interval
cannot simply be raised to a very large value once notifications exist, and it
should be stated in the configuration documentation.

### Channel shape

Prefer a single channel per schema, carrying the table name as the notification
payload, over one channel per table. kafkaman generates per-type tables, so
per-table channels would scale listener connections with type count, whereas one
channel plus payload dispatch holds a single `PgListener` connection per worker.
The channel name must be namespaced per schema, consistent with the per-schema
advisory locking already used by `migrate()`.

### Both paths

The send path notifies on `enqueue`. The receive path notifies when ingest
commits a received row, which lets the dispatcher react to freshly ingested work
on the same mechanism.

## Consequences

Idle database load falls substantially for deployments with many low-traffic
message types, and the sweep interval becomes a tunable bounded by retry
punctuality rather than by responsiveness.

The costs:

- Each worker holds a dedicated connection outside the pool for `PgListener`,
  which must be accounted for in pool sizing guidance.
- The scheduler loops gain a select between listener receive, sweep timer, and
  cancellation, which is more concurrency surface in the code paths that M3
  already tests heavily for cancellation and in-flight completion.
- Tests must prove the degradation path explicitly: notifications lost, listener
  connection dropped mid-run, and notify storms coalescing into a single drain.

## Open Questions

1. Should notification-driven wakeup be on by default, or opt-in per runtime
   binding?
2. What is the channel naming scheme, and how is it derived from the schema name?
3. Should the notification payload carry only the table name, or also a hint such
   as the earliest `next_attempt_at`, to let a worker skip a drain it cannot
   service yet?
4. Should notify storms be debounced explicitly, or is the existing
   drain-while-work-remains loop sufficient coalescing?

## Promotion Target

If accepted, promote into a decision that fixes the channel naming scheme,
payload format, configuration surface, default on/off state, pool-sizing
guidance, and the required degradation tests.
