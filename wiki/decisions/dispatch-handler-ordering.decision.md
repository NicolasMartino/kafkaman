# Dispatch Handler Ordering

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-25
- Category: Message consumption semantics
- Scope: Where an application handler runs relative to the cache upsert inside
  the received-row transaction, what a handler may and may not suppress, and the
  handler registration shapes the runtime builder exposes.
- Sources:
  - crates/kafkaman-sqlx/src/dispatch_cache.rs
  - crates/kafkaman-worker/src/dispatcher.rs
  - examples/product/src/lib.rs
  - wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md
- Related:
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/missing-handler-dispatch-policy.decision.md
  - wiki/decisions/dispatch-stats-semantics.decision.md
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/plans/runtime-builder-and-axum-composition.plan.md
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
- Promotion Target: `wiki/specs/entity-first-propagation.spec.md`, whose
  "Receive-side convergence" section currently states "Receive dispatch still
  runs the handler first" as validated truth. That sentence is the exact line
  this decision invalidates, and it may only be edited after the reorder ships
  with passing tests.

## Decision

1. **`handle::<T>(f)` runs *after* the cache upsert.** This reverses the
   current `dispatch_once` order and becomes the default handler position. A
   handler that derives state from the cache — the common case — sees the
   incoming message already applied and needs no compensation for its own row.

2. **`handle_before::<T>(f)` runs *before* the cache upsert.** It is the
   explicit opt-in for handlers that need the entity's previous version, which
   is unrecoverable once the upsert lands. Naming it separately makes choosing
   the pre-image a decision rather than an inherited default.

3. **Neither hook may suppress the cache upsert.** There is no ingest-time
   filter hook, and adding one is out of scope permanently, not merely
   unimplemented. See *Why* below.

   This constrains *registered handlers*. It does not change
   `MissingHandler`, which today short-circuits ahead of the upsert:
   `dispatch_once` looks the handler up before it applies the cache, so an
   unregistered type parks the row `Retryable` and the cache does not advance.
   That stays as it is. The row is parked, not dropped, so convergence is
   deferred rather than broken, and the deferral is the point — a replica that
   has not been deployed yet must not silently converge a cache it has no code
   to derive from. The lookup therefore remains *before* the upsert even though
   the handler call moves after it, and
   `wiki/decisions/missing-handler-dispatch-policy.decision.md` is unchanged by
   this decision.

4. **`handle_before` may skip the post-upsert handler, never the upsert.** It
   returns a control-flow value. Skipping downstream work is a cost decision the
   application is entitled to make; skipping the upsert is not, because it
   breaks convergence.

5. **A post-upsert handler does not run when the cache apply outcome is
   `Ignored`.** `upsert_cache_from_received` already returns
   `CacheApplyOutcome::Ignored` for a record at or behind the entity's applied
   offset, and distinguishes it from `Applied` and from `Migrated` — the
   cross-topic origin reset. (`Migrated` is the variant's name in
   `crates/kafkaman-sqlx/src/dispatch_cache.rs`; the log line it drives reads
   "adopted the declared topic", which is prose, not an identifier.) The handler
   runs for `Applied` and `Migrated` and is skipped for `Ignored`: an ignored
   record carries no new state, so re-deriving from it is wasted work at best
   and a stale recomputation at worst. A pre-upsert handler cannot make this
   distinction, which is a further argument for the post-upsert default.

6. **Both hooks may be registered for the same message type.** This is the only
   legal two-handler registration. Every other duplicate registration for one
   type is a conflict and must be rejected before any database or broker I/O.

7. **Handler and upsert remain in one transaction, behind one savepoint taken
   before the upsert.** A handler failure rolls back the upsert, the handler's
   own writes, and any consume-then-produce enqueue together. Reordering within
   the transaction changes no durability property — but only if the savepoint
   moves with it. `create_dispatch_handler_savepoint` currently runs
   immediately before the handler, which under the new order would sit *after*
   the upsert and leave a failed dispatch with an advanced cache row and a
   `Retryable` received row: on retry the record is then at or behind the
   applied offset, yields `Ignored`, and the post-upsert handler is skipped
   forever. The savepoint therefore opens before the upsert and covers both
   hooks. This is the one place where the reorder is not mechanically safe, and
   it must be tested, not reasoned about.

8. **The handler signature must be able to carry a tombstone before it becomes
   public — and the shape is not yet chosen.** Deletions are not implemented:
   the cache table carries `deleted BOOLEAN NOT NULL DEFAULT false` and the
   upsert hardcodes `deleted = false`, so the column is reserved and nothing
   writes it. A tombstone has no payload bytes, so *something* in the signature
   has to represent absence.

   What is decided here is only that the question may not be deferred past
   first publication. What is **not** decided is the shape, and this decision
   does not settle it — an earlier draft claimed the builder plan had, which was
   wrong. `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`
   (Status: Proposed) already carries a direction: put the deletion flag in
   `ReceivedMeta` rather than change the shape of `P`, "which avoids forcing
   every payload type into an enum wrapper and keeps the M3 handler signature
   stable." That remains the leading candidate and the burden of proof is on any
   alternative. Phase 0 of the builder plan chooses between it and an
   `Option<T>`-style payload, and whichever wins is recorded back onto proposal
   07 rather than only in the plan.

## Why

### The footgun is documented in the example, not hypothetical

`examples/product/src/lib.rs` carries a doc section titled *The exclusion that
is not obvious*:

> `dispatch_once` runs this handler *before* it upserts the message into the
> cache. The row for the entity being processed is therefore exactly one version
> stale — absent on the first snapshot, and holding the previous status on every
> transition after that. So the query excludes this order's own row and the
> incoming status is added back explicitly. Without the exclusion a Placed →
> Fulfilled transition counts the order at both its old and its new status.
>
> It is deterministic […] but it is a genuine footgun for any handler that
> derives state from its own cache.

The runtime builder makes assembly declarative. Leaving this in place would mean
shipping a blessed API whose easiest correct-looking handler is subtly wrong.
The trap costs more debugging time than the boot wiring the builder removes.

### Post-upsert is what derivation wants

`apply_order_snapshot` recomputes availability as a `SUM` over cached fulfilled
orders. Under the new order the exclusion predicate `entity_key <> $3` and the
`this_order` add-back both disappear, and the doc section above deletes with
them. The example's most heavily annotated function gets shorter, which is the
clearest available evidence that the previous order was working against its
users.

### Filtering would break convergence

The same file already argues why suppression cannot be offered:

> Caching only fulfilled orders would break convergence — an order going
> Fulfilled → Cancelled would leave a stale Fulfilled row in the cache forever —
> and it is not even expressible: kafkaman upserts every consumed message into
> the cache inside `dispatch_once`, with no ingest-time filter hook.

The cache is the converged current state of every entity on the topic. A handler
that can suppress the upsert produces a stale row that nothing will ever correct,
because the correcting message is exactly the one that was filtered. Selection
belongs at query time, where `apply_order_snapshot` already puts it — and where a
cancelled order simply stops matching the predicate with no compensating logic
anywhere.

### Pre-image is a cost guard, not a correctness guard

Every candidate use in the two-service example — cancelling outstanding orders
when a product is discontinued, flagging price movement, skipping an unchanged
recompute — is expressible post-upsert, idempotently, at higher cost. That is
worth recording rather than obscuring: it tells a reader that `handle_before` is
the rare choice, and it keeps the default honest.

## Consequences

- `dispatch_once` changes ordering for low-level users too, not only for builder
  users. This is a semver-relevant behavior change and needs a compatibility
  note when it lands.
- `apply_order_snapshot` simplifies, and the example loses a documented
  workaround. The doc comment should be replaced with a short note recording
  that the ordering is deliberate, so the reasoning is not lost with the
  workaround.
- `handle_before` adds a second hook position to the dispatch path. The
  dispatcher must run at most one of each per type per message.
- The `Ignored` skip means a post-upsert handler is not guaranteed to run once
  per received row. Handlers that must observe every delivery — auditing, for
  instance — belong in `handle_before`, and this must be documented.
- `DispatchStats.processed` stops implying "the handler ran". A row that yields
  `Ignored` is still claimed, still marked processed, and still counted, while
  its post-upsert handler is skipped. `wiki/decisions/dispatch-stats-semantics.decision.md`
  defines `processed` as a row-disposition count rather than a handler-execution
  count, so the numbers stay correct — but the prose that reads them must stop
  treating the two as the same thing.
- Redrive gains a sharper edge. Re-dispatching an already-applied row after
  deploying new handler code produces `Ignored`, so the new handler does not
  run. Deriving state from a redrive now requires clearing the cache row's
  applied offset first, which is what the rebuild path already does.
- Tombstone support becomes a signature constraint on the first public release
  of the handler API rather than a later additive feature.

## Alternatives Considered

- **Keep the pre-upsert order and document the exclusion harder.** Rejected.
  The documentation already exists and is unusually good; the trap survived it.
- **Offer only post-upsert.** Rejected. Change detection genuinely cannot
  recover the pre-image, and auditing every delivery needs a hook the `Ignored`
  skip does not bypass.
- **Let a handler suppress the upsert.** Rejected on convergence grounds above.
- **Pass the pre-image to the post-upsert handler instead of adding a second
  hook.** Rejected for the first implementation: it makes every handler pay for
  a read that almost none of them use, and it cannot express "skip the expensive
  derivation entirely."
- **Defer the tombstone question.** Rejected. The handler signature is the most
  semver-sensitive surface in the builder, and absence cannot be retrofitted
  into `T` without breaking it.

## Revisit When

- Tombstone and deletion semantics are implemented and the reserved `deleted`
  column becomes writable.
- A handler needs to observe the cache apply outcome itself rather than merely
  being skipped on `Ignored`.
- Consume-then-produce grows a batching model that changes what "one
  transaction" means for the dispatch path.
