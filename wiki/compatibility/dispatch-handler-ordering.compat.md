# Dispatch Handler Ordering Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-26
- Category: Message consumption semantics
- Scope: Records the behaviour and API changes from moving the application
  handler to run after the cache upsert, adding the pre-upsert position, and
  skipping the post-upsert handler on `CacheApplyOutcome::Ignored`.
- Sources:
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-sqlx/src/router.rs
  - crates/kafkaman-core/src/rows.rs
  - tests/durable-send/tests/entity_first_propagation/dispatch_ordering.rs
- Related:
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - wiki/decisions/missing-handler-dispatch-policy.decision.md
  - wiki/decisions/dispatch-stats-semantics.decision.md
  - wiki/specs/entity-first-propagation.spec.md
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md

## Behaviour Changes

**Breaking, and silent.** This affects every caller of `dispatch_once`, not only
users of the runtime builder, and nothing about it is a compile error. A handler
that was correct under the old ordering can be wrong under the new one and will
still build.

1. **`MessageRouter::handler` now runs *after* the cache upsert.** A handler that
   reads the cache row for the entity it is processing previously saw it exactly
   one version stale — absent on the first snapshot, holding the previous value
   on every transition after. It now sees the incoming record already applied.

   **Migration.** Any handler that compensated for the staleness must have the
   compensation removed. In this repository that was `apply_order_snapshot` in
   `examples/product/src/lib.rs`, which excluded its own `entity_key` from a
   `SUM` and added the incoming value back by hand; both halves are now deleted.
   Left in place, such a compensation double-counts.

   A handler that does *not* read its own entity's cache row is unaffected.

2. **The post-upsert handler is skipped when the cache apply is `Ignored`.** It
   runs for `Applied` and for `Migrated`. An `Ignored` record is at or behind the
   entity's applied offset and carries no new state.

   **Consequence.** A post-upsert handler is no longer guaranteed to run once per
   received row. A handler that must observe every delivery — auditing, metering,
   anything counting arrivals — belongs at the pre-upsert position, which the
   skip does not bypass.

   **Consequence for redrive.** Re-dispatching an already-applied row after
   deploying new handler code produces `Ignored`, so the new handler does not
   run. Deriving state from a redrive now requires clearing the cache row's
   applied offset first, which is what the rebuild path already does.

3. **`DispatchStats.processed` stops implying the handler ran.** A row yielding
   `Ignored` is still claimed, still marked processed, and still counted. The
   numbers remain correct against
   `wiki/decisions/dispatch-stats-semantics.decision.md`, which defines
   `processed` as a row disposition rather than a handler execution — but prose
   and dashboards that read the two as the same thing become wrong.

4. **The dispatch savepoint now opens before the cache upsert.** A handler
   failure rolls back the upsert along with the handler's own writes and any
   consume-then-produce enqueue. No durability property changes; what changes is
   that it keeps being true under the new ordering.

   This is the one part of the reorder that is not mechanically safe. With the
   savepoint left where it was, a failed post-upsert handler would commit an
   advanced cache row, the retry would find the record at the applied offset,
   get `Ignored`, and skip the handler permanently. Verified by deliberate
   breakage rather than by inspection.

## Public API Changes

**Additive.**

- `MessageRouter::handler_before<P>(..)` registers a handler at the pre-upsert
  position. Registering both positions for one message type is the only legal
  two-handler registration; the dispatcher runs at most one of each per record.
- `HandlerFlow { Continue, SkipHandler }` is what a pre-upsert handler returns.
  `SkipHandler` skips the post-upsert handler for that record. There is
  deliberately no variant that suppresses the cache upsert, and adding one is
  permanently out of scope: the cache is the converged current state of every
  entity on a topic, so a filtered record leaves a stale row that only the
  filtered message could ever have corrected.
- `BeforeHandlerFuture<'a>` is the pre-upsert twin of `HandlerFuture<'a>`,
  differing only in yielding a `HandlerFlow`.

**Breaking.**

- `ReceivedMeta` is now `#[non_exhaustive]`. It was a plain struct with sixteen
  public fields, so code constructing it with a struct literal no longer
  compiles. `impl From<&ReceivedRow>` is the supported constructor and is
  unchanged.

  The change is what makes the *next* one additive. `ReceivedMeta` is the
  declared carrier for entity deletion — a tombstone has no payload bytes, so
  absence has to be representable somewhere in the handler signature, and
  metadata is where a per-delivery fact most handlers ignore belongs. It gains
  `deleted: bool` (`#[serde(default)]`) and `ReceivedMeta::is_deleted()`, both
  reserved: nothing writes `true`, tombstone ingestion is unimplemented, and the
  cache table's `deleted` column stays unwritten. A handler branching on
  `is_deleted()` today writes dead but forward-compatible code.

## Unchanged

- The `MissingHandler` lookup stays ahead of the upsert, and now checks both
  positions. An unregistered type parks its received row `Retryable` with the
  cache untouched, so a replica that has not been deployed yet defers
  convergence rather than silently advancing a cache it has no code to derive
  from. `wiki/decisions/missing-handler-dispatch-policy.decision.md` is
  unaffected.
- Handler, upsert, and processed mark remain in one transaction.
- `dispatch_once`'s signature is unchanged.

## Verification

`tests/durable-send/tests/entity_first_propagation/dispatch_ordering.rs` covers
each item behaviourally: a post-upsert handler observing its own record applied,
a pre-upsert handler observing the previous version, the `Ignored` skip, the
`SkipHandler` flow not preventing the upsert, a failing handler at either
position leaving the cache untouched, an unregistered type parking its row, and
a pre-upsert-only registration not counting as a missing handler.

The savepoint placement was additionally verified by deliberate breakage: moved
back after the upsert, `a_failing_post_upsert_handler_unwinds_the_cache_upsert_too`
fails with the cache holding the failed handler's value.
