# Missing Handler Dispatch Policy

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-22
- Category: Durable receive
- Scope: How `dispatch_once` handles a received row whose message type has no registered handler.
- Sources:
  - wiki/plans/m3-durable-completion.plan.md
  - wiki/proposals/05-deep-durability-testing.proposal.md
  - crates/kafkaman-sqlx/src/lib.rs
  - tests/durable-send/tests/durable_receive/
- Related:
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/decisions/dispatch-stats-semantics.decision.md
  - wiki/decisions/dispatch-infrastructure-error-classification.decision.md

## Decision

`MissingHandler` is a per-row dispatch failure, not a scheduler-fatal error.

When `dispatch_once` claims a row and no handler is registered for its
`message_type`, it records a receive failure in the received table under the
same claimed-row transaction and returns `DispatchStats { claimed: 1, processed:
0, failed: 1, panicked: 0 }` after that durable failure row update commits.

The failure parks the row by setting it to `Retryable` with `next_attempt_at =
NULL`, matching existing receive failure behavior. A missing-handler row must not
remain `Pending` and repeatedly block younger valid rows in the same table.

## Amendment, 2026-08-26

Two later decisions touch this policy without changing it.

`wiki/decisions/dispatch-handler-ordering.decision.md` moves the application
handler to run *after* the cache upsert. The router lookup does **not** move with
it: it stays ahead of the upsert, so an unregistered type still parks its row
without advancing the cache. That is deliberate. The handler-ordering decision
forbids a registered handler from suppressing the upsert, on convergence
grounds; a missing handler is a different case, because the row is parked rather
than dropped and convergence is deferred rather than broken. A replica that has
not been deployed yet must not silently converge a cache it has no code to
derive from.

`wiki/decisions/runtime-builder-and-axum-composition.decision.md` narrows how
often this policy fires: a `cache::<T>()` role installs a no-op handler, so a
builder-registered type cannot reach `MissingHandler` at all. The policy stays
in force for hand-wired routers, for types consumed without any role, and for
the partial-deployment window this decision exists to survive.

## Rationale

The receive dispatcher loop must be resilient: one unregistered type should not
kill the worker or cause head-of-line blocking forever. Recording the error in
the durable ledger keeps the operator-visible failure near the row that needs
configuration or deployment repair.

## Evidence

- `missing_handler_is_recorded_and_does_not_block_younger_rows`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
