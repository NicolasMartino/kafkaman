# Missing Handler Dispatch Policy

Document Class: Decision
Status: Accepted
Date: 2026-06-22
Category: Durable receive
Scope: How `dispatch_once` handles a received row whose message type has no registered handler.
Sources:
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md
- crates/kafkaman-sqlx/src/lib.rs
- tests/durable-send/tests/durable_receive.rs
Related:
- wiki/decisions/dispatch-stats-semantics.decision.md
- wiki/decisions/dispatch-infrastructure-error-classification.decision.md

## Decision

`MissingHandler` is a per-row dispatch failure, not a scheduler-fatal error.

When `dispatch_once` claims a row and no handler is registered for its
`message_type`, it records a receive failure in the received table under the
same claimed-row transaction and returns `DispatchStats { claimed: 1, processed:
0, failed: 1 }` after that durable failure row update commits.

The failure parks the row by setting it to `Retryable` with `next_attempt_at =
NULL`, matching existing receive failure behavior. A missing-handler row must not
remain `Pending` and repeatedly block younger valid rows in the same table.

## Rationale

The receive dispatcher loop must be resilient: one unregistered type should not
kill the worker or cause head-of-line blocking forever. Recording the error in
the durable ledger keeps the operator-visible failure near the row that needs
configuration or deployment repair.

## Evidence

- `missing_handler_is_recorded_and_does_not_block_younger_rows`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
