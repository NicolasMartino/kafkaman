# Dispatch Infrastructure Error Classification

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-22
- Category: Durable receive
- Scope: How dispatch classifies infrastructure failures that happen after a handler returns `Ok`.
- Sources:
  - wiki/plans/m3-durable-completion.plan.md
  - wiki/proposals/05-deep-durability-testing.proposal.md
  - crates/kafkaman-sqlx/src/lib.rs
  - tests/durable-send/tests/durable_receive.rs
- Related:
  - wiki/decisions/missing-handler-dispatch-policy.decision.md
  - wiki/decisions/dispatch-stats-semantics.decision.md

## Decision

If a handler returns `Ok(())` but the receive transaction is no longer usable
when kafkaman marks the row `Processed`, the error is recorded as receive failure
accounting for that row.

`dispatch_once` creates a handler savepoint before invoking the handler. If the
handler returns `Ok(())` but marking the row `Processed` fails because the
transaction is unusable, kafkaman rolls back to the savepoint, records the
mark-processing error as `Infrastructure` while still holding the claimed row
lock, commits the failure row, and reports a failed dispatch.

If the transaction connection is already unusable and savepoint rollback cannot
run, rollback is best-effort and failure accounting falls back to a separate
pool connection. A rollback failure must not prevent the row-local
`Infrastructure` failure from being recorded.

## Rationale

A handler can accidentally swallow a SQL error and leave the transaction in
Postgres `25P02` aborted state. Treating the resulting mark-processed error as a
plain scheduler error loses the row-local error record and allows the loop to
retry without durable explanation. Recording it as receive failure preserves the
ledger invariant and makes the handler bug visible.

## Evidence

- `poisoned_handler_transaction_is_recorded_as_dispatch_failure`
- `rollback_failure_still_records_infrastructure_dispatch_failure`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
