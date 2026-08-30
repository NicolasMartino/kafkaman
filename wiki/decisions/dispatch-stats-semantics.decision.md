# Dispatch Stats Semantics

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-22
- Category: Durable receive
- Scope: Meaning of `DispatchStats` counters returned by `dispatch_once` and the receive dispatcher loop.
- Sources:
  - wiki/plans/m3-durable-completion.plan.md
  - wiki/reviews/m3-durable-receive-implementation-review.reference.md
  - crates/kafkaman-sqlx/src/lib.rs
  - tests/durable-send/tests/durable_receive.rs
- Related:
  - wiki/decisions/missing-handler-dispatch-policy.decision.md
  - wiki/decisions/dispatch-infrastructure-error-classification.decision.md

## Decision

`DispatchStats.failed` counts committed durable failure records, not merely
attempted handler failures.

Normal handler and missing-handler failure accounting holds the claimed row lock
until the retryable failure row commits. A competing dispatcher therefore skips
the locked row and reports
`DispatchStats { claimed: 0, processed: 0, failed: 0, panicked: 0 }` while the
first dispatcher is still recording the failure. The dispatcher that commits the
failure reports
`DispatchStats { claimed: 1, processed: 0, failed: 1, panicked: 0 }`, or
`panicked: 1` when the failure was a caught handler panic.

## Rationale

The dispatcher loop's logs and future metrics should describe durable state
changes, not transient attempts that were suppressed by effective-once guards.
This aligns receive stats with send-side mark outcomes.

## Evidence

- `failure_recording_holds_row_lock_until_retryable_commit`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
