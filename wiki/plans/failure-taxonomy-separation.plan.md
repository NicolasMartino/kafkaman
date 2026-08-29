# Failure Taxonomy Separation Plan

- Document Class: Plan
- Status: Completed 2026-08-30.
- Date: 2026-08-30
- Category: Error classification and operability
- Scope: Executes the decision to derive a failure's persisted class from the error alone and to record the frame it surfaced in as its own field.
- Sources:
  - wiki/decisions/failure-taxonomy-and-blame-separation.decision.md
  - wiki/proposals/22-failure-taxonomy-and-blame.proposal.md
- Related:
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Deliverable

One classification function, one blame field, and a test that the two
vocabularies cannot drift.

## Phase 1 - The coarsening, in one place

`crates/kafkaman-core/src/failure_kind.rs`:

```rust
impl ReceivedFailureKind {
    pub fn coarsening(problem_type: &str) -> Self { ... }
}
```

A `match` over the fifteen `problem::*` constants onto the four variants. Not a
`from_problem_type` overload — that one is the exact inverse over four URIs and
is what reads stored rows back; this one is a lossy many-to-one and must not be
confused with it.

`crates/kafkaman-core/src/problem.rs`: `ProblemType` gains a provided method

```rust
fn failure_kind(&self) -> ReceivedFailureKind {
    ReceivedFailureKind::coarsening(self.problem_type())
}
```

so a caller cannot re-derive the relationship differently.

**Tests.** Totality over `ALL_PROBLEM_TYPES` — every URI coarsens, and the four
URIs that *are* kinds coarsen to themselves, which is the round-trip that keeps
`from_problem_type` and `coarsening` agreeing on their overlap.

## Phase 2 - One disposition

`crates/kafkaman-sqlx/src/dispatch_failure.rs`: delete
`handler_failure_disposition` and `received_failure_disposition`; add

```rust
pub(crate) fn failure_disposition(error: &Error, stage: FailureStage) -> FailureDisposition
```

- `kind` from `error.failure_kind()`.
- `terminal` from the error alone: `CacheOriginMismatch`, `MissingEntityKey`,
  `InvalidEntityKey` — the three whose predicate cannot become true on a retry.
- `stage` carried through, so nothing downstream needs a new parameter.

`FailureDisposition` gains `stage`. Call sites in `dispatch.rs` already know it.

## Phase 3 - Persist the blame

`FailureStage` moves from `dispatch.rs` to `kafkaman-core`, gains
`Serialize`/`Deserialize` and its three lowercase spellings, and becomes public.

`ReceivedError` gains `#[serde(default, skip_serializing_if = "Option::is_none")]
pub stage: Option<FailureStage>`. Absent means "written before this existed", so
`Option` rather than a defaulted variant.

`ReceivedError::new` takes the stage; `record_received_failure` writes it;
`DlqRowSummary.latest_error` carries it out unchanged.

## Phase 4 - Tests

- The disagreement, pinned: `Error::Sqlx` from a handler frame classifies
  `Infrastructure` and its problem type is `infrastructure`. This is the exact
  case the decision exists for and it belongs in a named test.
- A stored `ReceivedError` round-trips with and without `stage`, so old rows keep
  reading.
- `tests/durable-send`: a handler returning a database error dead-letters as
  `Infrastructure` and is selected by `ReceivedFailureFilter::kind(Infrastructure)`
  — the operator-visible payoff.
- The span's `kafkaman.failure.kind` equals `coarsening(error.type)` on a real
  dispatch failure.

## Phase 5 - Documentation

Compatibility note: the changed classification with its migration guidance,
`FailureStage` as new public API and a persisted value, the `ReceivedError`
field, and the terminal-ness change. Index and log entries.

## Verification

1. `cargo check --workspace --all-features --all-targets`, `cargo clippy`, `cargo fmt --check`.
2. `cargo test --workspace --all-features --lib`.
3. `cargo test -p observability-tests`.
4. `cargo test --manifest-path tests/durable-send/Cargo.toml --tests`.
5. `just examples telemetry-test`.
6. A fault run against the live stack, reading the DLQ row back to confirm the
   persisted shape.

## Risks

- The stored-value change is the whole risk, and it is not mitigable by testing —
  only by documenting. See the decision's cost section.
- `FailureStage` becoming public means its spellings are frozen; they are chosen
  to match the existing `kafkaman.failure.stage` span attribute exactly, so the
  trace and the row cannot disagree about a stage name.
