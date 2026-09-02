# M4 Retry Backoff DLQ Plan

- Document Class: Plan
- Status: Completed
- Date: 2026-06-22
- Category: Reliability execution
- Scope: Implements runtime retry scheduling, terminal table-backed DLQ state, and redrive/admin surfaces after M3 durable receive.
- Sources:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/specs/m3-durable-receive.spec.md
  - crates/kafkaman-config/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - tests/durable-send/tests/durable_receive/
- Related:
  - wiki/plans/m3-durable-completion.plan.md
  - wiki/proposals/05-deep-durability-testing.proposal.md

## Deliverable

M4 turns M3's parked receive failures into policy-driven operational convergence.
Handlers that fail should either be retried after a configured backoff or moved
to the table-backed DLQ terminal state once attempts are exhausted.

## In Scope

- Resolve retry policy from `kafkaman.toml` defaults and per-message overrides.
- Apply retry policy in receive dispatch failure accounting.
- Compute `next_attempt_at` from injected dispatch time and exponential backoff.
- Stop dispatching rows before they are due.
- Move exhausted rows to terminal `Failed` status with durable error history.
- Enforce configured bounded error history.
- Add tests for retry scheduling, due/not-due dispatch, terminal failure, and bounded errors.
- Add redrive/admin surfaces for retryable and terminal rows.

## Out Of Scope

- Kafka DLQ topics; V1 DLQ source of truth remains Postgres.
- Operator dashboard UI.
- Send-side retry policy unification unless it becomes necessary for receive DLQ correctness.
- Chaos/model tests from the deep-durability backlog.

## Phases

1. Policy-backed receive retry scheduling.
   - Add failing tests for retryable failure scheduling and due/not-due dispatch.
   - Carry resolved retry policy into dispatch.
   - Use configured `initial_backoff`, `max_backoff`, `multiplier`, and `errors_limit`.
2. Terminal table-backed DLQ.
   - Add failing tests for `max_attempts` exhaustion.
   - Move exhausted rows to `Failed`.
   - Preserve bounded final failure history.
3. Redrive/admin surface.
   - Define APIs to inspect terminal rows.
   - Add explicit redrive operation from `Failed`/DLQ to retryable or pending work.
   - Prove redrive does not erase forensic history unless explicitly requested.
4. Operational proof.
   - Extend Redpanda/full-loop coverage only if broker behavior can change retry outcomes.
   - Update specs after behavior is fully validated.

## Progress

- 2026-06-22: First M4 slice landed for receive dispatch. `ResolvedConfig`
  retains `RetryConfig`; `ReceivedTable` carries the per-message policy;
  failure accounting computes retry `next_attempt_at`, honors `errors_limit`,
  and moves exhausted rows to `Failed`. Evidence:
  `retryable_failure_schedules_backoff_and_due_dispatch`,
  `max_attempts_moves_received_row_to_failed_with_bounded_errors`,
  `received_failure_errors_keep_most_recent_twenty_entries`.
- 2026-06-22: Repointed `Replay::received` redrive at terminal `Failed` rows
  (Phase 3 start). Backoff scheduling left the prior M3 `Retryable` +
  `next_attempt_at IS NULL` parked shape unreachable, stranding the replay
  surface; redrive now targets exhausted `Failed` rows, moving them back to
  `Pending` for one reprocessing pass while preserving attempts and error
  history. Test: `replay_received_redrives_failed_rows_without_replaying_processed_rows`.
- 2026-06-22: Added the Phase 3 DLQ inspect surface: `received_failed_rows`
  (oldest-first, `limit`-bounded list of terminal `Failed` rows with attempts and
  error history intact) and `received_failed_count`. Operators inspect the
  terminal backlog before redriving with `Replay::received`. Test:
  `received_failed_rows_inspect_surface_lists_terminal_dlq_rows`.
- 2026-06-22: Completed Phase 3 and Phase 4. Added `ReceivedFailureFilter`
  (business `since` + most-recent failure `kind`) narrowing on the inspect
  surface, `Replay::failure_kind` to scope redrive by kind, and
  `Replay::clear_history` to redrive terminal rows to a clean slate (attempts
  reset, errors cleared) while the default redrive preserves forensics. Phase 4
  added no Redpanda coverage by design: retry/backoff/DLQ is Postgres-side and
  broker behavior cannot change retry outcomes. Promoted validated behavior into
  `wiki/specs/m4-retry-backoff-dlq.spec.md` and marked this plan completed.
  Tests: `received_failed_filter_narrows_by_kind_and_since`,
  `replay_received_redrive_filters_by_kind_and_clears_history_on_request`. Proof:
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`;
  `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
  (26 passed).

## Verification Gates

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

## Closure Criteria

M4 closes when retry/backoff scheduling, terminal table-backed DLQ state, and
redrive/admin recovery behavior are implemented, tested, and promoted into an
active reliability spec.
