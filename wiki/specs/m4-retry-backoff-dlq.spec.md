# M4 Retry / Backoff / DLQ

Document Class: Spec
Status: Active
Date: 2026-06-22
Category: Reliability
Scope: Validated M4 behavior for receive-side retry scheduling, exponential
  backoff, terminal table-backed DLQ state, the DLQ inspect surface, and guarded
  redrive of terminal failures.
Sources:
- wiki/plans/m4-retry-backoff-dlq.plan.md
- wiki/decisions/retry-backoff-dlq-policy.decision.md
- wiki/compatibility/m4-retry-backoff-runtime-api.compat.md
- crates/kafkaman-config/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- tests/durable-send/tests/durable_receive.rs
Related:
- wiki/specs/m3-durable-receive.spec.md
- wiki/roadmaps/path-to-v1.roadmap.md

## Validated Behavior

M4 turns M3's durable receive failures into policy-driven operational
convergence. A failed dispatch is either retried after a configured backoff or,
once its attempt budget is exhausted, moved to a terminal table-backed DLQ state
that operators inspect and redrive.

### Retry policy resolution

Retry policy is resolved on the config boot path. `RetryConfig` carries a default
`RetryPolicy` plus per-message-type overrides. When `kafkaman.toml` declares a
`[retry]` section, defaults and overrides are validated against the registered
message types; an override for an unregistered type is a boot error. When no
`[retry]` section is present, a built-in default policy applies: `max_attempts =
10`, `initial_backoff = 1s`, `max_backoff = 300s`, `multiplier = 2.0`,
`errors_limit = 20`, `dlq = Table`. Each `ReceivedTable` carries the policy
resolved for its message type, so failure accounting is per-type.

### Backoff scheduling and due-gating

Receive dispatch failure accounting computes the next attempt time from the
injected dispatch time and an exponential schedule:
`next_attempt_at = dispatch_time + initial_backoff * multiplier^attempts`, capped
at `max_backoff` (the first retry uses `initial_backoff`). A failed-but-not-yet-
exhausted row is recorded as `Retryable` with that scheduled `next_attempt_at`.
The dispatcher claim query gates on the schedule: `Retryable` rows are claimed
only when `next_attempt_at <= now`, and `Pending` rows are claimed when
`next_attempt_at` is null or due, so a parked retry is not redispatched before it
is due.

### Terminal table-backed DLQ

When the next attempt would reach `max_attempts`, the row is moved to terminal
`Failed` with `next_attempt_at = NULL` instead of being rescheduled. Postgres is
the DLQ source of truth in V1; there is no Kafka DLQ topic. The stored `errors`
array is bounded to the policy's `errors_limit`, keeping the most recent entries
so terminal rows retain forensic history without unbounded growth.

### DLQ inspect surface

`received_failed_count` and `received_failed_rows` expose the terminal backlog.
`received_failed_rows` lists `Failed` rows oldest-first by `created_at`, bounded
by a caller `limit`, with attempts and bounded error history intact. A
`ReceivedFailureFilter` narrows both by business `occurred_after` and by the most
recent failure `kind` (matched against the last element of the stored `errors`
array), so an operator can scope a view to, for example, recent `Handler`
failures.

### Guarded redrive

`Replay::received` is the guarded redrive surface for terminal failures. It is an
M2 operational changeset (versioned, context-gated, idempotent, dry-run-aware)
that targets `Failed` rows and moves them back to `Pending`, clearing
`next_attempt_at` and `processed_at`. By default it preserves `attempts` and the
`errors` history so a redriven row carries its forensic trail. `failure_kind`
narrows the redrive to rows whose most recent failure was of a given kind.
`clear_history` opts into a clean slate: it resets `attempts` to zero and clears
the `errors` array so the row redrives with a full retry budget. The redrive
options participate in the changeset checksum, so a changed filter or clear-flag
is detected as changeset drift rather than silently skipped.

## Evidence

Postgres integration coverage includes:

- `retryable_failure_schedules_backoff_and_due_dispatch`
- `max_attempts_moves_received_row_to_failed_with_bounded_errors`
- `received_failure_errors_keep_most_recent_twenty_entries`
- `replay_received_redrives_failed_rows_without_replaying_processed_rows`
- `received_failed_rows_inspect_surface_lists_terminal_dlq_rows`
- `received_failed_filter_narrows_by_kind_and_since`
- `replay_received_redrive_filters_by_kind_and_clears_history_on_request`

Recorded verification commands:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

## Limitations

Retry/backoff/DLQ is entirely Postgres-side. Broker behavior cannot change retry
outcomes, so M4 adds no new Redpanda full-loop coverage; the M3 ingest/dispatch
full-loop gates remain the broker-level proof.

The backoff schedule is deterministic with no jitter, so many rows that fail
together retry in lockstep against a recovering downstream. Jitter is later work.

Kafka DLQ topics, retention/purge of terminal rows, richer admin/redrive APIs,
and operator dashboards are out of scope for M4. Send-side retry policy is not
unified with receive retry. These remain later milestones (M6+).
