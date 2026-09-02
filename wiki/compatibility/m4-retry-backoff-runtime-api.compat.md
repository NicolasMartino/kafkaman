# M4 Retry Backoff Runtime API Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-06-22
- Category: Receive retry API and behavior compatibility
- Scope: Records public API and runtime behavior changes from the first M4 retry/backoff/DLQ implementation slice.
- Sources:
  - wiki/plans/m4-retry-backoff-dlq.plan.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - crates/kafkaman-config/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - tests/durable-send/tests/durable_receive/

## Public API Changes

- `kafkaman_config::RetryConfig` and `RetryPolicy` now implement `Default`
  using the accepted V1 defaults: max attempts 10, initial backoff 1s, max
  backoff 5m, multiplier 2.0, errors limit 20, table DLQ.
- `kafkaman_sqlx::ResolvedConfig` now stores the resolved `RetryConfig`.
  Direct struct construction must provide the new field; constructor-style use
  through `ResolvedConfig::new` or `ResolvedConfig::from_config` is unchanged.
- `kafkaman_sqlx::ReceivedTable` now stores the resolved per-message
  `RetryPolicy`. Direct struct construction must provide the new field;
  `ReceivedTable::new` uses default retry policy, and `ReceivedTable::for_message`
  uses the configured policy for that message type.
- New DLQ inspect surface: `kafkaman_sqlx::received_failed_rows` and
  `received_failed_count`, both taking a `&ReceivedFailureFilter`. The new
  `ReceivedFailureFilter` (`{ occurred_after, kind }`, with `since`/`kind`
  builders) narrows by business time and most-recent failure kind.
- `kafkaman_sqlx::Replay` gains `failure_kind(ReceivedFailureKind)` and
  `clear_history()` builders for received redrive. `Replay::received` now targets
  terminal `Failed` rows (was the M3 `Retryable` + null-`next_attempt_at` parked
  shape, which retry backoff made unreachable). The redrive options are part of
  the changeset checksum.

## Runtime Behavior Changes

- Receive dispatch failures no longer park retryable rows with
  `next_attempt_at = NULL` by default. They compute a due time from the retry
  policy and dispatch only when due.
- Exhausted receive rows transition to terminal `Failed` status instead of
  remaining indefinitely `Retryable`.
- Receive error history retention now honors the configured `errors_limit`
  rather than the previous hard-coded 20-entry cap.

## Deferred

- Kafka DLQ topics remain deferred; V1 DLQ truth is table-backed.
- Backoff jitter, terminal-row retention/purge, and operator dashboards remain
  later milestones.
