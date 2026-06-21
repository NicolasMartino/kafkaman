# M3 Durable Receive Review-Fix API Compatibility

Document Class: Compatibility Note
Status: Active
Date: 2026-06-22
Category: Receive API and operational data compatibility
Scope: Records public API and persisted JSON changes made while resolving the M3 durable-completion review findings.
Sources: wiki/plans/m3-durable-completion.plan.md; wiki/reviews/m3-durable-completion-implementation-review.reference.md

## Summary

The 2026-06-22 review-fix slice changes receive-side API shape and operational
JSON data but does not require table DDL changes.

## Public API Changes

- `kafkaman-rdkafka` now has a `test-hooks` feature for integration tests.
  When enabled, it exposes `IngestCommitContext`,
  `RdkafkaConsumer::with_post_durable_write_hook`, and
  `Error::TestHook` so tests can inject a failure after the durable receive
  write commits but before Kafka offset commit.
- `kafkaman-sqlx` now has a `test-hooks` feature for integration tests. When
  enabled, it exposes `DispatchTestHooks`, `DispatchFailureHookContext`, and
  `dispatch_once_with_hooks` so tests can pause before handler-failure savepoint
  rollback fallback and before locked-row failure accounting is recorded.
- `kafkaman_rdkafka::IngestLoopStats` is new. It aggregates completed ingest
  loop cycles, consumed/inserted/duplicate/skipped/committed counts, and
  transient retryable loop errors.
- `RdkafkaConsumer::run_ingester::<P>(&PgPool, &ResolvedConfig, Duration,
  CancellationToken)` is new. It runs `ingest_once` until cancellation, retries
  transient Kafka/SQL errors after the configured delay, and returns
  `ConsecutiveSkipLimitExceeded` instead of retrying a tripped poison breaker.

- `kafkaman_rdkafka::IngestStats` now has `skipped: usize`. Code constructing
  the struct directly must add the field. Consumers can use it to distinguish
  deterministic broker-record skips from inserted or duplicate durable rows.
- `kafkaman_rdkafka::Error` now includes `UnexpectedTopic { expected, actual }`
  for records consumed by `ingest_once::<P>` from a topic other than `P::TOPIC`.
  `ingest_once` classifies this as a deterministic skip and commits the offset.
- `kafkaman_core::ReceivedError` now has `kind: ReceivedFailureKind`. Existing
  serialized error entries remain readable because the field defaults to
  `ReceivedFailureKind::Handler` when absent.

## Operational Behavior Changes

- Deterministic ingest poison records now commit their Kafka offset and return a
  skipped ingest result. Transient SQL insert/commit failures still do not commit
  the offset.
- `insert_received` treats any uniqueness conflict as a duplicate/conflict by
  using untargeted `ON CONFLICT DO NOTHING`; this includes reused message ids
  with different idempotency keys.
- `Replay::received` now unparks parked retryable rows only
  (`Retryable / next_attempt_at IS NULL`) and no longer resets `Processed` rows.
- Receive failure entries now carry structured operator-facing causes:
  `MissingHandler`, `InvalidPayload`, `Infrastructure`, or `Handler`.

## 2026-06-22 Quarantine Policy Follow-up

Additional API and persistence changes were made for the ingest poison
quarantine policy:

- `kafkaman_rdkafka::Error` now includes
  `ConsecutiveSkipLimitExceeded { limit, partition, offset }`.
- `RdkafkaConsumer::with_max_consecutive_skips(usize)` configures the breaker
  threshold. The default threshold is 10 consecutive skipped records.
- `kafkaman_core::ReceivedIngestFailureKind` identifies durable ingest
  quarantine causes.
- `kafkaman_sqlx::ReceivedInsertOutcome` and
  `insert_received_with_outcome` distinguish `Inserted`,
  `DuplicateIdempotencyKey`, and `MessageIdConflict`.
- `kafkaman_sqlx::ReceivedIngestFailure` /
  `ReceivedIngestFailureRow`, `insert_received_ingest_failure`, and
  `received_ingest_failure_by_source` expose the quarantine ledger.
- Fresh and migrated schemas get a shared
  `<schema>.received_ingest_failures` table via migration bootstrap. The table
  is keyed by `(source_topic, source_partition, source_offset)` and stores raw
  payload bytes when present, headers, expected message identity, failure kind,
  and error text.

Operational behavior changes:

- The rdkafka ingest runner returns aggregate stats on graceful cancellation,
  backs off and retries transient loop errors, and stops loudly on
  `ConsecutiveSkipLimitExceeded` with the breaker offset still uncommitted.
- Deterministic skipped records are committed only after their quarantine row is
  durable.
- Repeated deserialize/schema failures trip the consecutive-skip breaker; the
  breaker row is quarantined but its Kafka offset is not committed.
- `message_id` conflicts with a different idempotency key are quarantined as
  `MessageIdConflict` instead of being reported as normal duplicates.
- `Replay::received` preserves retry attempts and error history when it unparks
  retryable rows.
- Handler-returned SQL errors are classified as handler failures; SQL errors
  from the dispatch mark path remain infrastructure failures.
