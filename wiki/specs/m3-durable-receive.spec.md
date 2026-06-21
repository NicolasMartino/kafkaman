# M3 Durable Receive

Document Class: Spec
Status: Active
Date: 2026-06-22
Category: Durable receive
Scope: Validated M3 behavior for Kafka ingest, received-message storage, dispatch, retry replay, and atomic consume-then-produce.
Sources:
- wiki/plans/m3-durable-completion.plan.md
- wiki/plans/m3-durable-receive.plan.md
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/decisions/receive-handler-surface-scope.decision.md
- wiki/decisions/kafka-ingest-identity-and-ordering.decision.md
- wiki/decisions/ingest-poison-quarantine-policy.decision.md
- wiki/decisions/missing-handler-dispatch-policy.decision.md
- wiki/decisions/dispatch-infrastructure-error-classification.decision.md
- wiki/decisions/dispatch-stats-semantics.decision.md
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/proposals/05-deep-durability-testing.proposal.md
- tests/durable-send/tests/durable_receive.rs
- tests/durable-send/tests/redpanda_full_loop.rs
Related:
- wiki/specs/m1-durable-send.spec.md
- wiki/specs/m2-change-engine-config.spec.md
- wiki/roadmaps/path-to-v1.roadmap.md

## Validated Behavior

M3 adds the durable receive half of kafkaman's Postgres-backed Kafka runtime. The validated runtime uses two schedulers:

1. Kafka ingest consumes broker records, writes a durable received row or durable quarantine row in Postgres, then commits the Kafka offset.
2. Receive dispatch claims due received rows, executes the registered handler inside a Postgres transaction, and marks the row processed or records retryable failure state.

Received storage is per message type. Migrated schemas include per-type received tables with required `idempotency_key` identity, `message_id`, payload, metadata, status, attempts, `next_attempt_at`, processed timestamp, and bounded JSONB error history. Fresh schemas also include shared `received_ingest_failures` quarantine storage for ingest records that cannot safely become received rows.

Deduplication is based on the required business `idempotency_key`. `insert_received_with_outcome` distinguishes `Inserted`, `DuplicateIdempotencyKey`, and `MessageIdConflict`. A duplicate idempotency key is a normal redelivery outcome; a `message_id` conflict with a different idempotency key is an identity anomaly and is quarantined rather than collapsed into duplicate accounting.

Kafka ingest parses kafkaman metadata headers, requires `kafkaman-idempotency-key`, accepts only the expected topic for the message type, and copies user headers outside the reserved `kafkaman-*` namespace. The durable write is ordered before offset commit. If the process crashes after the durable write and before offset commit, redelivery deduplicates against the existing row.

Malformed payloads, unexpected source topics, schema/deploy-skew deserialize failures, and message-id conflicts are committed past only after an idempotent quarantine row is written to `received_ingest_failures`. Quarantine identity is `(source_topic, source_partition, source_offset)`. Ingest has a consecutive-skip circuit breaker: the default limit is 10 skipped records, configurable through `RdkafkaConsumer::with_max_consecutive_skips`.

The production ingest loop is `RdkafkaConsumer::run_ingester`. It accumulates `IngestLoopStats`, retries transient errors, honors cancellation, and stops loudly on `ConsecutiveSkipLimitExceeded`. Test hooks prove the crash window between durable write and offset commit without changing production ordering.

Receive dispatch uses `dispatch_once` and `run_dispatcher`. Claimed rows are selected from due `Pending` / `Retryable` work with row locks and `SKIP LOCKED`, so one locked or failed row does not permanently block younger due rows. The dispatcher loop drains immediately while work is claimed, sleeps only after an empty cycle, continues after per-cycle failures, and finishes an in-flight dispatch before shutdown.

`MessageRouter::handler::<P>` accepts the M3 handler surface:

```rust
Fn(&mut PgConnection, ReceivedMeta, P) -> HandlerFuture
```

Handlers receive the transaction-bound Postgres connection, message metadata, and the decoded payload. `enqueue_on_connection` lets a handler enqueue a send-side outbox message on the same receive transaction, proving atomic consume-then-produce: handler business writes, follow-up outbox enqueue, and receive row completion commit or roll back together.

Receive failures are durable row facts. `MissingHandler` is recorded as a per-row retryable failure under the claimed row lock, not a scheduler-fatal error. Handler failures run behind a dispatch savepoint; on failure kafkaman rolls handler effects back to the savepoint, records the retryable failure while still holding the row lock, and commits the failure row. Corrupted stored payloads are `InvalidPayload`. Handler-domain SQL and business failures are recorded as `Handler`. Infrastructure failures after a handler returns `Ok`, such as an unusable transaction while marking processed, are recorded as `Infrastructure`; if the transaction connection is already unusable during savepoint rollback, failure accounting falls back to a separate pool connection.

`DispatchStats.failed` counts durable failure records. Normal handler and missing-handler failure accounting is single-flight because the claimed row lock is held until the retryable failure update commits; a second dispatcher skips the locked row with `SKIP LOCKED`.

`Replay::received` is an operational changeset for terminal receive failures. Once retry backoff is in effect (M4), a receive row reaches `Failed` only after its retry budget is exhausted; non-exhausted failures stay `Retryable` with a scheduled `next_attempt_at` and recover on their own. `Replay::received` therefore targets bounded `Failed` rows, changes them back to `Pending` for one more reprocessing pass, clears `next_attempt_at` and `processed_at`, and preserves attempts and error history for triage.

## Evidence

Postgres integration coverage includes:

- `receive_table_uses_required_idempotency_key`
- `dispatch_once_commits_handler_effect_and_processed_status`
- `dispatch_exposes_message_metadata_to_handler`
- `dispatch_failure_rolls_back_effect_and_parks_retryable`
- `missing_handler_is_recorded_and_does_not_block_younger_rows`
- `poisoned_handler_transaction_is_recorded_as_dispatch_failure`
- `rollback_failure_still_records_infrastructure_dispatch_failure`
- `corrupted_received_payload_is_recorded_as_invalid_payload_failure`
- `handler_sql_constraint_error_is_recorded_as_handler_failure`
- `failure_recording_holds_row_lock_until_retryable_commit`
- `duplicate_received_rows_converge_to_one_dispatch_effect`
- `random_redeliveries_converge_to_one_effect_per_idempotency_key`
- `handler_enqueues_outbox_atomically_with_receive_transaction`
- `dispatcher_loop_processes_due_rows_and_stops_on_cancellation`
- `dispatcher_loop_finishes_in_flight_dispatch_before_shutdown`
- `replay_received_redrives_failed_rows_without_replaying_processed_rows`

Redpanda full-loop coverage includes:

- `full_loop_ingests_from_redpanda_and_dispatches_received_row`
- `full_loop_consume_then_produce_deduplicates_duplicate_input`
- `ingest_deduplicates_redelivery_after_crash_before_offset_commit`
- `run_ingester_deduplicates_redelivery_after_offset_commit_uncertainty`
- `ingest_skips_poison_record_then_deduplicates_redelivery`
- `ingest_circuit_breaker_stops_committing_repeated_schema_failures`
- `run_ingester_processes_records_until_cancelled`
- `run_ingester_stops_loudly_on_consecutive_schema_failures`
- `ingest_skips_records_from_unexpected_source_topic`

Recorded verification commands:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

## Limitations

M3 does not claim exactly-once external side effects. It makes broker redelivery and receive dispatch effective-once for effects committed through the handler transaction and kafkaman outbox helpers.

The accepted M3 handler surface is the minimal closure-based surface. Larger Tower, `FromMessage`, state extractor, and layer ergonomics remain deferred.

Runtime retry/backoff/DLQ terminal policy is M4 scope. The M4 first slice adds policy-driven backoff scheduling (`Retryable` rows carry a computed `next_attempt_at`), exhaustion to terminal `Failed`, bounded error history, and `Replay::received` redrive of `Failed` rows. DLQ retention, purge, richer admin APIs, and operator dashboards remain later work.

Kafka is the durable receive transport in M3. HTTP synchronous outcomes and non-Kafka direct receive modes remain deferred.

The deep-durability proposal still tracks post-M3 hardening candidates, including database-backed property/model tests, ambiguous offset commit chaos, broader cancellation variants, timestamp precision oracles, and send-side mirror chaos tests.
