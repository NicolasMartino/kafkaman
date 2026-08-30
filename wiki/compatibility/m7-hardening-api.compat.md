# M7 Hardening API Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-31
- Category: Public API and operational surface
- Scope: Public API, runtime behavior, and documentation changes introduced during the M7 V1 hardening milestone.
- Sources:
  - wiki/plans/m7-v1-hardening.plan.md
  - wiki/decisions/outbox-retention-policy.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - crates/kafkaman/src/runtime/tasks.rs
  - crates/kafkaman/src/runtime/error.rs
  - crates/kafkaman/src/runtime/builder.rs
  - crates/kafkaman/src/runtime/subsystems.rs
  - crates/kafkaman/src/axum.rs
  - crates/kafkaman-core/src/idempotency.rs
  - crates/kafkaman-core/src/status.rs
  - crates/kafkaman-sqlx/src/generated_changelog.rs
  - crates/kafkaman-sqlx/src/operability.rs
  - crates/kafkaman-sqlx/src/queries.rs
  - crates/kafkaman-sqlx/src/replay.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman-rdkafka/src/topics.rs
  - crates/kafkaman-worker/src/relay.rs
  - examples/product/src/bin/worker.rs
  - tests/distributed-cache/tests/boot_surface.rs
  - tests/distributed-cache/tests/runtime_builder.rs
  - tests/durable-send/tests/redpanda_full_loop/ingest_dedup.rs
  - tests/durable-send/tests/redpanda_full_loop/retry_dlq.rs
  - kafkaman.example.toml
  - README.md
  - examples/README.md
- Related:
  - wiki/compatibility/m6-observability-operability-api.compat.md
  - wiki/compatibility/m5-outbox-retention.compat.md
  - wiki/specs/v1-acceptance.spec.md

## Phase 1: Ingest Quarantine Summary

M7 adds a read-only summary for the schema-wide ingest quarantine table. This is
visibility, not retention. Quarantine rows are records the ingester could not
turn into received rows, so deleting them by age would delete the only durable
diagnosis for poison records, unexpected topics, missing idempotency keys, and
message-id conflicts.

`kafkaman-sqlx` adds:

- `ReceivedIngestFailureSummary`
- `received_ingest_failure_summary(pool, cfg, now)`

The summary groups rows by `message_type`, `expected_topic`, and
`ReceivedIngestFailureKind`, and returns the bucket count plus the oldest row's
created time and age. It deliberately does not return quarantined `payload`,
`headers`, `key`, or free-text `error`, because the common operator need for M7
is growth visibility and failure class, not payload inspection through an
unauthenticated route.

`kafkaman-axum::admin_router` adds:

- `GET /ingest-failures`

The route is read-only and remains separate from `redrive_router`. It is still
unauthenticated like the other admin routes, so adopters must mount it on an
internal listener or behind their own auth middleware.

## Retention Semantics

No new purge behavior is introduced.

- Outbox rows remain the only kafkaman rows with an implemented purge loop.
- Received rows are still retained because they are the processed-marker and
  dedupe ledger.
- Cache rows are still retained because they are state.
- Ingest quarantine rows are still retained because they are the only durable
  diagnosis for records skipped before a received row existed.

Any future quarantine purge would need its own explicit operator opt-in, bounded
batching, and compatibility note. M7 does not guess that policy.

## Phase 2: Facade Runtime Supervision

M7 hardens `kafkaman::RuntimeTasks`, `Runtime::run`, and the facade
`kafkaman::axum` composition helpers.

New public surface:

- `kafkaman::DEFAULT_DRAIN_TIMEOUT`
- `kafkaman::runtime::DEFAULT_DRAIN_TIMEOUT`
- `RuntimeTasks::shutdown_with_timeout(Duration)`
- `kafkaman::axum::RunningService::shutdown_with_timeout(Duration)`
- `RuntimeError::LoopExited { loop_name, message }`
- `RuntimeError::DrainTimeout { timeout, remaining }`

Behavior changes:

- `RuntimeTasks::shutdown()` now uses the 30-second default drain bound instead
  of waiting forever.
- `RuntimeTasks::wait()` treats a clean loop completion before shutdown as a
  supervision error. A relay or dispatcher that ends while the process is still
  serving is not healthy.
- `RuntimeTasks::shutdown()` also reports a loop that had already completed
  before shutdown was requested.
- A loop error observed during drain outranks a later drain timeout from another
  wedged task.
- Panics keep the named loop in `RuntimeError::Panicked { loop_name, source }`.
- Builder-created task names now include the role and message type:
  `relay:{message_type}`, `purger:{message_type}`, `ingester:{message_type}`,
  and `dispatcher:{message_type}`. The queue sampler remains `queue-metrics` and
  the facade HTTP task remains `http`.

Compatibility impact:

- `RuntimeError::Loop.loop_name` changed from `&'static str` to `String`.
- `RuntimeError::Panicked` changed from a tuple variant to a named-field variant.
- Callers matching `RuntimeError` exhaustively must update those patterns.
- Code that directly called `RuntimeTasks::shutdown()` after a loop had already
  completed may now receive `LoopExited` instead of `Ok(())`.

The recommended manual supervision pattern is:

```rust
let mut tasks = runtime.into_tasks()?;
let first = tasks.wait().await;
let drained = tasks.shutdown().await;
first.and(drained)?;
```

`Runtime::run(shutdown)` and `RunningService::run()` already use this pattern.

## Phase 3: Worker-Role Subsystem Selection

M7 adds explicit runtime-topology selection for host-owned worker binaries. The
role declaration remains the source of truth for schema, topics, table handles,
and handlers; subsystem selection only decides which background loops this
process starts after `build()`.

New public surface:

- `kafkaman::Subsystems`
- `kafkaman::runtime::Subsystems`
- `RuntimeBuilder::subsystems(Subsystems)`

`Subsystems` supports bitwise composition and names these flags:

- `Subsystems::RELAY`
- `Subsystems::INGEST`
- `Subsystems::DISPATCH`
- `Subsystems::PURGE`
- `Subsystems::QUEUE_METRICS`
- `Subsystems::PIPELINE`

It also exposes `empty()`, `all()`, `pipeline()`, `contains`, `intersects`, and
`without`.

Behavior:

- The default remains `Subsystems::all()`, preserving the pre-M7 builder shape.
- `PURGE` still starts nothing unless `[retention]` is configured.
- `PIPELINE` is the no-purge worker preset: relay, ingest, dispatch, and queue
  metrics.
- A consumed role requires `RuntimeBuilder::consumer_group(...)` only when
  `INGEST` is selected. Dispatch-only and migration-only workers do not join
  Kafka and do not need a group.
- Queue metrics remain behind the `metrics` feature and start only when
  `QUEUE_METRICS` is selected and the runtime has at least one outbox or
  received table.

`examples/product` now ships `product-worker`, a no-HTTP binary that uses the
facade builder and `.subsystems(Subsystems::PIPELINE)` to run relay, ingest, and
dispatch without binding a listener or assembling an Axum router.

Compatibility impact:

- Additive for normal adopters. Existing `RuntimeBuilder::new()` call sites keep
  the same loop behavior.
- Code that depended on a missing consumer group being reported for every
  consumed role can now see the next missing input instead when ingest is
  explicitly disabled.
- Partial selections are operational, not simulated: relay-only can drain
  outbox rows, ingest-only can fill received rows, and dispatch-only can drain
  already-ingested rows. Omitting one side can intentionally create backlog.

## Phase 4: Full-Loop Acceptance Evidence

Phase 4 adds no public API, schema, config, or route surface. It tightens the V1
acceptance evidence for behavior that already existed but was not proven at the
broker-backed altitude.

New Redpanda/Postgres tests:

- `ack_before_mark_republish_is_deduplicated_after_real_broker_hop` simulates a
  relay crash after broker ack and before outbox `Published` marking, expires the
  claim lease, republishes through a real broker, ingests both broker records,
  and proves the duplicate does not rewrite the durable received row or run the
  handler twice.
- `redpanda_input_exhausts_dlq_and_redrives_to_success` starts from real broker
  input, ingests to the received table, exhausts the dispatch retry budget,
  parks the row in the DLQ, redrives it, preserves source offset and failure
  history, and then processes successfully.

Quality-gate cleanup:

- A public doc comment in `kafkaman-core` no longer links to a private macro
  module, so `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features
  --no-deps` stays clean.
- The facade Axum external-shutdown test now waits for the listener before
  cancelling, removing startup scheduling from the assertion.

## Phase 5: Docs And V1 Acceptance Closeout

Phase 5 adds no public API, schema, config, route, metric, or span surface.

The compatibility impact is documentary:

- `wiki/specs/v1-acceptance.spec.md` records the accepted V1 envelope as the
  composition of M1-M7 behavior and evidence.
- `README.md` no longer describes M7 as the active implementation milestone.
- `examples/README.md` aligns the failure-scenario count with the seven
  asserted scenarios and distinguishes compose smoke coverage from the heavier
  Redpanda/Postgres testcontainers acceptance tier.
- `wiki/roadmaps/path-to-v1.roadmap.md` marks M7 complete and records that the
  remaining action is release tagging, not another M7 implementation phase.

## Post-Review Remediation

The 2026-08-31 M7 hardening review closed before the V1 tag and produced these
compatibility-relevant changes:

- `ReceiveStatus::Processing` was removed. `RECEIVED_TEMPLATE_VERSION` is now
  `1`, and generated changelogs include `RemoveReceivedProcessingStatus`, which
  rewrites any legacy `Processing` row to due `Pending` before tightening the
  received-table CHECK constraint.
- `Replay::outbox::<T>()` remains a deliberate tombstone returning
  `Err(Error::UnsafeOutboxReplay)`, but no longer accepts a discarded version
  argument.
- `Subsystems::non_destructive()` was removed before V1; use
  `Subsystems::PIPELINE` or `Subsystems::pipeline()`.
- ~~The older direct `kafkaman-axum` supervision API is deprecated.~~ It was
  deleted outright before the V1 tag rather than shipped deprecated — see
  [v1-legacy-removal](v1-legacy-removal.compat.md). Through the facade,
  `kafkaman::axum::RuntimeError` and `kafkaman::axum::DEFAULT_DRAIN_TIMEOUT`
  resolve to the canonical runtime API, and now do so because it is the only
  one rather than because it shadows another.
- Facade `BuildError` and `RuntimeError` now implement `ProblemType`, so
  boot-time failures carry the same APM classification vocabulary as running
  loop failures.
- Derived idempotency keys now use explicit recursively sorted canonical JSON
  bytes instead of relying on serde_json's map ordering. The pinned digest is
  unchanged.
- DLQ inspection queries emit `db.query` spans.
- The relay loop continues immediately after non-empty batches instead of
  sleeping one poll interval per batch.
- `[topics] mode = "create"` re-verifies a newly created topic before boot
  continues.
- `product-worker` defaults to the same `product-service` Kafka consumer group
  as the HTTP binary.
