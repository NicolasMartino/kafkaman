# Wiki Index

Project: kafkaman
Stage: M6 observability completed; M7 hardening active; runtime-builder examples
shipped; example OpenTelemetry wiring active
Updated: 2026-08-27

One-line: A Rust library plus optional worker runtime for Kafka-backed
distributed caches of compact domain entity snapshots, using Postgres as the
durable entity propagation ledger and local cache store.

## Specs

- [specs/m1-durable-send.spec.md](specs/m1-durable-send.spec.md) - Validated M1
  durable-send behavior: transactional enqueue, per-type outbox DDL, minimal
  migrate, claim-lease relay, publisher boundary, Harness seed, and Axum example.
  Status: Active.
- [specs/m2-change-engine-config.spec.md](specs/m2-change-engine-config.spec.md) -
  Validated M2 behavior: `kafkaman.toml` loader, fail-fast resolved config,
  migration reports, checksums, `applied_by`, `changelog!`, dry-run, and guarded
  send-side `Replay`. Status: Active.
- [specs/m4-retry-backoff-dlq.spec.md](specs/m4-retry-backoff-dlq.spec.md) -
  Validated M4 reliability behavior: per-type retry policy resolution, jittered
  exponential backoff with due-gated dispatch, terminal table-backed DLQ on exhaustion,
  bounded error history, the `received_failed_rows`/`received_failed_count` DLQ
  inspect surface with `ReceivedFailureFilter`, and guarded `Replay::received`
  redrive (kind filter, history-preserving by default, opt-in `clear_history`).
  Status: Active.
- [specs/entity-first-propagation.spec.md](specs/entity-first-propagation.spec.md)
  - Validated M5 entity-cache behavior: required `entity_key`, per-type cache
  tables, received-row entity identity, offset-guarded cache upsert,
  per-entity outbox supersede and claim-time pending collapse,
  `Replay::outbox` rejection, wire-carried producer metadata, jittered retry,
  and opt-in outbox retention. Status: Active.
- [specs/m3-durable-receive.spec.md](specs/m3-durable-receive.spec.md) -
  Validated M3 durable receive behavior: Kafka ingest writes a durable received
  row or quarantine row before committing offsets, idempotency-key dedup,
  message-id conflict quarantine, dispatcher row claiming, locked-row receive
  failure accounting with handler savepoints, `Replay::received`, production
  ingest/dispatcher loops, and atomic consume-then-produce through `ReceivedMeta` plus
  `enqueue_on_connection`. Status: Active.
- [specs/m6-observability-operability.spec.md](specs/m6-observability-operability.spec.md)
  - Validated M6 observability/operability behavior: runtime observability
  config with per-message overrides (three fields reserved, four live), direct
  OpenTelemetry metrics behind a default-on `metrics` feature, index-served SQL
  queue depth/age/stuck inspection, sanitized Axum admin/health routes with
  bounded destructive input, correlation middleware, draining runtime
  supervision, and descriptor-driven received DLQ redrive. Includes the
  pre-merge review remediation. Status: Active.

## Reviews

- [reviews/m6-opentelemetry-readiness-review.reference.md](reviews/m6-opentelemetry-readiness-review.reference.md)
  - External readiness review of the post-M6 OpenTelemetry state, scored 6.5/10.
  All twelve line citations verified accurate; all five gaps confirmed. Records
  where the review understates (metrics are unconditionally inert, not
  conditionally), where it is generous (credits same-day decision pages as
  practice), and the two findings adopted into the plan — logs promoted to a
  named signal, and example SDK wiring moved from last to Phase 0. Status: Sourced.
- [reviews/m1-durable-send-implementation-review.reference.md](reviews/m1-durable-send-implementation-review.reference.md)
  - Sourced verification of the post-implementation M1 durable-send review,
  confirming the main gaps around status centralization, clock ownership,
  Redpanda/full-loop scope, idempotency durability, worker resilience, and
  migration concurrency. Status: Sourced.
- [reviews/m1-durable-send-implementation-rereview.reference.md](reviews/m1-durable-send-implementation-rereview.reference.md)
  - Fresh re-review after attempted fixes. Confirms several fixes, but identifies
  the missing idempotency schema-upgrade migration as a blocker, with Redpanda
  full-loop scope, Harness concurrency, strict clippy, and reserved Kafka header
  handling still open. Status: Sourced.
- [reviews/m2-change-engine-config-implementation-review.reference.md](reviews/m2-change-engine-config-implementation-review.reference.md)
  - Line-by-line review of the M2 change-engine + config implementation. Confirms
  the engine and config loader are largely delivered, but flags retry-config
  validation never being wired into any boot path, the checksum being FNV-1a (not
  the specified SHA-256), a non-side-effect-free dry-run, and stale `attempts` on
  replay. Status: Sourced.
- [reviews/m3-durable-receive-implementation-review.reference.md](reviews/m3-durable-receive-implementation-review.reference.md)
  - Review of the first M3 durable-receive slice. Follow-up implementation
  resolves the stale failure-accounting race, the Harness send/receive
  registration conflict, the missing stale-failure/crash/randomized-redelivery/
  bounded-error-ring/transient-processing-write/missing-received-row gates, and
  the handler metadata-access (`ReceivedMeta`) and `correlation_id` nullability
  findings. All H/M/L findings are closed; only out-of-scope new M3 surface area
  (`FromMessage`/`Rx`/Tower, `Replay::received`, injected clock, macros, Kafka
  ingest, full-loop) remains, tracked in the plan. Status: Resolved.
- [reviews/m3-durable-completion-implementation-review.reference.md](reviews/m3-durable-completion-implementation-review.reference.md)
  - Adversarial review of the M3 durable-completion slice (ingest loop,
  dispatcher loop, `Replay::received`). Original F1-F9 findings are closed by
  the 2026-06-22 follow-up implementation: deterministic ingest skips with
  offset commit, receive insert conflict hardening, dispatcher drain and
  mid-dispatch shutdown tests, retryable-only `Replay::received`, structured
  receive failure causes, and Redpanda poison/redelivery/topic-provenance
  coverage. Broader M3 closure gates remain in the active plan. Status: Sourced.
- [reviews/m3-durable-completion-implementation-rereview.reference.md](reviews/m3-durable-completion-implementation-rereview.reference.md)
  - Second pass after the review-fix slice. Confirms F1-F5 closed and test-pinned,
  but finds the fixes traded partition stalls for silent drops: deterministic
  ingest skip loses data with no durable trace (G1), schema skew is treated as
  poison and silently dropped topic-wide (G2), untargeted `ON CONFLICT` silently
  drops a different logical message on `message_id` collision (G3), failure `kind`
  is derived from the error variant not the failure domain (G4), and replay erases
  the error history of the rows it targets (G5). Follow-up implementation adds
  durable ingest quarantine, a consecutive-skip circuit breaker, explicit
  message-id conflict outcome/quarantine, handler-domain SQL classification, and
  replay history preservation. Status: Sourced.

- [reviews/m3-m4-pre-merge-branch-review.reference.md](reviews/m3-m4-pre-merge-branch-review.reference.md)
  - Adversarial pre-merge review of branch `implementation/m3-durable-receive`
    against local `main`. Finds two high-severity idempotency-contract gaps
    (send-side optional idempotency vs receive-side required header, and accepted
    empty idempotency keys), medium DLQ/redrive time-semantics mismatches, and a
    low-severity outbox replay checksum no-op issue. Verification: workspace
    all-features compile/tests and clippy passed; default durable-send package
    test had one `PoolTimedOut` flake that passed in isolation. Carries a
    2026-08-12 resolution section closing all five findings, and recording two
    places the fix departed from the review's proposed remedy: the suggested
    `errors -> -1 ->> 'occurred_at'` cast is not viable, and failure-time
    ordering needed a `created_at` tiebreak to make bounded redrive
    deterministic. Status: Sourced.

## Compatibility

- [compatibility/m1-durable-send-schema-and-api-changes.compatibility.md](compatibility/m1-durable-send-schema-and-api-changes.compatibility.md)
  - Records the review-fix schema/API changes: durable `idempotency_key` column
  plus its `AddIdempotencyKey` upgrade changeset for pre-existing tables,
  `mark_publish_failed` retry-duration signature, and reserved `kafkaman-` header
  rejection. Status: Active.
- [compatibility/m2-change-engine-config-schema-and-api.compat.md](compatibility/m2-change-engine-config-schema-and-api.compat.md)
  - M2 schema/API changes: nullable `checksum` and `applied_by`
    `changelog_history` columns, `migrate(..., MigrationContext, ...) ->
    MigrationReport` signature break, dry-run, and guarded replay behavior.
    Status: Draft.
- [compatibility/m3-durable-receive-review-fix-api.compat.md](compatibility/m3-durable-receive-review-fix-api.compat.md)
  - M3 receive review-fix API and operational-data changes:
    `IngestStats.skipped`, `RdkafkaConsumer::Error::UnexpectedTopic`,
    `RdkafkaConsumer::Error::ConsecutiveSkipLimitExceeded`,
    `IngestLoopStats`, `RdkafkaConsumer::run_ingester`,
    `kafkaman-test` ingest/dispatch hook APIs, `ReceivedError.kind`,
    `ReceivedIngestFailureKind`, `ReceivedInsertOutcome`, deterministic ingest
    quarantine, receive insert conflict classification, and retryable-only
    `Replay::received`. Status: Active.

- [compatibility/m4-retry-backoff-runtime-api.compat.md](compatibility/m4-retry-backoff-runtime-api.compat.md)
  - M4 retry/backoff runtime API and behavior changes: default retry policy,
    `ResolvedConfig.retry`, `ReceivedTable.retry`, scheduled `next_attempt_at`,
    terminal `Failed` status, and configured error-history bounds. Status:
    Active.
- [compatibility/typed-idempotency-identity-api.compat.md](compatibility/typed-idempotency-identity-api.compat.md)
  - Compatibility note for typed SHA-256 idempotency identity,
    `idempotency_source` JSON retention, digest Kafka headers, typed row fields,
    and transactional invalid-send audit rows. Also records stored failure
    records becoming RFC 9457 problem details with RFC 9557 timestamps, the new
    `last_failed_at` / `last_failure_kind` columns that replace casting the audit
    JSON, and the resulting changeset checksum changes. Verified against
    Docker-backed PostgreSQL and Redpanda. Status: Active.
- [compatibility/m5-entity-first-cache-api.compat.md](compatibility/m5-entity-first-cache-api.compat.md)
  - M5 first-slice API and behavior changes after the entity-only narrowing:
    required `KafkaMessage::entity_key`, removal of `RetentionClass`,
    `KafkaMessage::retention_class`, and `MessageDescriptor.retention_class`,
    plus `CacheTable`, `CreateCacheTable`, received-row entity-key persistence,
    and receive dispatch cache upsert guarded by Kafka offset for registered
    entity message types. Status: Active.
- [compatibility/m5-entity-first-outbox-supersede.compat.md](compatibility/m5-entity-first-outbox-supersede.compat.md)
  - M5 outbound schema/API and relay behavior changes: `OutboxStatus::Superseded`,
    `OutboxRow.entity_key`, nullable outbox `entity_key`, `AddOutboxEntityKey`,
    entity-type advisory enqueue serialization, pending-row supersede, and
    same-entity `Publishing` rows blocking newer pending relay claims. Status:
    Active.
- [compatibility/m5-code-audit-remediation.compat.md](compatibility/m5-code-audit-remediation.compat.md)
  - Public API, schema, and behavior changes from the full-workspace code audit
    remediation: received-table `entity_key` column and `AddReceivedEntityKey`,
    `Replay::outbox` rejected as unsafe, `CacheApplyOutcome` /
    `Error::CacheOriginMismatch`, `OutboxRow::record_key`, wire-carried
    `occurred_at` and idempotency source, jittered retry backoff, and removal of
    the panicking `Envelope::with_idempotency_key`. Amended by the review pass:
    per-entity Pending collapse at claim time, terminal disposition for
    deterministic cache errors, and the deleted `kafkaman-entity-key` resolution
    tier. Status: Active.

- [compatibility/m5-outbox-retention.compat.md](compatibility/m5-outbox-retention.compat.md)
  - Outbox retention surface: `PurgeConfig`/`PurgeStats`, `purge_outbox_once`,
    `run_purger`, the optional `[retention]` config section, and the terminal-status
    partial index emitted by `CreateOutboxTable` plus `AddOutboxRetentionIndex` for
    existing tables. Retention is opt-in; absent config nothing is deleted. Records
    that the index build blocks writes, because changesets apply inside a
    transaction and `CONCURRENTLY` is therefore unavailable. Status: Active.
- [compatibility/module-test-separation-internal-hooks.compat.md](compatibility/module-test-separation-internal-hooks.compat.md)
  - Source-organization and test API cleanup: production test hook features are
    renamed to hidden `internal-hooks`, consumer-facing hook ergonomics live in
    `kafkaman-test`, and large production/test files are split into focused
    `mod` modules with explicit imports and named root re-exports, keeping public
    paths and `rustfmt` coverage intact. Carries one behavior change — `migrate`
    now scopes its advisory lock to a transaction, so a cancelled migration
    cannot leave a pooled connection holding it — and requires a pool of at
    least two connections. Status: Active.
- [compatibility/m6-observability-operability-api.compat.md](compatibility/m6-observability-operability-api.compat.md)
  - M6 public API/config/dependency changes: `ResolvedConfig.observability`,
    observability config types, SQL queue inspection and runtime redrive helpers,
    OpenTelemetry metrics, and the new `kafkaman-axum` crate/facade feature.
    Dynamic Redpanda test ports came from the module-separation refactor, not
    from M6; the note carries the attribution correction. Now also carries the
    metric provider-ordering contract: install the `MeterProvider` before
    constructing kafkaman components or starting loops, since the publisher binds
    its instruments earlier than the loops do, and the full metric schedule —
    names, kinds, units, bucket boundaries, attributes — as a public
    compatibility surface, plus the `run_queue_metrics` sampler loop and the
    trace-context surface: two nullable columns on every outbox and received
    table, `TraceContext` and the row fields that carry it, the `traces` feature,
    the W3C header namespace, and the four kafkaman span names.
    Status: Active.

- [compatibility/dispatch-handler-ordering.compat.md](compatibility/dispatch-handler-ordering.compat.md)
  - Breaking and **silent**: `MessageRouter::handler` now runs after the cache
    upsert, so a handler that compensated for seeing its own entity one version
    stale now double-counts, and nothing about that is a compile error. Adds
    `handler_before`, `HandlerFlow`, and `BeforeHandlerFuture`; records the
    `Ignored` skip (a post-upsert handler is no longer guaranteed to run once per
    row, so auditing belongs pre-upsert, and a redrive after deploying new
    handler code will not run it), the savepoint moving ahead of the upsert, and
    `ReceivedMeta` becoming `#[non_exhaustive]` with a reserved `deleted` flag.
    Status: Active.
- [compatibility/runtime-builder-and-axum.compat.md](compatibility/runtime-builder-and-axum.compat.md)
  - Additive: `RuntimeBuilder`, `Runtime`, `RuntimeContext`, `RuntimeTasks`,
    `HandlerCtx`, `BuildError`/`RuntimeError`, and a re-exported
    `CancellationToken` on the facade behind the existing `rdkafka` feature, plus
    a new `axum` feature carrying `kafkaman::axum`. Adds `Role`/`RoleRegistry`
    and the generated-version scheme to `kafkaman-sqlx`. Records that generated
    identity keys on the **table kind** rather than the registration role,
    that adopting the builder on a hand-migrated database is safe but leaves
    orphan history rows, the never-edit-a-template rule and why
    `checksum_material` makes it necessary, the builder's steady-state connection
    demand, the now-wired opt-in purger, and that `.meter(..)` is deferred until
    M6 merges. Status: Active.
- [compatibility/topic-convergence-api.compat.md](compatibility/topic-convergence-api.compat.md)
  - Breaking: `MessageDescriptor` gains `topic_spec`, `CacheApplyOutcome` gains
  `Migrated`. Adds `kafkaman_core::topics`, `[topics] mode`, and
  `TopicAdmin`/`converge_topics`. Records that a cross-topic cache-origin change
  is no longer always terminal, and that metadata reads are deliberately
  configured so they cannot auto-create the very topic they are checking. This
  supersedes the M5 closeout's statement that any topic or partition mismatch
  fails as `CacheOriginMismatch`; the M5 compatibility notes are left as release
  records and the entity-first spec carries the correction. Revised 2026-08-25
  for phase 5: `ResolvedConfig` gains `topics: TopicMode`, both example services
  call `converge_topics` at boot, and a verified deployment now needs its topics
  provisioned first. Status: Active.

## Decisions

- [decisions/telemetry-pipeline-ownership.decision.md](decisions/telemetry-pipeline-ownership.decision.md)
  - Host owns the OpenTelemetry SDK; library crates depend on the API crate only.
  Instruments move from a process-wide `OnceLock` to per-run-loop construction,
  fixing a silent binding hazard that made the metric surface both fragile and
  untestable. Status: Accepted.
- [decisions/metric-instrument-and-attribute-schema.decision.md](decisions/metric-instrument-and-attribute-schema.decision.md)
  - Metric names, kinds, units, and attributes as a public compatibility surface:
  keep the `kafkaman.*` namespace, add standard messaging attributes, add latency
  histograms and queue-depth gauges, and pin the ingest disjointness invariant
  with a test rather than a `debug_assert`. Status: Accepted.
- [decisions/trace-context-propagation-and-w3c-headers.decision.md](decisions/trace-context-propagation-and-w3c-headers.decision.md)
  - W3C `traceparent`/`tracestate` as a third Kafka header namespace, amending the
  ratified two-namespace model; trace context persisted on the outbox row so the
  publish span stays causally linked to the enqueue that created it; consumer
  spans link rather than parent. Status: Accepted.
- [decisions/telemetry-backend-and-example-topology.decision.md](decisions/telemetry-backend-and-example-topology.decision.md)
  - Elastic as the reference backend (not a required one); the example exports
  direct to Elasticsearch over OTLP/HTTP with no collector, while the collector is
  documented as the production topology. Status: Accepted.
- [decisions/missing-handler-dispatch-policy.decision.md](decisions/missing-handler-dispatch-policy.decision.md)
  - Missing handlers are row-level durable dispatch failures recorded under the
    claimed row lock, parking the row without head-of-line blocking younger
    rows. Amended 2026-08-26: the router lookup stays ahead of the cache upsert
    when handlers move after it, so an unregistered type still parks its row
    without advancing the cache — and a builder `cache::<T>()` role installs a
    no-op handler, so builder-registered types cannot reach this path at all.
    Status: Accepted.
- [decisions/outbox-retention-policy.decision.md](decisions/outbox-retention-policy.decision.md)
  - Only the outbox is purged: `Published` and `Superseded` rows past a configured
    window, in bounded batches, with `Failed` audit rows spared unless opted in.
    Received tables are never purged because their dedupe window *is* their
    retention window; cache tables are the state. Scope is inherited from the
    restore proposal's drop/protect/rebuild split rather than argued fresh. Status:
    Accepted.
- [decisions/observability-operability-policy.decision.md](decisions/observability-operability-policy.decision.md)
  - M6 accepts tracing as the span/event surface, direct OpenTelemetry metrics,
    low-cardinality labels, runtime observability config with per-message
    overrides, sanitized reusable Axum admin routes, and host-owned exporters,
    subscribers, and admin-route access control. Status: Accepted.
- [decisions/dispatch-infrastructure-error-classification.decision.md](decisions/dispatch-infrastructure-error-classification.decision.md)
  - Post-handler infrastructure errors, including poisoned transactions after a
    swallowed SQL error, are recorded as receive failure accounting with a
    separate-connection fallback when rollback cannot run. Status: Accepted.
- [decisions/dispatch-stats-semantics.decision.md](decisions/dispatch-stats-semantics.decision.md)
  - `DispatchStats.failed` counts committed durable failure records; competing
    dispatchers skip a locked row while failure accounting is in flight. Status:
    Accepted.
- [decisions/kafka-ingest-identity-and-ordering.decision.md](decisions/kafka-ingest-identity-and-ordering.decision.md)
  - Kafka ingest requires an idempotency key, writes the received row before
    committing the broker offset, and preserves reserved kafkaman metadata
    headers separately from user headers. Status: Accepted.
- [decisions/ingest-poison-quarantine-policy.decision.md](decisions/ingest-poison-quarantine-policy.decision.md)
  - Ingest may commit past deterministic poison only after a durable quarantine
    row is written; repeated schema/deserialization skips trip a circuit breaker;
    message-id conflicts are identity anomalies, not normal duplicates. Status:
    Accepted.
- [decisions/receive-handler-surface-scope.decision.md](decisions/receive-handler-surface-scope.decision.md)
  - M3 keeps the closure + `ReceivedMeta` handler surface and adds
    `enqueue_on_connection` for consume-then-produce atomicity; the larger Tower
    handler surface remains deferred. Status: Accepted.
- [decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md](decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md)
  - Idempotency is a typed SHA-256 digest plus retained caller-provided JSON
    source, while send and receive share transactional error-row semantics:
    record invalid/problem work, return an error, allow rollback, and have
    workers claim only non-error rows. Bounds the rule to sends that are safe to
    persist, so a reserved-header rejection deliberately leaves no audit row —
    recording it would write the offending header into the ledger. Explains the
    send/receive difference as transaction ownership rather than inconsistency,
    and why rollback is recovered by redelivery rather than by the audit row.
    Status: Accepted.

- [decisions/entity-first-propagation-model.decision.md](decisions/entity-first-propagation-model.decision.md)
  - Propagation is now kafkaman's product purview: compact domain entity
    snapshots, durable entity outbox/inbox plumbing, guarded cache tables,
    advisory origin intent, and soft-delete-first deletion. **Amended
    2026-08-13:** the
    convergence ordinal is the Kafka offset already stored on received rows,
    made trustworthy by key-serialized per-entity outbox supersede. Removes the
    producer-side version, the high-water table, the monotonicity constraint, the
    `kafkaman-entity-version` header, and the domain-version override; adds
    state-sourced republish and topic-lifecycle invalidation. **Amended
    2026-08-14:** non-entity work items, commands, jobs, emails, payments,
    analytics events, and direct transport are outside kafkaman's product scope.
    Status: Accepted.

- [decisions/messaging-scope-and-receive-model.decision.md](decisions/messaging-scope-and-receive-model.decision.md)
  - Originally accepted a durable-execution-first core, Kafka-only transport in
  v1, and fire-and-forget receive with durable status. **Amended 2026-08-14:**
  compact entity-cache propagation is the v1 product scope; non-entity durable
  work and direct transport are outside purview. Status: Accepted.
- [decisions/message-identity-and-header-namespace.decision.md](decisions/message-identity-and-header-namespace.decision.md)
  - V1 persisted messages require an `idempotency_key`; received tables dedup on
  per-type unique idempotency keys; `kafkaman-*` Kafka headers are reserved and
  user headers with that prefix are rejected. Status: Accepted.
- [decisions/retry-backoff-dlq-policy.decision.md](decisions/retry-backoff-dlq-policy.decision.md)
  - Retry/backoff/DLQ policy is runtime config in `kafkaman.toml`, with common
  defaults plus per-message-type overrides, injected-clock tests, and a
  table-backed V1 DLQ surface. Status: Accepted.
- [decisions/v1-roadmap-execution-policy.decision.md](decisions/v1-roadmap-execution-policy.decision.md)
  - Draft decisions may contain accepted sub-decisions while full status waits
  for milestone validation; V1 work may use dependency-aware parallel worktrees.
  Status: Accepted.
- [decisions/schema-and-change-management.decision.md](decisions/schema-and-change-management.decision.md)
  - Dedicated `kafkaman` schema; distinct per-type tables from one template; a
  Rust Flyway-style change engine; no SQL functions. Amended 2026-08-26 with the
  standing rule that a table template is never edited in place — add an upgrade
  changeset and bump `template_version` — because `checksum_material` does not
  cover DDL, which makes an in-place edit invisible to the migration engine.
  Status: Accepted.
- [decisions/configuration-and-environment-model.decision.md](decisions/configuration-and-environment-model.decision.md)
  - One flat `kafkaman.toml`, rendered per environment by CI/CD from vault;
  `apply(env)` selects values not structure; tunable settings, including
  per-message retry/backoff/DLQ policy, are runtime config, not changesets.
  Status: Accepted.
- [decisions/runtime-composition-and-topology.decision.md](decisions/runtime-composition-and-topology.decision.md)
  - Schedulers are spawnable units; topology is a host choice; send-side Axum UX
  is opinionated around request transactions but M1 implements only the generic
  enqueue/relay core. Amended 2026-08-26: the planned `kafkaman-axum` crate
  becomes `kafkaman::axum` behind a feature on the facade. The HTTP-free core
  boundary this decision draws is unchanged and is why the later decision holds;
  only the crate boundary became a module boundary. Status: Draft.
- [decisions/runtime-builder-and-axum-composition.decision.md](decisions/runtime-builder-and-axum-composition.decision.md)
  - Core kafkaman gets a role-driven runtime builder for
    publish/cache/handle/handle_before declarations, generated kafkaman
    changelogs identified by `(role, message_type, template_version)`, topic
    convergence, migrations, worker loops, and shutdown wiring. Axum composition
    ships as a facade feature rather than a separate crate, retiring the planned
    `kafkaman-axum`; the builder takes a `PgPool` and an explicit `Meter` and
    never resolves a global. A normative list of host-owned concerns keeps the
    Tokio runtime, business schema, signals, process exit, telemetry install,
    config discovery, and the **topic-creation decision** with the caller —
    `build()` runs convergence in whatever mode the host configured and never
    defaults to or upgrades to `create`. Generated version bands carry a
    normative width, reserved range, hash-stability rule, and collision error.
    The Tower layer surface stays explicitly out of scope. Status: Accepted.
- [decisions/dispatch-handler-ordering.decision.md](decisions/dispatch-handler-ordering.decision.md)
  - Application handlers move to run *after* the cache upsert inside the
    received-row transaction, with `handle_before` as the explicit pre-image
    opt-in, and the dispatch savepoint moves ahead of the upsert so a failed
    handler still unwinds it. Neither position may suppress the upsert —
    convergence is non-negotiable and there is no ingest-time filter hook —
    while `MissingHandler` keeps short-circuiting ahead of the upsert unchanged.
    A post-upsert handler is skipped when the cache apply outcome is `Ignored`,
    so `DispatchStats.processed` stops implying the handler ran. The handler
    signature must be able to carry a tombstone before publication; the shape is
    deliberately **not** settled here. Promotion target:
    `specs/entity-first-propagation.spec.md`. Status: Accepted.
- [decisions/message-consumption-and-handler-model.decision.md](decisions/message-consumption-and-handler-model.decision.md)
  - Amended 2026-08-26 twice: the handler's position relative to the cache
    upsert is now fixed by the handler-ordering decision, and the sketch's
    `kafkaman_axum::serve` reads as `kafkaman::axum::serve`. Its Tower/extractor
    surface remains unbuilt future design space — `MessageRouter` has only `new`
    and `handler` — which the runtime builder neither builds nor forecloses.
    Receive side uses ingest and dispatch schedulers, per-type received tables,
    required idempotency-key dedup-as-log, bounded errors, and a Tower-style
    message handler stack. M3 accepts the closure + `ReceivedMeta` surface with
    `enqueue_on_connection` for atomic consume-then-produce. Status: Accepted.
- [decisions/library-test-strategy.decision.md](decisions/library-test-strategy.decision.md)
  - kafkaman tests itself with unit, Postgres integration, and full-loop tiers;
  dogfooding-first where tests sit at or above toolkit abstractions. Durable-send
  Postgres tests now use owned containers per test/harness because
  Testcontainers cleanup is `Drop`-based and static `OnceCell<ContainerAsync<_>>`
  sharing leaked containers. Status: Draft.
- [decisions/consumer-test-tooling.decision.md](decisions/consumer-test-tooling.decision.md)
  - `kafkaman-test` is the consumer-facing test toolkit with Harness,
  deterministic one-step drivers, and future macro sugar. Status: Draft.

- [decisions/topic-convergence-and-rebuild.decision.md](decisions/topic-convergence-and-rebuild.decision.md)
  - A topic's required configuration belongs on `MessageDescriptor`, defaulted to
  `cleanup.policy=compact` alone by the model rather than declared per contract,
  and is converged at boot: `verify` by default, `create` opt-in, `off` as an
  escape hatch. Repartitioning is never performed — topics are **rebuilt** by
  republishing every entity from the owner's state, which the entity-first model
  makes possible because the log is a derived artifact, at a cost bounded by
  entity count rather than event count. The declared topic is what authorizes
  cache-origin invalidation, so a migration converges while a misconfigured
  consumer still fails terminally. Provisioning covers environment (databases,
  topics) and never schema (tables). Status: Accepted.

## Roadmaps

- [roadmaps/path-to-v1.roadmap.md](roadmaps/path-to-v1.roadmap.md) - Seven
  milestones to V1. M1 durable send, M2 change-engine/config, M3 durable
  receive, M4 retry/DLQ, M5 entity-first propagation, and M6 observability are all Completed,
  merged, and promoted to specs.
  **Updated 2026-08-13:** entity-first propagation is Active as M5 ahead of
  observability, because it changes the table layout dashboards would otherwise
  be built on; observability and hardening shift to M6 and M7. **Updated
  2026-08-14:** M5 is compact entity-cache purview only; delete-retention
  work-item APIs are removed from the public surface. **Updated 2026-08-24:**
  M5 is completed; M6 observability proceeded in parallel with the two-service
  example. **Updated 2026-08-24:** M6 is completed; M7 hardening is active while
  the two-service example remains active in parallel. Status: Draft.

## References

- [references/rust-kafka-outbox-ecosystem.reference.md](references/rust-kafka-outbox-ecosystem.reference.md)
  - Rust Kafka clients, nearest Rust outbox crate, and JVM/.NET comparables.
  Status: Sourced.

## Proposals

- [proposals/01-kafkaman-objectives.proposal.md](proposals/01-kafkaman-objectives.proposal.md)
  - What kafkaman is, its core promise, crate shape, and design requirements.
  Status: Proposed.
- [proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md](proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md)
  - Kafka-command-first vs propagation-first vs durable-execution-first. Status:
  Accepted, promoted to the messaging-scope decision.
- [proposals/03-direct-transport-mode.proposal.md](proposals/03-direct-transport-mode.proposal.md)
  - Explicit non-durable direct Kafka producer and consumer modes for
  high-throughput or low-durability workloads. Status: Proposed.
- [proposals/04-observability-logging-policy.proposal.md](proposals/04-observability-logging-policy.proposal.md)
  - Configurable tracing, logging, metrics, payload safety, and per-message-type
  observability policy. Status: Accepted, promoted to the observability
  operability decision and M6 spec.
- [proposals/05-deep-durability-testing.proposal.md](proposals/05-deep-durability-testing.proposal.md)
  - Reviewed living catalog of adversarial concurrency, crash, ingest, send,
  cancellation, identity-collision, atomic-chain, observability, and chaos/model
    tests for durable-execution paths. Tracks landed receive regressions and
    prioritizes rare-failure tests such as `MissingHandler` head-of-line
    blocking, failure-accounting crash windows,
  poisoned transactions after swallowed handler DB errors, consume-then-produce
  API gaps, suppressed stale-failure stats, ambiguous commit, identity conflicts,
  reserved status drift, received message-version drift, and timestamp
  test-oracle precision. Status: Proposed.
- [proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md](proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md)
  - Accepted proposal to replace raw string idempotency with a typed SHA-256
    digest plus retained JSON source and to make send/receive invalid-work
    recording symmetric and transactional. Status: Accepted.
- [proposals/07-tombstone-and-deletion-semantics.proposal.md](proposals/07-tombstone-and-deletion-semantics.proposal.md)
  - Revised 2026-08-12: the emission half is superseded by soft-delete-first in
    proposal 09, because real tombstones are reclaimed after
    `delete.retention.ms` and a long-offline cache can miss a delete. Residual
    scope is per-message-type opt-in real-tombstone *ingestion*, still required
    for foreign producers such as Debezium. Amended 2026-08-26: its *Handler
    surface* section — deletion travels in `ReceivedMeta` rather than changing
    the shape of `P` — is now on the critical path, because the runtime builder
    publishes the handler signature and absence cannot be retrofitted into `T`.
    Settled in Phase 0 of the builder plan, 2026-08-26, and written back here:
    `ReceivedMeta` is `#[non_exhaustive]` and carries a reserved `deleted` flag.
    Status: Proposed.
- [proposals/08-listen-notify-scheduler-wakeup.proposal.md](proposals/08-listen-notify-scheduler-wakeup.proposal.md)
  - Postgres LISTEN/NOTIFY as a best-effort accelerator for the relay and
    dispatch schedulers, with the interval sweep retained as the correctness
    path and as the bound on retry punctuality. Noted 2026-08-26 as wanted for
    the next developer-UX increment, paired with `max_in_flight` from proposal
    15 — the two both change `run_dispatcher` and decide different things (*when*
    a dispatcher wakes versus *how many* rows are in flight). Its own
    self-demotion to "an efficiency proposal, not a latency proposal" should
    survive implementation. Status: Proposed.
- [proposals/09-entity-first-propagation.proposal.md](proposals/09-entity-first-propagation.proposal.md)
  - Entity-first reference propagation as the headline use case: three identity
    axes, one message type across an inbox plus a guarded per-entity cache
    table, advisory `#[non_exhaustive]` origin intent, and soft-delete-first
    deletion. **Revised 2026-08-13:** selects Kafka offset as the convergence
    ordinal paired with key-serialized per-entity outbox supersede, replacing the
    outbox sequence and domain override; adds ordinal validity boundaries and
    state-sourced republish. Status: Accepted, promoted to the entity-first
    propagation decision.
- [proposals/10-cache-bootstrap-and-readiness.proposal.md](proposals/10-cache-bootstrap-and-readiness.proposal.md)
  - **Revised 2026-08-26: written for a per-process cache, which kafkaman's is
    not.** The cache is a shared Postgres table every replica queries, so the
    per-instance consumer groups it is built around all dissolve, along with its
    first open question. The hazard it misses is worse: a fresh database under a
    surviving consumer group starts at that group's committed offset, and every
    entity below it is missing permanently and silently. The replay survives,
    reframed as assign-based and non-committing; the readiness typestate survives
    too, now gating the *qualified table name* rather than a `get()` that
    deliberately does not exist. Original open questions 1-5 are dissolved or answered in place; two
    new ones replace them, and `RuntimeTasks::wait()` treating a completing loop
    as a shutdown signal is a blocking prerequisite. Status: Proposed.
- [proposals/11-restore-retention-and-schema-boundaries.proposal.md](proposals/11-restore-retention-and-schema-boundaries.proposal.md)
  - Names the operational invariants durability silently depends on: entity
    topics use compaction alone (never with delete-by-age), the outbox is
    excluded from backups and starts empty, the inbound ledger is never rewound
    past its processed-markers, a business restore needs a resync sweep, and
    irreversible actions carry their own idempotency record in an independent
    restore domain. **Amended 2026-08-24:** adds storage growth of kafkaman's own
    Postgres tables (nothing purges any table) and the invariant that index-adding
    changesets block writes, since changesets apply inside a transaction. Splits kafkaman's tables three ways by reconstructibility —
    drop / protect / rebuild — while recording that PITR is cluster-wide, so the
    split buys backup-set composition rather than independent restore. GDPR
    erasure is resolved by publishing a redacted entity; storage growth is not.
    Status: Proposed.
- [proposals/12-entity-only-message-model.proposal.md](proposals/12-entity-only-message-model.proposal.md)
  - Collapses the message model to compact entity-cache propagation only.
    **Revised 2026-08-13 after review:** temporarily selected a uniform entity
    model with a declared `compact` / `delete` retention class. **Revised
    2026-08-14:** supersedes that model and selects hard purview exclusion for
    non-entity work: emails, payments, commands, analytics events, generic jobs,
    `mutation_jobs`-style queues, and direct transport belong outside kafkaman.
    The same-day implementation removes the public retention-class API instead
    of carrying `delete` compatibility forward. Status: Accepted, promoted as
    amendments to the entity-first propagation and messaging-scope decisions.

- [proposals/13-topic-convergence-and-environment-provisioning.proposal.md](proposals/13-topic-convergence-and-environment-provisioning.proposal.md)
  - Closes the deferred boot-time compact-topic validation, and establishes topic
  rebuild as the answer to repartitioning. Motivated by direct evidence: the
  example services were observed running with `cleanup.policy=delete` on both
  `products` and `orders`, silently unable to support the rebuild the model
  promises. Rejects fixing it with a broker default, rejects a per-contract
  `topic_spec()`, and rejects a provisioner that creates tables. Status: Accepted.
- [proposals/14-runtime-builder-and-axum-composition.proposal.md](proposals/14-runtime-builder-and-axum-composition.proposal.md)
  - Accepted developer-UX proposal to replace the example's manual config,
  topic, changelog, migration, relay, ingest, dispatch, and shutdown wiring with
  a repeatable publish/cache/handle/handle_before runtime builder, while keeping
  HTTP and every host-owned concern out of core kafkaman. Records why changeset
  identity must come from a library-owned table template version rather than a
  contract-side wire version, and the supporting finding that
  `checksum_material` does not cover DDL at all. Revised 2026-08-26: the
  generated-version band is now normative (width, reserved range, hash
  stability, collision error), topic-creation *authority* rather than the
  mechanism is host-owned, and the Tower layer section is corrected — that
  surface does not exist and the handler-model decision calls it future design
  space. Status: Accepted.

- [proposals/15-dispatch-concurrency-and-middleware.proposal.md](proposals/15-dispatch-concurrency-and-middleware.proposal.md)
  - Opt-in concurrent dispatch (`max_in_flight`, default 1) plus the
    three-times-deferred Tower question. Concurrency is opt-in rather than a
    tuning knob because of one finding: `recompute_availability` reads a `SUM`
    and writes it back, so two orders for one product dispatched concurrently
    under READ COMMITTED both compute a partial sum and the last writer wins —
    correct today *only* because dispatch is serial. Rejects `tower::Service`
    (handlers borrow the transaction connection, so the future is lifetime-bound;
    `retry`/`buffer` need `Clone`/`'static` requests a unique mutable borrow
    cannot give) and adopts its vocabulary instead: a `Layer` trait carrying
    `'a`. The claim layer is already concurrent-ready — `received_rows.rs:38`
    uses `FOR UPDATE SKIP LOCKED`. Leaves open whether `max_in_flight` is
    per-type or runtime-wide, and which config section owns it.
    Status: Proposed.
- [proposals/16-message-contract-derive.proposal.md](proposals/16-message-contract-derive.proposal.md)
  - `#[derive(KafkaMessage)]` for the 18 hand-written impls in the tree. Fixes
    two rules that matter more than the macro: `message_type` and `topic` stay
    explicit rather than inferred from the type name, because inferring makes a
    struct rename a silent wire-contract change; and the derive is sugar that
    never becomes load-bearing, per the standing principle that dropped
    `#[kafkaman::handler]`. Settles the finding that both example contracts define
    `partition_key` returning the same value as `entity_key`: redundant for
    routing, but load-bearing for headers, because `outbox_enqueue.rs:54` tests
    the *declared* key rather than the effective record key and so would start
    emitting `kafkaman-entity-key` on every record the moment a derive omitted it.
    Narrowing that condition is a prerequisite for the derive's default, not a
    follow-up. Status: Proposed.
- [proposals/13-telemetry-pipeline-completion.proposal.md](proposals/13-telemetry-pipeline-completion.proposal.md)
  - Completes OpenTelemetry from instrumentation-only to a working pipeline:
  metrics, traces, logs, W3C context across the outbox and Kafka hops, and an
  Elastic/Kibana reference deployment. Records that no `opentelemetry_sdk` exists
  in the workspace, so every M6 counter is inert. Status: Accepted.

## Plans

- [plans/opentelemetry-completion.plan.md](plans/opentelemetry-completion.plan.md)
  - Turns M6's OpenTelemetry instrumentation into a working pipeline: SDK-backed
  export, the provider-ordering fix, latency histograms and queue-depth gauges,
  `tracing`-to-OTel span bridging, W3C trace context across the outbox and Kafka
  hops, an OTel log appender, and a `tests/observability/` suite. Phase 0 is
  completed 2026-08-25: instruments are built by the loop or component that owns
  them instead of a process-wide `OnceLock`, counters declare units,
  `tests/observability/provider_ordering` pins that an SDK installed after a relay
  has run still collects, and the example app installs and flushes an OTLP metrics
  pipeline of its own. Phase 1 completed the same day: latency histograms,
  observable queue-depth gauges behind a sampler loop, semconv messaging
  attributes, and five `tests/observability` binaries. Phases 2 and 3 followed:
  W3C trace context persisted on outbox *and* received rows through an additive
  migration, four kafkaman spans, a third Kafka header namespace, and sampled
  success events emitted inside the publish span so they carry the trace they
  belong to. Phases 5 and 6 followed: twelve `tests/observability` binaries
  including an OTLP export asserted on the wire, and the M6 spec corrected.
  Phase 4's compose profile is deferred — the example that would carry it is
  being replaced. Status: Active.
- [plans/first-poc-outbox-publisher.plan.md](plans/first-poc-outbox-publisher.plan.md)
  - Smallest durable-send slice: per-type outbox table, minimal `migrate()`,
  claim-lease relay, publisher, Axum example, and crash/idempotency gates. Status:
  Completed.
- [plans/m1-durable-send-implementation.plan.md](plans/m1-durable-send-implementation.plan.md)
- Code-level M1 implementation plan for workspace/crate layout, core types,
SQLx DDL/primitives, relay, Harness, tests, and example. Status: Completed.
- [plans/m3-durable-receive.plan.md](plans/m3-durable-receive.plan.md)
- Active M3 execution plan for durable receive: received tables, deterministic
  dispatch, handler API, Harness maturity, and Kafka ingest. First Postgres
  storage/dispatch slice implemented 2026-06-21. Status: Active.
- [plans/m3-durable-completion.plan.md](plans/m3-durable-completion.plan.md)
- Sequenced completion of M3 after the first slice and its review fixes.
  Tests-lead phases: harden the `dispatch_once` seam against the deep-testing
  catalog (gap-revealing C1/C3/M3-stats + forced decisions), then operational
  replay/injected clock, Kafka ingest + dispatcher loop, the decision-gated
  handler surface, and spec promotion. Completed by
  [specs/m3-durable-receive.spec.md](specs/m3-durable-receive.spec.md);
  remaining chaos/model cases stay in the deep-durability hardening backlog.
  Status: Completed.

- [plans/m4-retry-backoff-dlq.plan.md](plans/m4-retry-backoff-dlq.plan.md)
- M4 reliability plan for policy-driven receive retry scheduling, table-backed
  terminal `Failed`/DLQ state, bounded error history, and redrive/admin
  surfaces. Completed by
  [specs/m4-retry-backoff-dlq.spec.md](specs/m4-retry-backoff-dlq.spec.md).
  Status: Completed.

- [plans/entity-first-propagation.plan.md](plans/entity-first-propagation.plan.md)
- Completed M5 execution plan for entity-first propagation. Validated behavior
  is promoted to `specs/entity-first-propagation.spec.md`; deferred work remains
  cache bootstrap/readiness, boot-time broker topic validation, advisory origin
  intent, a positive state-sourced republish API, proactive topic-lifecycle
  re-bootstrap hooks, soft-delete workflow/reclamation, and the two-service
  wiring example. Status: Completed.
- [plans/two-service-distributed-cache-example.plan.md](plans/two-service-distributed-cache-example.plan.md)
- Replaces the single send-only example app with two services — `product` and
  `order` — proving entity-first cache convergence end to end over HTTP alone.
  Records that the current example **cannot start** (no `kafkaman.toml` exists, and
  its test bypasses `Config::discover`), that nothing today tests wiring above
  `Harness`, and the derive-don't-decrement argument: availability is recomputed
  from converged state rather than decremented, so redelivery cannot double-count
  and cancellation restores the count with no compensating logic. Also records the
  handler-runs-before-cache-upsert constraint and the exclude-and-substitute
  correction it forces. Shipped 2026-08-24: `examples/{contracts,order,product}`
  and `tests/distributed-cache`, with the dispatcher-missing and
  cache-table-missing breaks demonstrated to fail the test. Two deviations, both
  recorded in the plan: per-entity version columns carry the idempotency identity,
  and the availability-only rejection runs before discontinuation rather than
  after. **Superseded in part 2026-08-26:** the handler-runs-before-cache-upsert
  constraint recorded here, and the exclude-and-substitute correction it forces,
  are reversed by
  [decisions/dispatch-handler-ordering.decision.md](decisions/dispatch-handler-ordering.decision.md);
  the rest of the plan stands. Status: Completed.
- [plans/runtime-builder-and-axum-composition.plan.md](plans/runtime-builder-and-axum-composition.plan.md)
- Completed execution plan for the accepted builder split. All seven phases
  shipped 2026-08-26: `RoleRegistry` and the generated changelog, the dispatch
  reorder with the savepoint move, `RuntimeBuilder` plus `HandlerCtx`,
  `kafkaman::axum`, and both example services refactored — `service.rs` down from
  229 to 51 lines in `order` and 201 to 55 in `product`, with
  `examples/product/src/service_manual.rs` proving the low-level path equivalent
  under the same end-to-end suite. Three plan items were revised by
  implementation: identity keys on table kind rather than registration role, the
  `handle_before` example role was dropped because skipping the recompute would
  delete the event the end-to-end test waits on, and `.meter(..)` is deferred
  until M6 merges. The four original open questions were resolved before
  implementation, and the tombstone representation was settled onto proposal 07. Seven phases:
  symbol-absence and handler-signature spec, roles plus generated changelog
  identity, the dispatch handler reorder (landable ahead of the builder, but not
  alone — it ships with the `examples/product` simplification), the core
  builder, handler context and the Axum feature, example refactor with a
  hand-wired `service_manual.rs` proving equivalence under the same end-to-end
  suite, and closeout. Carries the cross-worktree coordination items owed to
  `implementation/m6-observability`, which are the only part still outstanding.
  Status: Completed.
- [plans/outbox-retention.plan.md](plans/outbox-retention.plan.md)
- Two changes proposed together after measuring R1. The claim-order index was
  **refuted by measurement and not shipped** — the candidate predicate is an `OR`
  across two statuses, so no index can order it, and the plan is byte-identical with
  and without the index. Outbox retention shipped, with its index verified
  index-served rather than assumed. Status: Completed.
- [plans/typed-idempotency-identity-error-row-fix.plan.md](plans/typed-idempotency-identity-error-row-fix.plan.md)
- Completed fix plan for typed idempotency identity, transactional send/receive
  error-row symmetry, DLQ latest-failure-time semantics, and outbox replay
  checksum cleanup before merging M3/M4. All five pre-merge review findings are
  closed and every verification gate passes. Status: Completed.

- [plans/topic-convergence.plan.md](plans/topic-convergence.plan.md)
- Five-phase execution of the topic-convergence decision, in dependency order:
  declaration and config with no I/O, `verify`, `create`, authorized cache-origin
  invalidation, then the `examples/provision` binary. Records that the
  provisioner is Phase 5 rather than Phase 1 because it is a thin caller of
  `create` mode. Each phase carries its own deliberate-break gate, and all three
  were demonstrated by deliberate breakage rather than assumed. All five phases
  landed 2026-08-25: `example-provision` builds the databases and the compacted
  topics, both services verify at boot, `examples/initdb` is gone, and both M5
  compatibility notes strike the boot-time-validation deferral. Status: Complete.

## Checklists

(none yet)

## Archive

(none yet)
