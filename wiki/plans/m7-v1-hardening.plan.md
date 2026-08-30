# M7 V1 Hardening Plan

- Document Class: Plan
- Status: Completed
- Date: 2026-08-31
- Category: V1 hardening
- Scope: Tactical execution plan for the final V1 hardening milestone: storage-growth policy, graceful shutdown, worker-role polish, full-loop testcontainers coverage, and docs/examples closeout.
- Sources:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/outbox-retention-policy.decision.md
  - wiki/decisions/handler-panic-containment-and-fault-injection.decision.md
- Related:
  - wiki/specs/m1-durable-send.spec.md
  - wiki/specs/m2-change-engine-config.spec.md
  - wiki/specs/m3-durable-receive.spec.md
  - wiki/specs/m4-retry-backoff-dlq.spec.md
  - wiki/specs/entity-first-propagation.spec.md
  - wiki/specs/m6-observability-operability.spec.md
  - wiki/plans/failure-examples.plan.md
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/plans/apm-waterfall-traces.plan.md
- Promotion Target: Completed in wiki/specs/v1-acceptance.spec.md and
  wiki/roadmaps/path-to-v1.roadmap.md.

## Deliverable

M7 makes kafkaman ship-quality for V1. It does not add a new product pillar.
It hardens the pillars already delivered by M1-M6: durable send, durable receive,
retry/DLQ recovery, entity-cache convergence, runtime composition, and
observability.

The milestone closes when the V1 acceptance bar is met:

- every durable path has a realistic full-loop or integration proof;
- worker and embedded topologies shut down predictably;
- table-growth policy is explicit and enforced where kafkaman owns enforcement;
- examples demonstrate the happy path and failure path without hand-waving;
- the fast and heavy verification gates are green;
- wiki specs, compatibility notes, and docs match the validated behavior.

## In Scope

- Validate the existing panic-containment baseline, especially the distinct-row
  breaker semantics.
- Close remaining storage-growth policy outside the already-shipped outbox
  retention surface.
- Harden graceful-shutdown ordering and supervision across runtime tasks.
- Polish worker-role topology so a no-HTTP host binary is documented and tested
  as a first-class shape.
- Complete the `testcontainers` full-loop acceptance suite promised by the
  library test strategy.
- Update docs, examples, specs, compatibility notes, and roadmap state as each
  phase produces evidence.

## Out Of Scope

- Non-entity work queues, payments, command topics, analytics events, or generic
  job processing.
- Changing the default Kafka trace relationship from linked to parented.
- Making kafkaman own an OpenTelemetry SDK or exporter.
- A generic standalone daemon. Worker role remains the host's own binary without
  HTTP.
- Purging received rows by age. The received table's retention window is its
  dedupe window.

## Commit Protocol

Each phase lands in its own commit after its verification gate passes. If a
phase discovers that the documented behavior already exists and is adequately
tested, the phase commit records that validation in the plan/log rather than
adding duplicate implementation.

Every implementation phase updates this plan's status notes before commit. Public
API, schema, config, metrics, span, or example behavior changes also update the
matching compatibility note or spec in the same phase commit.

## Phase 0 - Baseline And Panic Breaker

Status: Completed 2026-08-31.

Purpose: prove the branch starts M7 from a correct failure-containment baseline.
The current M6 closeout found and fixed the critical breaker bug: the dispatcher
must count distinct panicking rows, not panic attempts by one row.

Steps:

- Done. Re-ran the focused panic-containment tests.
- Done. Confirmed the dispatcher tracks distinct panicking `message_id`s and
  clears the streak after a row succeeds.
- Done. Confirmed one poison row can spend its retry budget and dead-letter
  without tripping the fleet-wide breaker.
- Done. Recorded the verification command and result here.

Verification:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive dispatch_handler_panic -- --nocapture` - 9 passed, 37 filtered out.
- `rtk cargo test -p kafkaman-core dispatcher_config` - 4 passed, 109 filtered out.
- `rtk cargo test -p kafkaman-config dispatcher` - 6 passed, 35 filtered out.

Exit:

- The panic-containment baseline is known green on this branch. No new code was
  needed because the branch already has the distinct-row breaker coverage:
  single poison row, two poison rows below the threshold, distinct rows tripping
  the breaker, and successful row clearing the streak.

## Phase 1 - Storage-Growth Policy

Status: Completed 2026-08-31.

Purpose: close the remaining M7 storage-growth deliverable without weakening
dedupe or cache correctness.

Steps:

- Done. Audited current growth surfaces: outbox, received, cache, and the
  schema-wide `received_ingest_failures` quarantine.
- Done. Kept received and cache tables non-purged. Received rows remain the
  processed-marker/dedupe ledger, and cache rows remain state.
- Done. Kept quarantine rows non-purged. They are the only durable diagnosis for
  records skipped before a received row existed.
- Done. Added a read-only visibility surface instead of unsafe deletion:
  `received_ingest_failure_summary` in `kafkaman-sqlx` and
  `GET /ingest-failures` in `kafkaman-axum::admin_router`.
- Done. Added a durable regression proving outbox retention does not touch
  received, cache, or ingest quarantine rows.
- Done. Updated public docs, example config comments, and the M5/M7
  compatibility notes.

Verification:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive received_ingest_failure_summary -- --nocapture` - 1 passed, 46 filtered out.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test outbox_retention -- --nocapture` - 8 passed.
- `rtk cargo test -p kafkaman-core purge_config` - 2 passed, 111 filtered out.
- `rtk cargo test -p kafkaman-config retention` - 2 passed, 39 filtered out.
- `rtk cargo test -p observability-tests --test admin_http every_admin_route_answers_over_http -- --nocapture` - 1 passed, 4 filtered out.
- `rtk cargo test -p kafkaman-axum --all-features` - 25 passed, 2 ignored.
- `rtk cargo test -p kafkaman-sqlx --lib the_shipped_example_config_resolves_and_covers_every_section` - 1 passed, 81 filtered out.
- `rtk cargo clippy -p kafkaman-sqlx -p kafkaman-axum -p observability-tests --all-targets --all-features -- -D warnings` - clean.
- `rtk cargo clippy --manifest-path tests/durable-send/Cargo.toml --test outbox_retention --all-features -- -D warnings` - clean.
- `rtk cargo fmt --all -- --check` - clean.

Exit:

- No kafkaman-owned table growth behavior is implicit. Outbox remains the only
  purged table family; received, cache, and ingest quarantine rows remain
  retained for correctness, and quarantine growth is visible without exposing
  quarantined payloads or headers.

## Phase 2 - Graceful Shutdown Ordering

Status: Completed 2026-08-31.

Purpose: make runtime shutdown behavior predictable under embedded and worker
topologies.

Steps:

- Done. Audited the facade supervisor, builder-created loops, facade Axum
  composition, worker relay/dispatcher/purger loops, ingester cancellation
  points, and queue-metrics sampler shutdown behavior.
- Done. Hardened `RuntimeTasks` so clean loop completion before shutdown is a
  supervision error, including when the caller goes directly to `shutdown()`.
- Done. Added bounded drain to the facade runtime with
  `DEFAULT_DRAIN_TIMEOUT` and `shutdown_with_timeout(Duration)`, aborting
  stragglers after the bound.
- Done. Preserved the first observed loop failure when another task later wedges
  during drain.
- Done. Made panic and loop errors carry named task context, and made
  builder-created task names role/message-specific.
- Done. Added facade Axum tests for external HTTP shutdown and delegated drain
  timeout.
- Done. Updated doc examples and the M7 compatibility note.

Verification:

- `rtk cargo test -p kafkaman --all-features runtime` - 21 passed.
- `rtk cargo test -p kafkaman --all-features running_service -- --nocapture` - 2 passed, 19 filtered out.
- `rtk cargo test -p kafkaman --all-features` - 23 passed.
- `rtk cargo test -p kafkaman --all-features --doc` - 2 passed.
- `rtk cargo test -p kafkaman-worker` - 2 passed.
- `rtk cargo test -p kafkaman-axum --all-features runtime` - 5 passed, 20 filtered out.
- `rtk cargo test -p kafkaman-axum --all-features` - 25 passed, 2 ignored.
- `rtk cargo test -p distributed-cache-tests --test runtime_builder -- --nocapture` - 1 passed.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send relay_and_publish -- --nocapture` - 5 passed, 18 filtered out.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive dispatcher_loop -- --nocapture` - 2 passed, 45 filtered out.
- `rtk cargo test -p observability-tests --test queue_gauges -- --nocapture` - 1 passed.
- `rtk cargo test -p observability-tests --test queue_gauge_staleness -- --nocapture` - 1 passed.
- `rtk cargo test -p observability-tests --test queue_gauge_ordering -- --nocapture` - 1 passed.
- `rtk cargo clippy -p kafkaman --all-targets --all-features -- -D warnings` - clean.
- `rtk cargo fmt --all -- --check` - clean.

Exit:

- Shutdown order and failure propagation are tested at the public facade runtime
  surface and at the loop-level surfaces that own relay, dispatcher, and
  queue-metrics cancellation.

## Phase 3 - Worker-Role Topology Polish

Status: Completed 2026-08-31.

Purpose: prove the no-HTTP worker role is a supported host shape, not a side
effect of the embedded runtime.

Steps:

- Done. Added `Subsystems` and `RuntimeBuilder::subsystems(Subsystems)` so roles
  still declare schema/topics/handlers while a host binary explicitly selects
  the loops it owns.
- Done. Preserved the existing default: `RuntimeBuilder::new()` still starts all
  role-implied loops, with purgers additionally gated by `[retention]`.
- Done. Added `Subsystems::PIPELINE` as the copyable no-purge worker shape:
  relay, ingest, dispatch, and queue metrics.
- Done. Made Kafka consumer group validation depend on selected ingest, so a
  dispatch-only worker does not need a group it will never use.
- Done. Added `examples/product`'s `product-worker` binary, which runs the
  product pipeline without Axum, a listener, or HTTP router assembly.
- Done. Added source-level and infrastructure-backed tests for worker topology
  selection and no-HTTP worker shape.
- Done. Updated README, examples docs, and compatibility notes.

Verification:

- `rtk cargo test -p kafkaman --all-features subsystem` - 1 passed, 22 filtered out.
- `rtk cargo test -p kafkaman --all-features dispatch_only_runtime` - 1 passed, 22 filtered out.
- `rtk cargo check -p example-product --bin product-worker` - passed.
- `rtk cargo test -p distributed-cache-tests --test boot_surface` - 4 passed.
- `rtk cargo test -p distributed-cache-tests --test runtime_builder -- --nocapture` - 1 passed.
- `rtk cargo test -p kafkaman --all-features` - 25 passed.
- `rtk cargo test -p example-product --all-targets` - 11 passed.
- `rtk cargo clippy -p kafkaman -p example-product -p distributed-cache-tests --all-targets --all-features -- -D warnings` - clean.
- `rtk cargo fmt --all -- --check` - clean.

Exit:

- A developer can copy the documented worker-role pattern and know which loops
  run, which loops do not, and how shutdown is triggered. Partial subsystem
  selections are explicit deployment topology and can intentionally create
  backlog in omitted pipeline halves.

## Phase 4 - Full-Loop Testcontainers Acceptance

Status: Completed 2026-08-31.

Purpose: complete the `kafkaman-test` full-loop tier promised by the V1 roadmap
and library test strategy.

Steps:

- Done. Audited existing Redpanda/Postgres full-loop coverage against the V1
  invariants: no loss, effective-once, duplicate/redelivery convergence, crash
  windows, poison quarantine, retry/DLQ, and consume-then-produce atomicity.
- Done. Filled the smallest high-risk broker-backed gap on the send side:
  broker ack before outbox `Published` mark, lease expiry, real-broker
  republish, receive dedupe, and one dispatch effect.
- Done. Filled the broker-backed retry/DLQ/redrive altitude gap: Redpanda input
  ingests to a received row, exhausts the dispatch retry budget, parks in the
  DLQ, redrives, preserves source offset/history, and processes successfully.
- Done. Kept both new tests in the existing optional `redpanda_full_loop` gate
  and used one-step ingest/dispatch loops rather than sleeping schedulers.
- Done. Ran the service-level distributed-cache suite as the cross-service
  acceptance proof and the repository fast gate as the non-infra proof.
- Done. Fixed a fast-gate rustdoc warning from a public doc link to a private
  macro module, and made the facade Axum external-shutdown test wait for the
  listener before cancellation.

Verification:

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop ack_before_mark_republish_is_deduplicated_after_real_broker_hop -- --nocapture` - 1 passed, 12 filtered out.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop redpanda_input_exhausts_dlq_and_redrives_to_success -- --nocapture` - 1 passed, 12 filtered out.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1 --nocapture` - 13 passed.
- `rtk cargo test -p distributed-cache-tests --test two_service_cache -- --test-threads=1 --nocapture` - 3 passed.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive retry_budget -- --nocapture` - 3 passed, 44 filtered out.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive redrive -- --nocapture` - 6 passed, 41 filtered out.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send ack_before_mark -- --nocapture` - 1 passed, 22 filtered out.
- `rtk cargo test -p kafkaman --all-features running_service_wait_allows_http_exit_after_external_shutdown -- --nocapture` - 1 passed, 22 filtered out.
- `rtk just lint` - passed outside the sandbox after the sandboxed workspace lib
  test hit local-socket `PermissionDenied`.

Exit:

- The full-loop tier proves the highest-risk V1 durable paths across real
  Postgres and Redpanda. The remaining Phase 5 work is documentation and
  closeout, not another acceptance-test discovery phase.

## Phase 5 - Docs, Examples, And V1 Closeout

Status: Completed 2026-08-31.

Purpose: make the implemented V1 behavior teachable and close the roadmap.

Steps:

- Done. Updated README/example docs for runtime builder closeout, worker role,
  admin routes, retention, failure scenarios, telemetry, and full-loop test
  expectations.
- Done. Fixed stale example package docs: post-upsert handler ordering,
  role-derived service assembly, collection endpoints, `product-worker`,
  `/ingest-failures`, topic config, subsystem-selected purging, and the seven
  failure scenarios.
- Done. Promoted the validated M1-M7 envelope into
  `wiki/specs/v1-acceptance.spec.md`.
- Done. Amended the M1, M4, and M6 specs with the M7 acceptance evidence that
  changed how those active specs should be read.
- Done. Amended the runtime-composition decision to correct the old subsystem
  default claim.
- Done. Updated the M7 compatibility note, wiki index, and V1 roadmap. The
  roadmap is ready for a V1 tag and later archival after release management.
- Done. Marked this plan completed only after the Phase 5 verification matrix
  passed.

Verification:

- `rtk cargo fmt --all -- --check` - clean.
- `rtk git diff --check` - clean.
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings` -
  clean.
- `rtk cargo test --workspace --all-features --lib` - 317 passed across 18
  suites.
- `rtk cargo test --workspace --lib` - 317 passed across 18 suites.
- `rtk just lint` - passed outside the sandbox after the sandboxed run hit the
  known Axum local-socket `PermissionDenied`. This reran fmt, workspace clippy,
  no-default-features check, OpenTelemetry opt-out checks, `cargo doc -D
  warnings`, and workspace lib tests.
- M7 heavy Redpanda/Postgres gates are recorded in Phase 4 and were not rerun in
  Phase 5 because this phase changed documentation and comments only after the
  Phase 4 acceptance commit.
- Example smoke/fault/telemetry commands remain documented in `examples/README.md`;
  no compose example command was rerun in Phase 5.

Exit:

- V1 acceptance evidence is recorded, the roadmap is ready for release-tag
  closeout, and there are no active M7-only docs claiming future intent as
  validated truth.
