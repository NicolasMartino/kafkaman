# V1 Acceptance Envelope

- Document Class: Spec
- Status: Active
- Date: 2026-08-31
- Category: V1 acceptance
- Scope: Validated behavior accepted as the V1 envelope after M7 hardening. This records the library contract and evidence; cutting a release tag is release management outside this document.
- Sources:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/plans/m7-v1-hardening.plan.md
  - wiki/compatibility/m7-hardening-api.compat.md
  - wiki/specs/m1-durable-send.spec.md
  - wiki/specs/m2-change-engine-config.spec.md
  - wiki/specs/m3-durable-receive.spec.md
  - wiki/specs/m4-retry-backoff-dlq.spec.md
  - wiki/specs/entity-first-propagation.spec.md
  - wiki/specs/m6-observability-operability.spec.md
  - README.md
  - examples/README.md
  - tests/durable-send/tests/redpanda_full_loop
  - tests/distributed-cache
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/library-test-strategy.decision.md

## Accepted Envelope

V1 kafkaman is a Rust library for compact entity-cache propagation. Every
in-scope message is a full snapshot of one domain entity, keyed by that entity,
published to a compacted Kafka topic, and applied to a local Postgres cache
table with Kafka topic, partition, and offset as the convergence ordinal.

The accepted envelope is the composition of M1 through M7:

- Durable send: enqueue happens transactionally through per-type outbox tables.
  The relay publishes at least once, claims by lease, and republishes after an
  uncertain outcome. Duplicate publish after broker ack but before `Published`
  marking is expected and is absorbed by receive-side identity.
- Change and config: schema changes run through the versioned changelog engine,
  configuration resolves fail-fast from `kafkaman.toml`, and topic convergence
  runs in the configured mode before loops start.
- Durable receive: Kafka ingest writes either a received row or an ingest
  quarantine row before committing an offset. Idempotency keys dedupe normal
  redelivery, message-id conflicts quarantine, and one bad record does not stall
  the partition.
- Recovery: dispatch claims due rows with row locks, runs handlers inside
  savepoints, schedules retryable failures with backoff, records terminal
  failures in the table-backed DLQ, and supports bounded received-row redrive
  while preserving history by default.
- Entity cache: received rows carry entity identity, cache rows are guarded by
  Kafka offset, pending outbox rows are superseded per entity, and row-sourced
  `Replay::outbox` is rejected as unsafe for entity snapshots. The example
  demonstrates app-owned state-sourced republish; a generic positive republish
  API remains outside the accepted V1 library surface.
- Runtime topology: declared roles derive tables, migrations, topics, workers,
  publishers, and handlers. `RuntimeBuilder` starts all role-implied loops by
  default, while `Subsystems` lets worker binaries explicitly select relay,
  ingest, dispatch, purge, and queue-metrics loops.
- Supervision: embedded HTTP and worker topologies share cancellation. A loop
  that exits cleanly before shutdown is a supervision error, loop panics carry
  the named task, and facade runtime shutdown uses a bounded drain.
- Operability: OpenTelemetry instruments, spans, and logs are emitted but the
  host owns the SDK/exporter. Admin routes expose queue depth, stuck rows, DLQ
  inspection, bounded redrive, and read-only ingest-quarantine summaries.
- Storage growth: outbox is the only kafkaman-owned table family with an
  implemented purge loop. Received rows, cache rows, and ingest quarantine rows
  are retained because they are respectively dedupe history, state, and durable
  diagnosis.

## Verification Evidence

The V1 evidence is layered, with fast gates for library logic and heavier gates
for real infrastructure behavior.

- Unit and integration coverage under the workspace validates schema changes,
  config resolution, role-derived runtime assembly, purge policy, panic
  containment, runtime supervision, and admin surfaces.
- `tests/durable-send` validates the Postgres-backed send, receive, retry, DLQ,
  redrive, outbox retention, and entity-cache primitives.
- The optional `redpanda_full_loop` suite validates the real broker path,
  including broker publish/readback, ingest, dispatch, duplicate ingest, poison
  quarantine, consecutive-skip breaker behavior, offset-commit uncertainty,
  consume-then-produce, ack-before-mark republish dedupe, and broker-input
  retry/DLQ/redrive.
- `tests/distributed-cache` validates the two-service example over HTTP only,
  on both the facade builder and hand-wired boot paths.
- `examples/smoke.sh`, `examples/faults.sh`, and the telemetry example commands
  validate the local compose packaging and the operator-facing happy and failure
  paths. These are example smoke gates, not substitutes for the testcontainers
  acceptance tier.

The exact M7 command evidence is recorded in
`wiki/plans/m7-v1-hardening.plan.md`.

## Outside V1

V1 deliberately does not cover generic durable jobs, commands, payments,
analytics streams, synchronous service calls, non-entity work queues, a
standalone generic daemon, a kafkaman-owned OpenTelemetry SDK/exporter, received
row purge, cache row purge, or quarantine purge.
