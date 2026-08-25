# M6 Observability and Operability API

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-24 (attribution corrected 2026-08-25)
- Category: Public API and operational surface
- Scope: Public API, dependency, config, and test-infrastructure changes introduced by M6 observability/operability.
- Sources:
  - wiki/specs/m6-observability-operability.spec.md
  - wiki/decisions/observability-operability-policy.decision.md
  - crates/kafkaman-config/src/observability.rs
  - crates/kafkaman-sqlx/src/operability.rs
  - crates/kafkaman-sqlx/src/replay.rs
  - crates/kafkaman-worker/src/metrics.rs
  - crates/kafkaman-rdkafka/src/metrics.rs
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman/src/lib.rs
  - crates/kafkaman-core/src/lifecycle.rs
  - crates/kafkaman-core/src/status.rs
  - apps/axum-outbox/src/main.rs
  - kafkaman.example.toml
  - tests/durable-send/src/containers.rs
- Related:
  - wiki/compatibility/m4-retry-backoff-runtime-api.compat.md
  - wiki/compatibility/m5-outbox-retention.compat.md

## Public API Additions

`kafkaman-config` adds:

- `ObservabilityConfig`
- `ObservabilityPolicy`
- `ObservabilityPolicyOverride`
- `ObservabilityLevel`
- `LifecycleLogging`
- `PayloadLogging`
- `HeaderLogging`
- `Config::observability`
- `Config::observability_config`

- `ObservabilityPolicy::lifecycle_emission`

`kafkaman-core` adds:

- `LifecycleEmission`
- `LifecycleSampler`
- `RelayConfig.lifecycle`
- `OutboxStatus::is_terminal`
- `ReceiveStatus::is_terminal`
- `rfc9557::option`, a serde adapter for `Option<OffsetDateTime>`.

`kafkaman-sqlx` adds:

- `ResolvedConfig.observability`
- `ResolvedConfig::with_observability`
- `ReceivedTable::for_descriptor`
- `Replay::received_descriptor`
- `Replay::RUNTIME_VERSION`
- `outbox_status_summary`
- `received_status_summary`
- `outbox_stuck_rows`
- `received_stuck_rows`
- `redrive_received`
- summary/stuck row structs for outbox and received inspection.

`kafkaman-axum` is a new crate. The facade crate re-exports it behind the new
`kafkaman/axum` feature. Its public surface is:

- `CorrelationLayer`, `CorrelationService`, `CorrelationId`,
  `CORRELATION_ID_HEADER`, `MAX_CORRELATION_ID_LEN`
- `AdminState`, `admin_router`, `AdminError`
- `StuckResponse`, `DlqSummary`, `DlqRowSummary`, `RedriveRequest`,
  `RedriveResponse`, `MAX_REDRIVE_ROWS`
- `serve`, `RuntimeServer`, `RuntimeTask`, `RuntimeError`,
  `DEFAULT_DRAIN_TIMEOUT`.

## Breaking Changes

These are additive-looking but source-breaking for existing callers:

- `outbox_status_summary` and `received_status_summary` take a `max_queue_age:
  Duration` argument, which decides the new `over_max_queue_age` flag. It never
  filters rows, so counts are unaffected.
- `run_dispatcher` takes a `lifecycle: LifecycleEmission` argument before
  `shutdown`. `LifecycleEmission::default()` preserves the previous behavior
  (summary only, no per-message events).
- `RelayConfig` gained a `lifecycle` field. `RelayConfig::default()` and the
  config-resolution path both fill it; struct-literal construction must add it.
- `Replay::message_type` was removed before any release used it. It had no
  callers.
- `ReceivedStatusSummary`/`OutboxStatusSummary` gained `over_max_queue_age`.

The timestamp fields on every M6 inspection struct now serialize as RFC 9557
strings rather than `time`'s derived nine-integer array. Any consumer written
against the array form must be updated; the array form was not a usable wire
contract.

## Config Changes

`kafkaman.toml` now accepts optional `[observability.defaults]` and
`[observability.messages.<message_type>]` sections. Omitted observability config
uses defaults and remains backwards-compatible. If the section is present,
unknown fields, invalid enum values, bad sampling values, zero thresholds, or
overrides for unregistered message types fail config resolution.

`kafkaman.example.toml` now documents the section and is covered by the existing
executable example-config test.

## Dependency Changes

The workspace adds direct `opentelemetry = 0.32` API usage for metrics, behind a
default-on `metrics` feature on `kafkaman-worker` and `kafkaman-rdkafka` and
forwarded by the facade. Building with `--no-default-features` drops the
dependency entirely while keeping the loops. Worker and rdkafka crates record
through the global `kafkaman` meter; host applications still choose exporters and
SDK setup.

The workspace adds a reusable `kafkaman-axum` crate and a facade `axum` feature.
Applications that do not enable the feature do not pull Axum through the facade.

The workspace adds a shared `tower = 0.5` entry, which `kafkaman-axum` uses for
`Layer`/`Service` and which `apps/axum-outbox` now inherits instead of pinning
its own copy.

`kafkaman-axum` does not depend on `kafkaman-config`; it reads observability
policy through `ResolvedConfig`.

## Semver Notes

Adding `ResolvedConfig.observability` is semver-sensitive for downstream code
that constructs `ResolvedConfig` with a struct literal. `ResolvedConfig::new` and
builder-style helpers are the compatibility-preserving construction path.

The new SQL inspection APIs expose sanitized operational summaries. They do not
promise full durable-row JSON shapes and deliberately do not expose payloads or
arbitrary user headers from admin routes.

`redrive_received` is operational and mutating. It retains the existing M4 guard:
only terminal received rows are redriven, and history is preserved unless the
caller explicitly clears it. It requires `max_rows`, and the admin route caps
that at `MAX_REDRIVE_ROWS`. Because a runtime redrive never enters the changelog,
callers should pass `Replay::RUNTIME_VERSION` rather than inventing a version
number.

Timestamps on all inspection structs serialize through `kafkaman_core::rfc9557`.
This is a deliberate per-field choice rather than enabling
`time/serde-human-readable`, which would also change `ReceivedError`'s
established wire format.

## Test Infrastructure

Redpanda full-loop tests do not bind the fixed host port `19092`; the helper
allocates a free host port and advertises it to host-side clients, so the
all-features suite can run alongside local example infrastructure already using
the old port. Port selection is inherently advisory — the container binds the
port after the helper releases it — so the helper retries on a fresh port up to
five times.

**Attribution corrected 2026-08-25.** An earlier revision of this note credited
that change to M6 and named the helper `start_redpanda`. On the current base it
came from the module-and-test-separation refactor, not from M6, and the helper
is `redpanda()` in `tests/durable-send/src/containers.rs` — `start_redpanda_on`
performs the bind and `available_host_port` picks the port. M6 hit the same
fixed-port collision independently and fixed it the same way; the two changes
were redundant and only one survives. The behavior described above is accurate;
only the provenance and the helper name were wrong.

## Example Application Change

**Superseded 2026-08-26. `apps/axum-outbox` carries none of this on the
observability branch, and is being replaced.**

The M6 work described here — enabling `kafkaman/axum`, mounting `admin_router`
under `/internal/kafkaman`, applying `CorrelationLayer`, and supervising the
relay with `serve().with_runtime()` — landed in the example on `main` and is
recorded because it describes behavior adopters copy. It is left in place as the
record of what M6 did to the example, not as a description of this branch, whose
`apps/` tree is identical to `main`'s.

The host-side OpenTelemetry pipeline was prototyped there and reverted: the
example is being replaced wholesale by the one under construction in a separate
worktree, and telemetry wiring belongs in the example that survives rather than
being written twice. What that wiring has to do is not lost — the ordering
contract is below, and `tests/observability/otlp_wire` builds all three providers
and asserts their export on the wire, which makes it a working reference for the
port.

Exporter dependencies remain confined to `apps/` and `tests/`; on this branch
that means `tests/` alone. No `crates/` manifest carries an SDK or an exporter.

## Metric Provider Ordering Contract

**Added 2026-08-25 (OpenTelemetry completion, Phase 0).** No public type changed,
but host-visible behavior did, so it is recorded here rather than left implicit.

An OpenTelemetry instrument binds to whichever `MeterProvider` is installed when
the instrument is *created*, and the API offers no way to rebind one. M6 cached
its instruments in process-wide `OnceLock`s, so the provider installed at the
first metric recording was the provider every later component reported to, for
the life of the process. A host that started a relay before finishing its
telemetry pipeline got a permanently silent metric surface — no error, no
warning, no way to work out why.

Instruments are now built by the component that owns them:

| Instruments | Bind when |
| --- | --- |
| `SchedulerMetrics` (relay, dispatcher, purger) | the run loop starts |
| `PublishMetrics` (`RdkafkaPublisher`) | the publisher is constructed |
| `IngestMetrics` | the ingest loop starts |

The contract for adopters is therefore **install your `MeterProvider` before
constructing kafkaman components or starting kafkaman loops**, not merely before
starting loops — the publisher binds earlier than the loops do. A host that gets
the order wrong now loses metrics only for the components it built early, rather
than for the whole process.

`PublishMetrics` is `Clone` because `RdkafkaPublisher` is; cloning shares the
instrument handles, so a cloned publisher reports to the same provider as its
original rather than rebinding to whatever is installed later.

All counters now declare units (`{cycle}`, `{row}`, `{error}`, `{record}`,
`{commit}`). Backends use them for axis formatting; no series name or attribute
changed.

`opentelemetry_sdk` is a workspace **dev-dependency**. No `crates/` manifest
gained an SDK or exporter dependency, per
`wiki/decisions/telemetry-pipeline-ownership.decision.md`.

## Metric Schedule

**Added 2026-08-25 (OpenTelemetry completion, Phase 1).** Instrument names,
kinds, units, bucket boundaries, and attribute keys are a public compatibility
surface: each one is a dashboard, an alert, or a recording rule we cannot see.
Changing a row below carries the weight of a public API change. Fixed by
`wiki/decisions/metric-instrument-and-attribute-schema.decision.md`.

| Instrument | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `kafkaman.scheduler.cycles` | Counter | `{cycle}` | `scheduler`, `message_type` |
| `kafkaman.scheduler.rows` | Counter | `{row}` | `scheduler`, `message_type`, `status` |
| `kafkaman.scheduler.errors` | Counter | `{error}` | `scheduler`, `message_type` |
| `kafkaman.kafka.publish.records` | Counter | `{record}` | `topic`, `outcome`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.kafka.ingest.records` | Counter | `{record}` | `message_type`, `outcome`, `messaging.system` |
| `kafkaman.kafka.ingest.commits` | Counter | `{commit}` | `message_type`, `messaging.system` |
| `kafkaman.kafka.ingest.errors` | Counter | `{error}` | `message_type`, `reason`, `messaging.system` |
| `kafkaman.relay.publish.duration` | Histogram | `s` | `topic`, `outcome`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.dispatch.duration` | Histogram | `s` | `message_type`, `outcome` |
| `kafkaman.outbox.time_to_publish` | Histogram | `s` | `message_type` |
| `kafkaman.outbox.depth` | Observable gauge | `{row}` | `message_type`, `status` |
| `kafkaman.outbox.oldest_age` | Observable gauge | `s` | `message_type`, `status` |
| `kafkaman.received.depth` | Observable gauge | `{row}` | `message_type`, `status` |
| `kafkaman.received.oldest_age` | Observable gauge | `s` | `message_type`, `status` |
| `kafkaman.queue.sample_age` | Observable gauge | `s` | none |

Everything below the `ingest.errors` row is new in this slice; the rows above it
gained only units and the two `messaging.*` attributes, so an existing dashboard
keeps working.

Histogram bucket boundaries, in seconds, are declared rather than defaulted — the
OpenTelemetry defaults are millisecond-scaled and would put every observation in
the first bucket of a series declared in seconds:

- `relay.publish.duration`, `dispatch.duration`:
  `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`
- `outbox.time_to_publish`:
  `0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900`

Semantics worth knowing before alerting on these:

- `dispatch.duration` spans the whole `dispatch_once` call — claim, handler, and
  commit — not the handler alone, and cycles that claimed nothing are not
  recorded. Its `outcome` is `processed`, `failed`, or `stale`.
- `outbox.time_to_publish` measures `occurred_at` to broker acknowledgement, and
  is recorded on acknowledgement regardless of what the subsequent mark returns.
  A row whose `occurred_at` is in the future — database and process clocks
  disagreeing — is dropped rather than recorded as a negative latency.
- The queue gauges report an explicit `0` for a status with no rows, so a
  draining queue draws a line to zero rather than stopping. `oldest_age` reports
  nothing for an empty status, because the age of no rows is not zero.
- `kafkaman.queue.sample_age` is how staleness is reported. An asynchronous gauge
  under cumulative temporality republishes its last value forever, so a stalled
  sampler cannot produce a gap; alert on the sample age, not on the absence of a
  depth reading.

## Queue Metrics Loop

**Added 2026-08-25 (Phase 1).** `kafkaman-worker` adds, behind its `metrics`
feature:

- `run_queue_metrics(pool, outbox_tables, received_tables, cfg, shutdown)`
- `QueueMetricsConfig` — `refresh_interval` (15s), `query_timeout` (5s),
  `max_queue_age` (300s) — and `QueueMetricsConfig::validate`
- `Error::InvalidQueueMetricsConfig { field, reason }`

It is a loop like the relay, the dispatcher, and the purger: run one bounded unit
of work, log it, pace against a shutdown token, and return `Err` only for a
configuration that can never succeed. It owns the summary queries and the gauge
callbacks read the snapshot it maintains, because an observable-gauge callback is
synchronous and cannot await a database round trip.

Two operational notes for adopters:

- **Run one per process.** The gauges register with the global meter when the
  loop starts, and two loops covering the same tables report the same rows twice.
- **The refresh interval drives the queries, not the scrape interval.** Each
  refresh is a `GROUP BY status` aggregate per table. Scraping faster does not
  query harder, which is the reason the interval is the loop's rather than the
  SDK's.

`opentelemetry-semantic-conventions` (with `semconv_experimental`) is a new
optional dependency of `kafkaman-worker` and `kafkaman-rdkafka`, enabled by their
`metrics` features. It contributes attribute-key constants only — no runtime, no
SDK — so the ownership boundary is unchanged.

## Trace Context: Schema, API, and Wire Format

**Added 2026-08-25 (OpenTelemetry completion, Phase 2).** This is the only part
of the telemetry work that is not purely additive code: it migrates every
per-type outbox *and* received table. Adopters who apply it cannot unapply it,
so it carries the review weight of a public API change.

### Schema

Two nullable, unindexed columns on both table families:

| Column | Type | Meaning |
| --- | --- | --- |
| `traceparent` | `TEXT` | W3C trace context, `version-trace_id-parent_id-flags` |
| `tracestate` | `TEXT` | The vendor list, when an upstream set one |

Fresh tables get them from `create_outbox_table_sql` / `create_received_table_sql`.
Existing tables are upgraded by two new changesets, `AddOutboxTraceContext` and
`AddReceivedTraceContext`, both `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

**Existing rows keep `NULL`, and null is a state the runtime already treats as
ordinary** — a row with no trace context publishes and dispatches exactly as
before. Nothing reads these columns expecting a value, so a deployment that
applies the changesets and rolls the binary back keeps working.

### Public API

`kafkaman-core` adds:

- `TraceContext`, with `from_parts`, `traceparent`, and `tracestate`
- `capture_trace_context`, `set_parent`, `add_link`
- `OutboxRow.trace` and `ReceivedRow.trace`, both `Option<TraceContext>`

The two row-struct fields are a **breaking change to struct literal
construction**: code building an `OutboxRow` or `ReceivedRow` by hand must add
`trace: None`. Field access and pattern matching with `..` are unaffected.

`kafkaman-sqlx` adds the `AddOutboxTraceContext` and `AddReceivedTraceContext`
changesets.

Nothing exposes an OpenTelemetry type. `TraceContext` is two strings and the
operations are verbs, so the public surface does not change shape when the
feature is off.

### The `traces` feature

Default-on, with a no-op twin, mirroring `metrics`: `kafkaman-core`,
`kafkaman-sqlx`, `kafkaman-worker`, `kafkaman-rdkafka`, and the `kafkaman` facade
all carry it. Turning it off drops the `opentelemetry` and
`tracing-opentelemetry` dependencies and stops this process from *producing*
trace context — but stored context is still read from a row and still forwarded
onto the wire, so a service with tracing compiled out does not break propagation
for the services around it.

`tracing-opentelemetry` is a new library-crate dependency. It is the API-side
bridge and pulls no SDK; see the amendment on
`wiki/decisions/telemetry-pipeline-ownership.decision.md`.

### Wire format

`traceparent` and `tracestate` are a third Kafka header namespace, alongside the
reserved `kafkaman-` namespace and user headers. They carry **no `kafkaman-`
prefix**: the value of the standard is that a consumer which has never heard of
kafkaman still recognizes them.

- **On publish**, kafkaman sets them from the span the publish is running in —
  which the relay parented from the context stored at enqueue. A `traceparent`
  or `tracestate` present in a row's *user* headers is dropped rather than
  published, and logged at debug.
- **On ingest**, they are matched case-insensitively, resolved first-wins like
  the reserved namespace, removed from the user headers a handler receives, and
  routed to trace extraction. A producer sending them is the normal case, not an
  error. A malformed value costs the trace and nothing else.

### Spans

Four spans, whose names are as much a compatibility surface as the metric names:

| Span | Where | Parenting |
| --- | --- | --- |
| `kafkaman.enqueue` | inside the caller's transaction | child of the caller's span, or a root |
| `kafkaman.relay.publish` | per row, in the relay loop | child of the row's stored context |
| `kafkaman.ingest` | per consumed record | root, **linked** to the producer |
| `kafkaman.dispatch` | around handler execution | child of the received row's stored context |

Each carries `messaging.system`, `messaging.destination.name`, and
`messaging.operation.name` alongside kafkaman's own attributes.

The consumer links rather than parents because it polls a batch that may hold
records from many unrelated traces. `kafkaman.dispatch` parents rather than
links, because by then exactly one row has been claimed.

## Lifecycle Success Events Move Into The Publish Span

**Changed 2026-08-25 (Phase 3).** Sampled per-message success events
(`lifecycle = "per-message"` with a non-zero `sample_success`) were emitted from
the relay loop after each cycle, outside any span. They are now emitted per row,
inside that row's `kafkaman.relay.publish` span, with the span's OpenTelemetry
context attached.

The rate is unchanged: `LifecycleSampler` still emits every n-th success and its
count still carries across cycles. What changes is that each event is now
attributable to the message it describes and carries `trace_id`/`span_id`, which
is the log-to-trace pivot the setting exists for. An event that cannot be traced
back to its message is a line in a log file.

`kafkaman-core` adds `attach` and `TraceScope` for this. **The reason it is
needed is an ecosystem detail worth knowing**: `opentelemetry-appender-tracing`
stamps a log record from the *OpenTelemetry* context current at emission, and
does not read the `tracing` span stack. `tracing-opentelemetry` bridges spans
without attaching them to the OpenTelemetry context, so a `tracing` event inside
a `tracing` span reaches the log signal with no trace ids unless something
attaches them. `attach` is that something, and it is scoped so the guard never
crosses an `await`.

An adopter emitting their own events inside kafkaman's spans — a dispatch
handler, say — hits the same gap and can use the same call. The receive-side
lifecycle event is still emitted from the dispatcher loop and is **not**
correlated, because the `kafkaman.dispatch` span closes inside `dispatch_once`
before the loop sees the stats; closing that gap means moving the sampler into
`kafkaman-sqlx` or widening `DispatchStats`, and neither is worth doing before
someone wants it.
