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
  - crates/kafkaman-rdkafka/src/consumer.rs
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman-otel/src/lib.rs
  - crates/kafkaman/src/lib.rs
  - crates/kafkaman-core/src/lifecycle.rs
  - crates/kafkaman-core/src/status.rs
  - apps/axum-outbox/src/main.rs
  - kafkaman.example.toml
  - examples/order/kafkaman.toml
  - examples/product/kafkaman.toml
  - tests/observability/tests/trace_parented_handoff.rs
  - tests/example-telemetry/tests/binary_telemetry.rs
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
- `KafkaTraceHandoff`
- `LifecycleLogging`
- `PayloadLogging`
- `HeaderLogging`
- `Config::observability`
- `Config::observability_config`

- `ObservabilityPolicy::lifecycle_emission`
- `ObservabilityPolicy::kafka_trace_handoff`
- `ObservabilityPolicyOverride::kafka_trace_handoff`

`kafkaman-core` adds:

- `LifecycleEmission`
- `LifecycleSampler`
- `RelayConfig.lifecycle`
- `OutboxStatus::is_terminal`
- `ReceiveStatus::is_terminal`
- `rfc9557::option`, a serde adapter for `Option<OffsetDateTime>`.
- `record_error`, which marks a span failed with a **bounded** description.
- `db_span!` and `db_poll_span!`, the SQL client span shape below.
- `capture_trace_context_of`, which captures the context of a **named** span
  rather than of whichever span is current. See "Durable capture names its span"
  below; this is the safe one for anything that persists what it captures.

The two macros are exported from the crate root and reachable through the facade
as `kafkaman::db_span!` / `kafkaman::db_poll_span!`. They exist because the same
span was being hand-rolled in `kafkaman-sqlx` and in both example services, and a
span shape that four places re-derive is a span shape that drifts — these
attributes are a documented compatibility surface, so there is now one definition
of them.

They are macros rather than functions so the exported span name is
`concat!("db.query ", $summary)`, built at compile time from a string literal.
The functions they replaced called `format!` on every statement on every path,
whether or not any subscriber had the span enabled. `$summary` must therefore be
a literal; a caller needing a runtime summary should build the span itself.

`record_error` truncates at 256 bytes on a character boundary. A status
description is attacker-influenced the way a log line is — a database error can
quote the value that violated a constraint — and the status exists to say what
went wrong, which the first 256 bytes do.

`kafkaman-core` also re-exports `tracing` as `__tracing`. It is `#[doc(hidden)]`
and exists only so the two macros expand in a crate that does not depend on
`tracing` itself. It is not public API and may disappear.

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
- `ObservabilityPolicy` gained `kafka_trace_handoff`, and
  `ObservabilityPolicyOverride` gained the matching optional override. Defaults
  and config resolution fill both, but direct struct literals must add the field.
- `insert_received_with_outcome` gained a trailing
  `trace: Option<TraceContext>` parameter. Its caller passes the ingest span's
  context explicitly rather than letting the function read the ambient span —
  see below. `insert_received`, which the test harnesses and examples use, keeps
  its signature and captures ambiently, which is correct for a caller that has no
  phase span.
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

`observability.*.kafka_trace_handoff` accepts `linked` or `parented`. Omitted is
`linked`, preserving the OpenTelemetry messaging default where `kafkaman.ingest`
links to the propagated producer context. `parented` makes ingest continue the
producer trace, intended for single-record APM waterfalls. Per-message overrides
win over `[observability.defaults]`.

`kafkaman.example.toml` now documents the section and is covered by the existing
executable example-config test. The shipped product and order example configs
set `observability.defaults.kafka_trace_handoff = "parented"` so `just examples
all` produces one Elastic APM trace across the HTTP, relay, Kafka ingest, and
dispatch work.

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

`kafkaman-rdkafka` now depends directly on `kafkaman-config` to read the resolved
`KafkaTraceHandoff` value in the consumer ingest path. The dependency is spelled
`default-features = false`, but that is defensive rather than load-bearing:
`kafkaman-config` declares no features, and the guard that keeps `opentelemetry`
out of an opt-out build is its own `kafkaman-core = { default-features = false }`
pin. `just opt-out` is what proves the contract, and it still passes.

## Semver Notes

Adding `ResolvedConfig.observability` is semver-sensitive for downstream code
that constructs `ResolvedConfig` with a struct literal. `ResolvedConfig::new` and
builder-style helpers are the compatibility-preserving construction path.

Adding `ObservabilityPolicy.kafka_trace_handoff` and
`ObservabilityPolicyOverride.kafka_trace_handoff` has the same struct-literal
cost for callers that construct those config structs directly. TOML users get a
backwards-compatible default of `linked`.

Both structs also gained `Copy`. `policy_for` is now called per record on the
ingest path, and every field of both is already `Copy`, so resolving a policy is
a map lookup and a memcpy with nothing to allocate and nothing to drop. Adding
`Copy` is additive for callers; the only observable difference is that a moved
value stays usable.

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
| `kafkaman.kafka.ingest.records` | Counter | `{record}` | `message_type`, `outcome`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.kafka.ingest.commits` | Counter | `{commit}` | `message_type`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.kafka.ingest.errors` | Counter | `{error}` | `message_type`, `reason`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.relay.publish.duration` | Histogram | `s` | `topic`, `outcome`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.dispatch.duration` | Histogram | `s` | `message_type`, `outcome` |
| `kafkaman.outbox.time_to_publish` | Histogram | `s` | `message_type` |
| `kafkaman.outbox.depth` | Observable gauge | `{row}` | `message_type`, `status` |
| `kafkaman.outbox.oldest_age` | Observable gauge | `s` | `message_type`, `status` |
| `kafkaman.received.depth` | Observable gauge | `{row}` | `message_type`, `status` |
| `kafkaman.received.oldest_age` | Observable gauge | `s` | `message_type`, `status` |
| `kafkaman.queue.sample_age` | Observable gauge | `s` | none |

Everything below the `ingest.errors` row is new in this slice. The rows above it
kept their names and gained only units and the `messaging.*` attributes — but an
added attribute is an added dimension, so the three `ingest.*` counters split
into per-topic series. See *Ingest counters gained `messaging.destination.name`*
below for what that costs a dashboard already querying them.

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

**Amended 2026-08-27 — `RuntimeBuilder` now derives it.** Until then nothing
outside `tests/observability` called `run_queue_metrics`, so the five queue
gauges never registered in any real process. A host could not fix that itself:
the function takes `OutboxTable` and `ReceivedTable`, both on the forbidden list
in `tests/distributed-cache/tests/boot_surface.rs`. `Runtime::into_tasks` now
assembles the sampler alongside the other loops whenever the runtime owns any
outbox or received table and the `metrics` feature is on.

Two consequences worth stating:

- **`into_tasks` returns one more task than it used to.** Count on the roles you
  declared rather than on a fixed number, and note that this one is
  feature-dependent.
- **A host that already calls `run_queue_metrics` itself keeps winning.** The
  second sampler in a process gets `QueueMetricsAlreadyRunning`; the derived one
  logs a warning naming the tables it could not cover and then parks on the
  shutdown token rather than failing the runtime. Losing telemetry coverage must
  not become an outage — and two runtimes in one process is a real shape, which
  is how `tests/distributed-cache` runs.

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
| `kafkaman.ingest` | per consumed record | root linked to the producer by default; child of producer context in `parented` mode |
| `kafkaman.dispatch` | around handler execution | child of the received row's stored context |

Each carries `messaging.system`, `messaging.destination.name`, and
`messaging.operation.name` alongside kafkaman's own attributes.

The consumer links rather than parents by default because it can poll a batch
that holds records from many unrelated traces. `kafkaman.dispatch` parents
rather than links, because by then exactly one row has been claimed.

### Kafka trace handoff policy

**Added 2026-08-29.** The live observability policy now includes
`kafka_trace_handoff`, resolved from `[observability.defaults]` and
`[observability.messages.<message_type>]`:

| Value | `kafkaman.ingest` relationship | Trace shape |
| --- | --- | --- |
| `linked` | adds a span link to the propagated producer context | default two-trace OpenTelemetry messaging shape |
| `parented` | sets the propagated producer context as the parent | one distributed trace for single-record APM waterfalls |

The received row still stores the ingest span's context in both modes, so
dispatch and dispatch SQL naturally remain under ingest. The acceptance contract
for parented mode is the parent edge and shared trace id, not the presence of a
duplicate link.

### APM waterfall span additions

**Added 2026-08-29.** The first HTTP/SQL/span-kind slice changed the exported
trace schema. Dashboards and alerts that select spans by name or attribute
should account for these additions.

`kafkaman-axum::CorrelationLayer` now makes each request an OpenTelemetry server
span. The internal tracing span remains `http.request`, while the exported OTel
span name is route-shaped, for example `POST /products`, so Elastic APM groups
useful transactions separately from `/health`. The span records:

- `http.request.method`
- `http.route`
- `url.path`
- `http.response.status_code`

`http.route` and the exported span name are what a backend groups transactions
by, so both are bounded. A request that matched no route reports
`http.route = "<unmatched>"` rather than its raw URL: reporting the path there
would mint one transaction group per URL a scanner invents. The raw path is still
recorded, as `url.path`, which nothing groups by.

Error status is recorded for 5xx only. A 4xx is the server correctly refusing a
bad request, and marking those failed makes an APM error rate track client
mistakes rather than service health.

The two shipped examples now apply `CorrelationLayer` to their public routers,
so example request handlers run under that HTTP span. A service that uses
`kafkaman::axum::serve` directly still has to layer correlation itself; serving
does not rewrite the router.

The example applications and `kafkaman-sqlx` now emit client spans for
representative business and durable-state SQL. The internal tracing span remains
`db.query`, while the exported OTel span name is summary-shaped, for example
`db.query insert product` or `db.query mark outbox published`, so Kibana's
waterfall rows are readable. Every such span uses:

- `db.system.name = "postgresql"`
- `db.operation.name`
- `db.collection.name`
- `db.query.summary`

No bind values, request bodies, payloads, entity ids, tenant ids, or full SQL
text are recorded. `db.query.summary` is intentionally bounded vocabulary such
as `insert outbox row`, `mark outbox published`, or `apply received row to
cache`.

Recurring scheduler poll statements that often claim no work use the same bounded
schema but are emitted at `debug` level: `claim outbox batch`, `collapse stale
pending outbox rows`, `claim received row`, and the transactions around them —
`open outbox claim transaction`, `commit outbox claim transaction`, `open
received claim transaction`, and `commit empty received claim transaction`. The
default example `info` filter therefore shows lifecycle work without hundreds of
empty poll spans, while a debug run can still inspect scheduler polling itself.
`tests/example-telemetry` asserts that none of the seven reaches the wire at
`info`.

The existing kafkaman messaging spans keep their names and now set OpenTelemetry
span kind/status through the tracing bridge:

- `kafkaman.enqueue`: producer
- `kafkaman.relay.publish`: producer
- `kafkaman.ingest`: consumer
- `kafkaman.dispatch`: consumer

Failures on enqueue, relay publish/mark, ingest, and dispatch record error
status on the owning kafkaman span, through `kafkaman_core::record_error` and so
with a bounded description. Successful spans leave status unset except where the
Elastic APM enrichment derives `event.outcome` and transaction result.

`kafkaman.dispatch` marks ERROR only when the dispatch cycle itself could not
complete. A handler that returns an error and gets a retry scheduled is a
successful cycle by `dispatch_once`'s contract — the failure is recorded on the
row and counted in `DispatchStats` — so marking the span would make an APM error
rate count work the system is handling as designed.

### Bounding exported trace volume

**Added 2026-08-29.** Spans scale with traffic, and the accepted decision makes
sampling and retention part of the feature rather than a follow-up. Three
mechanisms, cutting at three points, none of them new API:

| Knob | Bounds | Owner |
| --- | --- | --- |
| `RUST_LOG` | what is recorded at all | the host's subscriber |
| `OTEL_TRACES_SAMPLER` / `_ARG` | which recorded traces are exported | the OpenTelemetry SDK, read directly |
| collector pipeline | what the backend stores | the reference deployment |

kafkaman intercepts none of them. The example collector uses the third to drop
spans whose `http.route` is `/health`, because compose polls each service's
health endpoint every two seconds and those spans would otherwise dominate the
stack. `examples/README.md` documents all three under "Keeping the trace volume
honest".

### Durable capture names its span

**Added 2026-08-29.** `capture_trace_context()` returns the context of whichever
span is current. Three call sites *persist or transmit* what it returns:

| Site | Where the context ends up |
| --- | --- |
| outbox enqueue | the outbox row's `traceparent` column |
| received insert | the received row's `traceparent` column |
| Kafka publish | the `traceparent` header on the wire |

Each means one particular span — the `kafkaman.enqueue`, `kafkaman.ingest`, or
`kafkaman.relay.publish` that names the phase — and nothing enforced that. Any
span opened between the phase span and the capture silently became the stored
context. The row stayed well-formed; it just pointed at a private function
instead of the documented phase, and in the publish case that value went onto the
wire for other services to parse.

All three now name their span. `capture_trace_context_of(&span)` is the primitive;
enqueue and ingest capture from their phase span and pass the value down, and the
publisher captures at the `Publisher::publish` boundary, where the relay's
`.instrument` guarantees the current span is still the phase span. The
`Publisher` trait is unchanged.

**The rule for anyone adding instrumentation:** a function that reads the ambient
span must not itself be wrapped in one. Three functions are in that position —
`RdkafkaPublisher::publish`, `publish_row`, and `insert_received` — and they are
excluded from the internal span tier for exactly this reason.

`RdkafkaPublisher::publish_row_traced` is new and public: it takes the context to
write. `publish_row` keeps its signature and captures ambiently, which is right
for a caller invoking it directly rather than from inside the relay.

### `kafkaman.handler`

**Added 2026-08-29.** A span around each application handler call, at `info`, so
a slow handler is no longer unattributed time inside `kafkaman.dispatch`. It
carries `otel.kind = "internal"` — the consuming happened at ingest; this is the
application's own work — plus `message_type` and `handler.position`, which is
`before` or `after` for the pre-upsert hook and the post-upsert handler.

Handler failures record their error status here rather than on
`kafkaman.dispatch`, which stays unmarked because a handler error that schedules
a retry is a successful dispatch cycle by `dispatch_once`'s contract. This is the
first span that distinguishes the two.

`kafkaman.handler` is a **stable** span name.

### The `kafkaman::internal` span tier

**Added 2026-08-29.** 74 functions across `kafkaman-sqlx`, `kafkaman-axum`,
`kafkaman-worker`, and `kafkaman-rdkafka` carry
`#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]`.
Reach them with:

```
RUST_LOG=info,kafkaman::internal=debug
```

The dedicated target exists so that reaching kafkaman's functions does not also
enable `sqlx` and `rdkafka` debug logging. `skip_all` is not stylistic: bare
`#[instrument]` records every argument via `Debug`, which would put envelopes,
rows, and payload bytes into spans and violate the no-payload rule above.

**These span names are not a compatibility surface.** They are function names and
will change whenever the functions do. Build dashboards on the `kafkaman.*` phase
spans and the `db.query` summaries.

Fifteen functions are deliberately excluded, in four categories: those that build
a phase span (a second one would nest a duplicate), those that run until shutdown
(a span covering hours is not a waterfall row), those that read the ambient span
(see the rule above), and bodiless trait declarations. `kafkaman-core` is
excluded entirely — it performs no I/O, so there is no time there for a span to
attribute.

**Measured cost.** A product-create request goes from 17 spans to 25. The
scheduler loops dominate everything else: with the tier on for one example
service and the stack otherwise idle, that service produced 2322 spans in two
minutes against 5 from the service without it. Nearly all of the difference is
`claim_batch`, `claim_received_row`, and the other per-cycle functions. This tier
is for a debugging session, not for a deployment.

The relay publish span now covers the post-publish outbox mark as well as the
broker call, so a slow or failing `mark outbox published` appears under
`kafkaman.relay.publish` rather than as an unrelated root database span.

With the example configs' `parented` handoff, Elastic APM trace samples can show
the full demo path in one timeline:
`POST /products` -> product SQL -> `kafkaman.enqueue` -> outbox SQL ->
`kafkaman.relay.publish` -> `kafkaman.ingest` -> `kafkaman.dispatch` -> order
cache SQL -> mark received processed -> mark outbox published. In `linked` mode,
the dashboard handoff panel and `just examples handoffs` remain the debugging
path for joining producer and consumer traces.

The example collector also duplicates the OpenTelemetry resource
`service.name` onto each trace span as the span attribute `service.name`. In
Elasticsearch's `otel` mapping that appears as `attributes.service.name`. This
is deliberately an example/backend usability transform, not a library span-name
change: the canonical OpenTelemetry service identity remains the resource
attribute, and the existing span names stay stable.

### `kafkaman-otel::init` filters telemetry layers directly

**Added 2026-08-29, corrected the same day.** The default `init` subscriber now
applies the `RUST_LOG` `EnvFilter` directly to the fmt layer, the OpenTelemetry
trace layer, and the OpenTelemetry log layer, instead of installing one filter on
the registry.

**This was first recorded here with the wrong reason.** The note claimed the
older registry-wide filter let the unfiltered OpenTelemetry layer enable debug
spans. It did not: in `tracing-subscriber`, a filter layer's `enabled` is ANDed
into the subscriber's, so it bounds every layer added after it too. That is now
pinned by `a_registry_wide_filter_also_bounds_the_layers_added_after_it` in
`crates/kafkaman-otel/src/tests.rs`, precisely so the wrong explanation cannot
come back.

What actually keeps scheduler polling out of the default export is that those
spans are `debug_span!` rather than `info_span!`, asserted end to end over real
OTLP bytes by `tests/example-telemetry`.

The change is therefore behaviour-neutral at the same filter, and is kept for a
different reason: with the filter attached per layer, a host can bound exported
telemetry differently from stdout. Composing your own subscriber now needs
`tracing_subscriber::Layer` in scope and a `.map(|layer| layer.with_filter(..))`
around each optional layer — see the crate docs for the current shape.

## Lifecycle Success Events Move Into The Publish Span

**Changed 2026-08-25 (Phase 3).** Sampled per-message success events
(`lifecycle = "per-message"` with a non-zero `sample_success`) were emitted from
the relay loop after each cycle, outside any span. They are now emitted per row,
inside that row's `kafkaman.relay.publish` span, with the span's OpenTelemetry
context attached.

The rate is unchanged by *that* move: the sampler's count still carries across
cycles. (The rate itself changed separately — see
"`sample_success` honours any rate" below.) What changes is that each event is now
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
handler, say — hits the same gap and can use the same call.

The receive-side event is correlated too, as of the review pass below. It is
emitted from inside `kafkaman.dispatch` rather than from the dispatcher loop,
which required the sampler to cross into `kafkaman-sqlx` — see
`dispatch_once_sampled`.

## Review-Pass Changes

An implementation review of this branch produced the following changes on top of
everything above. Each is here because it changes something an adopter can
observe.

### `kafkaman-sqlx::dispatch_once_sampled` (new)

`dispatch_once` is unchanged and emits no lifecycle events. `dispatch_once_sampled`
takes `&mut LifecycleSampler` and emits the sampled per-message success event
from inside the row's `kafkaman.dispatch` span, so the event carries the trace and
span ids of the dispatch it describes. `run_dispatcher` uses it; a host driving
dispatch by hand and wanting correlated success events should too.

The `kafkaman.dispatch` span also now opens when the row is *claimed* rather than
when the handler is called, so it covers the missing-handler path and the failure
accounting. A row whose message type has no registered handler previously
produced no span at all.

### `kafkaman-axum::redrive_router` (new), `admin_router` (narrowed)

**Breaking.** `admin_router` no longer carries `POST /dlq/{message_type}/redrive`;
it is read-only. The destructive route moved to `redrive_router`, so mounting it
is an explicit decision and can take a different auth policy from the reads:

```rust
let admin = admin_router(state.clone())
    .layer(read_auth)
    .merge(redrive_router(state).layer(write_auth));
```

A deployment that wants the previous surface merges the two with no layer, which
is fine where the whole listener is already private.

### `Config::observability` returns `Option`

**Breaking.** It was `Result<ObservabilitySection>` and returned `MissingKey` for
an absent section that the example config marks OPTIONAL. It is now
`Result<Option<ObservabilitySection>>`, matching `retention()`, and
`observability_config()` applies the defaults itself. `ResolvedConfig` behaviour
is unchanged — it was already special-casing this.

### Stuck rows gained `stuck_for_ms`

**Breaking for the receive side.** `OutboxStuckRow` and `ReceivedStuckRow` both
gain `stuck_for_ms`, and `ReceivedStuckRow::age_ms` changed meaning. The two
fields now mean the same thing on both types:

| Field | Meaning |
| --- | --- |
| `age_ms` | Time since `created_at` — how long the message has been waiting |
| `stuck_for_ms` | Time since the row became late — `claim_expires_at` for an outbox row, `due_at` for a received row |

`stuck_for_ms` is what `stuck_after` filters on. Previously `age_ms` measured
from `created_at` on the outbox side and from `due_at` on the receive side: one
field name, two answers, on one operator screen. Both reach the JSON as separate
keys.

### `outcome = "published"` on the Kafka publish counter

**Breaking for dashboards.** `kafkaman.kafka.publish.records` labelled a
successful send `acknowledged` while `kafkaman.relay.publish.duration` labelled
the same event `published`. It is `published` on both now, so `sum by (outcome)`
composes across them.

### Trace context is forwarded when nothing is tracing

`RdkafkaPublisher` falls back to the row's stored `traceparent` when there is no
current span — a build with `traces` off, or a host with no tracer. Previously
such a relay stripped the header, breaking the trace for downstream services that
*were* instrumented. Downstream now links to the enqueue rather than to the
publish: one hop coarser, same trace.

### `traceparent` parsing follows the W3C grammar exactly

Values that were previously accepted and are now dropped as malformed: uppercase
hex in any field, and any trailing content on a version-`00` header. Higher
versions still accept appended fields. A dropped value costs a trace and nothing
else — a record is never rejected for it — but a producer emitting uppercase hex
will see its context stop propagating through kafkaman.

### `run_queue_metrics` refuses a second concurrent sampler

Returns `Error::QueueMetricsAlreadyRunning` rather than starting. The gauges are
registered once per process and all read one snapshot, so two loops would
overwrite each other on every refresh. Stopping and restarting is supported and
is the case the process-wide registration exists to make safe; the consequence is
that the gauges bind to the meter provider installed when the *first* sampler in
the process starts.

### `sample_success` honours any rate

`LifecycleSampler` emitted every `round(1/rate)`-th success, so `0.75` emitted
100% and `0.66` emitted 50%. It now hits any rate in `0.0..=1.0` exactly. A
deployment that set one of the affected rates will see its event volume change to
what it asked for — downward, in both of those cases.

### The `metrics`/`traces` opt-out is now real

`kafkaman --no-default-features` previously still linked `opentelemetry`, because
`kafkaman-config` and `kafkaman-axum` pulled `kafkaman-core` with default
features. No API changed; the dependency graph did. `just opt-out` asserts it and
`just features` compiles all six `metrics`/`traces` combinations.

## Second Review-Pass Changes

A second implementation review followed the first. Everything below is on top of
the section above.

### Received tables: two new changesets

`AddReceivedFailureMetadata` and `AddReceivedFailedIndex`, in that order — the
index is on `last_failed_at`, so the column has to exist first.

`AddReceivedFailureMetadata` is **not optional** for a received table created
before `last_failed_at` and `last_failure_kind` existed, which is the one thing
that distinguishes it from every other additive changeset here. Those columns are
named by every DLQ inspection query and written by every failed dispatch, so a
table without them does not degrade — `received_failed_rows`,
`received_failed_count`, and `redrive_received` raise, on the path an operator
reaches for during an incident. The columns are nullable and existing rows keep
`NULL`, so the DLQ simply has no failure history from before the upgrade.

`AddReceivedFailedIndex` is a performance change and optional. It carries the
same warning as `AddOutboxRetentionIndex`: changesets apply inside a
transaction, so the index is built non-concurrently and blocks writes on the
received table while it runs — on a table large enough to want the index, that is
a window in which ingest cannot insert.

Fresh tables need neither. `CreateReceivedTable` now emits the partial index
alongside the table, which is a fourth statement in that changeset. Its
`checksum_material` is unchanged — it is built from the version, name, and
descriptor — so an already-applied `create_received_table` will **not** re-run and
will not gain the index. Existing deployments add `AddReceivedFailedIndex` to
pick it up.

### `TraceContext` has exactly one constructor

`Default` is gone (an empty `traceparent` is not a trace context) and
`Deserialize` is hand-written to validate rather than assign fields.
Deserializing a malformed `TraceContext` is now an error.

Rows are unaffected: `OutboxRow::trace` and `ReceivedRow::trace` read through a
lenient adapter that drops context which no longer parses, exactly as a column
read does. A row serialized before the grammar was tightened still loads, minus
the trace link. Nothing in kafkaman deserializes a bare `TraceContext`; adopters
who do will see the error.

### `tracestate` is validated and normalized

Previously forwarded verbatim if non-empty. Now checked against the W3C list
grammar and rewritten canonically: whitespace around commas dropped, empty list
members dropped, and more than 32 members truncated from the right as the
Recommendation prescribes. A `tracestate` that breaks the grammar is discarded on
its own — the `traceparent` beside it survives, because the trace id is what
correlates.

Adopters see two changes: a stored or forwarded `tracestate` may differ
byte-for-byte from what arrived (`"a=1, b=2"` becomes `"a=1,b=2"`), and a
malformed one is no longer passed on under kafkaman's name.

### `traceparent`: every appended field is checked

The previous pass rejected an empty field appended after the flags. It checked
only the first, so `01-…-01-aa-` was accepted. Every appended field is now
checked, and a value with more than 12 of them is not treated as a
`traceparent` — the header is stored in a column and rewritten on every hop, so
an unbounded field count is an unbounded header travelling under kafkaman's name.

### Redrive accepts the failure kind the DLQ prints

`POST /dlq/{message_type}/redrive` now accepts either spelling of
`failure_kind`: the RFC 9457 `type` URI that DLQ inspection renders in
`latest_error.type`, or the bare discriminant. Purely additive — the discriminant
still works. An unrecognized value is still refused rather than resolved to a
default, because on a destructive route the difference is which rows move.

### `kafkaman.ingest` opens before the record is decoded

The span now covers envelope decoding, and a record that fails to decode is
traced rather than silently unspanned. Adopters see decode time inside
`kafkaman.ingest` where previously the span excluded it, so recorded durations
grow by the decode cost.

### OpenTelemetry features are narrowed per axis

The workspace dependency is `default-features = false`, and each crate enables
only the axis it uses: `kafkaman-core` the trace API, `kafkaman-worker` and
`kafkaman-rdkafka` the metrics API.

What each axis actually links — both columns stated, because naming only the
first invites reading the second as a promise it does not make:

| Build | `opentelemetry` features enabled |
| --- | --- |
| `--features metrics` | `metrics`, and nothing else |
| `--features traces` | `trace`, plus the optional dependencies `trace` itself enables: `futures`, `futures-core`, `futures-sink`, `pin-project-lite`, `thiserror` |

So a `metrics`-only build links none of `trace`, `logs`, `internal-logs` or
`futures`. A `traces` build **does** link `futures`: that is upstream's own
composition of its `trace` feature, not something kafkaman adds, and narrowing
cannot remove it.

Nothing is lost. Cargo features are additive, so a host that wants any of them
adds it — and a host installing `opentelemetry_sdk` gets all four back through
the SDK's own defaults. `just opt-out` asserts both the forbidden pairs and the
exact metrics set, so the narrowness cannot decay. The traces set is documented
rather than asserted: it is upstream's optional-dependency list, and pinning it
would turn one of their patch releases into a red build here.

### Ingest counters gained `messaging.destination.name`

`kafkaman.kafka.ingest.records`, `.commits` and `.errors` now carry the topic.
They previously carried `message_type` and `messaging.system` only, which left
them with no attribute in common with `kafkaman.kafka.publish.records` beyond a
constant — so "are we consuming this topic as fast as we publish to it" was not a
question the telemetry could answer.

Adding an attribute changes series identity. An existing dashboard or recording
rule on these three instruments sees the series split until it is updated.

### Queue gauges: install the provider before the first sampler

Not a change, but a consequence that was undocumented at the API and is now
stated on `run_queue_metrics`. The gauges register once per process — OpenTelemetry
0.32 offers no way to unregister an observable gauge — so they bind to whichever
`MeterProvider` is installed when the first sampler in the process starts, and no
later install or sampler restart can rebind them. Every other kafkaman instrument
is exempt, because the run loops build theirs at start.

The failure mode is silent: every other kafkaman series arrives and the queue
series are simply absent. `tests/observability/queue_gauge_ordering` pins it.

## Third Review-Pass Changes

A third implementation review followed the second. Everything below is on top of
both sections above.

### `AddReceivedFailureMetadata` now backfills

The changeset adds `last_failed_at` and `last_failure_kind` **populated**, from
the `errors` audit trail they were always a projection of. Adding them empty —
which is what it did before — left every pre-existing dead letter visible and
unreachable at the same time: `/dlq` renders the newest audit entry's `type`,
while every filter reads the columns, so a redrive narrowed to the kind an
operator could plainly see matched nothing and reported success. The
`occurred_after` filter missed the same rows, and because `ORDER BY
last_failed_at` sorts NULLs last, a paged DLQ view dropped exactly those rows off
the end while the count beside it still counted them.

The backfill reads both spellings a stored kind can have — the RFC 9457 `type`
URI and the bare discriminant that predates it — and strips the `[UTC]`
annotation from the RFC 9557 timestamp. A row whose newest entry names a failure
class this binary does not know keeps `NULL` rather than being guessed at, and a
timestamp that does not parse costs only itself.

**Corrected 2026-08-26.** "Costs only itself" was the intent and not the
behaviour. The two columns are filled by two statements: the kind pass is string
equality against a generated `CASE` and cannot raise, but the timestamp pass ends
in a `::timestamptz`, and a shape test is not a parser. `2026-99-99T10:00:00Z`
matches every regex that describes an RFC 3339 date and still raises
`datetime_field_overflow` — so does `2026-02-30`, and no pattern can rule out a
day the calendar does not have. Inside one set-based `UPDATE` that raise aborted
the statement, the migration transaction, and the boot behind it: a single
corrupted audit entry turned into a process that would not start.

The timestamp pass is now a `DO` block — one set-based `UPDATE`, and only if that
raises, a second pass one row at a time with each cast in its own subtransaction.
**The operational consequence:** a table holding even one unparseable audit
timestamp backfills row-at-a-time rather than in a single statement, which is
slower on a large DLQ. Every other table pays nothing. Either way the migration
completes, the rows around the bad one recover, and the bad one keeps `NULL` for
its timestamp while still recovering its kind.

Adopters upgrading a table that already has the columns see the statement run and
match nothing: it is guarded on `last_failed_at IS NULL OR last_failure_kind IS
NULL` and coalesces rather than overwrites.

### The DLQ partial index gained a fourth key column

`(last_failed_at, created_at, message_id, last_failure_kind) WHERE status =
'Failed'`. The kind trails the ordering chain rather than leading it, so a
kind-filtered DLQ page keeps the index ordering *and* rejects other kinds inside
the index. Leading with the kind would have inverted the trade. The index is new
in M6 and unreleased, so this is a redefinition rather than a migration — no
adopter has the three-column form.

### `tracestate` rejects two more shapes, and is bounded

Values containing a tab, and lists with a repeated key, are now discarded — the
`traceparent` beside them survives, as always. A `tracestate` longer than 2048
characters is dropped without being parsed.

**This can change what an adopter has stored.** A `tracestate` kafkaman forwarded
before and rejects now stops being written to the column and stops being emitted
as a header. The `traceparent` is unaffected, so no trace loses its correlation —
only its vendor decoration.

Repeated `tracestate` *headers* on one record remain first-wins, matching
`traceparent`. That is a documented deviation from the Recommendation's
combine-with-commas rule, which exists for HTTP field splitting and does not
describe a Kafka multimap; see the trace-context decision.
