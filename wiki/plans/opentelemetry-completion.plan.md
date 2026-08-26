# OpenTelemetry Completion Plan

- Document Class: Plan
- Status: Active — Phases 0-3, 5, and 6 completed 2026-08-25 and corrected across two review passes on 2026-08-25/26; Phase 4's example pipeline and compose profile ported 2026-08-27, with full Kibana visibility verification still pending
- Date: 2026-08-25
- Category: Observability execution
- Scope: Turns M6's OpenTelemetry instrumentation into a working, verified, exportable telemetry pipeline — metrics, traces, and logs — and proves it end to end against a real backend.
- Sources:
  - wiki/specs/m6-observability-operability.spec.md
  - wiki/decisions/observability-operability-policy.decision.md
  - wiki/compatibility/m6-observability-operability-api.compat.md
  - crates/kafkaman-worker/src/metrics.rs
  - crates/kafkaman-rdkafka/src/metrics.rs
  - crates/kafkaman-core/src/lifecycle.rs
  - crates/kafkaman-rdkafka/src/publisher.rs
  - crates/kafkaman-rdkafka/src/ingest_record.rs
- Related:
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/metric-instrument-and-attribute-schema.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## The Premise

M6 delivered OpenTelemetry *instrumentation*. It did not deliver an
OpenTelemetry *pipeline*.

The workspace depends on `opentelemetry = "0.32"` — the API crate — and nothing
else. There is no `opentelemetry_sdk` anywhere in the repository. Every counter
resolves through `global::meter("kafkaman")` to the no-op provider, in every
build and every test. `apps/axum-outbox` installs a `tracing_subscriber` but no
`MeterProvider`, so an adopter copying the example gets no metrics at all.

The consequence is not that the metrics are wrong. It is that nothing in this
repository has ever observed one. The single metrics defect found so far —
`ingest.records` counting every record twice — was caught by reading the code
during review, and the fix is guarded by a `debug_assert` that disappears in
release builds.

Three of the four telemetry surfaces an operator expects are absent entirely:

| Surface | State |
| --- | --- |
| Metrics | Instrumented, never exported, never asserted |
| Traces | 17 `tracing::` sites, no OTel tracer, no export, no propagation |
| Logs | `tracing` events only; no OTel log bridge, no trace correlation |
| Context propagation | None; a trace cannot cross a Kafka hop |

## Deliverable

An adopter sets one environment variable, points it at a collector or an
Elasticsearch OTLP endpoint, and sees kafkaman's queue depth, publish and
dispatch latency, per-message traces that span the Kafka hop between two
services, and logs that pivot to those traces by `trace_id`.

## In Scope

- An SDK-backed pipeline the library never hard-codes but the example fully wires.
- Instrument construction that survives an SDK installed after process start.
- The missing instrument kinds: latency histograms and queue-depth gauges.
- `tracing` → OpenTelemetry span bridging.
- W3C trace context persisted across the outbox hop and propagated through Kafka
  headers.
- An OTel log appender with `trace_id`/`span_id` correlation.
- A `tests/observability/` suite that asserts all of the above against real
  Postgres, real Redpanda, and a real exporter.
- Docker compose for the example: Elasticsearch and Kibana, added to the
  example's existing stack behind a profile, exporting direct over OTLP/HTTP.

## Out Of Scope

- Any exporter dependency in a library crate. Choosing an exporter is the host's
  decision; library crates depend on the API crate only.
- Profiling and continuous-profiling signals.
- Replacing the `kafkaman-axum` admin routes. They are an operator escape hatch,
  not the metrics pipeline, and they stay.
- A hosted-Elastic account. The end-to-end gate runs against a local container.

## Phase 0 — Make Instruments Observable At All

**Completed 2026-08-25.** Steps 1-4 landed in `otel phase 0: own metric
instruments per loop`. Step 5 — wiring the example — was prototyped in
`apps/axum-outbox` and then reverted: that example is being replaced wholesale by
the one under construction in a separate worktree, and telemetry wiring belongs
in the example that survives. Its purpose is served instead by
`tests/observability/otlp_wire`, which builds all three providers in a real
process and asserts the export on the wire; the reasoning that step 5 was meant
to force — that a host installs its pipeline before anything kafkaman builds — is
pinned by `tests/observability/provider_ordering`, which runs a relay *before*
installing an SDK and asserts the second run still collects.

Two deviations from the text below, both recorded in the log entry: instrument
construction is owned by whatever owns the instrument rather than by the loop
alone — the publisher binds at construction, which widens the host contract — and
a host with no OTLP endpoint configured should install no provider at all rather
than export into a closed port.

**The defect this phase fixes.** `worker_metrics()` and `kafka_metrics()` cache
their `Counter` handles in a process-wide `OnceLock`, built from
`global::meter("kafkaman")` at first use. `global::meter()` binds to whatever
provider is installed *at that moment*. Install the SDK after the first record
and every instrument stays bound to the no-op for the life of the process —
silently. No error, no warning, no metrics.

This is a live adopter footgun: a host that starts a kafkaman relay before
building its OTel pipeline gets a permanently silent telemetry surface and no
indication why. It also makes the whole surface untestable, because an
integration test binary is one process with one global provider.

**The fix.** Move instrument construction out of the process-wide `OnceLock` and
into the scheduler that owns the loop, built when the loop starts. This keeps
`global::meter()` as the source — which is what OpenTelemetry recommends for
instrumentation libraries — while removing the process-lifetime binding. Two
providers in one process then work, which is what makes Phase 5 possible.

Steps:

1. Replace `OnceLock<WorkerMetrics>` / `OnceLock<KafkaMetrics>` with instruments
   owned by the run loop, created at loop start alongside `SchedulerAttrs`.
2. Keep the `enabled`/`disabled` twin structure and the identical call sites.
   The pair must not drift; that is why both live in one file.
3. Add `opentelemetry_sdk = "0.32"` as a **dev-dependency** only.
4. Prove it: a test that records once, *then* installs an SDK provider, then
   starts a loop, and asserts the loop's records reach the exporter.
5. **Wire a minimal SDK into `apps/axum-outbox`** — a metrics-only
   `MeterProvider` with an OTLP/HTTP exporter, installed before any runtime loop
   starts, and flushed on the existing drain path.

**Why step 5 belongs here rather than in Phase 4.** The example currently
installs a `tracing_subscriber` and no `MeterProvider`, which means it
demonstrates precisely the startup ordering this phase exists to warn against.
Every adopter copying it inherits a silent metric surface. Leaving that until
the end also means every instrument added in Phases 1–3 is verifiable only
inside the test suite, never in a real process — and the host-side ordering
contract, which is a contract *about* application startup, would never be
exercised by an application.

Minimal is the operative word: metrics only, one exporter, no traces, no logs,
no compose changes. Phase 4 completes it.

**Exit:** a metric recorded by a kafkaman loop is readable from an in-memory
exporter installed after process start, and the shipped example exports its own
metrics to a real endpoint.

## Phase 1 — Complete The Metrics Surface

**Completed 2026-08-25.** All five steps landed, with their Phase 5 binaries
alongside rather than trailing: `metrics_surface` (names, kinds, units,
attributes, values from a real relay loop), `queue_gauges`,
`queue_gauge_staleness`, `single_cycle_silence`, and `ingest_disjointness`
(`--features redpanda`, real broker, the invariant of step 4 asserted against
real ingest cycles rather than only by `debug_assert`). Three findings are
recorded as amendments to the schema decision: bucket boundaries are part of the
compatibility surface and must be declared, the semconv attributes belong on
every series keyed by topic rather than only on the publish counter, and gauge
staleness cannot be a gap — an async gauge under cumulative temporality
republishes its last value, so `kafkaman.queue.sample_age` reports the snapshot's
age instead. Step 5's decision was already filed, and needed no change beyond
those amendments.

Only counters exist today. The two instrument kinds an operator actually opens a
dashboard for are both missing.

1. **Latency histograms.** `kafkaman.relay.publish.duration`,
   `kafkaman.dispatch.duration`, and — the one that matters for an outbox —
   `kafkaman.outbox.time_to_publish`, the wall time from `occurred_at` to broker
   ack. That last one is the number that tells an operator whether the outbox is
   keeping up, and it cannot be derived from any counter.
2. **Observable queue-depth gauges.** `outbox_status_summary` and
   `received_status_summary` already compute depth and oldest-row age against
   real Postgres, and both are integration-tested. Registering them as
   observable gauges converts a route an operator has to remember to curl into a
   series a dashboard scrapes. Highest value-per-line in this plan.
3. **Declare units.** No instrument currently calls `.with_unit(...)`. Backends
   use it for axis formatting and conversion.
4. **Promote the disjointness invariant.** `record_ingest_stats`'s
   `debug_assert_eq!` is the only thing standing between us and a repeat of the
   double-count defect, and it is compiled out of release. Assert it in the test
   suite against real ingest cycles.
5. **Semantic conventions — decision required.** Instrument names are
   `kafkaman.*`, which is correct for a library-specific namespace. But the
   *attributes* are ad-hoc (`topic`, `message_type`, `scheduler`, `status`,
   `outcome`, `reason`). Adding the standard messaging attributes alongside them
   — `messaging.system=kafka`, `messaging.destination.name`,
   `messaging.operation.name` — lets Kibana group kafkaman's series by the same
   fields as every other messaging library in the deployment. Recommendation:
   keep the `kafkaman.*` instrument names, add the standard attributes. Needs a
   decision page before implementation.

**Exit:** depth, age, and latency are all scrapable series with declared units
and documented attributes.

## Phase 2 — Traces

**Completed 2026-08-25.** All six steps landed, including the migration. Four
findings are recorded as amendments to
`wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`: the
received table needs the same two columns as the outbox (ingest and dispatch are
separated by a durable gap the decision's diagram did not draw), a tracer with no
caller span still produces context because `enqueue` opens its own span, outgoing
user headers named `traceparent` are dropped at publish, and the W3C format is
implemented by hand because the propagators live in the SDK. A fifth is recorded
against the ownership decision: `tracing-opentelemetry` is now allowed in library
crates, having been checked to pull no SDK.

Pinned by `tests/observability/trace_propagation` (`--features redpanda`: one
message, both durable gaps, a real broker, and the full parent/link shape),
`trace_absent`, `trace_root_enqueue`, and three unit tests in
`kafkaman-rdkafka` covering the header namespace.


This is the half M6 did not build, and the half that makes Kibana's APM view
work rather than just its dashboards.

1. **Bridge.** `tracing-opentelemetry = "0.33"` (the release that pairs with
   `opentelemetry` 0.32) exports the existing `tracing` spans as OTel spans. The
   17 instrumentation sites become real spans without rewriting them.
2. **Span model.** `kafkaman.enqueue` (inside the caller's transaction),
   `kafkaman.relay.publish`, `kafkaman.ingest`, `kafkaman.dispatch`.
3. **The outbox trace-continuity problem.** Enqueue and publish are separated in
   time by the relay — that is the entire point of the pattern. A producer span
   opened at publish time is therefore orphaned from the business transaction
   that created the row, and the trace an operator most wants to see is exactly
   the one that breaks. Trace context must be persisted *with the row* and
   restored at publish.

   There is precedent, and it is exact: `correlation_id` already survives this
   hop. It travels `Envelope.correlation_id` → outbox row column →
   `kafkaman-correlation-id` header. W3C `traceparent`/`tracestate` follow the
   same path, and the outbox row grows one nullable column for them.
4. **Cross-service propagation.** Inject `traceparent` into Kafka record headers
   on publish; extract on ingest. Per messaging semantic conventions the
   consumer span **links** to the producer span rather than parenting from it,
   because a consumer polls a batch that may span many traces. This is what
   produces a service map for the two-service distributed-cache example, and it
   is the single most visible payoff in this plan.
5. **This phase contains the plan's only irreversible step.** Persisting trace
   context adds columns to every per-type outbox table, in a library whose
   change-management contract is itself a specced surface. Every other step in
   this plan is additive code that can be revised or reverted; a migration that
   adopters have run cannot. It needs a changeset, a compatibility note entry,
   and review weight matching a public API change — not the weight of adding an
   instrument.
6. **Header namespace — decided, and it amends a ratified decision.**
   `traceparent` is a W3C standard name and must **not** carry the `kafkaman-`
   prefix; a non-kafkaman consumer has to be able to read it. But
   `RecordHeaders` today partitions headers into exactly two namespaces —
   reserved `kafkaman-` and user — so `traceparent` would land in the user
   namespace and be handed to application code as though the producer set it.
   W3C trace headers need to be a third, explicitly-recognized namespace.

   OQ5 — the host-context boundary — is **already ratified** as opaque headers
   with a reserved `kafkaman-*` namespace, and
   `wiki/decisions/message-identity-and-header-namespace.decision.md` is the
   Accepted decision fixing it at two namespaces. So this is not an open
   question being closed; it is a **ratified decision being amended**, and it is
   treated as the heavier act it is. The amendment is deliberately minimal: a
   third namespace holding exactly `traceparent` and `tracestate`, every other
   rule untouched. Recorded in
   `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`.

**Exit:** a message enqueued in service A and consumed by service B produces one
trace, with the relay publish attributed to the originating transaction.

## Phase 3 — Logs

**Completed 2026-08-25**, in the example. `opentelemetry-appender-tracing`
exports `tracing` events as OTel log records, stamped with the `trace_id` and
`span_id` of the span they were emitted in — which is the whole feature, and the
reason it had to follow Phase 2 rather than precede it. The lifecycle events
`LifecycleSampler` already emits become correlated log records with no further
work, which is what `sample_success` was for.


1. **`opentelemetry-appender-tracing = "0.32"`** exports `tracing` events as
   OTel log records, automatically stamped with the active `trace_id`/`span_id`
   once Phase 2 lands. That stamping is the whole feature: it is what lets an
   operator click a log line in Kibana and land in the trace waterfall.
2. **Keep the plain-JSON path documented.** `tracing-subscriber`'s JSON output
   shipped by Filebeat is the classic ELK route, needs none of this plan, and is
   the right answer for an adopter who wants logs and nothing else. Document
   both; default the example to OTLP.
3. **Lifecycle events feed this.** `LifecycleEmission`/`LifecycleSampler` already
   decide which per-message successes emit an event. Under Phase 3 those become
   sampled OTel log records correlated to their trace — which is what
   `sample_success` was for.

**Exit:** a sampled success event in Kibana links to the trace that produced it.

## Phase 4 — Wiring And Example

**Steps 1 through 4 are now ported onto the rebased two-service example.** No
exporter appears in any `crates/` manifest; the SDK and OTLP exporter are held by
the `order` and `product` example binaries, and by `tests/observability`. Step 4
still stands: no `kafkaman-otel` crate, because pinning adopters to our choice of
exporter versions is a real cost in an ecosystem that releases breaking 0.x
versions in lockstep.

The 2026-08-27 port put the example's own pipeline in the shared
`examples/telemetry` crate, which both service binaries depend on, and
extended `examples/compose.yaml` with an `observability` profile containing
Elasticsearch and Kibana. The binaries install no provider when every OTLP
endpoint variable is absent or blank, so the default example remains quiet; `just
examples observe` supplies `OTEL_EXPORTER_OTLP_ENDPOINT=http://elasticsearch:9200/_otlp`
and starts the observed stack.

`tests/observability/otlp_wire` is the working reference for that port: it builds
a `MeterProvider`, a `TracerProvider`, and a `LoggerProvider` over OTLP/HTTP,
installs the subscriber that bridges `tracing` into both traces and logs, runs a
real relay loop, flushes all three on shutdown, and asserts kafkaman's own
instrument names, span names, and log lines on the wire.


1. **No exporter in a library crate.** `opentelemetry-otlp` appears in the test
   suite, and in whichever application ships as the example. Never in `crates/`.
2. **Complete the example's pipeline.** The two service binaries install a
   metrics provider, tracer provider, log provider, `tracing-opentelemetry`
   bridge, and OTel log bridge before building any kafkaman runtime loops. Their
   shutdown path drains the service first, then shuts down all installed
   providers so the final export window is not skipped.
3. **Docker compose — smaller than expected.** The two-service example already
   ships `examples/compose.yaml` with `postgres`, `redpanda`, `console`,
   `product`, and `order`, alongside a `Dockerfile`, `README.md`, and
   `smoke.sh`. Telemetry adds `elasticsearch` and `kibana` to that existing file
   behind a compose profile, so the example stays runnable without them. This is
   an extension of a working stack, not a new one — a material scope reduction.
   The example exports **directly to Elasticsearch over OTLP/HTTP** with no
   collector; the collector is documented as the production topology. Rationale
   in `wiki/decisions/telemetry-backend-and-example-topology.decision.md`. A
   Kibana dashboard artifact is still pending.
4. **A `kafkaman-otel` convenience crate is deferred, not rejected.** One call
   that builds the standard pipeline is attractive, but it pins adopters to our
   choice of exporter crate versions — a real cost in an ecosystem that releases
   breaking 0.x versions in lockstep. Document the wiring first; extract the
   crate only if the example proves it is genuinely repetitive.

**Exit still pending:** run `just examples observe`, walk the smoke lifecycle,
and confirm kafkaman metrics, traces, and correlated logs are visible in Kibana.
Kibana currently opens with no data view over the OTLP indices, so packaging one
is part of clearing this exit rather than a separate nicety.

## Phase 5 — The Test Suite

**Completed 2026-08-25.** Every binary the table below names exists, plus five
the plan did not anticipate. `metrics_disabled` is the one exception, and it is
covered differently: a test binary cannot assert a `--no-default-features` build
from inside a default build, so `just lint` now runs
`cargo check -p kafkaman --no-default-features` and the no-op twins are compiled
by the same gate as everything else.

The unanticipated binaries, each of which exists because a property turned out to
need pinning: `single_cycle_silence` (the public `relay_once` records nothing),
`queue_gauge_staleness` (an async gauge republishes its last value, so staleness
is a series rather than a gap), `trace_absent` and `trace_root_enqueue` (the two
different meanings of "no trace context"), and `otlp_wire` (kafkaman's telemetry
asserted on the wire in all three signals, moved here from the example so the
proof does not depend on which example currently ships).

Two more came out of the second review pass, at fourteen binaries in total:
`ingest_span_covers_decode` (a record that fails to decode still gets a
`kafkaman.ingest` span and still links to the producer — the path nobody watches,
because quarantine keeps the partition moving and the queue looking healthy) and
`queue_gauge_ordering` (the gauges bind to the provider installed when the first
sampler starts, permanently, so a host that starts sampling before wiring its
pipeline gets silence).


`tests/observability/` as a sibling workspace member, mirroring `durable-send`'s
layout: a shared `src/` harness, one `tests/<concern>/main.rs` per binary.

| Binary | Proves |
| --- | --- |
| `metrics_surface` | Instrument names, kinds, units, attribute sets, and values, from real relay/dispatch/ingest cycles through an in-memory SDK exporter. Includes the disjointness invariant as a real assertion. |
| `provider_ordering` | Phase 0's fix: an SDK installed after first record still collects. |
| `trace_propagation` | Context survives both durable gaps: enqueue → relay publish in one trace, ingest → dispatch in another, joined by a link. **Corrected 2026-08-26:** this row originally said "one trace id spans enqueue → … → dispatch", which contradicts Decision 6 of the propagation decision — a consumer links, and a link starts a new trace. The test asserts two trace ids and a link between them. |
| `admin_http` | Every admin route over real HTTP against real Postgres, plus the correlation round trip. The routes are currently only exercised through `admin_free_router()`. |
| `lifecycle_events` | `sample_success = 0.5` yields exactly ⌊n/2⌋ events across cycles; the default is silent. |
| `metrics_disabled` | `--no-default-features` compiles and records nothing. |

**Constraint that shapes the split:** one global provider per process. Metric
assertions inside a binary must be separable by attribute (`message_type`,
`topic`) or serialized. Phase 0 relaxes this but does not remove it.

**Optional end-to-end gate.** A feature-gated test exporting OTLP into an
`otel/opentelemetry-collector` or Elasticsearch testcontainer, asserting the
backend received what we sent. It tests the SDK more than it tests kafkaman, so
it is opt-in like the `redpanda` feature — but it is the only thing that proves
the wire format, and it is what makes the ELK claim in this plan a fact rather
than an expectation.

## Phase 6 — Documentation

**Completed 2026-08-25**, except the deployment procedure, which is deferred with
the compose profile it documents.


- ~~Decision page: metric semantic conventions and attribute schema.~~ Filed
  2026-08-25 as
  `wiki/decisions/metric-instrument-and-attribute-schema.decision.md`.
- ~~Decision page: W3C trace headers as a third header namespace.~~ Filed
  2026-08-25 as
  `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`.
- **Still deferred, and for a second reason now:** a reference page documenting
  the Elastic deployment as a procedure — compose invocation, Kibana setup,
  dashboard import, troubleshooting. The two-service example it extends is
  mid-merge, and the `apps/axum-outbox` example that briefly carried the host
  wiring has been reverted to its `main` state because it is being replaced
  wholesale by the example under construction in a separate worktree. Telemetry
  wiring belongs in the example that survives, and the procedure is filed once it
  has been executed there.
- ~~Update `wiki/specs/m6-observability-operability.spec.md`: it currently
  describes metrics as delivered without recording that no pipeline exists.~~
  Done 2026-08-25. The spec now records that M6 shipped instrumentation rather
  than a pipeline, names the two statements that became wrong rather than merely
  partial, and points at the compatibility note for the current surface.
- ~~Compatibility note: the new outbox column, the header namespace change, the
  instrument additions, and the provider-ordering contract.~~ Done, across four
  sections: the provider-ordering contract, the metric schedule, the queue
  metrics loop, and the trace-context schema/API/wire format.
- ~~`kafkaman.example.toml`: the three `[observability]` fields currently marked
  **reserved** — `level`, `payload`, `headers` — should be re-examined here.~~
  Re-examined 2026-08-25 and **left reserved**, with the reasoning recorded in
  the M6 spec's Limitations so it is not re-opened: `level` would duplicate and
  fight the host subscriber's filter, and `payload`/`headers` still have nothing
  to govern — the metric-schema decision forbids deriving any attribute from
  message content, which makes the reserved answer permanent on that surface.

## Sequencing

Phase 0 is a prerequisite for everything, and it is small. It now also carries
the minimal example wiring, so that from the first phase onward there is a real
process exporting real telemetry to look at.

The remaining work is **three signals**, and naming them that way is deliberate:

| Phase | Signal | Depends on | Cost |
| --- | --- | --- | --- |
| 1 | Metrics | Phase 0 | Moderate; gauges are nearly free, histograms are new call sites |
| 2 | Traces | Phase 0 | Largest. Schema migration, header amendment, cross-service propagation |
| 3 | Logs | Phase 2 for correlation | Smallest. Largely an appender and configuration |

Phases 1 and 2 are independent and can run as parallel lanes. Phase 4 needs
1–3. Phase 5 tracks each phase rather than trailing it — the suite is how each
phase is known to work, so each phase lands with its binary.

**Correcting earlier guidance in this plan.** An earlier revision said "if the
plan must be cut, cut Phase 3 before Phase 2." That is wrong in a way worth
recording, because it looks reasonable. Phase 3 is cheap *precisely because*
Phase 2 has already run: once spans exist, the log appender stamps every record
with `trace_id`/`span_id` almost for free, and that stamping is the entire
log→trace pivot in Kibana. Cutting Phase 3 after paying for Phase 2 therefore
forfeits most of the log-side value for a very small saving. The correct cut, if
one is needed, is **Phase 2 and Phase 3 together** — dropping traces as a unit
and shipping metrics only. Traces without logs is a coherent product; logs
without traces is only the classic Filebeat path, which needs none of this plan.

Logs are also the signal most easily forgotten — an external readiness review of
this work omitted them entirely while covering metrics and traces in detail.
That is the reason they get a named row above rather than a trailing phase.

## Relationship To The Merge

M6's stated exit criterion — an operator can see queue depth and age, inspect
and redrive the DLQ, and detect stuck rows — is met, and that surface is
integration-tested against real Postgres. This plan is not a prerequisite for
that criterion.

It *is* a prerequisite for the spec's claim that kafkaman delivers OpenTelemetry
metrics being verifiable rather than merely asserted. Phase 0 and Phase 5's
`metrics_surface` binary are the minimum that makes the existing claim true.
Everything after that is new capability.

## What The Review Passes Changed

**Added 2026-08-26.** Three implementation reviews followed the phases above.
They matter to this plan rather than only to the changelog, because each found
defects in work the plan had already marked completed — and in each case the
completion was recorded honestly against the phase's own exit criterion. The
criteria were the problem.

- **Phase 0's exit was "an instrument is observable".** It was, and the
  `metrics`/`traces` opt-out the same phase advertised was nevertheless false:
  `kafkaman --no-default-features` still linked `opentelemetry`, because two
  sibling crates pulled `kafkaman-core` with default features and Cargo unifies
  features across the graph. No build failed, because there is no build that
  could. Only `cargo tree` can answer it, which is why `just opt-out` now does,
  in the fast gate and in CI.
- **Phase 3's exit was "events carry trace ids".** They did. The rate at which
  they were emitted was wrong: `LifecycleSampler` rounded `sample_success` to the
  nearest reciprocal, so `0.75` emitted every success. Invisible from the config
  file, the logs, and the metrics alike.
- **Phase 2's exit was "context survives both durable gaps".** It did, and a
  relay built without `traces` stripped `traceparent` from every message it
  published — breaking propagation for the instrumented services *around* it,
  which is not a gap this phase thought to look at.

- **The migration written in response to the first review had its own version of
  this.** `AddReceivedFailureMetadata` added the DLQ's two failure columns, and
  its exit was "a legacy table converges on the current shape". It did. The rows
  in it did not: the columns arrived empty, so every pre-existing dead letter
  rendered with a failure kind it could not be filtered or redriven by. A
  migration's exit criterion is what the data looks like afterwards, not what the
  schema does.

The generalisable lesson, recorded because it will apply to the next plan as
much as this one: a phase whose exit criterion is "the feature works" is
verified against the path the author had in mind. The three defects above all
sat one step to the side of it — in the dependency graph rather than the build,
in the rate rather than the record, in the *absence* of a feature rather than
its presence. Where a phase makes a claim about something a build cannot check,
the exit criterion has to name the check.

The full list of changes is in
`wiki/compatibility/m6-observability-operability-api.compat.md` under
"Review-Pass Changes", "Second Review-Pass Changes" and "Third Review-Pass
Changes".
