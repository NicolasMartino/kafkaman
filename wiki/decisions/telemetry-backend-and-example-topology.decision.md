# Telemetry Backend and Example Topology

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-25
- Category: Observability tooling and examples
- Scope: Selects Elastic as kafkaman's reference observability backend, fixes the export topology the two-service example demonstrates, and separates that from the topology recommended for production.
- Sources:
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/plans/two-service-distributed-cache-example.plan.md
  - examples/compose.yaml (two-service example worktree)
  - Elastic Docs, "Elasticsearch OTLP/HTTP endpoint", retrieved 2026-08-25
  - Elastic Observability Labs, "Native OpenTelemetry support in Elastic Observability", retrieved 2026-08-25
- Related:
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/metric-instrument-and-attribute-schema.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md

## Decision

1. **Elastic is the reference backend.**
   Elasticsearch for storage, Kibana for the operator view. It is the backend the
   documented example targets and the one the end-to-end gate runs against.

2. **A reference backend is not a required backend.**
   kafkaman emits OTLP through the host's exporter and has no knowledge of any
   backend. Nothing in `crates/` names Elastic, and any OTLP-compatible backend
   works identically. "Reference" means *the one we prove it against*, not the
   one adopters must use, and the documentation must not blur that.

3. **The example exports directly to Elasticsearch over OTLP/HTTP.**
   No collector in the example topology. Elasticsearch ingests OTLP natively,
   derives metric dimensions and mappings from OTLP metadata, and creates the
   time-series data streams itself. One fewer service in the compose stack, one
   fewer configuration file, and it demonstrates the native path.

4. **The production recommendation is a collector.**
   Documented explicitly and separately from the example, so nobody reads a
   compose file as an architecture recommendation. See *Why the Example and
   Production Differ*.

5. **OTLP/HTTP, never OTLP/gRPC.**
   Elasticsearch's OTLP endpoint is HTTP-only. This is the most likely
   misconfiguration in the whole plan and it fails in a confusing way, so it is
   stated in the decision, in the example configuration comments, and in the
   troubleshooting section of the eventual reference page.

6. **The stack extends the example's existing compose file.**
   The two-service example already has `examples/compose.yaml` with `postgres`,
   `redpanda`, `console`, `product`, and `order`, plus a `Dockerfile`,
   `README.md`, and `smoke.sh`. Telemetry adds `elasticsearch` and `kibana` to
   that file. It does not create a parallel stack, and the example keeps one
   entry point.

7. **Telemetry is opt-in within the example.**
   The example must remain runnable without it. A compose profile turns the
   telemetry services on; with the profile off, the services run with no exporter
   configured and behave exactly as they do today. An example whose subject is
   entity propagation should not require two extra containers to demonstrate
   entity propagation.


**Note added 2026-08-26.** The example does not yet exist, and neither does the
pipeline it will build. `tests/observability/otlp_wire` is the working reference
in the meantime: it constructs a `MeterProvider`, a `TracerProvider` and a
`LoggerProvider` over OTLP/HTTP, installs the subscriber that bridges `tracing`
into both traces and logs, flushes all three on shutdown, and decodes the
exported protobuf to assert what arrived. The example's wiring should match it
rather than being derived independently — a second construction of the same
pipeline is a second thing to get subtly wrong, and only one of the two has a
test.

## Why Elastic

The requirement that decides it: **all three signals, one backend, no
translation layer.** Elasticsearch ingests OTLP metrics, traces, and logs
natively over HTTP without a collector, and Kibana's APM view is built around
trace-and-log correlation — which is exactly the artifact Phase 2 and Phase 3
produce. A backend that handled metrics well and traces poorly would leave two of
three signals unproven by the example, which would defeat the point of having one.

The alternative worth taking seriously was Prometheus and Grafana: the
conventional Rust choice, and better-known to most Rust developers. It was
rejected for the reference deployment on signal coverage. Prometheus is a metrics
system; traces and logs need Tempo and Loki beside it, which turns one backend
into three and makes the example about observability plumbing rather than about
kafkaman.

Jaeger plus Prometheus was rejected for the same reason, more so.

## Why the Example and Production Differ

Stating this in the decision, because a compose file is the most-copied artifact
in any repository and will be copied into production by someone.

**The example goes direct** because the fewest moving parts make the clearest
demonstration. The reader is learning what kafkaman emits, not how to operate a
collector.

**Production wants a collector** for four reasons the example does not care
about:

- **Batching and retry.** An application exporting directly holds telemetry in
  process memory when the backend is unreachable, and drops it on restart. A
  collector owns that buffer outside the application's lifecycle.
- **Redaction and enrichment.** Attribute policy — dropping, hashing, adding
  deployment metadata — belongs in one place, not recompiled into every service.
- **Fan-out.** Sending the same telemetry to more than one destination is a
  collector's job.
- **Credential surface.** Direct export puts backend credentials in every
  application. A collector reduces that to one component.

The documentation presents the collector as the production topology and the
direct path as the example's deliberate simplification. Both configurations are
shown.

## What Kibana Shows, By Phase

| After | Kibana capability |
| --- | --- |
| Phase 1 | Metric dashboards: queue depth, oldest-row age, publish and dispatch latency, time-to-publish |
| Phase 2 | APM traces: per-message waterfalls, and a service map spanning the Kafka hop |
| Phase 3 | Log records pivoting to their trace by `trace_id`, including sampled lifecycle events |

The service map is the demonstration this is ultimately for. The two-service
distributed-cache example is, as it stands, two applications that happen to share
a broker — nothing renders the relationship between them. With trace context
crossing the Kafka hop, Kibana draws `order` and `product` as one system with a
messaging edge between them. That picture is the clearest available statement of
what kafkaman does.

## Sequencing Against the Example

The telemetry work and the two-service example are separate deliverables in
separate worktrees, and this decision does not couple their schedules.

Phases 0 through 3 of the telemetry plan are proven by `tests/observability/`
against Postgres and Redpanda testcontainers, and need nothing from the example.
The Elastic stack is a *demonstration* built on top of a finished example, not a
dependency of the instrumentation.

The example's worktree is presently mid-merge with unresolved conflicts — it is
being reconciled against the same module refactor the M6 port was replayed onto.
The telemetry documentation therefore describes the compose extension as designed
but does **not** yet document it as a working procedure. A reference page with
run instructions is filed only once the stack has been run. Documenting a
procedure for something that cannot currently build is the failure mode the lint
pass exists to catch, and filing it early would put a self-inflicted contradiction
in the wiki.

## Consequences

- The example's compose stack grows two services behind a profile. Elasticsearch
  wants roughly 2 GB of memory; the README must say so, because a stack that
  silently OOMs on a laptop is worse than one that documents its appetite.
- A Kibana dashboard definition for the kafkaman instruments becomes a
  maintained artifact, and it depends on the metric schedule fixed by
  [metric-instrument-and-attribute-schema](metric-instrument-and-attribute-schema.decision.md).
  Renaming an instrument breaks it, which is one more reason that schedule is a
  compatibility surface.
- Elastic version drift becomes a maintenance item, as it already is for the
  pinned Redpanda and Postgres images in the same file.
- The optional end-to-end gate — exporting into a real Elasticsearch
  testcontainer and asserting what it received — becomes possible. It is the only
  check that proves the wire format rather than our belief about it, and it stays
  opt-in like the `redpanda` feature because it is slow.
