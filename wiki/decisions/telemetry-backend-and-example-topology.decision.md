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

   **Amended 2026-08-27 — reversed.** The premise was false: Elasticsearch's
   `/_otlp` endpoint is metrics-only. See *Amendments*.

4. **The production recommendation is a collector.**
   Documented explicitly and separately from the example, so nobody reads a
   compose file as an architecture recommendation. See *Why the Example and
   Production Differ*.

5. **OTLP/HTTP, never OTLP/gRPC.**
   Elasticsearch's OTLP endpoint is HTTP-only. This is the most likely
   misconfiguration in the whole plan and it fails in a confusing way, so it is
   stated in the decision, in the example configuration comments, and in the
   troubleshooting section of the eventual reference page.

   **Still holds after the 2026-08-27 amendment**, for a different reason:
   `kafkaman-otel` builds an OTLP/HTTP exporter and offers no gRPC path, so the
   collector listens on 4318 and never on 4317.

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

> **Amended 2026-08-27.** The sentence below — "natively over HTTP without a
> collector" — is false for self-managed Elasticsearch: `/_otlp` serves metrics
> only. The *conclusion* survives, because Elastic still stores all three signals
> and is still the one backend; what was wrong is how they get there. Read the
> paragraph as the case for Elastic as a store, not as the case against a
> collector. See *Amendments*.

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

> **Amended 2026-08-27 — they no longer differ in this respect.** The example
> runs a collector, because it turned out it could not work without one. The
> four production reasons below still stand and are still the reason to run one
> deliberately rather than by accident; what is gone is the claim that the
> example demonstrates a simpler topology. It demonstrates the same one. The
> paragraph immediately below is kept because the reasoning it records is what
> the measurement overturned, and a decision that quietly deletes its own
> rejected premise teaches the next reader nothing.

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

The documentation presents the collector as the production topology. It is now
also the example's topology, so only one configuration is shown — and the
example's `examples/otel-collector.yaml` is a working starting point rather than
a sketch, which is a better artifact than the two-configuration split this
originally promised.

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

## Amendments

### 2026-08-27 — the example needs a collector after all

Decision 3 chose a direct export topology on the strength of one factual claim,
stated in *Why Elastic*: "Elasticsearch ingests OTLP metrics, traces, and logs
natively over HTTP without a collector." **That claim is false for
self-managed Elasticsearch**, and the example shipped for two days exporting two
of three signals into a void.

Measured against 9.3.5 on 2026-08-27, running the example stack:

| Path | POST | GET | Meaning |
| --- | --- | --- | --- |
| `/_otlp/v1/metrics` | 200 | 405 | Handler exists, POST-only |
| `/_otlp/v1/traces` | 400 | 400 | `no handler found for uri` |
| `/_otlp/v1/logs` | 400 | 400 | `no handler found for uri` |
| `/_otlp/v1/profiles` | 400 | 400 | `no handler found for uri` |

The `405` on metrics against `400` on the rest is what makes this conclusive
rather than a configuration guess: a `405` is a registered route rejecting a
verb, a `400 no handler found` is no route at all.

Two things made it survive review. The Rust SDK reports the failure as
`BatchSpanProcessor.ExportError ... HTTP export failed: network error`, which
names neither the status code nor the path, and reads like a transient
connectivity problem. And nothing in CI or the test suite exercises this — it
needs a running Elasticsearch, which is exactly the end-to-end verification
`wiki/plans/opentelemetry-completion.plan.md` Phase 4 had recorded as *pending*
the entire time. The pending item was not a formality.

**Even the metrics half was partial.** Sums arrived; the three explicit-bucket
histograms — `kafkaman.dispatch.duration`, `kafkaman.relay.publish.duration`,
`kafkaman.outbox.time_to_publish` — did not, with no export error, no
`_ignored` fields, and an empty failure store. The collector later named the
cause in one line that Elasticsearch never gave: `dropping cumulative
temporality histogram`. Elasticsearch stores delta temporality; the SDK emits
cumulative, which is the OTLP default.

**What changes.** The `observability` profile gains an
`otel/opentelemetry-collector-contrib` service configured by
`examples/otel-collector.yaml`. Services export to `http://otel-collector:4318`;
the collector's `elasticsearch` exporter writes all three signals with
`mapping.mode: otel`, and `cumulative_to_delta` converts the histograms.

**What does not change.** Decisions 1, 2, 4, 6 and 7 stand, and two of them are
strengthened rather than weakened:

- Decision 2 — a reference backend is not a required backend. The temporality
  conversion lives in the collector precisely because temporality is a property
  of the backend, not of the instrumentation. `kafkaman-otel` gained nothing
  Elastic-specific and still works unchanged against a cumulative backend.
- Decision 4 — production should use a collector. The example and production
  topologies now agree, which removes the gap *Why the Example and Production
  Differ* was written to explain. That section is now describing a difference
  that no longer exists.

The cost decision 3 was buying — one fewer service, one fewer config file — was
real. It was just not worth two thirds of the telemetry.

### 2026-08-27 — the queue gauges were never sampled

Found while verifying the above, and unrelated to the backend. `run_queue_metrics`
had no caller outside `tests/observability`, so the five queue gauges
(`outbox.depth`, `outbox.oldest_age`, `received.depth`, `received.oldest_age`,
`queue.sample_age`) were never registered in a running service. Its own doc
comment predicts the symptom exactly: "every other kafkaman series arrives
normally and the queue series are simply absent."

The host could not fix this itself. `run_queue_metrics` takes `OutboxTable` and
`ReceivedTable`, and both are on the forbidden list in
`tests/distributed-cache/tests/boot_surface.rs` — a blessed boot file that named
them would fail the test asserting the builder's whole premise. So `RuntimeBuilder`
now derives the sampler alongside the relay, ingester and dispatcher.

Because the gauges register process-wide, a second runtime in one process cannot
have its own sampler — which `tests/distributed-cache` creates by starting both
example services in one binary. That case logs a warning and parks until
shutdown rather than failing: reducing telemetry coverage must not become an
outage.

### 2026-08-29 — the collector became Elastic Agent in OpenTelemetry mode

The APM waterfall work needed Kibana's Applications views, not only Discover and
a saved dashboard. Elastic documents the `elasticapm` processor and connector as
what derives the service, transaction, and span-destination fields those views
read, and neither component ships in `otel/opentelemetry-collector-contrib`.

**What changes.** The `observability` profile's collector image becomes
`docker.elastic.co/elastic-agent/elastic-agent`, run with
`ELASTIC_AGENT_OTEL=true` against the same `examples/otel-collector.yaml`. The
config gains `elasticapm` as both processor and connector, an aggregated-metrics
pipeline fed by the connector, a bounded `deployment.environment.name` resource
attribute, and a span-level copy of the resource `service.name` for Discover
rows. The temporality processor is spelled `cumulativetodelta` in this
distribution; the earlier `cumulative_to_delta` in this document describes the
contrib collector it replaced. `ELASTIC_VERSION` now moves the collector image
with Elasticsearch and Kibana, so the gateway can never run a different
generation from the store it writes to.

Two costs, both accepted and both recorded in `examples/compose.yaml`: the image
is an order of magnitude larger than the contrib collector's, and the pipeline is
now backend-specific in a way the contrib one was not. `just examples demo` still
pays for neither, and decision 2 still holds — the enrichment lives in the
reference deployment's gateway, not in any crate under `crates/`.

**What is also new, and is not about Elastic.** The traces pipeline drops spans
whose `http.route` is `/health`. Compose polls each service's health endpoint
every two seconds, so those spans would be the highest-volume thing in the stack
and say nothing about the demo. Dropping them is a deployment decision, which is
why it is here and not in `kafkaman-axum`, and it is done by route template
rather than raw path so the filter itself stays bounded.
