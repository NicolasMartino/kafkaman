# Telemetry Pipeline Completion

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-25
- Category: Observability
- Scope: Proposes completing kafkaman's OpenTelemetry surface from instrumentation-only to a working, exportable, verified pipeline covering metrics, traces, and logs, with an Elastic/Kibana reference deployment.
- Sources:
  - wiki/proposals/04-observability-logging-policy.proposal.md
  - wiki/specs/m6-observability-operability.spec.md
  - wiki/decisions/observability-operability-policy.decision.md
  - crates/kafkaman-worker/src/metrics.rs
  - crates/kafkaman-rdkafka/src/metrics.rs
  - Cargo.toml (workspace dependency set)
  - Elastic Docs, "Elasticsearch OTLP/HTTP endpoint", retrieved 2026-08-25
- Related:
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/metric-instrument-and-attribute-schema.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## Why This Proposal Exists

Proposal 04 asked *what observability policy should kafkaman have*. M6 answered
it: configurable per-message policy, direct OpenTelemetry metrics, `tracing`
spans without owning the host subscriber, sanitized admin routes. That proposal
is Accepted and its policy is implemented.

This proposal asks a different question, which nobody has asked yet: **does any
of it actually reach a backend?**

It does not. The finding is narrow and checkable: the workspace declares
`opentelemetry = "0.32"` — the API crate — and no `opentelemetry_sdk` exists
anywhere in the repository. Under the OpenTelemetry API contract, a metrics call
made with no installed `MeterProvider` is a no-op by design. Every counter M6
added is therefore inert in every build and every test, and has been since it was
written.

This is not a defect in M6's policy. It is the difference between *emitting*
telemetry and *having* telemetry, and M6 scoped only the first half.

## Evidence

| Claim | Verification |
| --- | --- |
| No SDK in the workspace | `git grep opentelemetry_sdk` over `*.toml` and `*.rs` returns nothing |
| Metrics are inert | `global::meter("kafkaman")` with no provider installed returns no-op instruments |
| The example exports nothing | `apps/axum-outbox/src/main.rs` installs `tracing_subscriber`, no `MeterProvider` |
| Traces are absent | 17 `tracing::` sites across 6 files, no tracer provider, no OTel span export |
| Logs are unbridged | no `opentelemetry-appender-tracing`; `tracing` events never become OTel log records |
| Context does not propagate | `publisher.rs` writes 6 `kafkaman-*` headers; no `traceparent` among them |
| No metric is asserted anywhere | the only guard is a `debug_assert_eq!` in `record_ingest_stats`, compiled out of release |

The last row is the one that should carry the most weight. The single metrics
defect found in this project so far — `ingest.records` double-counting every
record — was caught by a human reading the code during the M6 review. There is
no test that would have caught it, and after the fix there is still no test that
would catch its recurrence in a release build.

## Proposal

Complete the pipeline in four capability areas, each independently useful:

1. **Make instruments observable.** Fix the provider-binding hazard that makes
   the current surface both fragile in production and untestable, then add an
   SDK-backed test path. Detailed in
   [telemetry-pipeline-ownership](../decisions/telemetry-pipeline-ownership.decision.md).
2. **Complete the metric surface.** Add the two instrument kinds an operator
   actually opens a dashboard for — latency histograms and queue-depth gauges —
   and settle the attribute schema. Detailed in
   [metric-instrument-and-attribute-schema](../decisions/metric-instrument-and-attribute-schema.decision.md).
3. **Add traces.** Bridge `tracing` to OpenTelemetry, persist trace context
   across the outbox hop, and propagate W3C context through Kafka headers.
   Detailed in
   [trace-context-propagation-and-w3c-headers](../decisions/trace-context-propagation-and-w3c-headers.decision.md).
4. **Add logs and a reference backend.** Bridge `tracing` events to OTel log
   records correlated by `trace_id`, and prove the whole thing against Elastic
   and Kibana in the two-service example. Detailed in
   [telemetry-backend-and-example-topology](../decisions/telemetry-backend-and-example-topology.decision.md).

Execution sequencing lives in
[opentelemetry-completion.plan.md](../plans/opentelemetry-completion.plan.md).

## Options Considered

### On scope

**A. Metrics only — fix the pipeline, skip traces and logs.** Smallest change.
Makes the existing spec claim true and gives dashboards. Rejected as the
end state, because for a store-and-forward messaging library the *trace* is the
artifact that answers the question operators actually ask — "where did this
message go, and why was it slow?" — and a counter cannot answer it. Retained as
the correct *first increment*, which is why Phases 0–1 stand alone.

**B. Full pipeline: metrics, traces, logs, propagation.** Proposed. The three
signals are individually useful but jointly much more so: the reason to bridge
logs at all is that Phase 2 stamps them with `trace_id`, and the reason traces
are worth having across services is that they cross the Kafka hop. Cutting the
set produces less than proportional value.

**C. Defer everything to M7.** Rejected. M7's telemetry-adjacent deliverables are
docs and examples; discovering there that no metric has ever been exported would
force this work anyway, with an example already written against it. The
provider-binding hazard also actively misleads adopters in the meantime.

### On who owns the SDK

**A. kafkaman installs and owns a `MeterProvider`/`TracerProvider`.** Rejected.
A library that installs a global provider fights every host that also has one,
and forces our exporter and version choices onto applications. It also breaks the
common case of an application already exporting its own telemetry.

**B. Host owns the SDK; kafkaman uses the API only.** Proposed. This is what
OpenTelemetry prescribes for instrumentation libraries, and what M6 already does
implicitly. It becomes explicit, documented, and — importantly — paired with a
stated ordering contract so the failure mode is understood rather than silent.

**C. Host owns the SDK, but kafkaman ships a `kafkaman-otel` convenience crate.**
Deferred, not rejected. A one-call pipeline builder is genuinely attractive; the
cost is pinning adopters to our chosen versions of exporter crates in an
ecosystem that ships breaking 0.x releases in lockstep across six crates. Revisit
once the example shows whether the wiring is actually repetitive.

### On metric naming

**A. Rename instruments to messaging semantic conventions**
(`messaging.client.published.messages` and similar). Rejected. It discards the
`kafkaman.*` namespace that makes our series identifiable in a deployment that
has several messaging libraries, and semconv does not cover outbox-specific
concepts like time-to-publish or claim expiry at all.

**B. Keep `kafkaman.*` names, keep ad-hoc attributes.** Rejected. Attributes are
where cross-library correlation actually happens; ad-hoc keys make our series
un-joinable with everything else in Kibana.

**C. Keep `kafkaman.*` names, add standard messaging attributes.** Proposed.
Identifiable series, joinable dimensions, no loss either way.

### On the reference backend

**A. Prometheus + Grafana.** The conventional Rust choice. Rejected as the
*reference* because it covers metrics well and traces and logs poorly, which
would leave two of three signals unproven by the example.

**B. Elastic (Elasticsearch + Kibana).** Proposed. It ingests all three OTLP
signals natively over HTTP, and its APM view is built around exactly the
trace-and-log correlation Phase 2 and Phase 3 produce. It is also what was asked
for.

**C. Jaeger + Prometheus + Loki.** Rejected for the reference deployment: three
backends to explain and wire in an example whose subject is kafkaman, not
observability plumbing.

## The Elastic Target

Elasticsearch ingests OTLP natively. There is no translation layer, no exporter
plugin, and — for the example — no collector required: an application can point
an OTLP/HTTP exporter directly at Elasticsearch, which derives metric dimensions
and mappings from OTLP metadata and creates the time-series data streams itself.

Two constraints are worth stating before anyone writes configuration:

1. **Elasticsearch supports OTLP/HTTP only, not OTLP/gRPC.** The exporter must be
   the HTTP variant. This is the single most likely misconfiguration.
2. **Direct-to-Elasticsearch is an example topology, not a production one.** It
   is right for a compose stack because it removes a moving part. Production
   wants a collector in front for batching, retry, redaction, and fan-out. The
   example documents both and runs the simpler one.

What each phase buys in Kibana:

| After | Kibana shows |
| --- | --- |
| Phase 1 | Metric dashboards: queue depth, oldest-row age, publish and dispatch latency |
| Phase 2 | APM traces: waterfalls per message, and a service map across the Kafka hop |
| Phase 3 | Logs pivoting to their trace by `trace_id`, including sampled lifecycle events |

The service map is the payoff worth naming explicitly. The two-service
distributed-cache example is, today, two applications that share a broker. With
trace context crossing the Kafka hop, it renders as one distributed system —
which is the claim the example exists to make.

## Risks

- **Version lockstep.** The OpenTelemetry Rust crates release breaking 0.x
  versions together. Adding four more of them multiplies upgrade friction. All
  four stay out of `crates/` for exactly this reason; only the example and the
  test suite carry them.
- **A schema change.** Persisting trace context adds a column to the outbox
  table. It is nullable and additive, but it is a migration, and it lands in a
  library whose schema-change contract is itself a specced surface.
- **It reopens a ratified question.** See below.
- **Cardinality.** `topic` and `message_type` are bounded by deployment; adding
  `messaging.destination.name` is a duplicate dimension, not a new one. No
  proposed attribute is unbounded, and none derives from message content.
- **Example scope creep.** The compose stack gains services. Mitigated by the
  example already existing and already having a compose file.

## The Ratified Question This Reopens

`wiki/roadmaps/path-to-v1.roadmap.md` records OQ5 — the host-context boundary —
as **already ratified**, as opaque headers with a reserved `kafkaman-*`
namespace, and
[message-identity-and-header-namespace](../decisions/message-identity-and-header-namespace.decision.md)
is the Accepted decision that fixes it: exactly two namespaces, reserved and
user.

W3C `traceparent` does not fit either. It cannot take the `kafkaman-` prefix,
because a non-kafkaman consumer must be able to read it. It must not be handed to
application code as a user header, because the producer did not set it — we did.

So this proposal does not *resolve* OQ5; it **amends a ratified decision**, which
is a heavier act and is treated as one. The amendment is deliberately minimal: a
third recognized namespace containing exactly the W3C trace context keys, with
the existing two-namespace rule otherwise untouched. Rationale and alternatives
are in
[trace-context-propagation-and-w3c-headers](../decisions/trace-context-propagation-and-w3c-headers.decision.md).

A related bookkeeping error surfaced while checking this: the roadmap says OQ5 is
ratified in its status section and simultaneously lists "**ratifying OQ5**" as an
M7 deliverable. Those cannot both be true. Corrected as part of filing this
proposal.

## Resolution

Direction accepted 2026-08-25. The four decisions above are Accepted.

**Superseded on execution status, 2026-08-26.** "Execution is planned but not
begun" was true on the day. It is not now: Phases 0-3, 5 and 6 of
[opentelemetry-completion.plan.md](../plans/opentelemetry-completion.plan.md)
have landed and been through two review passes. The four decisions and the
reasoning behind them stand unchanged; only this sentence about status was
overtaken. What remains outstanding is Phase 4's compose profile and the Elastic
reference page below. The Elastic reference deployment is deliberately
**not** yet documented as a procedure — see the deferral note in the plan — and
will be filed as a reference page once the two-service example builds and the
stack has actually been run.
