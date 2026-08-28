# APM Waterfall Traces

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-29
- Category: Observability and debugging
- Scope: Proposes extending kafkaman's OpenTelemetry work from exported signals and saved dashboards to APM-style request and message waterfalls that show where time is spent across HTTP, SQL, outbox relay, Kafka ingest, and dispatch.
- Sources:
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman-sqlx/src/outbox_enqueue.rs
  - crates/kafkaman-worker/src/relay.rs
  - crates/kafkaman-rdkafka/src/consumer.rs
  - examples/otel-collector.yaml
  - https://www.elastic.co/docs/solutions/observability/get-started/opentelemetry/use-cases/upstream-collector
  - https://www.elastic.co/docs/reference/edot-collector/components/elasticapmprocessor
  - https://www.elastic.co/docs/solutions/observability/apm/service-map
  - https://www.elastic.co/docs/solutions/observability/apm/traces-ui
  - https://opentelemetry.io/docs/specs/semconv/http/http-spans/
  - https://opentelemetry.io/docs/specs/semconv/db/database-spans/
  - https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
- Related:
  - wiki/decisions/apm-waterfall-trace-shape.decision.md
  - wiki/plans/apm-waterfall-traces.plan.md
  - wiki/plans/opentelemetry-completion.plan.md

## Why This Proposal Exists

The current example proves that metrics, traces, and logs leave the example
binaries, pass through the collector, and reach Elasticsearch. The saved Kibana
dashboard separates those signals so metrics no longer look like log spam.

That is useful, but it is not the debugging experience an operator reaches for
when a request is slow.

The desired artifact is a waterfall:

```text
HTTP POST /products
  db INSERT products
  kafkaman.enqueue product_snapshot
    db INSERT outbox_product_snapshot
  HTTP 201

later:
  kafkaman.relay.publish product_snapshot
    Kafka send products

order service:
  kafkaman.ingest product_snapshot
    db INSERT received_product_snapshot
  kafkaman.dispatch product_snapshot
    handler/cache apply
    db UPSERT cached_products
```

That is an APM problem, not a logging problem. More log lines would make the
example harder to run at production scale and still would not produce a
causally ordered timing tree. Spans are the right signal: they already have
start time, duration, parentage or links, status, and low-cardinality
attributes.

## Current Baseline

kafkaman already has the important message spans:

- `kafkaman.enqueue`
- `kafkaman.relay.publish`
- `kafkaman.ingest`
- `kafkaman.dispatch`

It also already persists W3C trace context across the outbox and received-row
durable gaps, and it propagates W3C context through Kafka headers.

The gaps are at the edges and inside the spans:

- HTTP requests have a thin `http.request` span with a correlation id, but not a
  proper APM server-span shape.
- SQL work is not visible as child spans, so a slow query is hidden inside the
  enclosing HTTP or kafkaman span.
- The example collector exports OTel-shaped data directly to Elasticsearch, but
  it does not add Elastic's APM enrichment path. That means raw data exists, but
  Kibana's richer Applications/APM UI may not light up fully.
- The existing Kafka trace decision correctly uses links across the broker hop.
  A link is honest for messaging, but less visually obvious than one
  parent-child tree in a waterfall UI.

## Proposal

Extend the observability example and instrumentation surface so one smoke flow
can be inspected as APM waterfalls:

1. **Enable Elastic APM interpretation in the reference stack.**
   The example remains backend-optional, but when the Elastic profile is enabled
   it should feed Kibana's Applications/APM views, not only raw Discover and a
   saved dashboard.

2. **Turn inbound HTTP requests into proper server spans.**
   HTTP spans should carry route template, method, status code, duration,
   correlation id, and error status without recording unbounded request data.

3. **Add database child spans where they explain time.**
   The first target is the example and kafkaman-owned SQL calls that matter to
   the outbox/cache flow: business writes, outbox insert, row claim, received
   insert, cache upsert, failure accounting, and retry scheduling.

4. **Normalize kafkaman span kinds and attributes for APM tools.**
   Existing span names stay stable unless implementation proves a backend needs
   another representation. Add semantic attributes rather than inventing
   dashboard-only fields.

5. **Keep logs sparse and trace-correlated.**
   Logs remain events for notable lifecycle transitions and errors. They are not
   the primary waterfall signal.

## Options Considered

### A. Add more logs

Rejected. Logs can answer "what happened", but they are poor at answering
"where did time go" unless every line is timestamped, correlated, and manually
reassembled. That is exactly what tracing systems already do. Increasing log
volume would also recreate the concern that prompted the dashboard split.

### B. Stop at custom Kibana dashboards

Rejected as the end state. Saved dashboards are good for queue depth and signal
orientation, but APM waterfall views, service maps, dependency views, and trace
sample timelines exist for this class of debugging. Rebuilding those as custom
tables would be fragile and worse than the native tool.

### C. Extend the current OTel pipeline into APM waterfalls

Accepted. This builds on the accepted telemetry pipeline rather than replacing
it. kafkaman continues to emit OTel-compatible spans and metrics; the example's
Elastic deployment gains the backend-specific enrichment needed for Kibana's APM
views; tests assert the span shape independent of Elastic.

### D. Add a full auto-instrumentation agent model

Rejected for the current deliverable. The Java experience that motivates this
work usually comes from mature runtime agents. Rust does not give this project
the same transparent, language-wide instrumentation path. kafkaman should add
explicit instrumentation at the framework boundaries it owns, and evaluate small
HTTP/SQL helper crates only where they reduce code without moving SDK ownership
into the library.

## Boundaries

This proposal does not make `kafkaman` own an OpenTelemetry SDK. The host-owned
SDK decision still stands. `crates/kafkaman-otel` may keep helping examples and
adopters install a pipeline, but facade-reachable library crates stay
instrumentation-only.

This proposal does not add payload attributes, user ids, raw SQL bind values, or
request bodies to spans. The waterfall should show timing and bounded route,
topic, message type, and status dimensions.

This proposal does not decide that Kafka consumer spans should parent from
producer spans by default. The existing link-based trace shape remains the
correct library default unless measurement proves a specific APM view cannot
work without an opt-in alternate shape.

## Risks

- **Cardinality.** HTTP route templates are safe; raw paths with ids are not.
  Message type and topic are bounded; user and entity ids are not.
- **Data volume.** Spans scale with traffic. This is acceptable for a debugging
  path only if sampling, retention, and dashboard defaults are documented.
- **Async trace readability.** Correct messaging links are less visually simple
  than a single parent-child tree. The plan needs an explicit Kibana spike
  before changing the instrumentation shape.
- **Backend coupling.** Elastic APM enrichment belongs to the reference stack,
  not the library.
- **Semantic-convention drift.** HTTP, database, and messaging conventions move.
  The implementation should avoid exposing unstable convention choices as new
  public API unless necessary.

## Resolution

Accepted 2026-08-29 as the next observability direction. Execution is tracked in
`wiki/plans/apm-waterfall-traces.plan.md`; durable instrumentation choices are
recorded in `wiki/decisions/apm-waterfall-trace-shape.decision.md`.
