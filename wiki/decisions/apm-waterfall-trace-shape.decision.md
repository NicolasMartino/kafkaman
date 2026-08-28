# APM Waterfall Trace Shape

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-29
- Category: Observability architecture
- Scope: Defines how kafkaman will pursue APM-style waterfalls without increasing log volume, moving SDK ownership into the library, or silently changing the default Kafka trace-link model.
- Sources:
  - wiki/proposals/19-apm-waterfall-traces.proposal.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman-sqlx/src/outbox_enqueue.rs
  - crates/kafkaman-config/src/observability.rs
  - crates/kafkaman-worker/src/relay.rs
  - crates/kafkaman-rdkafka/src/consumer.rs
  - tests/observability/tests/trace_parented_handoff.rs
  - tests/example-telemetry/tests/binary_telemetry.rs
  - examples/otel-collector.yaml
  - examples/order/kafkaman.toml
  - examples/product/kafkaman.toml
  - https://www.elastic.co/docs/solutions/observability/get-started/opentelemetry/use-cases/upstream-collector
  - https://www.elastic.co/docs/reference/edot-collector/components/elasticapmprocessor
  - https://opentelemetry.io/docs/specs/semconv/http/http-spans/
  - https://opentelemetry.io/docs/specs/semconv/db/database-spans/
  - https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
- Related:
  - wiki/plans/apm-waterfall-traces.plan.md
  - wiki/plans/opentelemetry-completion.plan.md

## Decision

1. **Waterfalls are built from spans, not logs.**
   Logs stay sparse, trace-correlated, and event-oriented. APM timing is a trace
   concern: every waterfall element must be a span with duration, status, and
   bounded attributes.

2. **The host still owns the SDK.**
   No facade-reachable kafkaman crate installs an OpenTelemetry provider or
   exporter. The example may use `kafkaman-otel` and may run Elastic-specific
   collector components, but the instrumentation library remains backend-neutral.

3. **HTTP request spans are application/server spans.**
   Inbound HTTP is the natural root for request waterfalls in the examples.
   `kafkaman-axum` may provide the middleware, but the span represents the
   application's server request, not an internal kafkaman operation. It should
   carry HTTP semantic attributes, including method, route template, status code,
   and bounded target metadata, plus the existing correlation id.

4. **Database timing appears as child spans under the operation that caused it.**
   SQL spans should sit under HTTP handlers, enqueue, relay, ingest, or dispatch
   spans. They must not include bind values, entity ids, request bodies, or other
   unbounded payload-derived data. Query summaries or operation names are
   preferable to full dynamic SQL when the query contains generated identifiers.

5. **kafkaman's existing message spans stay the durable phase vocabulary.**
   The public mental model remains:
   `kafkaman.enqueue`, `kafkaman.relay.publish`, `kafkaman.ingest`, and
   `kafkaman.dispatch`. The APM work enriches those spans with kind/status and
   child spans; it does not replace them with backend-specific names.

6. **The Kafka hop remains link-based by default, with an explicit APM opt-in.**
   The accepted trace-context decision says consumer-side work links to the
   producer context rather than parenting from it. That remains the library
   default. The measured Kibana/Elastic path proved the simple APM waterfall
   needs one trace id across the broker hop, so `kafka_trace_handoff =
   "parented"` is accepted as an opt-in for single-record processing and used by
   the examples.

7. **Elastic APM enrichment belongs to the reference deployment.**
   For self-managed Elastic with direct Elasticsearch export, Elastic documents
   the `elasticapm` processor and connector as required for full APM UI
   behavior. The example/reference stack may adopt EDOT/Elastic Agent gateway or
   another Elastic-supported path. That choice must not leak into kafkaman's
   facade or core crates.

8. **Sampling and retention are part of the feature.**
   APM spans scale with traffic. The implementation must document how to reduce
   trace volume for production and must keep high-cardinality attributes out of
   default spans.

## Span Shape

The desired producer-side request trace is:

```text
http.request POST /products        SERVER, application-owned
  db.query products insert         CLIENT
  kafkaman.enqueue product_snapshot PRODUCER/create
    db.query outbox insert          CLIENT
  http.response                     recorded on root span
```

The asynchronous send side is restored from the durable outbox context:

```text
kafkaman.relay.publish product_snapshot PRODUCER/send
  kafka produce products                CLIENT or PRODUCER, if separately visible
  db.query outbox mark published         CLIENT
```

The consumer side remains a linked trace by default:

```text
kafkaman.ingest product_snapshot    receives or stores broker record
  db.query received insert

kafkaman.dispatch product_snapshot  CONSUMER/process
  db.query received claim
  handler/cache apply
  db.query cache upsert
  db.query mark processed
```

If Kibana cannot expose the linked producer/consumer relationship well enough,
that is evidence for an explicit opt-in, not for changing the default. That
opt-in now exists as `observability.*.kafka_trace_handoff = "parented"`:

```text
kafkaman.relay.publish product_snapshot PRODUCER/send
  kafkaman.ingest product_snapshot      CONSUMER/receive, parented from publish
    db.query received insert
    kafkaman.dispatch product_snapshot  CONSUMER/process
      db.query cache upsert
      db.query mark processed
```

## Options Considered

### A. Make logs the waterfall

Rejected. A log line has no intrinsic duration and no parent-child timing
relationship. It can annotate a waterfall, but it cannot be the waterfall.

### B. Parent every consumer span from the producer

Rejected as the default. It makes Kibana useful in the simple case, but it
contradicts the accepted messaging trace decision for batch and ambient context
cases. Accepted only as the explicit `parented` trace handoff mode.

### C. Keep links, improve Kibana navigation around them

Accepted as the first implementation posture. It preserves correctness and
forces the project to measure the actual UI limitation before changing trace
semantics.

### D. Add SQL auto-instrumentation as a library dependency

Deferred. SQL child spans are required, but the first implementation should
evaluate the ergonomics and dependency impact in examples before adding another
library dependency to facade-reachable crates.

### E. Use Elastic-specific processors in the example collector path

Accepted for the reference deployment. It is backend-specific by definition, so
it belongs beside `examples/otel-collector.yaml`, not in kafkaman crates.

## Consequences

- The next observability work has two proof surfaces: local OTLP capture for
  span shape, and Kibana/Elastic for APM UI behavior.
- The existing dashboard remains useful. It is the signal-orientation view; APM
  becomes the latency-debugging view.
- The example stack may gain another Elastic component or switch collector image
  if the current contrib collector cannot provide APM enrichment.
- HTTP and SQL instrumentation must be low-cardinality from the first patch, not
  cleaned up later.
- A future compatibility note is required if public APIs, feature flags, config,
  or documented span names change.

## Revisit If

- Elastic's APM UI cannot represent the linked Kafka relationship in a usable
  way after the reference stack uses the documented APM enrichment path.
- A Rust SQLx instrumentation crate proves stable and small enough to justify a
  dependency in the example or library surface.
- OpenTelemetry messaging conventions stabilize in a way that changes the
  recommended span kind or parent/link shape.
- Production users need batching semantics for `parented` mode rather than the
  current single-record target.
