# APM Waterfall Traces Plan

- Document Class: Plan
- Status: Active. The APM waterfall, parented-handoff, and review-fix slices landed 2026-08-29; see Residual Work.
- Date: 2026-08-29
- Category: Observability execution
- Scope: Tactical plan for turning the exported OpenTelemetry signals into debuggable APM waterfalls across HTTP, SQL, outbox relay, Kafka ingest, and dispatch, including the explicit parented Kafka handoff used by the examples.
- Sources:
  - wiki/proposals/19-apm-waterfall-traces.proposal.md
  - wiki/decisions/apm-waterfall-trace-shape.decision.md
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - tests/example-telemetry
  - tests/otlp-capture
  - tests/observability/tests/trace_parented_handoff.rs
  - crates/kafkaman-config/src/observability.rs
  - crates/kafkaman-rdkafka/src/consumer.rs
  - examples/otel-collector.yaml
  - examples/kibana-dashboard.sh
  - examples/order/kafkaman.toml
  - examples/product/kafkaman.toml
  - https://www.elastic.co/docs/solutions/observability/get-started/opentelemetry/use-cases/upstream-collector
  - https://www.elastic.co/docs/reference/edot-collector/components/elasticapmprocessor
  - https://opentelemetry.io/docs/specs/semconv/http/http-spans/
  - https://opentelemetry.io/docs/specs/semconv/db/database-spans/
  - https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
- Related:
  - wiki/proposals/19-apm-waterfall-traces.proposal.md
  - wiki/decisions/apm-waterfall-trace-shape.decision.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Deliverable

A developer can run the observed example stack, open Kibana, and inspect a slow
or successful lifecycle as trace waterfalls:

- inbound HTTP request timing
- business SQL timing
- kafkaman enqueue timing
- durable outbox wait and relay publish timing
- Kafka ingest timing
- dispatch, handler, cache upsert, retry/failure accounting timing

The implementation should increase trace detail, not log volume.

## Implementation Record - 2026-08-29

Implemented the portable span shape, the opt-in consolidated Kafka handoff, and
the example Elastic APM gateway:

- `kafkaman-axum::CorrelationLayer` now emits APM-grade `http.request` server
  spans with route, method, path, response status, and error status. Their
  exported OTel names are route-shaped, such as `POST /products`, so Kibana APM
  does not group health checks and business requests into one `http.request`
  transaction.
- The order and product example routers now apply `CorrelationLayer`, so request
  handlers run under the HTTP span.
- The order and product examples emit bounded `db.query` spans for business
  reads/writes with exported names such as `db.query insert product`.
- `kafkaman-sqlx` emits bounded `db.query` spans around key outbox, received,
  cache, mark, and failure-accounting statements. Recurring empty scheduler
  claim polls use debug-level `db.query` spans so the default example trace
  volume stays focused on real request and message work. Non-poll SQL spans use
  exported names such as `db.query mark outbox published`.
- `kafkaman.enqueue`, `kafkaman.relay.publish`, `kafkaman.ingest`, and
  `kafkaman.dispatch` keep their names and now set OpenTelemetry span kind and
  error status fields.
- `kafkaman-otel::init` now applies the `RUST_LOG` filter directly to its fmt,
  trace, and log layers so debug-level spans are not exported by the unfiltered
  telemetry layer.
- The relay publish span now covers the post-publish outbox mark, so broker time
  and mark-published SQL are in one producer-side waterfall.
- `kafkaman-config` now exposes `KafkaTraceHandoff` through
  `observability.*.kafka_trace_handoff = "linked" | "parented"`. The library
  default remains `linked`; the product and order example configs set
  `parented` so the demo produces one APM trace across the Kafka hop.
- `kafkaman-rdkafka` applies that effective policy when opening
  `kafkaman.ingest`: linked mode adds the propagated producer context as a span
  link, while parented mode sets it as the ingest parent. The received row still
  stores the ingest context in both modes, so dispatch descends from ingest.
- `tests/example-telemetry` now decodes span ids, parent ids, attributes, and
  links, plus OTLP span kind, then asserts a representative parented waterfall:
  HTTP -> business SQL -> enqueue -> outbox SQL -> relay publish -> mark SQL,
  ingest -> dispatch -> cache SQL -> mark processed, all under one trace id. It
  also asserts that the default info-level export does not include recurring
  empty scheduler poll spans.
- The example observability profile now runs Elastic Agent in OpenTelemetry
  mode with the `elasticapm` processor and connector, while preserving the raw
  `*-generic.otel-*` OTel streams for the saved dashboard.
- The Kibana dashboard trace panel now includes `http.request`, `db.query`, and
  `kafkaman.*` spans by selecting route and query-summary attributes rather than
  the old generic exported span names.
- The example collector copies resource `service.name` onto every trace span as
  span attribute `service.name`, surfaced in Elasticsearch as
  `attributes.service.name`, so Discover rows and span detail views can identify
  the producing service without changing span names.
- The Kibana dashboard now includes a top-level Kafka handoff Discover panel
  over `kafkaman.ingest` spans with `parent_span_id` or `links.*`, so it remains
  useful in either parented or linked mode.
- `examples/trace-handoffs.sh`, exposed through `just examples handoffs`, remains
  the linked-mode/debug helper. It joins downstream `kafkaman.ingest` link
  fields back to the upstream `kafkaman.relay.publish` trace/span and prints
  direct producer and consumer APM waterfall URLs.

Verification performed:

- `rtk cargo check -p kafkaman-axum -p kafkaman-sqlx -p kafkaman-rdkafka -p
  kafkaman-worker -p example-product -p example-order -p
  example-telemetry-tests`
- `rtk cargo test -p kafkaman-axum correlation_layer`
- `rtk cargo test -p kafkaman-otel`
- `rtk cargo test -p example-telemetry-tests --no-run`
- `rtk cargo test -p kafkaman-config`
- `rtk cargo test -p kafkaman-rdkafka`
- `rtk cargo test -p observability-tests --features redpanda --test
  trace_propagation --test trace_parented_handoff -- --nocapture`
- `rtk just opt-out`
- `rtk just examples telemetry-test` passed after escalating for Docker access;
  rerun after the no-default-poll-spans and span-kind assertions also passed.
- `rtk cargo clippy -p kafkaman-otel -p kafkaman-axum -p kafkaman-sqlx -p
  kafkaman-rdkafka -p kafkaman-worker -p example-product -p example-order -p
  example-telemetry-tests --all-targets --all-features -- -D warnings`
- `rtk bash -n examples/kibana-dashboard.sh` and `rtk bash -n
  examples/trace-handoffs.sh`
- `rtk docker compose -f examples/compose.yaml --profile services --profile
  observability --profile ui config`
- `rtk docker compose -f examples/compose.yaml --profile observability up -d
  --force-recreate --wait --wait-timeout 900 otel-collector`
- `rtk bash examples/kibana-dashboard.sh` after escalation for local Kibana
  access
- `rtk just examples all` passed against the live compose stack after the
  Elastic Agent processor name was corrected to `cumulativetodelta`, passed
  again after the `kafkaman-otel` layer-filtering fix, and passed after the
  parented-handoff example config change.
- Elasticsearch showed enriched trace documents with `processor.event`,
  `transaction.name`, `span.type`, `span.destination.service.resource`, and
  APM transaction/span fields.
- A live product trace showed `POST /products` -> product SQL ->
  `kafkaman.enqueue` -> outbox SQL -> `kafkaman.relay.publish` ->
  `mark outbox published`.
- With `parented` handoff enabled in the examples, live Elasticsearch documents
  showed one trace id containing both services and the parent edges
  `kafkaman.relay.publish` -> `kafkaman.ingest` ->
  `kafkaman.dispatch`.
- A fresh trace after the collector transform showed every span carrying both
  `resource.attributes.service.name` and span attribute
  `attributes.service.name`, with product spans marked
  `kafkaman-example-product` and order spans marked `kafkaman-example-order`.
- A read-only Elasticsearch proof for trace
  `8031e51e27ec9dada599ad878b1000b4` showed services
  `kafkaman-example-product` and `kafkaman-example-order`, span names
  `POST /products`, `db.query insert product`, `kafkaman.enqueue`,
  `db.query insert outbox row`, `kafkaman.relay.publish`,
  `kafkaman.ingest`, `kafkaman.dispatch`,
  `db.query apply received row to cache`, `db.query mark received processed`,
  and `db.query mark outbox published`, with ingest parented to publish and
  dispatch parented to ingest.
- A recent live Elasticsearch count over the current-source stack showed zero
  default exported `db.query` spans for `claim outbox batch`, `collapse stale
  pending outbox rows`, or `claim received row`.
- Playwright debugging of Kibana APM showed the pre-rename UI grouped all HTTP
  routes under `http.request` and selected a `/health` trace sample, hiding the
  product SQL/enqueue/relay waterfall from the default transaction page.
- Playwright debugging also showed the span-link popover does not provide an
  obvious downstream waterfall navigation target for the OTLP-generic trace
  documents, so the helper performs that join explicitly.
- A later Playwright visual check could not be completed: the in-app browser
  client failed under `node_repl` because `node:process` imports are blocked, and
  the direct Playwright MCP browser was already locked by another profile. The
  live Elasticsearch query above is the available deployment proof for the
  parented waterfall data.
- `rtk env MESSAGE_TYPE=product_snapshot
  CONSUMER_SERVICE=kafkaman-example-order PRODUCER_TRANSACTION='POST /products'
  LIMIT=100 ./examples/trace-handoffs.sh` returned product-create producer URLs
  paired with order-ingest consumer URLs against the live Elasticsearch stack.

## Review Fixes - 2026-08-29

A line-by-line review of the slice above found three things worth changing and a
list of smaller ones. All are applied.

**The recorded reason for the `kafkaman-otel` layer-filter change was wrong.**
The compatibility note and the log entry both said a registry-wide `EnvFilter`
let the unfiltered OpenTelemetry layer enable debug spans. Reproducing the
pre-change composition showed it does not — a filter layer's `enabled` is ANDed
into the subscriber's, so it bounds every layer after it. What actually keeps
scheduler polling out of the export is the `debug_span!` demotion. Both documents
are corrected, the change is kept for its real merit (a host can now bound
exports differently from stdout), and
`a_registry_wide_filter_also_bounds_the_layers_added_after_it` pins the
semantics so the wrong story cannot return.

**Unmatched HTTP routes leaked raw paths into transaction names.** The
`MatchedPath` fallback reported `request.uri().path()` as both `http.route` and
the exported span name, so a 404 scan would mint one APM transaction group per
invented URL — against this plan's own cardinality rule. The fallback is now the
bounded constant `<unmatched>`; the raw path stays in `url.path`, which nothing
groups by.

**Decision 8 of the trace-shape decision — sampling and retention — was
unimplemented and undocumented.** `examples/README.md` now has "Keeping the trace
volume honest", covering `RUST_LOG` (what is recorded), `OTEL_TRACES_SAMPLER`
(what is exported, read by the SDK with no kafkaman involvement), and the
collector (what is stored). The collector now drops `/health` spans, which
compose was generating every two seconds per service and which would otherwise
have been the highest-volume span in the stack.

Smaller fixes in the same pass:

- The `db.query` span shape existed in three hand-rolled copies — `kafkaman-sqlx`
  and both examples. It is now `kafkaman_core::db_span!` / `db_poll_span!`, macros
  rather than functions so the exported name is a compile-time `concat!` instead
  of a `format!` on every statement whether or not the span was enabled.
- The five inline copies of "record error status on this span" are now
  `kafkaman_core::record_error`, which also truncates the description at 256
  bytes: a database error can quote the value that violated a constraint.
- The relay's and dispatcher's claim transactions now open `db.query` poll spans
  of their own, so `BEGIN`/`COMMIT` are not the one unspanned pair on the durable
  path. All seven poll summaries are in the gate's negative assertion.
- `mark_with_sql` took its span's summary as a hardcoded literal despite being a
  shared helper; it now takes the span.
- `trace_propagation` and `trace_parented_handoff` shared eighty duplicated lines
  of drive. Both now call `observability_tests::drive_kafka_round_trip`, differing
  only in the handoff argument — which is the point of having the pair.
- The parented gate now asserts the ingest span carries *no* link. Parented and
  linked are alternatives; an ingest that did both would be indistinguishable on
  the wire and would draw the broker hop twice.
- `examples/trace-handoffs.sh` printed URLs with empty service and transaction
  segments when the producer trace had not been indexed yet, because `jq` on
  empty input never reaches its `//` defaults. It now skips with a message. Its
  header documents that `LIMIT` bounds candidates rather than rows, and that each
  candidate costs two more searches.
- `just examples handoffs` checks Elasticsearch is reachable and names the arm to
  run instead, rather than failing at a socket.
- `ObservabilityPolicy` and its override are `Copy`, so the per-record
  `policy_for` on the ingest path allocates nothing.
- The collector's service-name transform runs with `error_mode: ignore`: a
  usability transform must not be able to drop a batch.
- `kafkaman-rdkafka`'s `default-features = false` on `kafkaman-config` was
  documented as load-bearing. It is defensive — that crate has no features — and
  the comment and compatibility note now say so.
- The Elastic Agent swap was not recorded in the decision that owns the example
  topology. It is now, along with its cost.

Verification performed:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --lib`
- `cargo test -p kafkaman-otel`
- `cargo test -p observability-tests --features redpanda --test trace_propagation
  --test trace_parented_handoff`
- `just opt-out` and `cargo check -p kafkaman --no-default-features`
- `just examples telemetry-test`
- `docker compose -f examples/compose.yaml --profile services --profile
  observability --profile ui config`
- `just examples all` from a fully removed stack, including provisioning

## Residual Work

- Add an explicit slow-query or injected-delay proof if this plan later needs a
  "where did it hang" regression test rather than the current structural
  waterfall test.
- Add focused failure-path tests that assert span error status structurally.
- Add browser-level regression automation for the Kibana Applications/APM page
  only if the APM page becomes a maintained UI contract rather than example
  documentation.

## Out Of Scope

- Production-ready Elastic deployment guidance beyond the existing example
  profile.
- Payload, request body, bind-value, user-id, entity-id, or tenant-id span
  attributes.
- Silently changing the default Kafka trace relationship from links to
  parent-child. The parented shape is explicit config.
- Replacing the current saved Kibana dashboard.
- Making `kafkaman` own the OpenTelemetry SDK.

## Phase 0 - Baseline the Current APM UI

Status: Completed after implementation; raw and APM-enriched trace documents
were verified in Elasticsearch.

Measure what Kibana Applications/APM shows from today's exported spans before
changing code.

Steps:

- Run `just examples all`.
- Open or query Kibana Applications, Services, Traces, Dependencies, and Service
  Map views over a four-hour window.
- Record whether the current `traces-generic.otel-default` documents appear in
  APM views, not only in Discover.
- Record whether span links are visible or navigable enough to follow the Kafka
  hop.

Verification:

- Elasticsearch confirms traces/logs/metrics still arrive.
- A screenshot or API evidence records which APM views are populated.
- `wiki/log.md` records the baseline result before implementation changes.

## Phase 1 - Add Elastic APM Enrichment To The Example Stack

Status: Implemented.

The current collector writes OTel-shaped documents to Elasticsearch. For full
Elastic APM behavior, add the documented APM enrichment path to the reference
deployment.

Steps:

- Decide whether `examples/compose.yaml` should use Elastic Agent in OTel mode,
  an EDOT collector image, or a custom collector containing the `elasticapm`
  processor and connector.
- Update `examples/otel-collector.yaml` or add a clearly named gateway config.
- Preserve the existing `metrics`, `traces`, and `logs` export behavior.
- Keep the temporality conversion for Elasticsearch-compatible histograms.
  (Landed as `cumulativetodelta`: the Elastic Agent distribution spells the
  processor without underscores.)
- Add `deployment.environment=local` as a bounded resource attribute so Kibana's
  environment selector is meaningful.

Verification:

- `just examples all` succeeds from a clean stack.
- Kibana Applications/Services lists `kafkaman-example-order` and
  `kafkaman-example-product`.
- Service Map and Traces views are populated, or the failure mode is recorded
  with exact missing fields.
- The saved dashboard still shows traces, queue metrics, and service logs.

## Phase 2 - Upgrade HTTP Server Spans

Status: Implemented.

Turn the current thin `http.request` span into an APM-grade server span.

Steps:

- Add or adapt an Axum/Tower trace layer that records HTTP method, route
  template, status code, duration, and error status.
- Preserve `x-correlation-id` behavior.
- Avoid raw path cardinality where route templates are available.
- Ensure route handlers run under the HTTP span so business SQL and
  `kafkaman.enqueue` become children.

Verification:

- `tests/example-telemetry` or a new focused test captures a POST request whose
  root span is the HTTP server span.
- Captured spans prove `kafkaman.enqueue` is a child of the HTTP request span.
- 4xx and 5xx behavior is tested without relying on visual Kibana inspection.

## Phase 3 - Add SQL Child Spans

Status: Implemented; explicit slow-query demonstration remains residual work.

Expose the database timing that makes a waterfall useful.

Steps:

- Evaluate `sqlx-otel`, `sqlx-tracing`, and a small local explicit-span helper
  against the example services.
- Prefer example-only adoption first if a helper crate changes executor types or
  creates dependency surface questions.
- Add child spans for business writes and reads in `examples/order` and
  `examples/product`.
- Add child spans for kafkaman-owned SQL operations that dominate the durable
  path: outbox insert, relay claim, mark published/failed, received insert,
  dispatch claim, cache upsert, mark processed, failure accounting, and retry
  scheduling.
- Use bounded operation names or query summaries; do not record bind values.

Verification:

- Local OTLP capture asserts SQL spans exist as children of HTTP, enqueue,
  relay, ingest, or dispatch spans.
- At least one deliberate slow-query or transaction-delay test demonstrates the
  waterfall points at the delayed SQL span.
- Existing SQL and integration tests still pass.

## Phase 4 - Normalize kafkaman Span Kinds And Status

Status: Partially implemented; span kind is emitted and structurally tested,
and error status is emitted, but failure-path status assertions remain residual
work.

Make the current kafkaman spans easier for APM tools to interpret.

Steps:

- Audit the four existing kafkaman spans against current OpenTelemetry messaging
  semantic conventions.
- Add span kind and status fields through the `tracing-opentelemetry` bridge
  without changing host SDK ownership.
- Record errors on spans when publish, ingest, dispatch, or handler work fails.
- Preserve existing span names unless a measured backend issue forces a
  documented migration.

Verification:

- Tests assert span kind/status attributes on success and failure paths.
- The OTLP capture fixture decodes those fields structurally, not by byte-string
  search. Span kind is now covered on the representative success path.
- Any renamed or newly documented span field is recorded in compatibility docs.

## Phase 5 - Decide The Linked-Kafka UX

Status: Completed for the backend trace-shape decision: linked mode remains the
library default, the helper keeps linked traces debuggable, and an explicit
single-record parented mode is implemented for the APM waterfall demo.
Browser-level APM UI evidence remains residual work.

Measure whether the correct link-based Kafka shape is usable in Kibana. Result:
links are queryable and debuggable through the helper, but the example APM
waterfall is clearer as one trace, so the decision added an explicit parented
mode rather than changing the default.

Steps:

- Inspect a smoke flow in Kibana after Phases 1-4.
- Verify whether `links.trace_id` can be followed from `kafkaman.ingest` back to
  the producer-side trace.
- If the UI is usable, document the workflow and keep the default shape.
- If the UI is not usable, file or amend a decision for an opt-in single-message
  APM parenting mode.

Verification:

- The result is recorded in `wiki/log.md`.
- `examples/README.md` teaches APM as the primary parented-mode workflow and the
  linked-mode helper as the debug fallback; the trace-shape decision records the
  alternate mode.

## Phase 6 - Extend The Binary Telemetry Gate

Status: Implemented for the parented example contract and the linked-mode
regression.

Promote the visual claim into a testable span-shape contract.

Steps:

- Extend `tests/otlp-capture` helpers to query traces by service, name, parent,
  kind, links, and attributes.
- Extend `tests/example-telemetry` to assert the representative waterfall shape.
- Keep a broker-backed linked-mode regression asserting the default link rather
  than parent relationship.
- Keep the Elastic/Kibana verification as a deployment proof, not the only
  correctness gate.

Verification:

- `just examples telemetry-test` fails if HTTP, SQL, or parented kafkaman span
  shape regresses; `trace_propagation` fails if the linked default regresses.
- A separate opt-in Elastic gate, if added, proves the APM UI path without
  slowing default tests.

## Phase 7 - Documentation And Closeout

Status: Implemented for README, dashboard, compatibility note, wiki log, and
example parented handoff docs; browser-level APM evidence remains residual work.

Teach the workflow and promote validated behavior.

Steps:

- Update `examples/README.md` with the APM navigation path.
- Update `examples/kibana-dashboard.sh` only if a saved link or dashboard panel
  materially helps the workflow.
- Update `wiki/compatibility/` if public span names, attributes, config, or
  feature flags change.
- Update `wiki/specs/m6-observability-operability.spec.md` only after behavior
  is implemented and verified.
- Mark this plan Completed only when tests and the live example prove the
  waterfall claim.

Verification:

- The final run records commands, Kibana evidence, and test output in
  `wiki/log.md`.
- `wiki/index.md` points readers to the accepted proposal, decision, and
  completed plan or active residual work.
