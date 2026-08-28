# Method-Level Timing Plan

- Document Class: Plan
- Status: Completed 2026-08-29.
- Date: 2026-08-29
- Category: Observability execution
- Scope: Records the continuous-profiling measurement, the `kafkaman.handler` span, the `kafkaman::internal` span tier, and the durable-capture fix that the tier uncovered.
- Sources:
  - wiki/proposals/20-method-level-timing.proposal.md
  - wiki/decisions/method-level-timing-and-span-depth.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - wiki/plans/apm-waterfall-traces.plan.md
  - crates/kafkaman-core/src/trace.rs
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-rdkafka/src/publisher.rs
  - examples/otel-collector.yaml
- Related:
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Deliverable

Answer "how long did each Rust method take" as well as Rust allows: close the
handler gap at the default filter, make kafkaman's internals reachable on demand,
and measure whether continuous profiling can do the part spans cannot.

## Phase 0 - Continuous profiling, measured

Status: Completed. **Result: not adopted.** Two blockers, both recorded here so
the measurement does not have to be repeated.

The reference stack already runs Elastic Agent in OpenTelemetry mode, which ships
a `profiling` receiver. Three obstacles were cleared before it ran at all, and
each is worth knowing:

| Obstacle | Resolution |
| --- | --- |
| `profiles` pipeline rejected outright | the signal is alpha, behind `--feature-gates=service.profilesSupport` |
| `unable to read kallsyms - check capabilities` | the image runs as uid 1000, and kallsyms addresses need `CAP_SYSLOG`; `privileged: true` alone is not enough, `user: "0"` is |
| `neither debugfs nor tracefs are mounted` | Docker Desktop's linuxkit VM mounts neither; mounting them inside the container at entrypoint works, and needs no change to the VM |

After that it ran: `eBPF tracer loaded`, `Attached tracer program`,
`Attached sched monitor`, `Everything is ready`. `xpack.profiling.templates.enabled`
turned out to be a *dynamic* cluster setting, so Elasticsearch needed no restart
and lost no data. Under load it produced **303 profiling events, 263 stack
traces, 18 executables**, and `_profiling/flamegraph` returned a 13,046-frame
graph. The profiled executables included `order` and `product`: the eBPF profiler
does see the Rust services across container boundaries, on arm64.

**What stopped it:**

- **Native frames are unsymbolised.** All 78 frames belonging to `order` and
  `product` had empty function names, and `profiling-symbols-global` held zero
  documents. Elastic symbolises native code through a separate
  `pf-elastic-symbolizer` service. It ships in the image but is not wired up, so
  the 9,622 Rust symbols in the binary are read by nothing.
- **Kibana has no profiling UI.** No profiling plugin among its 216, and
  `/api/profiling/v1/setup/es_resources` returns 404. The Elasticsearch API
  works; there is no first-class view onto it.

Adopting it would therefore mean a privileged root collector with tracefs mounted
at entrypoint, *plus* a symbolizer service, *plus* no supported UI. That is well
past what the demo stack should carry, so tier 3 was dropped. Nothing from the
spike reached the repository; the stack and the one dynamic cluster setting were
both reverted.

## Phase 1 - `kafkaman.handler`

Status: Completed.

Both handler positions in `converge_and_dispatch` are wrapped: the pre-upsert
hook and the post-upsert handler, distinguished by `handler.position`. Errors are
recorded here rather than on `kafkaman.dispatch`, which is the first time the two
are distinguishable — a handler failure that schedules a retry is a successful
dispatch cycle, and the dispatch span correctly stays unmarked.

Only `product` has an application handler in the examples; `order` declares
`ProductSnapshot` with `cache::<T>()` and needs none. So the binary gate asserts
the handler span on the order → product hop.

## Phase 2 - The `kafkaman::internal` tier, and what it uncovered

Status: Completed, after two failures that were worth more than the feature.

74 functions across the four I/O crates carry
`#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]`.
Fifteen are deliberately excluded, in the four categories the decision lists.

**The tier broke the durable trace shape twice before it worked**, and both
failures were the same latent bug: `capture_trace_context()` reads the *ambient*
span, and three call sites persist or transmit what it returns.

1. Annotating `enqueue_inner` put its span id into the outbox row, so
   `kafkaman.relay.publish` stopped descending from `kafkaman.enqueue`.
2. Annotating `Publisher::publish` wrapped the capture that was meant to read the
   caller's span, so the Kafka `traceparent` stopped pointing at the publish.

The second wrote a wrong span id onto the wire, where other services parse it.

An attempt to fix this by exclusion failed instructively: a name-based transitive
analysis of "which functions can reach a capture" removed 68 of 85 annotations,
because `new`, `run`, `get`, and `handle` collide across unrelated types.
Shipping the 17 survivors would have meant shipping a heuristic, so the tier was
reverted whole and the cause fixed instead.

`capture_trace_context_of(&span)` names the span. Enqueue and ingest capture from
their phase span and thread the value down; the publisher captures at the
`Publisher::publish` boundary, where the relay's `.instrument` guarantees the
current span is still the phase span. The `Publisher` trait is unchanged, and
`insert_received` keeps its signature so the test harnesses and examples are
untouched.

With that in place the tier re-applied mechanically, and both durable-gap tests
pass with it live.

## Phase 3 - Tests and documentation

Status: Completed.

- The two broker-backed trace tests run with the internal tier active, which
  makes them the regression test for this interaction. A structural guard in the
  shared drive fails them if the tier ever stops being exercised, so the coverage
  cannot evaporate silently.
- `the_internal_span_tier_is_gated_by_its_own_target` pins the documented
  directive against a real annotated function — `health`, the one that touches
  nothing — rather than a synthetic callsite.
- The binary gate asserts `kafkaman.handler` under product's dispatch, and that
  no internal-tier span reaches the wire at the default filter. That negative is
  structural rather than a list of names: every deliberate span carries a `.`, a
  `/`, or a space, so a bare identifier can only have come from `#[instrument]`.
  It survives the renames those names are explicitly allowed to have.
- `examples/README.md` gains "Going deeper than the waterfall", including why
  every-method timing is not on offer.

## Phase 4 - Measured against the live stack

Status: Completed.

Rebuilt `product` with `RUST_LOG=info,kafkaman::internal=debug` against the
running example stack and drove one request. Two numbers came out of it, and the
second corrected a documented estimate:

- **A request goes from 17 spans to 25**, nested as
  `POST /products -> enqueue -> kafkaman.enqueue -> enqueue_inner -> execute ->
  db.query insert outbox row`, with a duration on each. The durable shape was
  intact with the tier live: `kafkaman.relay.publish` still descended from
  `kafkaman.enqueue`, which is the capture fix holding in a deployment rather
  than in a test.
- **An idle service produces 2322 spans in two minutes with the tier on, against
  5 without it.** The documentation had estimated "several hundred per request",
  which understated the cost and misattributed it: the volume is the scheduler
  loops polling every cycle, not request work. `examples/README.md` and the
  compatibility note now carry the measurement instead.

A note on method, since it cost a wrong conclusion first: `docker compose up
--force-recreate` reuses the existing image. The first attempt measured a binary
built before the attributes existed and found no spans at all. `--build` is
required when the point is to observe a code change.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --lib`
- `cargo test -p observability-tests --features redpanda --test trace_propagation
  --test trace_parented_handoff` — the regression proof, failing twice before the
  capture fix and passing after
- `just examples telemetry-test`
- `just opt-out` and `cargo check -p kafkaman --no-default-features`
- Live: the profiling measurements above, against the running example stack

## Out Of Scope

- Instrumenting every Rust method, which Rust does not support in that form and
  which the proposal explains at length.
- Making internal span names stable.
- Adopting continuous profiling, unless the symbolizer and a UI both arrive.
