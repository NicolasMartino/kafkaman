# Example Telemetry Integration Test Boundary

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-28
- Category: Observability test strategy
- Scope: Decides how kafkaman will prove the example binaries' OpenTelemetry wiring without conflating that proof with the library observability harness or the Elastic reference deployment.
- Sources:
  - wiki/proposals/18-example-telemetry-integration-tests.proposal.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - tests/distributed-cache/src/lib.rs
  - tests/observability/tests/otlp_wire.rs
  - examples/order/src/main.rs
  - examples/product/src/main.rs
- Related:
  - wiki/plans/example-telemetry-integration-tests.plan.md
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## Decision

1. **The missing proof is binary-level, not service-library-level.**
   The examples' telemetry lifecycle lives in `examples/order/src/main.rs` and
   `examples/product/src/main.rs`. A test that calls `example_order::start` or
   `example_product::start_with` does not exercise that lifecycle, even if it
   runs the same domain service code.

2. **The automated gate launches the example binaries as child processes.**
   The test must observe the behavior an adopter gets from running the examples:
   environment discovery, `kafkaman_otel::init`, runtime construction, signal
   handling, service drain, and provider shutdown. Those are process-entry
   concerns and should be tested across a process boundary.

3. **The test receives OTLP locally and decodes protobufs.**
   Elasticsearch and Kibana are not part of this gate. The gate should run a
   small OTLP/HTTP receiver in the test process, capture `/v1/metrics`,
   `/v1/traces`, and `/v1/logs`, and assert decoded resource attributes and
   kafkaman signal names. This follows the shape already proven in
   `tests/observability/otlp_wire` while changing the subject under test from
   the library harness to the example binaries.

   That receiver is **moved rather than copied**. It currently lives privately at
   the foot of `otlp_wire.rs`; a second hand-rolled HTTP capture server in the
   tree is how two subtly different ones appear, and then three. It becomes
   shared test scaffolding that both suites depend on, which also puts its
   connection handling under the scrutiny of two callers instead of one.

4. **The distributed-cache suite stays in-process.**
   It remains the HTTP-only example-domain proof. It should not install the
   example telemetry pipeline, because doing so would turn two separate example
   processes into one process with shared OpenTelemetry globals. That topology is
   neither what operators run nor what this test needs.

5. **The full compose observability stack remains a deployment proof.**
   `just examples observe` still matters because it proves the collector and
   Elastic topology. This decision does not replace that system check. It adds a
   narrower automated guard for the binary code that hands telemetry to any OTLP
   endpoint.

6. **The first gate is opt-in.**
   It depends on Docker-backed infrastructure and child-process orchestration.
   It should be exposed as an explicit command or ignored integration test first,
   then promoted into the default CI lane only after runtime and flake behavior
   are measured.

## Why

The project already has the three lower-level proofs:

- `tests/observability` proves kafkaman metrics, traces, logs, and propagation.
- `crates/kafkaman-otel/tests` proves the convenience crate's provider and
  subscriber behavior.
- `tests/distributed-cache` proves the example services converge over real HTTP,
  Postgres, and Redpanda.

The gap lives between them. The binary entry point is where the host-owned
telemetry contract is actually implemented, and none of those tests execute it.
That gap is exactly the class that let prior example observability defects
survive until a manual end-to-end run: the code below the boundary worked, but
the application wiring was wrong.

## Alternatives Considered

**A. Cover this through `kafkaman-otel` tests only.** Rejected. A crate can be
correct while a host calls it in the wrong order or forgets to flush it.

**B. Extend `tests/observability/otlp_wire` to use examples.** Rejected. That
suite is intentionally a library telemetry suite. Making it depend on whichever
example currently ships would make a stable compatibility proof depend on sample
application shape.

**C. Extend `tests/distributed-cache`.** Rejected. It bypasses `main.rs` today
for good reasons: in-process tasks keep domain failures debuggable. Adding
OpenTelemetry globals there would make the test less representative and more
fragile.

**D. Test the full compose observability stack.** Deferred. Useful, but too
broad as the first automated guard. When it fails, it may be the collector, the
backend, the dashboard, Docker networking, or the binary wiring. This decision
chooses a smaller failure domain.

**E. Black-box subprocess gate with a local receiver.** Accepted.

## Consequences

- The example binaries need a deterministic way to report readiness and accept
  graceful shutdown in tests. If the current HTTP health surface and signal
  handling are insufficient, the plan may add test harness code around the
  process launcher, but not a telemetry-specific application API.
- The test should avoid asserting on exact export batch counts. OTLP batching is
  timing-sensitive. It should assert presence of service resources, known
  kafkaman instruments/spans/log records, and clean shutdown.

- **The gate proves the flush by making scheduled export impossible.** Each child
  runs with `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_BSP_SCHEDULE_DELAY`, and
  `OTEL_BLRP_SCHEDULE_DELAY` pushed past its own lifetime, so nothing can arrive
  on a timer and every record that arrives arrived because
  `Telemetry::shutdown()` forced it. Left at their defaults — 15s for metrics,
  5s for spans, 1s for logs — a Docker-backed run outlives all three, and the
  gate would pass equally against a binary that never flushes at all. This is
  what makes "shutdown flushed" a claim the gate can honestly make, and it is why
  the missing-shutdown break below is deterministic rather than lucky.

- **The gate sends SIGINT, because SIGINT is all the examples handle.** Both
  binaries select on `tokio::signal::ctrl_c()`, and `examples/compose.yaml` sets
  `stop_signal: SIGINT` on both services to match. That keeps the compose
  demonstration honest, and it bounds what this gate proves: a green run says the
  telemetry lifecycle is correct *for the signal the examples handle*. Under any
  orchestrator that stops a container the ordinary way — Kubernetes, systemd, a
  bare `docker run`, a plain `kill` — the examples receive SIGTERM, whose default
  disposition terminates the process before the drain and the flush, and this
  gate will not say so. Teaching the examples to handle SIGTERM is a change to
  the examples rather than to the tests, and is deliberately not folded into this
  plan; it is recorded here so the gate's silence on it is a known bound rather
  than an assumption.
- The gate should prove both services are represented in telemetry. One
  representative binary is not enough; either entry point can regress
  independently.
- The decision does not change the SDK ownership boundary. Applications still
  install providers; kafkaman library crates do not.

## Revisit If

- Cargo or the workspace layout makes stable binary discovery impossible without
  a brittle path convention.
- The test proves too slow or flaky for even an opt-in Docker lane; in that case,
  split it into per-binary subprocess smoke tests and leave the cross-service
  telemetry check manual.
- The examples stop being the reference host application for kafkaman telemetry.
- The examples learn to handle SIGTERM. The gate should then send it instead, or
  send both in separate cases, because at that point SIGTERM is the signal a real
  deployment delivers and SIGINT is the developer's convenience.
