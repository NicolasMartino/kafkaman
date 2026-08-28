# Example Telemetry Integration Tests

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-28
- Category: Observability test strategy
- Scope: Proposes an automated integration gate that runs the example binaries themselves with OTLP enabled, proving the example-owned telemetry startup and shutdown path rather than only the reusable telemetry crate or the library observability harness.
- Sources:
  - examples/order/src/main.rs
  - examples/product/src/main.rs
  - tests/distributed-cache/src/lib.rs
  - tests/distributed-cache/tests/two_service_cache.rs
  - tests/observability/tests/otlp_wire.rs
  - crates/kafkaman-otel/tests/init_installs_a_subscriber.rs
  - crates/kafkaman-otel/tests/endpoint_installs_providers.rs
  - crates/kafkaman-otel/src/lib.rs
  - examples/compose.yaml
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
- Related:
  - wiki/decisions/example-telemetry-integration-test-boundary.decision.md
  - wiki/plans/example-telemetry-integration-tests.plan.md
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/plans/two-service-distributed-cache-example.plan.md
  - wiki/plans/kafkaman-otel-extraction.plan.md

## Why This Proposal Exists

The worktree now has three useful telemetry test layers, but none of them proves
the thing an adopter is most likely to copy: the example binaries' own
`main.rs` telemetry lifecycle.

`tests/distributed-cache` depends on `example-order`, `example-product`, and
`example-provision`, then starts the services in-process through
`example_order::start` and `example_product::start_with`. That is exactly the
right shape for testing the distributed-cache domain workflow over HTTP, but it
bypasses the binary entry points where `kafkaman_otel::init(...)` is called and
where `Telemetry::shutdown()` is sequenced.

`tests/observability/otlp_wire` proves kafkaman telemetry reaches OTLP on the
wire in all three signals. It is deliberately independent of the current
example, builds the SDK providers itself, and runs through `kafkaman_test::Harness`.
That keeps the library telemetry proof stable, but it cannot catch an example
binary that stops calling `kafkaman_otel::init`, passes the wrong service name,
forgets the endpoint environment, or skips provider shutdown on Ctrl-C.

The `kafkaman-otel` integration tests prove the convenience crate in isolation.
That is necessary and still not enough. A crate can be correct while a binary
uses it incorrectly.

The gap is therefore narrow and real: **there is no automated test that starts
the example binaries with OTLP configured and asserts their own telemetry arrives
after a graceful shutdown.**

## What Is Proposed

Add an opt-in integration gate that launches the `example-order` and
`example-product` binaries as child processes, points them at a local OTLP/HTTP
capture endpoint, drives a minimal real workflow, asks the processes to shut
down cleanly, and asserts all three telemetry signals were flushed.

The test should prove binary-level wiring, not Elasticsearch:

- `service.name` includes both `kafkaman-example-order` and
  `kafkaman-example-product`.
- At least one kafkaman metric series arrives from a real runtime loop.
- At least one kafkaman span arrives from the example flow.
- At least one OTel log record arrives with the expected service resource.
- The shutdown path flushes buffered spans and logs before process exit.
- The test fails if either binary removes `kafkaman_otel::init(...)`.

The flush claim is the one that is easy to assert and hard to actually prove, so
it should be arranged rather than hoped for: each binary runs with its metric
interval and both batch schedule delays pushed past the test's own lifetime, so
no export can fire on a timer and everything captured was forced out by
`Telemetry::shutdown()`. At the SDK defaults — 15s, 5s, and 1s — a Docker-backed
run outlives all three, and every assertion above would pass against a binary
that never flushes.

The capture endpoint should be a small in-test OTLP receiver — the one
`tests/observability/otlp_wire` already owns privately, extracted into shared
test scaffolding rather than written a second time. It should not depend on
Elasticsearch, Kibana, or the example collector. Those remain the reference
deployment demonstration; this proposal is about application wiring.

## What Is Deliberately Not Proposed

**No in-process extension of `tests/distributed-cache`.** That suite already
starts both services in one test process. OpenTelemetry subscribers and providers
are process-global, and the queue-gauge sampler is also process-wide. Making the
existing suite call binary telemetry setup would test a topology the examples do
not run and would introduce global-state failures unrelated to the application
behavior.

**No claim that the Elastic/Kibana dashboard is covered.** `just examples
observe` and the collector profile remain the end-to-end deployment proof. This
new gate should be faster, more targeted, and deterministic enough to run in CI
or as an ignored Docker-backed test.

**No new telemetry surface on `RuntimeBuilder`.** The accepted ownership
decision still holds: the host owns the SDK. The examples are hosts, so their
binaries are the right place to test this.

**No gRPC, TLS, or backend-specific assertion.** The existing decisions keep the
example on OTLP/HTTP and keep backend temporality conversion in the collector.
The test should verify that the binaries send valid OTLP requests, not that a
specific backend stores them.

## Options Considered

**A. Treat the existing observability tests as enough.** Rejected. They prove
kafkaman's instrumentation and OTLP wire format, but not the example entry
points or shutdown path.

**B. Add telemetry to the in-process distributed-cache suite.** Rejected. It
would collide with process-global subscriber/provider state and still would not
execute the binaries' `main.rs`.

**C. Add binary-level tests inside each example crate.** Viable for per-binary
startup coverage, but weaker as the primary gate because the useful signal is the
two-service flow that exercises publish, ingest, dispatch, spans, logs, and
queue metrics together.

**D. Add a black-box binary integration gate with a local OTLP receiver.**
Accepted. It directly targets the missing proof, keeps the backend out of the
critical path, and fails on the regression this proposal exists to catch.

**E. Drive the full compose observability profile in CI.** Deferred. It is still
valuable as a manual or nightly system check, but it tests Elastic, Kibana, the
collector, Docker networking, and the examples all at once. That is too broad
for the first automated binary-wiring gate.

## Consequences

- The examples become tested as binaries, not only as library crates. This is the
  distinction that matters for host-owned telemetry.
- The gate will be slower than the current package-level example tests because
  it needs real Postgres, real Redpanda, two child processes, and an OTLP
  receiver. It should therefore be opt-in or ignored unless CI has a Docker lane
  intended for it.
- The test harness needs explicit child-process shutdown handling. A forced kill
  proves nothing about provider flushing; the success path must send the signal
  the binaries handle and wait for clean exit before assertions. That signal is
  SIGINT — both binaries select on `tokio::signal::ctrl_c()` and nothing else —
  and the standard library has no way to send it, so the harness takes a `libc`
  or `nix` dev-dependency to do so.

- The gate is therefore silent about SIGTERM, which is what every ordinary
  container stop delivers. `examples/compose.yaml` compensates with
  `stop_signal: SIGINT` on both services, so the shipped demonstration is honest;
  an adopter who lifts these binaries into Kubernetes or systemd without carrying
  that setting loses the drain and the flush entirely, and a green gate will not
  warn them. The boundary decision records this as a known bound rather than
  widening this proposal into an example change.
- The test should record deliberate break evidence before the plan closes.
  Removing `kafkaman_otel::init` or dropping `Telemetry::shutdown()` must fail
  the gate for the reason the proposal claims.

## Verification

- `cargo test -p kafkaman-otel` continues to cover the reusable crate in
  isolation.
- The new example telemetry gate starts both binaries, drives a real HTTP flow,
  shuts them down cleanly, and decodes captured OTLP metrics, traces, and logs.
- Deliberate break 1: remove `kafkaman_otel::init` from one binary and confirm
  that service's telemetry is absent and the test fails.
- Deliberate break 2: skip `Telemetry::shutdown()` and confirm that binary's
  telemetry does not arrive at all — which the stretched export intervals make a
  deterministic outcome rather than a race the test usually wins.
- The OpenTelemetry completion plan is updated only after the gate exists and
  has been run.
