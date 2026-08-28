# Example Telemetry Integration Tests Plan

- Document Class: Plan
- Status: Completed 2026-08-29
- Date: 2026-08-28
- Category: Delivery execution
- Scope: Add an opt-in integration gate that starts the order and product example binaries with OTLP enabled, drives a real two-service flow, shuts the binaries down cleanly, and proves metrics, traces, and logs were exported from those binaries.
- Sources:
  - wiki/proposals/18-example-telemetry-integration-tests.proposal.md
  - wiki/decisions/example-telemetry-integration-test-boundary.decision.md
  - wiki/plans/opentelemetry-completion.plan.md
  - tests/distributed-cache/src/lib.rs
  - tests/observability/tests/otlp_wire.rs
  - examples/order/src/main.rs
  - examples/product/src/main.rs
  - examples/compose.yaml
  - crates/kafkaman-otel/src/lib.rs
- Related:
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/kafkaman-otel-extraction.decision.md
  - wiki/decisions/telemetry-backend-and-example-topology.decision.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## Deliverable

A maintainer can run one explicit gate that starts the same example binaries a
user runs, points them at a local OTLP endpoint, exercises the distributed-cache
example over HTTP, sends graceful shutdown, and sees decoded metrics, traces, and
logs from both services.

## In Scope

- A Docker-backed integration harness using the existing example infrastructure:
  Postgres, Redpanda, and `example-provision`.
- Child-process launch of the order and product binaries, with environment
  variables set the way the examples document them.
- Extracting the OTLP capture receiver that `tests/observability/otlp_wire`
  currently owns privately into a shared support crate, and repointing
  `otlp_wire` at it.
- Assertions that prove both service binaries installed telemetry and flushed it
  on shutdown.
- A dedicated command for the gate, because this is slower and more operational
  than the fast unit and package integration tests.

## Out Of Scope

- Kibana data views or dashboards.
- Elasticsearch storage assertions.
- Changing `kafkaman-otel`'s public API.
- Adding any telemetry hook to `RuntimeBuilder` or the `kafkaman` facade.
- gRPC or TLS transport coverage.
- Making this part of the default fast test lane before runtime is measured.
- Teaching the example binaries to handle SIGTERM. The gap is real and recorded
  in the decision's Consequences, but it is an example change, not a test change,
  and this plan does not widen to it.

## Starting Point

Existing coverage is intentionally split:

- `tests/distributed-cache` uses the example service crates and drives them over
  HTTP, but starts library helpers instead of binaries.
- `tests/observability/otlp_wire` proves all three OTLP signals on the wire, but
  constructs providers manually and does not involve the examples.
- `crates/kafkaman-otel/tests` covers `init` and `builder` behavior in isolation.

That leaves the example entry points untested. The new gate closes only that
gap.

## Naming

Fixed here so that every phase below and the Verification section refer to one
thing:

- Package directory: `tests/example-telemetry/`
- Cargo package: `example-telemetry-tests`
- Integration test: `tests/example-telemetry/tests/binary_telemetry.rs`
- Shared capture crate: `tests/otlp-capture/`, package `otlp-capture`
- Command: `just examples telemetry-test`

`telemetry-test` rather than `telemetry`: `just examples all` now starts the
telemetry stack, so a bare `telemetry` arm would read as one more way to bring
something up rather than as the gate that checks it.

The package joins `[workspace] members` in the root `Cargo.toml`. It needs no
entry in the justfile's `opt-out` SDK-boundary guard: that recipe iterates a
fixed list of `crates/` members, and the ownership decision already permits
exporters and their vocabulary under `tests/`.

## Phase 1 - Make the current test map explicit

Record in test comments or module docs which layer owns which proof:

- `tests/distributed-cache`: example domain wiring over HTTP.
- `tests/observability`: kafkaman telemetry semantics and OTLP wire format.
- `crates/kafkaman-otel/tests`: reusable host pipeline behavior.
- New gate: example binary telemetry lifecycle.

This is not busywork. The failure mode is future duplication in the wrong place:
someone will naturally try to add telemetry to `tests/distributed-cache`, which
would put two example applications into one OpenTelemetry process and test the
wrong topology.

**Exit:** the ownership split is visible next to the tests that enforce it.

## Phase 2 - Extract and harden the OTLP capture harness

The receiver already exists, as roughly 120 lines at the foot of
`tests/observability/tests/otlp_wire.rs`: a `TcpListener` on port 0, a thread per
connection, `content-length` framing, and `Captured { path, headers, body }`.
Reimplementing that shape in a second place is how three subtly different
receivers appear. Move it instead.

**2a. Extract.** Lift the listener, `request_is_complete`, `find_headers_end`,
`Captured`, `drain`, `service_name`, and `kind_of` into a shared support crate
and repoint `otlp_wire` at it. `otlp_wire` passing unchanged afterwards is the
proof the extraction was faithful. It lives in `tests/otlp-capture` rather than
`kafkaman-test`: the suite scaffolding is not moving there, and this keeps the
OTLP vocabulary inside `tests/`, which is what the ownership decision permits.

**2b. Harden for more than one client.** The extracted loop answers one request
per connection and then drops the socket. That is sufficient for `otlp_wire` —
one process, three exporters, a handful of exports — and it is the most likely
source of flake here, where two child processes export repeatedly over a longer
run. The OTLP exporter's HTTP client pools connections, so a request written to
a socket the receiver is about to close is lost with no retry: OTLP POSTs are
not replayed.

Fix it in whichever direction is simpler to keep correct, and say which in the
code:

- read requests in a loop on each connection until the peer closes, or
- answer with `Connection: close`, so the client never reuses a socket.

**2c. Route by path.** Accept `POST /v1/metrics`, `/v1/traces`, and `/v1/logs`,
and keep decoded payloads in memory until the test finishes. Keep assertions
semantic rather than batch-count-based. The SDK is allowed to batch. The test
cares that expected resources and records arrive, not that they arrive in a
specific number of HTTP requests.

**Exit:** `cargo test -p observability-tests --test otlp_wire` passes against the
extracted receiver, and a harness-level test posts one fixture request per signal
over a single reused connection and sees all three stored.

## Phase 3 - Launch real example binaries

Create `tests/example-telemetry/tests/binary_telemetry.rs`, which:

1. Starts Postgres and Redpanda through the existing Testcontainers pattern.
2. Runs `example-provision` to create both databases and compact topics.
3. Starts the product and order binaries as child processes.
4. Waits for each service's `/health` endpoint.
5. Drives the smallest two-service flow that produces publish, ingest, dispatch,
   and HTTP request activity.
6. Sends SIGINT and waits for clean process exit.
7. Inspects captured telemetry only after both processes have exited.

The details below are the ones that decide whether this runs at all.

**Binary discovery is by environment variable, not by Cargo.** `CARGO_BIN_EXE_*`
is only defined for integration tests of the package that declares the binary, so
it is unavailable here and no conditional is needed. The `just examples
telemetry` recipe builds both binaries, then passes their paths as
`EXAMPLE_ORDER_BIN` and `EXAMPLE_PRODUCT_BIN`. The test must fail with a message
naming the recipe when either variable is unset — never skip. A gate that
silently passes when it did not run is worse than no gate.

**Each child runs with its own working directory.** `main.rs` calls
`Config::discover()`, which walks up from the current directory looking for
`kafkaman.toml`. Spawn the order binary with `current_dir` set to
`examples/order` and the product binary with `examples/product`. Without this,
both children die at boot on a config error that looks nothing like a telemetry
failure.

**The environment each child gets:**

- `DATABASE_URL` — that service's Postgres database.
- `KAFKA_BROKERS` — the Redpanda container's host address.
- `BIND_ADDR` — see the port note below.
- `KAFKA_CONSUMER_GROUP` — suffixed per run. Two runs sharing a group split the
  single partition, and the loser idles while still reporting healthy; that is
  the footgun `examples/compose.yaml` already documents for the case of a host
  process and a container sharing a group, and concurrent gates hit it the same
  way.
- `OTEL_EXPORTER_OTLP_ENDPOINT` — the capture server from Phase 2.
- `RUST_LOG=info` — the log signal only carries records that pass the
  subscriber's filter. Without it the log assertion in Phase 4 fails for filter
  reasons that read as an export failure. Compose sets the same default for the
  same reason.
- `OTEL_METRIC_EXPORT_INTERVAL=3600000`, `OTEL_BSP_SCHEDULE_DELAY=3600000`, and
  `OTEL_BLRP_SCHEDULE_DELAY=3600000` — all in milliseconds. See Phase 4; this is
  what turns the whole gate into a shutdown-flush proof rather than an
  export-happened-eventually proof. `kafkaman_otel` honours the metric variable
  rather than overriding it, deliberately, so this reaches the reader.

**Ports.** Ask the OS for a port by binding `127.0.0.1:0`, and keep the listener
alive until the moment the child is spawned rather than closing it and passing
the number along — the window between close and the child's bind is a race
against everything else on the machine. If holding it is awkward, retry the spawn
on a bind failure instead. Do not hard-code 3001 and 3002: a developer running
`just examples demo` in another terminal owns those.

**Shutdown is SIGINT, and needs a dependency.** Both binaries select on
`tokio::signal::ctrl_c()`, which is SIGINT only, and `examples/compose.yaml` sets
`stop_signal: SIGINT` on both services for exactly that reason. The standard
library cannot express this step at all — `Child::kill` sends SIGKILL — so the
harness needs a way to send a signal. Not `libc`: `unsafe_code` is `forbid` at
the workspace level, and `forbid` cannot be lifted by a crate-local `allow`, so
calling `libc::kill` would mean weakening the lint for every crate to reach one
line. `rustix::process::kill_process` is a safe wrapper over the same call, and
rustix is already in the dependency graph — the cost is a feature, not a
compilation. A forced kill proves nothing about provider flushing, so this is not
an implementation detail that can be deferred.

**Nothing may outlive the test.** A `Child` is not killed when it drops, so an
assertion panic leaves two service processes holding ports and consumer-group
membership, and the next run fails for reasons that have nothing to do with the
change under test. Wrap the children in a guard whose `Drop` sends SIGKILL to any
survivor, and bound the wait for clean exit with a timeout that escalates to
SIGKILL and fails the test with that fact stated.

**Capture child output.** Pipe each child's stdout and stderr and print them on
failure. When this gate fails, the reason is usually in the child's log and
otherwise invisible.

**Exit:** the runner can start both binaries, drive the flow, and shut them down
cleanly without asserting telemetry yet, and leaves no process behind when an
assertion fails.

## Phase 4 - Assert the telemetry contract

Assert the captured OTLP payloads prove:

- metrics, traces, and logs all arrived;
- both `kafkaman-example-order` and `kafkaman-example-product` appear as
  `service.name` resources;
- at least one kafkaman runtime metric arrives from the real loops;
- at least one of the known kafkaman spans arrives from the real flow;
- at least one log record arrives from each service;
- the assertions run only after graceful process exit, so buffered telemetry has
  had its flush window.

**The export intervals set in Phase 3 are what make these assertions mean
something.** With the periodic reader and both batch processors pushed out to an
hour, no scheduled export can fire inside the life of the test. Every record that
arrives therefore arrived because `Telemetry::shutdown()` forced it — so the
positive assertions above are simultaneously the proof that the shutdown path
works. Left at their defaults the numbers are 15s for metrics, 5s for spans and
1s for logs, all of them shorter than a Docker-backed run, and the gate would
pass just as happily against a binary that never flushes. `otlp_wire` already
uses this technique with a 3600s reader.

Prefer named span and instrument assertions already established by
`tests/observability` over new names. This gate should not become a second
metric-schema compatibility suite.

**Exit:** removing the OTLP endpoint, deleting `kafkaman_otel::init`, or skipping
shutdown makes the test fail for an observable reason.

## Phase 5 - Deliberate break gates

Before closing the plan, demonstrate the test catches the two regressions it is
being written for:

**Both demonstrated 2026-08-29, against `examples/order`.**

1. **`kafkaman_otel::init` bypassed** — replaced with a `builder()` chain calling
   `without_metrics().without_traces().without_logs()`, so no subscriber and no
   providers are installed. The service ran normally and the domain flow
   completed; the gate failed in 13.0s with
   `services ["kafkaman-example-product"]` and log records from `product` alone.
   `order` contributed nothing to any of the three signals.

2. **`Telemetry::shutdown()` skipped** — the value dropped the way it would if a
   maintainer simply forgot the call. Identical failure: nothing whatsoever
   arrived from `kafkaman-example-order`, deterministically, on the first run.

The second break also answered a question the plan did not think to ask. If
dropping `Telemetry` had flushed through the SDK's own `Drop` impls, break 2
would have passed and the explicit `shutdown()` call would have been
belt-and-braces. It does not, and it is not: the call is load-bearing, and a
binary that omits it loses everything it recorded. That is worth knowing about a
contract `kafkaman-otel`'s docs state but nothing previously enforced.

One correction came out of it. Both breaks fail on the same assertion, whose
message originally named only a missing `init`. With every export schedule pushed
past the test's lifetime, a missing pipeline and a missing flush are
indistinguishable from the receiver's side — so the message now names both causes
rather than sending a reader after the wrong one.

**Exit:** met.

## Phase 6 - Wire the command and documentation

Add `just examples telemetry-test`, alongside the existing `all`, `demo`,
`observe`, `up`, `ui`, and `logs` arguments of the same recipe — it belongs next
to `observe` and `all`, which are the manual proofs it complements. The recipe builds both example
binaries, exports `EXAMPLE_ORDER_BIN` and `EXAMPLE_PRODUCT_BIN`, and runs the
gate.

Then update:

- `wiki/plans/opentelemetry-completion.plan.md` Phase 4 with the new automated
  binary-level proof;
- `wiki/index.md`;
- `wiki/log.md`;
- example README text only if the developer workflow changes.

**Exit:** the command is discoverable and the wiki distinguishes the automated
binary test from the manual collector/Elastic observability demo.

## Verification

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets`
- `cargo test -p kafkaman-otel`
- `cargo test -p observability-tests --test otlp_wire`, which is what proves the
  Phase 2 extraction did not change the receiver's behavior
- `just examples telemetry-test`
- `cargo test -p distributed-cache-tests --test two_service_cache` or the
  nearest existing distributed-cache smoke gate
- `just opt-out`, because the workspace gained a member; that recipe's guard
  iterates a fixed list of `crates/` and should be unaffected, and running it is
  how that is known rather than assumed
- `docker compose -f examples/compose.yaml --profile services --profile observability config --quiet`

Run broader workspace checks if implementation touches shared runtime,
configuration, or telemetry crates.

## Closure

**Closed 2026-08-29.** The gate exists as `tests/example-telemetry`, runs as
`just examples telemetry-test`, and passes in 23.6s — fast enough that the
opt-in framing is about its Docker dependency rather than its runtime. Both
deliberate breaks are demonstrated above. `wiki/plans/opentelemetry-completion.plan.md`
Phase 4 no longer rests on `just examples observe` alone for the claim that the
examples' own telemetry wiring works.

Two things landed differently from the plan as written:

- **`rustix`, not `libc`.** `unsafe_code` is `forbid` at the workspace level and
  `forbid` cannot be lifted by a crate-local `allow`, so `libc::kill` would have
  meant weakening the lint for every crate to reach one line.
  `rustix::process::kill_process` is a safe wrapper over the same syscall and was
  already in the dependency graph.
- **`telemetry-test`, not `telemetry`.** `just examples all` now starts the
  telemetry stack, so a bare `telemetry` arm would have read as another way to
  bring something up.

Not closed, and deliberately so: the examples still handle SIGINT only. The gate
sends SIGINT because that is what they accept, so it is silent about the SIGTERM
that every ordinary container stop delivers. `examples/compose.yaml` compensates
with `stop_signal: SIGINT`. That bound is recorded in the decision's Consequences
and its Revisit If.
