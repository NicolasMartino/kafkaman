# Telemetry Pipeline Ownership

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-25
- Category: Observability architecture
- Scope: Defines who owns the OpenTelemetry SDK, where instruments are constructed, and the ordering contract between host pipeline setup and kafkaman runtime start.
- Sources:
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - crates/kafkaman-worker/src/metrics.rs
  - crates/kafkaman-rdkafka/src/metrics.rs
  - Cargo.toml (workspace dependency set)
- Related:
  - wiki/decisions/observability-operability-policy.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/metric-instrument-and-attribute-schema.decision.md
  - wiki/plans/opentelemetry-completion.plan.md

## Decision

1. **The host owns the SDK. kafkaman uses the API only.**
   No crate under `crates/` may depend on `opentelemetry_sdk`,
   `opentelemetry-otlp`, or any exporter. Library crates depend on
   `opentelemetry` — the API crate — and nothing more. Applications install the
   `MeterProvider`, `TracerProvider`, and log appender; applications choose the
   exporter and its version.

   **Amended 2026-08-27 — read this as "no *facade-reachable* crate".**
   `crates/kafkaman-otel` holds an SDK and an exporter deliberately. It is a leaf
   nothing depends on but the example binaries, and the facade does not re-export
   it, so the substance — an adopter who depends on `kafkaman` never links an SDK
   — is unchanged and is now asserted directly. See *Amendments*.

2. **kafkaman resolves telemetry through the global provider.**
   Instruments come from `global::meter("kafkaman")`; spans come from the host's
   `tracing` subscriber. kafkaman never installs a provider, never sets a global,
   and never configures a subscriber. This is what the OpenTelemetry
   specification prescribes for instrumentation libraries, and it is what makes
   kafkaman composable with a host that already exports its own telemetry.

3. **Instruments are constructed per run loop, not per process.**
   Instrument handles are created when a scheduler loop starts and owned by that
   loop, alongside its `SchedulerAttrs`. They are not cached in a process-wide
   `OnceLock`. This reverses the M6 implementation and is the substantive change
   in this decision; the reasoning is in *The Hazard* below.

4. **The ordering contract is stated, documented, and tested.**
   A host must install its `MeterProvider` before starting any kafkaman runtime
   loop. Decision 3 narrows the cost of violating it from "silent for the life of
   the process" to "silent for the life of that loop", but it does not eliminate
   it, because the OpenTelemetry API offers no way to rebind an instrument. The
   contract appears in the runtime documentation, in the example, and in a test
   that fails if the behavior regresses.

5. **The `enabled`/`disabled` twin structure is retained.**
   The `metrics` feature stays default-on with a no-op twin behind
   `#[cfg(not(feature = "metrics"))]`, and both twins stay in one file so call
   sites read identically and the pair cannot drift. This part of M6 was right
   and is unchanged.

6. **Exporter dependencies live in exactly two places.**
   `apps/` and `tests/`. This is checkable mechanically and should be checked.
   *Amended 2026-08-27 — see below: a third place, `crates/kafkaman-otel`.*

## Amendments

### 2026-08-25 — `tracing-opentelemetry` is allowed in library crates

Decision 1 says library crates depend on `opentelemetry` "and nothing more".
Implementing trace propagation required one addition, and it is recorded here
rather than taken silently.

Reading the ambient span's trace context — which is what makes a captured
`traceparent` the *caller's* rather than a fresh one — requires
`tracing_opentelemetry::OpenTelemetrySpanExt`. There is no other route: spans
created through `tracing` are held in the subscriber's registry, not on the
OpenTelemetry context stack, so `opentelemetry::Context::current()` does not see
them. Without the bridge, capture would have to be pushed onto every adopter as
a manual step at every call site.

`tracing-opentelemetry` 0.33 depends on `opentelemetry` (API), `tracing`,
`tracing-core`, and `tracing-subscriber`. **It does not depend on
`opentelemetry_sdk`**, which was checked against the published manifest rather
than assumed. So the substance of Decision 1 holds: no crate under `crates/`
pulls an SDK or an exporter, and the host still owns the pipeline. What changes
is that "the API crate" becomes "the API crate and its `tracing` bridge".

It is behind the default-on `traces` feature with a no-op twin, matching
`metrics`, so an adopter who wants no OpenTelemetry dependency at all still has
one flag to reach for.

### 2026-08-27 — a third place for exporters: `crates/kafkaman-otel`

Decision 6 says exporter dependencies live in exactly two places, `apps/` and
`tests/`. A third is added, and it is recorded here rather than taken quietly.

The pipeline the example services install was found duplicated byte-for-byte
across both of them, which is the condition
`wiki/plans/opentelemetry-completion.plan.md` Phase 4 step 4 named as the trigger
for extracting a convenience crate. It is now `crates/kafkaman-otel`, and it
depends on `opentelemetry_sdk`, `opentelemetry-otlp`, and
`opentelemetry-appender-tracing`.

**Decision 1 is untouched, and this is the reason the amendment is narrow.** The
constraint that matters is not "no exporter under `crates/`" — it is *no exporter
in anything an adopter gets by depending on `kafkaman`*. `kafkaman-otel` depends
on no other kafkaman crate, nothing in the workspace depends on it but the two
examples, and the facade deliberately does not re-export it. It is a leaf. An
adopter who never names it never sees an SDK, which is the property decision 1
exists to protect.

Mechanically: `just opt-out` still checks the seven facade-reachable crates
under `crates/`, and `kafkaman-otel` is deliberately absent from that list.
The list is therefore an allowlist by omission, and carries a comment saying so,
because a list with one conspicuous gap invites a well-meaning correction that
would break the build for the wrong reason.

The full reasoning, the rejected alternative of a facade re-export, and the
version-pinning cost this accepts are in
`wiki/decisions/kafkaman-otel-extraction.decision.md`.

## The Hazard

`global::meter()` resolves against whichever provider is installed **at the
moment of the call**, and the returned instruments are bound to that provider
permanently. The OpenTelemetry API deliberately offers no rebinding: an
instrument created against the no-op provider stays a no-op forever.

M6 cached its instruments in a process-wide `OnceLock`:

```rust
pub(crate) fn worker_metrics() -> &'static WorkerMetrics {
    static METRICS: OnceLock<WorkerMetrics> = OnceLock::new();
    METRICS.get_or_init(|| { /* global::meter("kafkaman") ... */ })
}
```

The cache is keyed on nothing but process lifetime. Whatever provider existed at
the first metric recording is the provider every instrument uses until the
process exits.

Two consequences follow, and both matter:

**In production it is a silent failure.** A host that starts a relay before
building its OTel pipeline — a plausible ordering, since the relay is the
application's job and telemetry is infrastructure — gets a permanently silent
metric surface. No error, no warning, no log line. The symptom is an empty
dashboard and no way to work out why.

**In testing it is fatal.** An integration test binary is one process with one
global provider. Under the `OnceLock`, a test that installs an in-memory exporter
can only observe metrics if it happens to run before every other test that
records one, which is not something a test can guarantee. This is the mechanical
reason no test in this repository has ever asserted a metric.

Moving construction into the run loop fixes both. Each loop resolves the meter
when it starts, so a provider installed before *that loop* is honored regardless
of what happened earlier in the process, and a test can install an exporter and
then start a loop.

## Options Considered

**A. Keep the `OnceLock`, document the ordering requirement.**
Zero code change. Rejected: it leaves the surface untestable, which is the
condition that let the double-count defect ship. Documentation does not make a
counter assertable.

**B. Resolve `global::meter()` on every record.**
Correct under any ordering. Rejected on cost: a relay cycle records six times per
poll interval, on the hot path of an idle worker, and meter lookup takes a global
read lock and allocates. M6 already removed a per-record allocation on this exact
path for this exact reason; reintroducing a heavier one to fix a startup-ordering
problem is the wrong trade.

**C. Construct instruments per run loop.** *Accepted.* Resolution happens once
per loop start — an operation that already does far more expensive things —
and the binding window shrinks from process lifetime to loop lifetime. Multiple
providers per process work, which makes the surface testable.

**D. Inject a `Meter` through `ResolvedConfig`.**
Fully explicit, no globals, trivially testable. Rejected: it puts an
OpenTelemetry type in kafkaman's public configuration surface, so every adopter
must reason about telemetry types whether or not they use telemetry, and the
`metrics` feature could no longer be cleanly optional. It also diverges from what
every other Rust instrumentation library does, which has real cost for an adopter
composing several.

**E. Warn when recording against a no-op provider.**
Attractive — it converts silence into a diagnostic. Deferred rather than
rejected: the OpenTelemetry API exposes no reliable way to ask whether the
installed provider is the no-op one, so any implementation would be a heuristic.
Revisit if the API gains the capability.

## Consequences

- `worker_metrics()` and `kafka_metrics()` are removed as free functions.
  Instruments become fields on the structures that own each loop. This is
  internal — both functions are `pub(crate)`.
- Loop start does slightly more work. Immaterial against the database round trip
  that follows it.
- Calling the public single-cycle helpers directly, as tests do, must continue
  not to inflate the counters of a running deployment. M6 established this by
  keeping `record_relay_stats` out of `relay_once`; that separation is preserved.
- Adopters gain a documented startup ordering requirement they did not have
  before — strictly better than an undocumented one they were already subject to.
- A build-level check that no `crates/` manifest names an exporter becomes
  possible, and should be added to the lint gate.

## Verification

- A test installs a `MeterProvider` **after** a first metric recording, then
  starts a loop, and asserts the loop's records reach the exporter. This fails
  against the `OnceLock` implementation and passes against the loop-owned one.
- A test asserts `--no-default-features` builds and records nothing.
- The lint gate asserts no facade-reachable crate depends on an exporter, and —
  since the 2026-08-27 amendment — that `kafkaman` does not reach
  `kafkaman-otel`, which is the form of the claim that actually protects an
  adopter.
