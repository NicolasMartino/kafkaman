# Method-Level Timing

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-29
- Category: Observability and debugging
- Scope: Asks whether kafkaman can show how long each Rust method took, the way a JVM agent does, and proposes what to build instead of that.
- Sources:
  - wiki/proposals/19-apm-waterfall-traces.proposal.md
  - wiki/decisions/apm-waterfall-trace-shape.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-core/src/trace.rs
  - examples/otel-collector.yaml
  - https://opentelemetry.io/docs/specs/otel/profiles/
  - https://www.elastic.co/docs/solutions/observability/infra-and-hosts/universal-profiling
- Related:
  - wiki/decisions/method-level-timing-and-span-depth.decision.md
  - wiki/plans/method-level-timing.plan.md

## Why This Proposal Exists

The APM waterfall shipped: a product-create request renders as seventeen spans
across two services, covering HTTP, SQL, enqueue, relay, ingest, and dispatch.

The next question asked of it was direct: **add every Rust method call to the
waterfall, so we can see how long each one took.** That is what a JVM agent
appears to give for free, and it is a reasonable thing to want.

It is also not available in Rust in that form, and the reasons matter enough to
write down rather than rediscover.

## What Rust Cannot Do

**There is no agent model.** A JVM agent rewrites bytecode at class load. Rust
compiles to a native binary; nothing is loaded that could be rewritten.

**Inlining erases most methods.** In a release build, small functions do not
exist as call frames. Instrumenting them means marking them `#[inline(never)]` —
deliberately deoptimising the program so it can report on itself.

**Spans are not free at that density.** A `tracing` span feeding an OTLP layer
costs hundreds of nanoseconds to low microseconds. That is nothing beside a
Postgres round trip, which is why the current spans are free in practice. It is
not nothing for a function called a million times inside one request.

**A waterfall of tens of thousands of spans is not a waterfall.** Backends
truncate, and the UI stops being readable long before they do.

## Proposal

Three tiers, answering different questions.

1. **Close the one real gap at the default filter.** A user's dispatch handler
   ran inside `kafkaman.dispatch` with no span of its own, so a slow handler was
   indistinguishable from slow kafkaman bookkeeping. That is a phase, and it
   deserves a phase span.

2. **Make kafkaman's own internals available on demand.** Instrument them at
   `debug`, behind a dedicated target, so the default waterfall stays readable
   and a developer debugging kafkaman itself can go deeper with one directive.

3. **Measure whether continuous profiling can answer the real question.** "Which
   code burned the CPU" is a profiler's question, not a tracer's. Sampled
   profiling costs a bounded percentage regardless of call density, sees inlined
   frames, and needs no annotations. If the reference stack can render it, it is
   strictly the better tool for what was asked.

## Options Considered

### A. Annotate every function

Rejected as the default. It is the literal request, and it is the wrong shape:
it makes spans do a profiler's job, at higher cost, with worse coverage
(inlined functions stay invisible), and it puts a maintenance burden on every
function signature. Accepted only as tier 2 — off unless asked for.

### B. A proc-macro over whole `impl` blocks

Rejected. It reduces the typing, not the cost or the volume, and it removes the
per-function judgement that keeps hot paths out of the tier.

### C. Continuous profiling

Accepted as the right tool, subject to measurement. See the plan for what
measurement found.

### D. Do nothing beyond the existing waterfall

Rejected. The handler gap is real, and "we cannot instrument every method" is
not a reason to leave the one span that a user's own code most needs.

## Boundaries

This proposal does not add span attributes derived from payloads, arguments, or
entity identity. The internal tier records **no** arguments at all.

This proposal does not make internal span names a stability surface. They are
function names and will change.

This proposal does not move SDK ownership into the library.

## Risks

- **Volume.** The internal tier multiplies span count by roughly an order of
  magnitude. It must be off by default and documented beside the sampler.
- **Trace-shape interference.** Spans nested between a phase span and the code
  that captures trace context can change what is captured. This risk turned out
  to be real and pre-existing; see the decision.
- **False confidence.** A tier that shows kafkaman's internals says nothing
  about the application's own code, which is where a user's time usually goes.

## Resolution

Accepted 2026-08-29. Durable choices are recorded in
`wiki/decisions/method-level-timing-and-span-depth.decision.md`; execution and
the profiling measurement are in `wiki/plans/method-level-timing.plan.md`.
