# Failure Examples and Panic Containment

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-29
- Category: Examples and runtime behaviour
- Scope: Asks what the example stack should do when things go wrong, and proposes making every failure path drivable, asserted, and visible.
- Sources:
  - wiki/proposals/20-method-level-timing.proposal.md
  - wiki/decisions/observability-operability-policy.decision.md
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-axum/src/lib.rs
  - examples/smoke.sh
- Related:
  - wiki/decisions/handler-panic-containment-and-fault-injection.decision.md
  - wiki/plans/failure-examples.plan.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Why This Proposal Exists

The APM waterfall shipped and reads well. The next question asked of it was
whether the examples could show *errors* too — unexpected ones, failed attempts,
a panic in a listener or a producer — and whether the program recovers.

The measurement that motivated everything below was taken before writing any
code, against the stack as it then ran:

| | |
| --- | --- |
| spans indexed | 227,394 |
| spans carrying any status field | **0** |
| log records at `WARN` or `ERROR` | **0** |
| rows in any dead-letter queue | **0** |

So the error half of the observability story was not merely undemonstrated. It
was **unproven**: nothing established that `otel.status_code = "ERROR"`, which
`kafkaman_core::record_error` sets, survived the collector's `mapping.mode: otel`
into a field Kibana could query. The examples could not produce a single error to
find out.

Three failure surfaces already existed in the library and none was reachable from
the examples: dispatch retry and the DLQ, ingest quarantine for records that can
never become rows, and the operator routes themselves — `kafkaman-axum` ships
`admin_router` and `redrive_router`, and neither example mounted either one.

## Proposal

1. **Give `product` a fault switch.** An explicit control plane that makes its
   dispatch handler return an error, or panic, for a bounded or unbounded number
   of calls. Deterministic, armable without a restart, and in the OpenAPI spec so
   it documents itself.

2. **Mount the operator routes in both examples.** The DLQ and redrive scenarios
   are not assertable without them, and shipping an operability surface that no
   example exercises is how it rots.

3. **Write an asserted walkthrough of the failure paths**, the counterpart to
   `examples/smoke.sh`: transient failure absorbed, permanent failure
   dead-lettered, redrive, panic contained, poison record quarantined, broker
   outage survived.

4. **Contain handler panics in the library.** This one is not an example change,
   and it is the reason the proposal exists at all — see below.

5. **Give Kibana a failures panel**, built on a field name verified against a
   real error document rather than guessed.

## The Panic Was Worse Than Expected

The question "what happens if a handler panics" had an answer nobody had looked
at. The panic unwound out of the handler, out of the dispatcher's task, and out
of the supervised runtime as `RuntimeError::Panicked`, taking the whole process
down — HTTP server included. Compose restarted it; the claim transaction had
rolled back, so the row was still `Pending`; it was claimed again and panicked
again. After `restart: on-failure:3` compose gave up.

**One bad message killed the service permanently**, and nothing in the repository
said so.

The fail-fast design was defensible against what it was guarding: a dispatcher
loop that dies quietly while `/health` keeps answering 204, so the service looks
healthy and silently stops processing forever. But there is a third option, and
it is the right one — catch the unwind at application-owned boundaries and hand
it to the retry, DLQ, or ingest-quarantine machinery that already exists.

## Options Considered

### A. A fault-injection endpoint on `product`

*Accepted.* Deterministic, scriptable, needs no restarts, and keeps the trigger
out of the business data.

### B. A sentinel in the domain payload

Rejected. A product named `fail:` would put test hooks into the entities the
example exists to model, and would be harder to un-arm than a `DELETE`.

### C. An environment variable at boot

Rejected. Every change needs a container restart, which muddies the recovery
behaviour the scenarios are trying to observe — and in the panic case, a restart
is precisely the thing under test.

### D. Leave the panic as fail-fast and just demonstrate it

Rejected after the question was asked directly. Demonstrating a sharp edge is
worth less than removing it, and the containment costs one narrow boundary.

### E. `catch_unwind` around the whole dispatch loop

Rejected. A panic inside kafkaman is a bug in kafkaman, and swallowing it leaves
a process that looks healthy while a loop is dead — the exact failure the
fail-fast design was built to prevent.

## Boundaries

This proposal does not add fault injection to any published crate. The switch
lives in `examples/product` and travels nowhere.

It does not make a panicking handler a supported way to report failure. Handlers
should return `Err`; what changed is the blast radius when one does not.

It does not add a configuration knob for panic policy. One behaviour, no switch,
until someone needs the other one.

## Risks

- **A demo that ships a way to break itself.** Contained by living in the example
  service only, and by being visible in the OpenAPI spec rather than hidden.
- **Assertions that depend on timing.** The DLQ scenario waits out real backoff.
  Bounded by generous deadlines and by asserting on state rather than duration.
- **Catching a panic mid-transaction.** Real, and already solved: the existing
  failure path abandons an unusable transaction and records on a fresh
  connection.

## Resolution

Accepted 2026-08-29. Durable choices are in
`wiki/decisions/handler-panic-containment-and-fault-injection.decision.md`;
execution, and the two library bugs the work uncovered, are in
`wiki/plans/failure-examples.plan.md`.
