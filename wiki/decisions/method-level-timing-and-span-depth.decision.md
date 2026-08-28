# Method-Level Timing and Span Depth

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-29
- Category: Observability architecture
- Scope: Fixes how deep kafkaman's spans go, how the deep tier is reached, and the rule that keeps durable trace context pointing at the phase that produced it.
- Sources:
  - wiki/proposals/20-method-level-timing.proposal.md
  - wiki/decisions/apm-waterfall-trace-shape.decision.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
  - crates/kafkaman-core/src/trace.rs
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-sqlx/src/outbox_enqueue.rs
  - crates/kafkaman-rdkafka/src/publisher.rs
  - crates/kafkaman-rdkafka/src/consumer.rs
  - crates/kafkaman-axum/src/lib.rs
  - tests/observability/src/lib.rs
  - tests/example-telemetry/tests/binary_telemetry.rs
- Related:
  - wiki/plans/method-level-timing.plan.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Decision

1. **Spans mark boundaries and waits; profiles measure code.**
   The waterfall answers "where did this request wait" — SQL, the broker, the two
   durable gaps. "Which function burned the CPU" is a profiler's question, and
   answering it with spans costs more and covers less, because inlined functions
   have no call frame to instrument.

2. **`kafkaman.handler` is a phase span, at the default filter.**
   An application handler is the one piece of *user* code inside a kafkaman
   cycle. It gets `otel.kind = "internal"`, `message_type`, and
   `handler.position` (`before` or `after`). Its name is stable.

3. **Handler failures are recorded on `kafkaman.handler`, not on
   `kafkaman.dispatch`.**
   A handler error that schedules a retry is a successful dispatch cycle by
   `dispatch_once`'s contract. Marking the dispatch span would make an APM error
   rate count work the system is handling as designed.

4. **kafkaman's internals are instrumented at `debug`, behind their own target.**
   `#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]`.
   The target is load-bearing: a plain `debug` filter also enables `sqlx` and
   `rdkafka` debug logging, which buries what the tier was turned on to show.
   `skip_all` is equally load-bearing — bare `#[instrument]` records every
   argument via `Debug`, which would put envelopes, rows, and payload bytes into
   spans and break the no-payload rule.

5. **Internal span names are not a compatibility surface.**
   They are function names and will change. Dashboards belong on the `kafkaman.*`
   phase spans and the `db.query` summaries.

6. **Four kinds of function stay out of the tier.**
   Those that build a phase span (a second span would nest a duplicate); those
   that run until shutdown (a span covering hours is not a waterfall row and
   never exports until the process ends); those that read the ambient span (see
   below); and bodiless trait declarations. `kafkaman-core` is excluded entirely,
   because it performs no I/O and therefore has no time to attribute.

7. **Code that persists or transmits trace context must name the span it means.**
   This is the rule the rest of this decision exists to protect, and it is stated
   in full below.

8. **Continuous profiling is deferred, with evidence rather than assumption.**
   It was measured against the reference stack. See the plan for what worked and
   the two things that did not.

## Durable Capture Names Its Span

`capture_trace_context()` returns the context of whichever span is current. Three
call sites persist or transmit what it returns:

| Site | Destination |
| --- | --- |
| outbox enqueue | the outbox row's `traceparent` column |
| received insert | the received row's `traceparent` column |
| Kafka publish | the `traceparent` header on the wire |

Each of the three means one particular span — the `kafkaman.enqueue`,
`kafkaman.ingest`, or `kafkaman.relay.publish` that names the phase. **Nothing
enforced that.** Any span opened between the phase span and the capture became
the stored context instead. The row stayed well-formed; it pointed at a private
function rather than the documented phase, and in the publish case that value
went onto the wire for other services to parse and parent from.

This was not hypothetical. Introducing the internal tier broke it twice, at two
different assertions, before the cause was understood:

- annotating `enqueue_inner` put *its* span id in the outbox row, so
  `kafkaman.relay.publish` stopped descending from `kafkaman.enqueue`;
- annotating `Publisher::publish` wrapped the very capture that was meant to read
  the caller's span, so the Kafka header stopped pointing at the publish.

**The rule.** Code that persists or transmits trace context captures from a named
span, via `capture_trace_context_of(&span)`. Enqueue and ingest capture from
their phase span and pass the value down. The publisher captures at the
`Publisher::publish` boundary, where the relay's `.instrument` guarantees the
current span is still the phase span.

**The corollary.** A function that reads the *ambient* span must never itself be
wrapped in one. Three functions are in that position and are excluded from the
internal tier for exactly this reason.

This amends nothing in the trace-context decision's model — the durable gaps, the
link-versus-parent choice, and the header namespace all stand. It closes the gap
between what that decision assumed and what the code enforced.

## Options Considered

### A. Instrument every function, at `info`

Rejected. It is the literal request and the wrong shape: spans doing a
profiler's job, at higher cost, blind to inlined frames, and turning a readable
seventeen-span waterfall into several hundred rows by default.

### B. Instrument every function at `debug`, on the default `debug` filter

Rejected in favour of a dedicated target. Reaching kafkaman's spans should not
require enabling every dependency's debug logging.

### C. Exclude the hazardous functions case by case

Rejected as the mechanism, though it remains as a narrow backstop. A
name-based transitive analysis of "which functions can reach a capture"
over-removed 68 of 85 annotations, because names like `new`, `run`, and `get`
collide across unrelated types. Correctness cannot rest on a heuristic that
coarse.

### D. Name the span at the capture site

*Accepted.* It fixes the cause rather than the symptom, needs no
signature change on the `Publisher` trait, and makes the tier safe to apply
mechanically.

### E. Change `Publisher::publish` to take the context

Rejected. It would break every implementor for a problem solvable at the
boundary, where `.instrument` already guarantees the right span is current.

## Consequences

- The durable trace contract no longer depends on nobody adding a span in the
  middle of it. That was previously true only by accident.
- The two broker-backed trace tests run with the internal tier live, which makes
  them the regression test for this interaction. A structural guard fails them if
  the tier ever stops being exercised.
- `kafkaman-core` gains `capture_trace_context_of`;
  `insert_received_with_outcome` gains a parameter; `RdkafkaPublisher` gains
  `publish_row_traced`. Recorded in the compatibility note.
- The internal tier costs a callsite check per annotated call when disabled,
  which is a cached atomic load, and roughly an order of magnitude more spans
  when enabled.

## Revisit If

- A Rust runtime agent or a stable OpenTelemetry profiling exporter makes
  whole-binary timing available without annotations.
- Elastic's symbolizer becomes part of the reference stack, which would make the
  profiling path worth re-measuring.
- The internal tier is found to be enabled in a deployment often enough that its
  volume, rather than its absence, is the problem.
- A fourth site starts persisting or transmitting trace context. The rule above
  applies to it from the first patch, not after it breaks.
