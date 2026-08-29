# Method-Level Timing and Span Depth

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-29 (amended 2026-08-30: points 4 and 5)
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
  - tests/observability/tests/dispatch_failure_status.rs
  - tests/example-telemetry/tests/binary_telemetry.rs
  - examples/kibana-dashboard.sh
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

3. **Handler failures are recorded on `kafkaman.handler` and the enclosing
   `kafkaman.dispatch`.**
   A handler error that schedules a retry is still handled by the durable
   receive contract: `dispatch_once` returns `Ok(DispatchStats)`, the row records
   the failure, and the retry/DLQ policy owns what happens next. The APM
   transaction outcome is a different question. Once the row-level failure has
   been durably recorded, `kafkaman.dispatch` is marked failed so service
   overview and transaction views can split receive work by ok/failed.
   `kafkaman.handler` carries the narrower handler span, including position and
   panic outcome; both spans carry the same bounded failure kind/type/stage
   attributes.

4. **kafkaman's internals are instrumented behind their own target, and split
   across two levels by what makes them run.** *(Amended 2026-08-30; the
   original text is preserved under "Amendment: the tier is split by shape,
   not off by default" below.)*

   `target = "kafkaman::internal"` on all of it, `skip_all` on all of it, and
   the level chosen by one question:

   - **Runs because there is a message to describe → `info`.** In the default
     trace. About sixty functions: the dispatch path, the cache path, the
     store and quarantine paths, enqueue's body, publish, the mark paths,
     migrations, and the admin route handlers.
   - **Runs on a timer whether or not there is work → `debug`.** Off by
     default, reached by the target. Sixteen functions: `claim_batch`,
     `collapse_stale_pending_rows`, `claim_received_row`, `relay_once`, the
     three `dispatch_once*`, `purge_outbox_once`, `refresh`, `collect`,
     `observe`, `outbox_status_summary`, `received_status_summary`, `health`,
     `ready`, and `enqueue`.

   The target is load-bearing for the second half: a plain `debug` filter also
   enables `sqlx` and `rdkafka` debug logging, which buries what the tier was
   turned on to show. `skip_all` is load-bearing for both — bare
   `#[instrument]` records every argument via `Debug`, which would put
   envelopes, rows, and payload bytes into spans and break the no-payload rule.

   Two functions are `debug` for reasons other than shape, and both are
   commented where they are defined. `health` and `ready` are `debug` because an
   orchestrator probes them forever — the same cost as a timer, from the other
   side. `enqueue` is `debug` to preserve a trace *root*: `kafkaman.enqueue` is
   the root span in a service with no caller span, and a function span above it
   would move the root onto a name point 5 explicitly refuses to stabilize.
   `outbox_status_summary` and `received_status_summary` are `debug` because the
   queue-metrics sampler calls them on `refresh_interval`, which makes them
   timer-shaped by way of their caller rather than by their own definition.

5. **Internal span names are not a compatibility surface, and neither are their
   levels.**
   They are function names and will change. Expect individual functions to move
   between the two halves of point 4 as the split is tuned; the *rule* is the
   commitment, not any function's current side of it. Dashboards belong on the
   `kafkaman.*` phase spans and the `db.query` summaries.

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

Rejected, and **partially adopted later** — see the amendment below. As stated
it is the wrong shape: spans doing a profiler's job, at higher cost, blind to
inlined frames, and turning a readable seventeen-span waterfall into several
hundred rows by default. What made two-thirds of it affordable was measuring
*which* functions produce the volume, which had not been done when this was
first rejected.

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

### F. Split the tier by what makes a function run

*Accepted, 2026-08-30.* See the amendment below.

## Amendment: the tier is split by shape, not off by default

The original point 4 read: *"kafkaman's internals are instrumented at `debug`,
behind their own target."* The whole tier was off at the default filter.

That was rejected in favour of the split above after the volume was measured
rather than assumed. Commit `f84290e` put numbers on it, and they do not say
what "an order of magnitude more spans" suggested:

| | spans |
| --- | --- |
| one product-create request, tier off | 17 |
| one product-create request, whole tier on | 25 |
| idle service, two minutes, tier off | 5 |
| idle service, two minutes, whole tier on | 2322 |

The request cost is eight spans. The idle cost is three orders of magnitude, and
none of it is request work — it is `claim_batch`, `claim_received_row`,
`observe`, `refresh`, and the other loop functions describing, over and over,
having found nothing. The two costs are unrelated, and the original decision
priced the tier at the larger one.

So the expensive half stays behind the target and the cheap half comes to the
default filter. This is not a new principle: the same rule already split
`db_span!` from `db_poll_span!`, which is why `insert outbox row` is visible at
`info` and `claim outbox batch` is not. Point 4 applies it to function spans
too, so a `db.query` span and the function that opens it are now always on the
same side of the line.

What this buys is the reason it was worth reopening: a dispatch waterfall with
no unattributed gaps. Before it, the time between `kafkaman.dispatch` opening
and the first `db.query` underneath belonged to no named frame, and an operator
reading a slow dispatch could see *that* it was slow before touching the
database but not *where*.

**Re-measured after the split**, on the same stack at `RUST_LOG=info`, one
product-create request end to end:

| | spans |
| --- | --- |
| total, across both services | 38 |
| promoted function spans | 16 (7 in `product`, 9 in `order`) |
| `db.query` | 16 |
| `kafkaman.*` phase | 5 |
| HTTP server | 1 |
| **idle stack, both services, two minutes** | **0** |

Eight promoted spans per service per request, matching the original +8, and
nothing at all at idle. The poll functions were confirmed absent from the export
by name, and the seven `db_poll_span!` summaries with them.

What it costs, beyond the eight spans: the internal names are now visible by
default, which makes point 5's disclaimer load-bearing rather than theoretical.
Anyone who builds a dashboard on a function span name is now able to do so
without turning anything on first, and it will still break.

The rule is enforced in two places rather than trusted: `binary_telemetry.rs`
asserts both halves at `RUST_LOG=info` against real binaries — the poll
functions absent *and* the message-path ones present, since either assertion
alone is satisfied by reverting the whole tier — and a unit test in
`kafkaman-axum` pins the same split plus the documented directive.

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
  which is a cached atomic load. The message-path half is now always enabled and
  costs about eight extra spans per request; the polling half remains disabled by
  default and is worth three orders of magnitude on an idle service, which is why
  it is the half that stayed behind the target.

## Revisit If

- A Rust runtime agent or a stable OpenTelemetry profiling exporter makes
  whole-binary timing available without annotations.
- Elastic's symbolizer becomes part of the reference stack, which would make the
  profiling path worth re-measuring.
- The polling half is found to be enabled in a deployment often enough that its
  volume, rather than its absence, is the problem.
- The eight-span-per-request cost of the message-path half turns out to matter at
  a traffic level this project has not measured. It is the reversible half: it
  was added independently of everything else in this decision and can be reverted
  by level alone, without touching a call site.
- A fourth site starts persisting or transmitting trace context. The rule above
  applies to it from the first patch, not after it breaks.
