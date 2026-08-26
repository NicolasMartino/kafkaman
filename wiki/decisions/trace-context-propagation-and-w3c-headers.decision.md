# Trace Context Propagation and W3C Headers

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-25
- Category: Messaging envelope and observability
- Scope: Defines how W3C trace context is carried across the outbox hop and the Kafka hop, and amends the two-namespace Kafka header model to recognize W3C trace headers as a third namespace.
- Sources:
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - crates/kafkaman-rdkafka/src/publisher.rs
  - crates/kafkaman-rdkafka/src/ingest_record.rs
  - crates/kafkaman-core/src/envelope.rs
  - W3C Trace Context Recommendation
- Related:
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## What This Amends

[message-identity-and-header-namespace](message-identity-and-header-namespace.decision.md)
is Accepted, and its clause 5 fixes the Kafka header model at exactly two
namespaces: the reserved `kafkaman-*` namespace that only kafkaman writes, and
user headers, which are rejected before persistence if they collide with the
reserved prefix. The roadmap records this as the ratification of **OQ5**, the
host-context boundary.

This decision amends that model. It does not supersede it: identity, dedup, and
the reserved prefix are untouched. It adds one narrowly-defined third namespace,
because W3C trace context fits neither existing one and the failure to notice
that is a gap in the original decision rather than a change of direction.

Amending a ratified decision is treated as heavier than resolving an open one.
The amendment is therefore stated as narrowly as it can be.

## Decision

1. **W3C trace headers are a third recognized namespace.**
   Exactly two keys: `traceparent` and `tracestate`, as named by the W3C Trace
   Context Recommendation. The set is closed. Extending it requires a further
   decision.

2. **They carry no `kafkaman-` prefix.**
   The entire value of W3C trace context is that any participant recognizes it. A
   `kafkaman-traceparent` header is invisible to every other consumer of the
   topic, every collector, and every broker-side tool, which defeats the purpose
   of adopting the standard.

3. **They are stripped from user headers on ingest.**
   `RecordHeaders` partitions incoming headers. `traceparent` and `tracestate`
   are removed from the user namespace and routed to trace extraction, exactly as
   `kafkaman-*` keys are removed today. Application code never receives them as
   though a producer had set them, because a producer did not — a tracing SDK
   did.

4. **They are not reservable against producers.**
   Unlike `kafkaman-*`, a producer sending `traceparent` is not an error. It is
   the normal case: an upstream service instrumented with any OpenTelemetry SDK
   sets it. kafkaman consumes it as trace context rather than rejecting it.

5. **Trace context is persisted on the outbox row.**
   One nullable `traceparent` column, and one nullable `tracestate` column, added
   by an additive migration. Populated at enqueue from the ambient span, if any.
   Read at publish time to restore the originating context.

6. **The consumer span links to the producer span; it does not parent from it.**
   A consumer polls a batch that may contain records from many unrelated traces.
   Per messaging semantic conventions, batch consumption uses span links. Parenting
   would attach unrelated work to whichever trace happened to be first in the
   batch.

7. **Absent context is normal and never an error.**
   A row enqueued outside any span, or a record from an uninstrumented producer,
   has no trace context. Publishing and ingest proceed unchanged. Trace context is
   never required for correctness of message flow — a rule that must survive
   contact with implementation.

## The Outbox Trace-Continuity Problem

This is the substantive design content and the reason the schema changes.

The outbox pattern separates enqueue from publish in time. That separation is the
entire point: enqueue is transactional with the business write, and publish
happens later, from a different task, possibly a different process, after a
crash, after a retry, after a lease expiry.

A producer span opened at publish time is therefore **orphaned**. It records that
the relay published a record, which is the least interesting fact available. The
trace an operator actually wants — *this HTTP request wrote this row and that
eventually became this Kafka record, which service B consumed* — is precisely the
one the pattern breaks, because the causal link crosses a transaction boundary
and a time gap that no in-memory context survives.

Trace context must therefore be durable, stored beside the row it describes, and
restored when the row is published.

**There is exact precedent for this in the codebase, which is what makes the
design obvious rather than novel.** `correlation_id` already makes this journey:
`Envelope.correlation_id` → an outbox row column → the
`kafkaman-correlation-id` header on the published record. It is the same problem
— identity that must survive store-and-forward — solved the same way. W3C trace
context follows the identical path, and the outbox row grows two nullable columns
rather than one.

The result is **two traces joined by a link**, not one trace:

```
trace A (the caller's)
  HTTP request span  (service A, host-instrumented)
  └── kafkaman.enqueue        — in the caller's transaction, writes traceparent
        ⋮  (durable gap: commit, relay poll, claim)
      kafkaman.relay.publish  — child of enqueue, restored from the row
        ⋮  (Kafka)

trace B (the consumer's)
  kafkaman.ingest             — service B, root span, LINKS to relay.publish
  └── kafkaman.dispatch       — child of ingest, restored from the received row
```

Decision 6 is what makes it two rather than one: a link is not a parent, and a
linked span starts its own trace. That is the intended shape and not a
concession — a consumer polls a batch that may hold records from many unrelated
traces, so a single trace would have to pick one of them to belong to. What the
link buys is that a reader who has either trace can reach the other; what it
costs is that no single trace id spans the broker hop, and any query written as
though one does will return half the story.

The dotted gaps are real elapsed time — potentially seconds, potentially longer
after a failure — and showing them is a feature. The gap between `enqueue` and
`relay.publish` **is** the outbox latency that
`kafkaman.outbox.time_to_publish` measures. A reader of the trace sees where the
time went.

## Amendments

Four things this decision fixed before implementation turned out to be
incomplete, and one turned out to be wrong. All five were found by building it.

### 2026-08-25 — The received row carries trace context too

Decision 5 persists trace context on the *outbox* row, and the trace shape above
draws `kafkaman.dispatch` as a child of `kafkaman.ingest` — with no durable gap
between them. There is one. Ingest stores a received row; dispatch claims it
later, from a different task, possibly a different process, after a retry or a
restart. It is the same problem as the outbox hop, and it has the same answer:
the received table gains the same two nullable columns, ingest writes the
context of *its own* span, and dispatch parents from it.

The alternative — dispatch links to the producer instead — was rejected for the
reason Decision 6 gives about consumers: dispatch handles exactly one row it has
already claimed, so there is no batch, no ambiguity, and nothing to be gained by
weakening a parent into a link.

### 2026-08-25 — A tracer with no caller span still produces context

Decision 7 says a row enqueued outside any span has no trace context. That is
true only of a process with no tracer installed. With one installed, `enqueue`
opens `kafkaman.enqueue` before capturing, so there is always a span — a **root**
span when the caller had none.

The behavior is better than the rule described, and it is what a background job
or a scheduled task should get: the row carries the root's context, the publish
descends from it, and the operator sees a two-span trace with the outbox wait
visible between them, rather than two orphans with nothing tying them together.
The substance of Decision 7 — absent context is never an error — is unchanged and
tested both ways: `tests/observability/trace_absent` for an untraced process,
`trace_root_enqueue` for a traced one with no caller span.

### 2026-08-25 — Outgoing user headers named `traceparent` are dropped

Decision 3 covers the inbound direction: trace keys are stripped from user
headers on ingest. The outbound direction was not stated. An application that
forwards an incoming request's headers wholesale into `Envelope.headers` — an
ordinary thing to do — would otherwise put a stale `traceparent` on the wire
*before* the one the publish belongs to, and Decision 3's own first-wins rule
would then make the stale copy the one a consumer links to.

They are dropped at publish rather than rejected at enqueue. Rejecting would turn
a reasonable pattern into a runtime error, and would contradict Decision 4's
posture that trace headers are not something to police. Dropped, logged at debug,
and the row keeps them in its `headers` column for triage.

### 2026-08-25 — Formatting and parsing are done by hand

The W3C propagator lives in `opentelemetry_sdk`, which no crate under `crates/`
may depend on. `traceparent` is a fixed four-field format and `tracestate` is an
opaque list, so `kafkaman-core::trace` implements both directly against the
Recommendation: it accepts unknown versions with trailing fields, per the
forward-compatibility rule, and rejects the all-zero ids and the reserved `ff`
version.


### 2026-08-26 — The trace shape is two traces, and the diagram said one

The shape diagram above drew `kafkaman.ingest` in the same tree as
`kafkaman.enqueue`, and the verification bullet asked for "one trace id" spanning
enqueue through dispatch. Both contradict Decision 6, which was ratified in the
same document: the consumer *links* to the producer, and a link starts a new
trace by definition. The implementation follows Decision 6 and the integration
test asserts exactly that — the two trace ids differ and a link joins them — so
the picture was the thing that was wrong.

Worth recording rather than quietly editing, because the wrong version is the one
an operator would naturally assume and would write queries against. A dashboard
built on "find the trace id and follow it end to end" silently stops at the
broker. The correct instruction is: follow the trace to `relay.publish`, then
follow its link.

### 2026-08-26 — `tracestate` is not opaque, and where malformed context is dropped

The amendment above called `tracestate` "an opaque list" and forwarded it
verbatim. That was wrong in one specific way: kafkaman does not merely *hold*
these values, it stores them in a column and writes them back onto the wire as
standard headers at the next hop, under its own name. Forwarding an unparseable
`tracestate` makes kafkaman a laundering step — whatever is downstream then has
to deal with a header this process chose to pass on. So it is validated against
the Recommendation's list grammar and normalized: whitespace around commas
dropped, empty members dropped, more than 32 members truncated from the right as
the specification prescribes.

The `traceparent` and the `tracestate` are validated **independently**, and that
asymmetry is deliberate. A `tracestate` that breaks the grammar is discarded on
its own and the `traceparent` beside it survives, because the trace id is what
correlates and the vendor list is only what decorates. Losing the second must
never cost the first.

**The rule for context that is already stored**, stated once because it comes up
at three different doors and the answers differ:

| Door | On a malformed value | Why |
| --- | --- | --- |
| A record header, or a database column | Dropped; the row is unaffected | Absent context is the ordinary case. A message must never stop for a field with no business meaning. |
| `TraceContext::deserialize` | Error | The caller has asserted it holds a trace context. The useful reply to a false assertion is to say so. |
| `OutboxRow::trace` / `ReceivedRow::trace` | Dropped; the row still loads | A stored row must stay readable by a binary whose vocabulary has moved on. The grammar has already been tightened once — lowercase hex, closed version `00` — and a row serialized before that must still load, minus a link that no longer means anything. |

The last row is the same reasoning `problem_type` uses when it degrades an
unrecognized failure kind rather than failing the read, and it is why
`TraceContext` has no `Default`: an empty `traceparent` is not a trace context,
and a type whose whole contract is "this value is well formed" cannot have a
constructor that produces one that is not.

### 2026-08-26 — Three holes in the `tracestate` grammar, and a deliberate deviation

The validation added above was checked against the Recommendation's ABNF and had
three gaps. All three matter for the same reason the validation exists at all:
kafkaman re-emits these bytes as a standard header under its own name.

**Tab was accepted inside a value.** `chr = %x20-2B / %x2D-3C / %x3E-7E` starts
at space, so the only legal place for a tab is the optional whitespace *between*
list members — which is trimmed before a value is ever examined. Accepting `0x09`
in the value position forwarded `vendor=a<TAB>b` as though it were well formed.

**A key could repeat.** `a=1,a=2` satisfies every production and is still not a
valid list: the Recommendation gives each key at most one member, and forwarding
a repeat hands every reader downstream an ambiguity to resolve on its own.
Uniqueness is judged over the whole value, not the part that survives the
32-member ceiling — truncation is what a *valid* list gets for being too long,
and a list with a repeated key was never valid at any length.

**Parsing was unbounded.** The member ceiling bounds what is *stored*; nothing
bounded what was *read*, and every member costs a uniqueness check against every
key before it. A `tracestate` longer than 2048 characters — four times the 512
the Recommendation asks implementations to propagate — is now dropped whole.

**Repeated `tracestate` *headers* are first-wins**, matching `traceparent`, and
this is a knowing deviation. The Recommendation says values from multiple headers
should be combined with commas, per RFC 7230 field order. That rule exists
because HTTP permits one logical field to be split across lines, so joining
restores what the sender actually wrote. Kafka headers are a genuine multimap: two
`tracestate` records are two values, not one value in halves. Joining them would
manufacture a list nobody sent — and, since each half may carry the same vendor
key, one the grammar above now rejects.

## Options Considered

### On the header namespace

**A. Prefix as `kafkaman-traceparent`.** Fits the existing two-namespace model
with no amendment. Rejected: it is unreadable by every other participant, which
forfeits the interoperability that is the sole reason to use W3C context. It
would also be a kafkaman-invented header pretending to be a standard one.

**B. Leave `traceparent` in the user namespace.** No amendment either. Rejected:
it hands application handlers a header they did not set, breaking the meaning of
"user headers" as *what the producer sent*. It also silently makes trace context
part of any user-header round trip.

**C. A third recognized namespace, closed at two keys.** *Accepted.* Minimal
amendment; the existing rules hold everywhere else.

**D. A general "protocol headers" namespace, open-ended.** Rejected as
premature. `baggage` is the obvious next candidate and it carries genuine
questions about propagating arbitrary key-value data across a trust boundary.
Open-ending the namespace now decides that question by omission. When `baggage`
is wanted, it gets its own decision.

### On persisting context

**A. Do not persist; open the producer span at publish.** Simplest, no
migration. Rejected: it produces an orphaned span and loses the causal link,
which is the specific thing this work exists to provide.

**B. Persist `traceparent`/`tracestate` as columns.** *Accepted.* Mirrors
`correlation_id`, which is already proven across this hop.

**C. Persist inside the existing `headers` JSON column.** No migration. Rejected:
it conflates user headers with protocol context in storage, which is the same
category error as option B in the namespace section, and it makes the values
invisible to SQL inspection — which the operability surface would immediately
want.

**D. Reconstruct context from `correlation_id`.** No migration and no new
columns. Rejected: `correlation_id` is a UUID with no sampling flags, no
`tracestate`, and no vendor context. It is a correlation key, not trace context,
and pretending otherwise produces traces that are wrong rather than absent.

### On consumer span shape

**A. Parent the consumer span from the producer.** Rejected: incorrect for batch
consumption, and attaches unrelated messages to an arbitrary trace.
**B. Link.** *Accepted.* What messaging semantic conventions prescribe.

## Consequences

- **A migration.** Two nullable columns on every per-type outbox table, additive,
  under the existing change-management contract. Requires a changeset and lands
  in the compatibility note.
- **Existing rows are unaffected.** Null trace context is the normal case and is
  already required to work by Decision 7.
- **The ingest header partition changes shape.** `RecordHeaders` grows a third
  bucket. Its existing duplicate-key semantics — reserved keys keep the first,
  user keys keep the last — must be decided for trace keys too; first-wins
  matches the reserved treatment and is what a duplicated `traceparent` should
  get.
- **Payload size grows by roughly 60 bytes per record** when context is present.
  `traceparent` is fixed-length; `tracestate` is bounded by the W3C
  Recommendation.
- **A trust-boundary question is now explicit.** kafkaman consumes `traceparent`
  from any producer on the topic. A malicious producer can therefore join our
  traces. This is inherent to distributed tracing and true of every OTel-
  instrumented consumer; it is recorded here so it is a known property rather
  than a discovery. Hosts that treat a topic as untrusted should not propagate
  context from it, and the configuration to refuse extraction is a reasonable
  future addition.
- **OQ5's ratified answer gains an exception**, and the roadmap must say so
  rather than continuing to describe a two-namespace model.

## Verification

- A test asserts the producer trace spans enqueue → relay publish across the
  outbox gap, that the consumer trace spans ingest → dispatch across the receive
  gap, that the two carry *different* trace ids, and that the ingest span links
  back to the publish span that produced the record.
- A test asserts `traceparent` sent by a producer never appears in the user
  headers handed to a handler.
- A test asserts a row enqueued outside any span publishes and dispatches
  normally, with no trace context and no error.
- A test asserts the consumer span carries a link, not a parent.
