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

The resulting trace has this shape:

```
HTTP request span  (service A, host-instrumented)
└── kafkaman.enqueue          — in the caller's transaction, writes traceparent
      ⋮  (durable gap: commit, relay poll, claim)
    kafkaman.relay.publish    — child of enqueue, restored from the row
      ⋮  (Kafka)
    kafkaman.ingest           — service B, LINKS to relay.publish
    └── kafkaman.dispatch     — handler execution
```

The dotted gaps are real elapsed time — potentially seconds, potentially longer
after a failure — and showing them is a feature. The gap between `enqueue` and
`relay.publish` **is** the outbox latency that
`kafkaman.outbox.time_to_publish` measures. A reader of the trace sees where the
time went.

## Amendments

Three things this decision fixed before implementation turned out to be
incomplete, and one turned out to be wrong. All four were found by building it.

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

- A test asserts one trace id spans enqueue → relay publish → Kafka → ingest →
  dispatch, across two harnesses, with the publish span parented to the enqueue
  span across a restart.
- A test asserts `traceparent` sent by a producer never appears in the user
  headers handed to a handler.
- A test asserts a row enqueued outside any span publishes and dispatches
  normally, with no trace context and no error.
- A test asserts the consumer span carries a link, not a parent.
