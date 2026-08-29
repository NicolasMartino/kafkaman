# Failures as Typed Exceptions

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-30
- Category: Observability architecture
- Scope: Fixes how a kafkaman failure reaches an APM backend as an *error* rather than only as a red span, establishes the permanent problem-type vocabulary that groups those errors, and makes the classification a property of the error type rather than of the call site.
- Sources:
  - crates/kafkaman-core/src/problem.rs
  - crates/kafkaman-core/src/span.rs
  - crates/kafkaman-core/src/failure_kind.rs
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman-worker/src/relay.rs
  - tests/observability/tests/exception_events.rs
  - tests/observability/tests/dispatch_failure_status.rs
- Related:
  - wiki/decisions/method-level-timing-and-span-depth.decision.md
  - wiki/decisions/observability-operability-policy.decision.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Context

A recorded failure marked its span red and set a bounded status description.
That gives Kibana a *failed transaction* — something to count, split, and chart.
It does not give Kibana an **error**.

Elastic's error surfaces — error groups, per-type occurrence counts, the
transaction-to-error pivot — are derived from OpenTelemetry `exception` span
events, and nothing in this repository emitted one. `examples/README.md` said so
in as many words: *"Elastic's Errors UI/card is a separate grouping over reported
exceptions or error log messages; failed transactions are the chart/table signal
this example emits today."* An operator looking at a failed dispatch could see
that it failed and could not open it.

## Decision

1. **Every failure on the durable path emits an `exception` span event.**
   Five phase spans: `kafkaman.enqueue`, `kafkaman.relay.publish`,
   `kafkaman.ingest`, `kafkaman.dispatch`, and `kafkaman.handler`. The event
   carries `exception.type` and `exception.message`.

2. **The classification comes from the error's own type, enforced by the
   compiler.** `kafkaman_core::ProblemType` is a public trait with one method,
   implemented by a hand-written exhaustive `match` on each error enum. A new
   error variant does not compile until someone decides how it appears in APM.

3. **The vocabulary is a set of permanent `urn:kafkaman:problem:*` URIs**,
   declared once in `kafkaman_core::problem` and listed in `ALL_PROBLEM_TYPES`.
   It extends the four URIs `ReceivedFailureKind` already persisted rather than
   inventing a second scheme.

4. **The exception vocabulary is finer than the persisted one, deliberately.**
   `ReceivedFailureKind` keeps four values because they are written into stored
   rows and cannot churn. `exception.type` has eighteen, which is what lets a
   handler *panic* group separately from a handler that returned an error while
   both still dead-letter under the same stored kind. The two reconcile on
   `error.type`, which every failed span carries.

5. **One failure produces one error document.** The event is emitted on the
   innermost span that owns the failure; enclosing spans carry status and the
   `kafkaman.failure.*` attributes without repeating the event. `db.query` spans
   record status and never an event.

6. **The message is truncated once, to 256 bytes, before either write.**

7. **`#[instrument(..., err)]` is banned.** After this change it is exactly the
   shape that becomes an exception event — an ERROR event with a field named
   `error` — with an unbounded `Debug` value, on every annotated function.

## Why classification lives on the type

The obvious implementation is a `&'static str` parameter at each call site.
It was rejected, and the reason is visible in the code it replaced:
`record_failure_span` took a pre-formatted `&dyn Display`, so its two callers
stringified the error one frame up and passed a `&String` with the concrete type
already thrown away. A call site that has lost the error cannot classify it, and
a call site that still has it can classify it *wrongly* with no compiler
complaint — the failure mode being a plausible URI on the wrong error, which is
invisible until an operator wonders why a group's contents do not match its name.

Deriving from the type moves the decision next to the variant, where the reason
for it is legible, and makes exhaustiveness the enforcement. This is the existing
house rule, stated in the `discriminant_enum!` doc: anything carrying data beyond
the variant's own name — an RFC 9457 URI, a human-readable title — stays a
hand-written `match`.

It also delivered something the string-passing design could not have: because the
classification is a function of the Rust type, `Error::HandlerPanicked` and
`Error::Handler` can differ without touching `ReceivedFailureKind`, whose four
persisted URIs must not change. The panic/error distinction had previously been
recorded as out of reach for exactly that reason.

## Why the event still carries `error = %message`

This is a mechanism constraint, not a preference. In `tracing-opentelemetry`
0.33, an event is renamed to `exception` **only** in `record_str`/`record_debug`
(`layer.rs:330`, `layer.rs:369`), when the event has no message and carries a
field literally named `error`. The bridge's `record_error` path — the one that
takes `&dyn std::error::Error` — attaches `exception.message` and
`exception.stacktrace` but **never renames the event**, so on its own it produces
an unrecognizable event with an empty name that no backend groups.

`exception.type` is never set by the bridge on any path, which is why it is
recorded explicitly.

## Why the message is truncated before recording, not after

`error_events_to_status` is on by default and overwrites the span status from the
event, formatting the value with `Debug` and **not truncating it**. Recording an
untruncated message and relying on `record_exception` to bound the status would
have silently defeated the 256-byte cap, which exists because error text is
attacker-influenced — a database error can quote the value that violated a
constraint.

Truncating once and passing the same bounded string to both writes makes the
result identical whichever lands last. `exception_events.rs` pins both halves,
because the hazard is a bridge default that can change under us.

## The one exemption

`relay.rs` records a `BoxError` from `Publisher::publish`, whose signature is
`Box<dyn StdError + Send + Sync>` in a **public trait**. Requiring `ProblemType`
there would break every implementor, which is the same call already made when
rejecting option E of the span-depth decision. That site uses
`record_exception_as` and passes `urn:kafkaman:problem:publish` explicitly.

One documented exemption at a genuine type-erasure boundary, rather than eight
undocumented ones everywhere.

## Options Considered

### A. Pass `exception.type` as a `&'static str` at each call site

Rejected. Eight sites, no compiler check, and the classification drifts from the
error it describes exactly as fast as the two are edited apart.

### B. Use the bridge's `&dyn std::error::Error` path

Rejected on evidence. It does not rename the event, so nothing groups it; and
the source chains it would walk are empty here anyway, because
`#[error(transparent)]` forwards `source()` *past* the wrapped error.

### C. Stop using `#[error(transparent)]` so source chains populate

Rejected. Those variants' `Display` output is persisted in the `last_error`
column and rendered in HTTP problem responses, so changing it is a stored-data
change rather than a formatting tweak. With `exception.type` carrying real
classification, the chain is worth much less than it would have been.

### D. Derive `ProblemType`

Rejected for the reason `discriminant_enum!` already gives: a URI is data beyond
the variant's name, and it belongs beside the variant so both are read together.

### E. Emit the exception on every span in the failing stack

Rejected. It is the cheapest implementation and produces four Elastic error
documents for one failed message, multiplying every error count and error rate by
the depth of the span tree.

## Consequences

- Elastic APM's error surfaces populate for the first time. `examples/README.md`
  no longer has to disclaim them.
- `ProblemType` is public API in `kafkaman-core`, so adding a required method is
  breaking. More subtly, **which URI a given error variant maps to is now a
  compatibility surface**: it is the APM grouping key, and moving a variant
  splits or merges an existing error group in every dashboard built on it.
- Every `record_exception` also becomes an OTel log record through
  `OpenTelemetryTracingBridge`. That is useful correlation and it doubles
  error-log volume where a failure is already logged; the dedicated
  `kafkaman::telemetry` target makes it filterable.
- `record_error` survives unchanged for the two cases that need status without an
  event: `db.query` spans, and the `format_args!` caller in `kafkaman-axum` that
  was never an error object.

## What the error documents do *not* carry

Both of these were found by querying a real stack rather than reasoning about it,
and both are worth knowing before someone files them as bugs.

**There is no `exception.stacktrace`.** The bridge only populates it on the
`&dyn std::error::Error` path this decision rejects, and that path derives it
from the source chain — which is empty for `Error::Handler(String)` and
`Error::HandlerPanicked(String)`, the two variants that account for essentially
every error an application will see. So switching paths to obtain it would obtain
nothing, and Elastic's Errors UI works without it: verified against a live stack,
error groups render with their name, type, occurrence count, `handled` flag, and
the pivot to the failing transaction.

A *real* Rust backtrace is possible and is not free. It needs a
`std::panic::set_hook` that captures `Backtrace::force_capture()` into a
thread-local for `catch_panic` to read, which means a library installing a
process-global hook over whatever the application set, and paying a capture on
every panic. That is a feature with its own decision to make, not an attribute to
add here.

**`code.file.path` on an error record is the *recording* site, not the failure
site.** It is `crates/kafkaman-core/src/span.rs`, the callsite of
`record_exception_as`'s `tracing::error!`, and it is identical on every kafkaman
error — `tracing` fixes an event's location at macro expansion, so it cannot name
the handler that failed. Elastic does not use it (`error.culprit` comes back
null), so it misleads nobody in the APM UI, but it is a visible column in
Discover and reads like an accusation against `span.rs`. Where the failure
happened is in the trace instead: the error's `parent.id` is the span that owns
it, and that span's name and `kafkaman.failure.stage` say which phase it was.

## Revisit If

- `tracing-opentelemetry` gains a first-class exception API, or changes the
  rename conditions in `record_str`/`record_debug`. `exception_events.rs` is the
  test that will notice.
- `error_events_to_status` changes its default or starts truncating, at which
  point the double write is no longer load-bearing.
- The URI set proves too coarse for a real triage workflow. Adding one is
  cheap; *moving* an existing variant onto a new one is the breaking change, so
  prefer splitting at a new variant.
- ~~`kafkaman.ingest` stops reporting `Ok` on the quarantine path.~~ **Closed
  2026-08-30.** It no longer does. `ingest_decoded`, `store` and `quarantine`
  return an `IngestOutcome` carrying the refusal, and `ingest_once` records it as
  an exception on the span it owns — so a quarantined record, a `message_id`
  conflict, and an unreadable payload each appear in APM with the
  `ReceivedIngestFailureKind`'s own problem type. The result is still `Ok`,
  because the offset still advances and the loop still continues; handled is not
  the same as fine. Pinned by `ingest_span_covers_decode.rs`.
- The exception event's **second emission** was closed with it. Every
  `record_exception` also reached the OTLP log bridge as a record with an empty
  body, one per error already reported as an error — 13 of 13 ERROR records on
  the example stack. `kafkaman-otel::init` now excludes
  `kafkaman_core::TELEMETRY_TARGET` from log export only; the span layer and
  stdout still see it. Revisit if a host wants those records back, which is a
  filter they compose rather than a change here.
