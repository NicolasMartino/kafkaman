# Failure Taxonomy and Blame Separation

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-30
- Category: Error classification and operability
- Scope: Fixes that a failure's persisted class is a function of the error alone, that the frame it surfaced in is recorded separately rather than folded into that class, and that there is exactly one function relating the telemetry vocabulary to the persisted one.
- Sources:
  - wiki/proposals/22-failure-taxonomy-and-blame.proposal.md
  - wiki/decisions/failures-as-typed-exceptions.decision.md
  - crates/kafkaman-core/src/failure_kind.rs
  - crates/kafkaman-core/src/rows.rs
  - crates/kafkaman-sqlx/src/dispatch_failure.rs
  - crates/kafkaman-sqlx/src/dispatch.rs
- Related:
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/decisions/dispatch-infrastructure-error-classification.decision.md
  - wiki/plans/failure-taxonomy-separation.plan.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Decision

1. **A failure's class is a function of the error, and of nothing else.**
   `ReceivedFailureKind` is derived from the error's `ProblemType` URI by one
   total coarsening. The frame a failure surfaced in is not an input to it.
   `Error::Sqlx` is `Infrastructure` whether kafkaman's bookkeeping raised it or
   a handler returned it.

2. **There is exactly one function relating the two vocabularies.**
   `ReceivedFailureKind::coarsening(&str)`, in `kafkaman-core` beside both, with
   a test asserting it is total over `ALL_PROBLEM_TYPES`. `ProblemType` gains a
   provided `failure_kind()` that calls it, so no caller can invent a third
   mapping.

3. **Blame is recorded as its own field.** `FailureStage` — `routing`,
   `handler`, `bookkeeping` — moves to `kafkaman-core`, becomes public, is
   carried on `FailureDisposition`, is persisted on `ReceivedError`, and is
   surfaced on the DLQ API. It answers "whose code produced this" and nothing
   else.

4. **One disposition function.** `handler_failure_disposition` and
   `received_failure_disposition` are replaced by `failure_disposition(&Error)`.
   With the stage removed as an input there was nothing left to distinguish
   them.

5. **Terminal-ness is also a property of the error alone.** A failure whose
   predicate can never become true again is terminal wherever it was raised.

6. **Old rows are not rewritten.** A row keeps the class it was written with.

## Why the Class Cannot Depend on the Frame

`ReceivedFailureKind` has four values, and three of them answer *what* while one
answers *who*:

| Variant | Answers |
| --- | --- |
| `MissingHandler` | what |
| `InvalidPayload` | what |
| `Infrastructure` | what |
| `Handler` | who |

Once one value in an enum answers a different question from the others, whichever
one is assigned last wins, and the other is unrecoverable. Because
`handler_failure_disposition` ended in `_ => Handler`, blame won for every error
that came out of the handler frame, and the taxonomy was erased.

The measured result, before this decision:

```
Error::Sqlx(PoolClosed)   APM  exception.type    = urn:kafkaman:problem:infrastructure
                          DLQ  latest_error.type = urn:kafkaman:problem:handler
```

Two values from one namespace for one failure. An operator filtering APM on
`infrastructure` and the DLQ on the same string gets two different populations,
and neither is wrong from its own side.

The codebase had already conceded the point in one place. That catch-all carved
out `Error::Serde(_) => InvalidPayload` because a payload that will not decode is
*"a different repair"* — which is the taxonomy argument, applied by hand to the
one variant somebody noticed.

## Why Blame Is Kept Rather Than Dropped

Dropping it would be the cheaper change and the wrong one. "Whose code failed" is
a real question with a different answer from "what kind of failure", and it is
the question that decides who gets paged. It is kept, in a field that answers
only that, so neither value has to be read as if it were the other.

The stage was already computed — it selected which classifier ran — and already
exported as `kafkaman.failure.stage`. This decision persists what was being
discarded rather than inventing a new concept.

## What This Costs

**Rows classify differently after the upgrade.** A handler returning a database
error used to write `Handler` and now writes `Infrastructure`. Existing rows are
untouched, so a table spans both conventions, and a saved redrive filter selects
a different population than it did. This is the reason three cheaper options were
considered first; the compatibility note carries the migration guidance.

**Terminal-ness changes for one path.** A handler returning
`Error::CacheOriginMismatch` or `Error::MissingEntityKey` now dead-letters on the
first attempt instead of spending its retry budget. The old behaviour was
incidental — the terminal list lived only in the classifier kafkaman used for its
own failures — and the reasoning applies regardless of who raised it: the guard's
predicate cannot become true again by retrying.

**The coarsening is lossy.** Fourteen of the eighteen URIs collapse onto
`Infrastructure`. That is acceptable only because the fine value is on the span
and in APM; if the row were the only record of a failure it would not be.

## Options Considered

### A. Collapse only — make the kind a pure function of the error

Rejected. The right first half, but it deletes the blame information instead of
relocating it.

### B. Persist the stage and leave the kind alone

Rejected, and recorded because it is the obvious first idea and it does not work.
A handler returning `Error::Handler` and one returning `Error::Sqlx` both have
`stage = handler`; the ambiguity is on the axis the catch-all erased.

### C. Persist the taxonomy URI beside the existing kind

Rejected. Additive and safe, and it makes the duplication worse: two
`urn:kafkaman:problem:*` values in one object, and a `type` field whose meaning
stays "sometimes taxonomy, sometimes blame" permanently.

### D. Collapse and relocate

*Accepted.* One field per question.

## Consequences

- `ReceivedFailureFilter::kind` selects honestly again. The motivating case — a
  pool exhaustion that dead-lettered four hundred rows — is now
  `kind = Infrastructure` and can be redriven without sweeping up genuinely
  poisoned messages.
- `FailureStage` becomes public API in `kafkaman-core` and a persisted value, so
  its three variants and their spellings are now a compatibility surface.
- `ReceivedError` gains an optional `stage`. Absent means "written before this",
  not "unknown frame", and it is `Option` rather than a defaulted variant so the
  two cannot be confused.
- The DLQ API's `latest_error` grows a field; existing consumers are unaffected.
- One classification function replaces two, and `ProblemType::failure_kind`
  makes the relationship between the vocabularies impossible to re-derive
  differently somewhere else.

## Revisit If

- A fourth stage appears. The three are the three places a dispatch can fail;
  a fourth would mean the dispatch shape changed.
- `Infrastructure` proves too coarse in practice for DLQ triage, which would
  argue for widening `ReceivedFailureKind` — a stored-vocabulary change, and the
  reason it has stayed at four values.
- ~~A handler's SQL constraint violation deserves better than
  `Infrastructure`.~~ **Done, 2026-08-30, in the amendment below.**
- Anyone asks to redrive by stage rather than by kind. Deliberately not built:
  the honest kind already covers the case that motivated this.

## Amendment: database errors are classified by what was refused

**2026-08-30, the same day.** Removing the catch-all made an existing coarseness
visible: `Error::Sqlx` wraps every `sqlx::Error`, so a closed connection pool and
a unique-constraint violation were one class. Since a handler's own query is the
most common way a handler fails, the largest bucket in APM was also the least
informative — and `Infrastructure` is an actively misleading name for a write the
database refused, because nothing was broken.

Three telemetry URIs now split it:

| URI | SQLSTATE | What it means |
| --- | --- | --- |
| `urn:kafkaman:problem:constraint` | class 23 | the schema refused the write |
| `urn:kafkaman:problem:contention` | class 40 | deadlock or serialization failure |
| `urn:kafkaman:problem:statement` | classes 22, 42 | the statement cannot run as written, by this role |

Everything else — 08 connection, 53 resources, 57 operator intervention, 58
system, and anything unrecognised — stays `INFRASTRUCTURE`, as do the
`sqlx::Error` variants that never reached the server at all.

`contention` rather than `serialization`: in a Rust codebase the latter reads as
a serde failure, and this is the opposite end of the system. It earns its own
group because it is *expected* under load and the retry is the correct response,
which is the opposite reading from a connection failure charted beside it.

`sqlx`'s own `DatabaseError::kind()` is asked first, because it names the four
constraint kinds portably and in sqlx's vocabulary. It returns `Other` for
everything else, which is why the SQLSTATE classes are still needed — and why the
unit tests use a stub that always answers `Other`, so they exercise the class
table rather than the shortcut. The shortcut is covered against a real
`PgDatabaseError` in `tests/durable-send`.

**This changes no stored data.** All three coarsen to `Infrastructure`, because
none of the four persisted kinds fits a constraint violation better and adding a
fifth is the stored-vocabulary change this decision already declined. The
refinement is what an APM error group is built on; the row keeps the class it can
hold. Terminal-ness is also unchanged: a constraint violation stays retryable,
since the conflicting row may be removed by something else, and an undefined
table may appear when a migration finishes.
