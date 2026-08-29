# Failure Taxonomy and Blame

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-30
- Category: Error classification and operability
- Scope: Asks why one failure gets two different `urn:kafkaman:problem:*` values depending on where it is read, and proposes separating "what kind of failure was this" from "whose code produced it" rather than collapsing both into one field.
- Sources:
  - wiki/decisions/failures-as-typed-exceptions.decision.md
  - crates/kafkaman-sqlx/src/dispatch_failure.rs
  - crates/kafkaman-core/src/failure_kind.rs
  - crates/kafkaman-core/src/rows.rs
  - crates/kafkaman-sqlx/src/queries.rs
- Related:
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/decisions/dispatch-infrastructure-error-classification.decision.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Why This Proposal Exists

The typed-exception work gave every failure a permanent problem URI derived from
the error's own Rust type. Reviewing it for duplication surfaced this, measured
against the real classifiers rather than reasoned about:

```
Error::Sqlx(PoolClosed)   APM  exception.type   = urn:kafkaman:problem:infrastructure
                          DLQ  latest_error.type = urn:kafkaman:problem:handler
```

One failure, one URI namespace, two different values, depending on which surface
an operator reads. It is not a bug in either classifier. It is two questions
being answered by one field.

## The Two Questions

**Taxonomy — what kind of thing went wrong?** A property of the error value. A
closed connection pool is an infrastructure failure regardless of which frame it
surfaced in.

**Blame — whose code produced it, so who repairs it?** A property of the call
frame, not the value. That same closed pool is kafkaman's problem when
kafkaman's `COMMIT` raised it and the application's when the handler's own query
raised it.

They are orthogonal, and every combination occurs.

## The Evidence That They Are Already Tangled

`ReceivedFailureKind` has four values, and they are not four of the same thing:

| Variant | Answers |
| --- | --- |
| `MissingHandler` | what |
| `InvalidPayload` | what |
| `Infrastructure` | what |
| `Handler` | **who** |

Three are taxonomy. The fourth is blame. Because
`handler_failure_disposition` ends in `_ => Handler`, the blame value shadows
every taxonomy value whenever the error came out of the handler frame.

Someone already noticed and patched exactly one case by hand. That function
carves out `Error::Serde(_) => InvalidPayload` with the comment: *"a
deserialization error is called out separately since it says the stored payload
does not match the handler's type, **which is a different repair**."* That is the
taxonomy argument, stated in the codebase, applied to one variant. It
generalises — a pool exhaustion is also a different repair — and the catch-all
swallows every other case.

## The Trace Already Solved This. The Row Did Not.

Dispatch computes both axes, and the stage selects which classifier runs:

```
DispatchFailure::Handler(e)     → stage=handler      → handler_failure_disposition(e)
DispatchFailure::Bookkeeping(e) → stage=bookkeeping  → received_failure_disposition(e)
pre-handler routing             → stage=routing      → received_failure_disposition(e)
```

A **span** carries three separate fields: `error.type` (taxonomy),
`kafkaman.failure.stage` (blame), and `kafkaman.failure.kind` (the collapsed
one). Nothing is lost; the axes can be read apart.

A **row** carries one. `ReceivedError` is `{type, title, detail, occurred_at}`,
`type` is the collapsed value, and the stage is not persisted at all. The
factoring is gone the moment it is written.

## Why This Costs Something

`ReceivedFailureFilter::kind` is what `POST /dlq/{type}/redrive` filters on, and
it filters the collapsed value.

A connection pool exhausts and four hundred rows dead-letter. The pool is fixed
and those four hundred rows should be replayed. Filtering
`kind = Infrastructure` returns **nothing** — every row was recorded `Handler`.
The only remaining option is redriving every `Handler` row, which sweeps up the
genuinely poisoned messages that were dead-lettered on purpose. The `detail`
string knows the difference; it is free text, not a filterable field.

That is the cost. Not tidiness — an operator who cannot select the rows they
need to replay.

## Proposal

1. **One classification.** `ReceivedFailureKind` becomes a total, tested
   coarsening of the fifteen-value problem vocabulary. The stage stops being an
   input. `Error::Sqlx` is `Infrastructure` wherever it is read.
2. **Blame gets its own field.** `FailureStage` becomes public, is persisted on
   `ReceivedError`, and is surfaced on the DLQ API — so the information the
   catch-all was encoding is kept rather than deleted, in the field that answers
   that question and no other.
3. **One disposition function.** `handler_failure_disposition` and
   `received_failure_disposition` collapse into one, because with the stage
   removed as an input there is nothing left to distinguish them.

## Options Considered

### A. Collapse only: make the kind a pure function of the problem type

Rejected on its own. It is the right first half, but it *deletes* the blame
information rather than relocating it: `kind = Handler` would stop meaning "the
handler frame raised this", and nothing else in the row would say it.

### B. Persist the stage, leave the kind alone

Rejected, and worth recording because it is the obvious first idea and it does
not work. Both failing cases already have `stage = handler` — a handler
returning `Error::Handler` and a handler returning `Error::Sqlx` are
indistinguishable by stage. The ambiguity lives on the taxonomy axis, which is
the one the catch-all erased.

### C. Persist the taxonomy URI beside the existing kind

Rejected. Additive and safe, and it makes the duplication worse rather than
better: two `urn:kafkaman:problem:*` strings in one JSON object with different
values, and a `type` field whose meaning stays "sometimes taxonomy, sometimes
blame" forever.

### D. Collapse *and* relocate — proposal above

*Accepted.* One field per question. It costs a change to values written into
`last_failure_kind`, which is the reason the other three were considered first.

## Boundaries

- **Old rows are not rewritten.** A row recorded before this change keeps the
  value it was written with. The error history is an audit trail; rewriting it
  to say something the system did not say at the time would be worse than the
  inconsistency it fixes.
- **`ReceivedIngestFailureKind` is untouched.** Ingest quarantine classifies
  records that never became rows, has no handler frame, and therefore has no
  blame axis to separate.
- **No new filter axis.** Once the kind is honest, `ReceivedFailureFilter::kind`
  already selects the pool-exhaustion rows in the motivating scenario. Adding a
  `stage` filter before anyone has asked for one is speculative surface.
- **The duplicate error emission is out of scope.** Every exception also lands as
  a log record through the OTLP log bridge; that is a separate question about
  layer filtering, not about classification.

## Risks

- **This changes stored values.** New rows classify differently from old ones,
  and a saved redrive filter selects a different population after upgrade. The
  project is pre-1.0 and the compatibility note carries the migration guidance,
  but it is a real event and not a refactor.
- **A behavioural change rides along.** Terminal-ness also stops depending on the
  stage, so a handler returning `Error::CacheOriginMismatch` now dead-letters
  immediately instead of spending its retry budget. That is the intended
  reading — the guard's predicate can never become true again no matter who
  raised it — but it is a change.
- **Four buckets for fifteen URIs is lossy by construction.** Most of the new
  vocabulary coarsens to `Infrastructure`. That is acceptable because the fine
  value is on the span and in APM; it would not be if the row were the only
  place a failure was recorded.

## Resolution

Accepted. See `wiki/decisions/failure-taxonomy-and-blame-separation.decision.md`
and `wiki/plans/failure-taxonomy-separation.plan.md`.
