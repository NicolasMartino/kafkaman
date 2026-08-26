# M6 OpenTelemetry Readiness Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-08-25
- Category: Observability review
- Scope: External readiness review of kafkaman's OpenTelemetry state after M6, with citation verification and assessment of which findings were adopted.
- Sources:
  - External review provided by the user, 2026-08-25
  - Citation verification run against `implementation/m6-observability` at `d9389d4`
- Related:
  - wiki/plans/opentelemetry-completion.plan.md
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/specs/m6-observability-operability.spec.md

## Verdict

The review scored OpenTelemetry readiness at **6.5/10** and characterized the
state as "a solid observability foundation with some OTel counters attached."
Its factual content is accurate and its recommended ordering is sound. Two of
its five findings were adopted into the plan; the rest were already covered.

## Citation Verification

Every line reference in the review was checked against the tree. **All twelve
resolve to exactly what the review claims.** This is worth recording because it
is the property that determines how much of a review can be trusted without
re-deriving it, and it is not the common case.

| Citation | Resolves to |
| --- | --- |
| `Cargo.toml:25` | `opentelemetry = { version = "0.32", features = ["metrics"] }` |
| `crates/kafkaman-config/src/observability.rs:39` | The doc comment marking `level`/`payload`/`headers` reserved |
| `kafkaman.example.toml:63` | `[observability.defaults]` |
| `apps/axum-outbox/src/main.rs:25` | `tracing_subscriber::fmt()` |
| `apps/axum-outbox/src/main.rs:88` | `admin_router(AdminState::new(pool, cfg))` |
| `crates/kafkaman-worker/src/metrics.rs:24` | `static METRICS: OnceLock<WorkerMetrics>` |
| `crates/kafkaman-worker/src/metrics.rs:29` | `.u64_counter("kafkaman.scheduler.cycles")` |
| `crates/kafkaman-rdkafka/src/metrics.rs:24` | `static METRICS: OnceLock<KafkaMetrics>` |
| `crates/kafkaman-rdkafka/src/metrics.rs:29` | `.u64_counter("kafkaman.kafka.publish.records")` |
| `crates/kafkaman-rdkafka/src/publisher.rs:98` | `fn managed_headers(...)` |
| `crates/kafkaman-rdkafka/src/ingest_record.rs:152` | `struct RecordHeaders` |
| `crates/kafkaman-sqlx/src/operability.rs:102` | `pub async fn outbox_status_summary(` |

Two substantive claims were additionally verified beyond their citations:

- **The four-live / three-reserved config split holds.** `stuck_after` and
  `max_queue_age` are consumed by `kafkaman-axum` and
  `kafkaman-sqlx/src/operability.rs`; `lifecycle` and `sample_success` reach
  `LifecycleSampler`, which is genuinely wired into both loops at
  `crates/kafkaman-worker/src/relay.rs:103` and
  `crates/kafkaman-worker/src/dispatcher.rs:40`. These are live knobs, not
  declared ones.
- **The gap list matches an independent pass.** The findings were reached
  separately while scoping the completion plan, from the same code, with the
  same conclusions.

## Findings Confirmed

All five gaps stand:

1. Metrics inert without a host-installed SDK; the example installs none.
2. `OnceLock`-cached instruments can bind permanently to the no-op provider.
3. Counters only — no units, histograms, or observable gauges.
4. No trace propagation; no `traceparent`/`tracestate` path.
5. No `tests/observability/` suite; telemetry is not tested as telemetry.

## Assessment

### Where the review understates

**"Likely inert in real use unless the host wires an SDK first" is conditional
where the fact is not.** No `opentelemetry_sdk` exists anywhere in the
repository, so no metric has ever been observed — in any build, in any test,
since the instruments were written. Not "likely," not "unless." The hedge frames
it as an adopter-side configuration gap rather than a hole on our side.

### Where the review is generous

**It credits documentation as implementation.** Its first strength cites
`wiki/decisions/telemetry-pipeline-ownership.decision.md:21` as evidence the
ownership boundary is right. That page was written the same day, describing work
not yet done. The underlying fact is true and predates it — M6 only ever
depended on the API crate — but citing the decision page reads as established
practice rather than as a recorded intention. A readiness score should not
include credit for the document describing the plan.

### Where the review is harsh

**6.5 undersells the operability surface.** Queue depth, oldest-row age,
stuck-row detection, DLQ inspection, and bounded redrive all work today against
real Postgres, are integration-tested, and are reachable over HTTP. An operator
can run this now and answer real questions. That is not OpenTelemetry, but it is
the part that survives whichever telemetry backend anyone chooses.

### On the score itself

The number averages two different measurements. "How good is the foundation" is
genuinely strong; "how much OpenTelemetry actually works" is near zero, since no
telemetry has ever left a process. Averaging them yields a figure that is hard
to act on, where the split states plainly where effort belongs.

## Findings Adopted Into The Plan

Two gaps in the review's own coverage were adopted as plan changes:

1. **Logs are absent from the review.** Traces receive a dedicated gap entry;
   logs appear once, as "log bridge," inside its final step. For an Elastic
   target that is a third of the value unaccounted for — trace-correlated log
   records are the log→trace pivot in Kibana, and the reason `sample_success`
   exists at all. The plan's sequencing section now names three signals in a
   table rather than leaving logs as a trailing phase.

   This also **corrected an error in the plan itself**: an earlier revision
   advised cutting Phase 3 before Phase 2 under pressure. That is backwards.
   Phase 3 is cheap precisely because Phase 2 has run; cutting it afterward
   forfeits most of the log-side value for a small saving. The correct cut is
   Phases 2 and 3 together.

2. **Example wiring was scheduled last and has been moved up.** The review's
   step 7 leaves the example without a `MeterProvider` through its steps 3–6,
   which means every instrument added along the way is verifiable only inside
   tests, and the host-side startup ordering contract — a contract about
   application startup — would never be exercised by an application. A
   metrics-only provider is now step 5 of Phase 0; Phase 4 completes the
   pipeline.

A third point was added independently: the review does not flag that its step 6
is the plan's **only irreversible step**. Persisting trace context is a
migration on every per-type outbox table. Everything else is additive code that
can be revised; a migration adopters have run cannot. Phase 2 now says so.

## Ordering Convergence

The review's recommended order maps onto the filed plan as:

| Review step | Plan phase |
| --- | --- |
| 1. Fix the `OnceLock` hazard | Phase 0 |
| 2. Dev-only SDK tests proving collection | Phase 5 (`metrics_surface`, `provider_ordering`) |
| 3. Units, semantic attributes, disjointness assertions | Phase 1 |
| 4. Histograms | Phase 1 |
| 5. Observable gauges from existing SQL | Phase 1 |
| 6. Trace context columns and W3C propagation | Phase 2 |
| 7. Wire the example | Phase 0 step 5, then Phase 4 |

Two independent passes over the same code produced the same sequence, differing
only on where example wiring belongs. That agreement is itself evidence the
ordering is right.

## Outcome

**Added 2026-08-26.** Every step this review recommended has landed, and three
later implementation reviews of the resulting branch found defects it did not
reach — which is not a criticism of it. This review read the M6 code and asked
"what is missing"; the later ones read the completed pipeline and asked "is what
is here correct". The three most consequential findings were of the second kind:

- The `metrics`/`traces` opt-out advertised in step 1's neighbourhood was false
  in the dependency graph while every build succeeded.
- `LifecycleSampler` rounded `sample_success` to the nearest reciprocal.
- The step 6 propagation this review called the plan's only irreversible step
  worked in both directions it was tested in, and a relay built *without*
  `traces` stripped `traceparent` from every message it published.

The third pass added a fourth of the same kind: `AddReceivedFailureMetadata`
added the DLQ's two failure columns and left them empty, which is invisible to a
reading of the changeset and obvious to one that asks what an operator sees next
— a dead letter that renders with a failure kind and cannot be redriven by it.

None of these is visible from a reading that asks what exists. All are visible
from one that asks what a specific configuration does. Recorded here
because this document is the one a future reader reaches for when asking how
well the OTel work was reviewed.

Residuals as of 2026-08-26: Phase 4's Elasticsearch/Kibana compose profile and
the Elastic deployment reference page, both deliberately deferred to the example
under construction rather than written against `apps/axum-outbox`, which is being
replaced. See `wiki/plans/opentelemetry-completion.plan.md`.
