# Path to V1

- Document Class: Roadmap
- Status: Draft
- Date: 2026-06-20
- Category: Delivery plan
- Scope: The milestone sequence from the current design baseline to a V1 kafkaman, the decisions each milestone realizes, and the exit criteria. Provisional — the M1 PoC will teach us things that reshape later milestones; this is a framing, not a contract.
- Sources:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/configuration-and-environment-model.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/decisions/v1-roadmap-execution-policy.decision.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md
- Related:
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Where We Are

The design surface is substantially decided: scope, schema/change-management,
config/env, runtime/topology, send, consumption, and testing (x2) — seven
foundation decisions plus accepted follow-up decisions for message identity,
retry/DLQ, roadmap execution, and entity-first propagation. Objectives open
questions OQ1-OQ4 are resolved, and OQ5 (host-context boundary) is ratified as
opaque headers with a reserved `kafkaman-*` namespace.

M1 through M5 are implemented on `main` and their validated behavior is promoted
to specs. All five M3/M4 pre-merge review findings are closed.

The previously open **retry / backoff / DLQ** taxonomy is accepted as a separate
decision and implemented in M4.

**Updated 2026-08-13:** entity-first propagation is inserted as M5, ahead of
observability. It was not in the original sequence because the propagation model
was decided after this roadmap was written. It goes before observability because
it adds per-type cache tables and a `Superseded` outbox status — building
dashboards, metrics and DLQ views on the current table layout would mean
rebuilding them a milestone later. Observability and hardening shift to M6 and
M7.

**Updated 2026-08-14:** proposal 12 narrowed kafkaman's purview to compact
entity-cache propagation only. Non-entity work items and delete-retention job
topics are outside the v1 product scope; the already-landed delete-retention API
surface is removed in the same-day follow-up.

**Updated 2026-08-24:** M5 is completed and promoted to
[entity-first-propagation.spec.md](../specs/entity-first-propagation.spec.md).
The two-service distributed-cache example remains active as a parallel wiring
proof, while M6 observability/operability can proceed against the post-M5 table
shape.

## Milestones

### M1 — Durable send (the PoC)

Status: Completed.
- **Goal:** prove the durable at-least-once send promise end-to-end.
- **Delivers:** outbox → rdkafka relay, per-type table, minimal `migrate()`, the
  Axum example, and the **minimal `kafkaman-test` `Harness` seed** (gates run
  through it). See [first-poc-outbox-publisher](../plans/first-poc-outbox-publisher.plan.md).
- **Realizes:** schema-and-change-management (per-type tables, `migrate()`),
  runtime-composition (send-side `Sender`/enqueue), messaging-scope (at-least-once).
- **Exit:** completed. The M1 spec records the verified subset: idempotent
  `migrate()`, template generalization, normal delivery, claim/stale-claim
  safety, publish-failure requeue, and the ack-before-mark duplicate window.

### M2 — Change-engine maturity + config/env

Status: Completed 2026-06-21. Proof: `wiki/specs/m2-change-engine-config.spec.md`;
integration gates cover config fail-fast, checksums/audit, legacy NULL-checksum
upgrade, `changelog!`, dry-run, and bounded send-side `Replay`.
- **Goal:** the real schema/operations engine and the configuration loader.
- **Delivers:** the `changelog!` macro, checksums + `applied_by` audit, advisory-
  locked concurrency, operational `Replay` + the guardrails, the `kafkaman.toml`
  loader (thin, by convention) + typed validation at boot.
- **Realizes:** schema-and-change-management (full engine + guardrails),
  configuration-and-environment-model.
- **Exit:** versioned changesets apply once-per-env with audit; a misconfigured/
  missing required key fails fast at boot; `Replay` runs under the guardrails.

### M3 — Durable receive (consume) + toolkit maturity
- **Status:** Completed. Validated behavior is promoted to
  [m3-durable-receive.spec.md](../specs/m3-durable-receive.spec.md); remaining
  chaos/model checks stay in the deep-durability hardening backlog.
- **Goal:** the receive half of the core promise.
- **Delivers:** the ingest + dispatch schedulers, per-type received tables, dedup-
  as-log with the bounded `errors` array, the `MessageRouter` Tower stack +
  `FromMessage` extractors + `#[derive(KafkaMessage)]`, receive-tx relocation,
  offset-after-durable-write. **The `kafkaman-test` toolkit matures here**
  (`dispatch_once()`, `oneshot` handler tests, the `#[kafkaman::test]` macro, the
  capturing sender, the injectable `Clock`) — just-in-time for the code that needs
  it, per dogfooding-first.
- **Realizes:** message-consumption-and-handler-model, consumer-test-tooling,
  library-test-strategy.
- **Execution Plan:** [m3-durable-receive.plan.md](../plans/m3-durable-receive.plan.md)
- **Exit:** effective-once under random redelivery; a slow/failing handler never
  stalls the partition; handler stacks are `oneshot`-testable.

### M4 — Reliability (retry / backoff / DLQ)
- **Status:** Completed. Validated behavior is promoted to
  [m4-retry-backoff-dlq.spec.md](../specs/m4-retry-backoff-dlq.spec.md); backoff
  jitter, terminal-row retention/purge, and operator dashboards stay later work.
  Execution plan:
  [m4-retry-backoff-dlq.plan.md](../plans/m4-retry-backoff-dlq.plan.md).
- **Goal:** turn “it’s durable” into “it recovers.”
- **Implements:** the accepted retry/backoff/DLQ taxonomy: retryable vs terminal
  errors, per-message config with common defaults in `kafkaman.toml`, backoff
  schedule, max attempts, poison classification, and terminal to table-backed
  DLQ.
- **Delivers:** the retry processor using `attempts` / `next_attempt_at` / `errors`,
  a DLQ surface, poison-message handling — built on the seams M3 reserved.
- **Exit:** retryable failures back off and recover; terminal/poison land in the
  DLQ with history; the `Clock`-driven tests prove the timing without `sleep`.

### M5 — Entity-first propagation (the distributed cache)
- **Status:** Completed 2026-08-24. Validated behavior is promoted to
  [entity-first-propagation.spec.md](../specs/entity-first-propagation.spec.md).
  Execution record:
  [entity-first-propagation.plan.md](../plans/entity-first-propagation.plan.md).
- **Goal:** deliver the headline use case — services read another domain's
  entities from a local store instead of calling that domain synchronously.
- **Delivers:** the required entity-key message surface, per-type cache tables
  that kafkaman owns and writes, offset-guarded cache upsert, received-row
  entity-key persistence, per-entity `Superseded` outbox supersede, claim-time
  collapse of stale same-entity pending rows, rejection of unsafe row-sourced
  outbox replay, wire-carried producer occurrence time/idempotency source,
  jittered receive retry backoff, and opt-in outbox retention.
- **Realizes:** entity-first-propagation-model, and the convergence half of
  message-consumption-and-handler-model.
- **Exit:** a cache converges to current state under the validated reorderings
  kafkaman itself produces today — retry backoff, receive redrive, concurrent
  dispatch, broker redelivery, and same-entity outbound retry/supersede — while
  outbound serialization is limited to the affected `(message_type, entity_key)`.

Deferred out of M5 and tracked separately: cache bootstrap and readiness
typestate ([proposal 10](../proposals/10-cache-bootstrap-and-readiness.proposal.md)),
boot-time broker topic validation, advisory origin intent, a positive
state-sourced republish API, proactive topic-lifecycle re-bootstrap hooks,
soft-delete workflow/reclamation, the two-service distributed-cache example
([plan](../plans/two-service-distributed-cache-example.plan.md)), and the
remaining restore/schema boundaries
([proposal 11](../proposals/11-restore-retention-and-schema-boundaries.proposal.md)).

**M5's shape is resolved by accepted
[proposal 12](../proposals/12-entity-only-message-model.proposal.md).** Every
in-purview kafkaman type is a compact entity-cache message. This changes the
implementation shape — entity keys, cache tables, compact-topic validation, and
state-sourced republish — without changing the milestone's position or its
convergence exit criterion.

### M6 — Observability & operability
- **Status:** Active. Safe to run in parallel with the two-service example.
- **Goal:** make it operable.
- **Delivers:** tracing spans + the `CorrelationLayer`, metrics, lag/age/stuck-job
  detection, DLQ inspection, the `kafkaman-axum` admin/health routes, the
  `serve().with_runtime()` shutdown helper.
- **Realizes:** runtime-composition (request-path layers/routes + shutdown helper).
- **Exit:** an operator can see queue depth/age, inspect/redrive the DLQ, and a
  stuck job is detectable.

### M7 — V1 hardening
- **Goal:** ship-quality.
- **Delivers:** remaining storage-growth policy outside outbox retention,
  graceful-shutdown ordering, worker-role topology polish, the full
  `testcontainers` full-loop suite, docs/examples, and **ratifying OQ5** (the
  opaque-headers host-context boundary) at envelope finalization.
- **Exit:** the V1 acceptance bar — durable send + durable receive + reliability +
  observability, documented, with the full test pyramid green.

## Cross-Cutting Track: `kafkaman-test`

Not a single milestone — **seeded in M1, matured in M3, completed in M7** (the
`testcontainers` full-loop feature). Dogfooding-first makes the toolkit lead the
code that uses it, so it threads every milestone rather than trailing at the end.

## Working Method: Outside-In TDD

Dogfooding-first has a concrete operational form. Each milestone **opens with the
`Harness`-level test we wish we could run** — the consumer-facing API call plus the
assertion — written first as code that does not yet compile, and that desired
ergonomics *drives* the core API shape. This is **not** "build the tools before the
code" (structurally impossible — the `Harness` wraps core seams like
`dispatch_once()`, which is exactly why the
[library-test-strategy decision](../decisions/library-test-strategy.decision.md)
draws a white-box boundary *beneath* the toolkit). It is "the test is the design
tool": M1 *opens* with a failing `Harness` test rather than closing with one, and
every later milestone does the same with the seam it adds.

## Sequencing Notes

- **M2 before M3** so the consume side is built on the real change engine and
  config loader, not the PoC's hardcoded minimal `migrate()`.
- **M4 has its policy decision**, but implementation still waits for M3 receive
  seams and the per-message config substrate from M2.
- **Do not blanket-Accept the four Draft decisions at M1 entry.** They describe
  M3/M4 code the PoC has not validated. Treat consumption, both testing decisions,
  and the *receive* half of runtime-composition as **Accepted design direction
  with a revisit gate at their own milestone's exit** — each flips to `Accepted`
  only once its milestone validates it. Only the **send-side** of the
  runtime-composition decision (the half M1 actually exercises) is eligible to be
  ratified at M1 exit.
- **Parallel worktrees are allowed only with dependency-aware lanes.** M1
  closeout, M2 substrate, M3 API/test sketches, retry/DLQ docs, and status/docs
  cleanup may proceed in separate worktrees, but M3 implementation waits for M2
  and M4 implementation waits for M3 seams.

## What Closes This Roadmap

A tagged V1 that meets the M7 acceptance bar, at which point the settled envelope +
schemas + the `migrate()`/runtime/consume contracts are promoted to `*.spec.md`
pages and this roadmap is archived.
