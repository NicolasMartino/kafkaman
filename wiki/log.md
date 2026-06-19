# Wiki Log

## [2026-06-20] promote | M1 durable-send implementation validated

Implemented the M1 durable-send slice on branch `implementation/m1-durable-send`.
The workspace now includes core types, SQLx migration/outbox primitives, claim
lease relay worker, rdkafka publisher, Docker-free Harness seed, testcontainers
Postgres integration gates, and a runnable Axum outbox example. Promoted the
validated behavior into an active spec and marked the two M1 plans completed.

Evidence:
- `cargo test --workspace` passed with 10 tests across 16 suites in 7.34s.
- Integration tests start Postgres through `testcontainers`.

Pages affected:
- specs/m1-durable-send.spec.md (created, Active)
- plans/first-poc-outbox-publisher.plan.md (Status: Completed)
- plans/m1-durable-send-implementation.plan.md (Status: Completed)
- roadmaps/path-to-v1.roadmap.md (M1 Status: Completed)
- index.md (catalog refreshed)

## [2026-06-20] update | M1 plan review fixes

Corrected the M1 implementation plan after review. The relay model now uses a
real claim lease/token flow instead of an impossible single transaction around
Kafka publish; `Publishing` rows are reclaimable after lease expiry. The plan now
uses object-safe changesets, typed message descriptors, validated SQL identifiers,
stale-claim-safe `mark_published`, `mark_publish_failed`, and a Docker-free
capturing-publisher Harness default with opt-in Redpanda/full-loop support.
Rewrote the first-PoC plan to match those mechanics and strengthened verification
gates so delivery claims require both a published/captured record and the expected
outbox row state.

Pages affected:
- plans/m1-durable-send-implementation.plan.md
- plans/first-poc-outbox-publisher.plan.md
- decisions/schema-and-change-management.decision.md

## [2026-06-20] lint | design review fixes (8 findings) + M1 implementation plan

Actioned an external design review of the four Draft decisions, the roadmap, and
the PoC plan. Fixes:
- **Receive state machine (#2):** pinned the dispatch claim predicate
  (`status IN (Pending, Retryable) AND next_attempt_at <= now()`) and the status
  set (`Pending→Processing→Processed`, `→Retryable→`, `→Failed`) in the
  message-consumption decision; only the retry *policy* stays deferred.
- **Effective-once scope (#1):** message-consumption point 6 now states the
  guarantee covers only writes through `Rx`; external side effects are
  at-least-once and need their own idempotency/outbox. Handler example comment
  corrected.
- **`send_now` footgun (#4):** runtime decision renames it
  `send_non_transactional`, quarantines it off the default `Sender` surface,
  requires a counter/span.
- **Subsystem default (#5):** runtime decision default is now the smallest
  non-destructive set (relay + consumers, never `Purge`); explicit selection.
- **Daemon vocabulary (#6):** reconciled "no standalone daemon" across the runtime
  decision and the proposal's `kafkaman-worker` note ("worker-role host binary").
- **"Full" vs bounded history (#7):** message-consumption `errors` reworded to
  "recent failure history" (bounded ring, explicitly not an audit log).
- **raw/ immutability (#8):** marked the design-discussion source as append-only
  provenance, distinct from compiled `wiki/` knowledge.
- **Roadmap (#3 + TDD):** dropped the blanket "flip four decisions to Accepted at
  M1"; now "Accepted design direction + revisit gate at each milestone's exit",
  only the send-side of runtime ratifiable at M1. Added an Outside-In TDD working
  method (the failing `Harness` test drives the API).
- Recorded the **dedup-identity open question** (idempotency key vs message_id vs
  offset) in the message-consumption decision, with a recommended default to
  ratify before M3.

Page created:
- plans/m1-durable-send-implementation.plan.md — code-level M1 plan (crate layout,
  deps, types/signatures, migrate engine + outbox DDL, enqueue/claim/mark, relay,
  Harness seed, ordered outside-in-TDD tasks → PoC gates).

## [2026-06-20] create | project bootstrap

Initialized `kafkaman` with the LLM Wiki framework.

## [2026-06-20] ingest | kafkaman objectives research bundle

Ingested `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/`
(research-summary.md, manifest.md, sources 02-12). The 01-cqrs-fullstack copy is
on disk but gitignored; its distilled form (research-summary.md §2) was used.

Pages created:
- proposals/01-kafkaman-objectives.proposal.md
- proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md
- references/rust-kafka-outbox-ecosystem.reference.md
- plans/first-poc-outbox-publisher.plan.md
- index.md updated.

Contradictions: none in the wiki (it was empty). One real architecture tension
recorded as proposal 02 (host architecture uses Kafka for propagation + internal
HTTP for commands, vs kafkaman's original Kafka-instead-of-REST idea) — filed as
a candidate awaiting ratification, not an accepted decision.

Gaps: outbox-pattern-processor docs.rs/crates.io version recency disagreed
(0.3.6 vs 0.4.0). Five v1-scope open questions remain in proposal 01.

## [2026-06-20] promote | messaging scope ratified to a decision

Ratified proposal 02 as a decision after reviewing the cqrs-fullstack migration
history (commands-over-Kafka was built and dropped in migration 010; replaced by
the durable HTTP `mutation_jobs` engine). Choice: durable-execution-first core,
Kafka-only transport in v1, HTTP/commands/synchronous outcomes deferred, receive
is fire-and-forget + durable status with `correlation_id`/`causation_id` in the
envelope and wait-for-outcome reserved as future primitives.

Pages affected:
- decisions/messaging-scope-and-receive-model.decision.md (created, Accepted)
- proposals/02-messaging-scope-... (Status → Accepted, promoted)
- proposals/01-kafkaman-objectives (open questions 2 and 4 resolved)
- index.md (stage → "Scope decided"; Decisions section populated)

## [2026-06-20] lint | external review fixes (provenance + delivery semantics)

Acted on an external review of the bootstrap state. Fixes:
- Provenance: the decision cited the gitignored cqrs-fullstack copy. Added a
  committed verbatim excerpt
  (sources/01-cqrs-fullstack-migration-evidence.md) and repointed the decision's
  sources to it; full project remains local-only.
- Delivery semantics: PoC plan claimed "exactly-once-effectively". Corrected to
  durable at-least-once (publish-then-mark duplicate window), added a
  crash-after-ack-before-mark verification test, noted effective-once belongs to
  the (out-of-scope) consumer idempotency layer.
- Provenance hygiene: expanded abbreviated `raw/research/...` Source paths to
  full resolvable paths in the ecosystem reference.
- Config hygiene: gitignored `.llm_wiki/runtime.toml` (per-machine install state,
  absolute paths); set Serena `languages: [rust]`; trimmed AGENTS.md EOF blank
  line; staged the previously-untracked decision so the index is commit-consistent.

Pages affected:
- raw/.../sources/01-cqrs-fullstack-migration-evidence.md (created)
- decisions/messaging-scope-and-receive-model.decision.md
- plans/first-poc-outbox-publisher.plan.md
- references/rust-kafka-outbox-ecosystem.reference.md

## [2026-06-20] update | execution model + reference stance recorded

From design discussion: recorded the agreed **durable message runtime** model
(durable table → scheduler → annotated handler; receive-tx owned by kafkaman and
handed to the handler; send offers transactional + fire-and-forget enqueue;
runtime `migrate()`), and the guiding stance that the cqrs-fullstack reference is
a discussion starting point, not a blueprint. Physical table layout (per-type vs
partitioning) and purge/retention remain under active discussion, flagged as
open in the proposal.

Pages affected:
- proposals/01-kafkaman-objectives.proposal.md (Execution Model section + reference stance)

## [2026-06-20] create | schema & change-management decision

Captured the design discussion outcome on persistence and operations so the
reasoning is not lost. Decision: dedicated `kafkaman` schema; distinct per-type
tables from one template; per-table UNIQUE for idempotency; DELETE-based purge
(partitioning deferred); a Rust Flyway-style change engine with versioned
changesets (structural + operational unified) tracked in
`kafkaman.changelog_history`; `migrate()` = CI/CD convergence, `Runtime::start()`
= subsystems; retention declared by changeset, enforced at runtime; no SQL
functions; plain tables for break-glass. Alternatives (single generic table,
partitioning, SQL functions, extending host sqlx migrations) recorded with
rejection rationale.

Pages affected:
- decisions/schema-and-change-management.decision.md (created, Accepted)
- proposals/01-kafkaman-objectives.proposal.md (schema/change-management bullet;
  table-layout + purge open items resolved)
- index.md (Decisions section)

## [2026-06-20] update | changeset versioning + placement settled

Settled two changeset details: sequential integer versions (not timestamps —
merge collisions are a deliberate reconciliation forcing function), and the
changelog lives in its own module out of `main` (one changeset per file;
directory-derive macro deferred).

Pages affected:
- decisions/schema-and-change-management.decision.md (point 10 added)

## [2026-06-20] update | realigned the first-PoC plan to the decisions

Rewrote the PoC plan to match the accepted decisions: per-type outbox table in
the `kafkaman` schema provisioned by a **minimal** `kafkaman::migrate()`
(structural changesets only — operational changesets, checksums, audit, and the
`changelog!` macro explicitly deferred); envelope carries
`correlation_id`/`causation_id`; example shows the changelog in its own module
and the two-phase `main`. Delivery semantics stay durable at-least-once. Now
validates the two most expensive commitments (per-type layout + `migrate()`
entry point), not just a generic outbox.

Pages affected:
- plans/first-poc-outbox-publisher.plan.md (rewritten)
- index.md (plan summary)

## [2026-06-20] lint | external review fixes (round 2)

Acted on a second external review. Fixes:
- Provenance: created raw/design/2026-06-20-kafkaman-architecture-discussion.md
  (curated design-discussion note) and repointed both decisions' dangling
  "Design discussion" source to it.
- messaging-scope decision: "without a schema migration" → "without changing the
  existing envelope fields" (the waiter store still adds tables).
- PoC plan: softened the per-type claim (validates the template + plumbing, not
  the operational advantages) and added a second `CreateMessageTable` + a
  template-generalizes gate at near-zero cost.
- schema decision: added an "Open Refinement: Guardrails for Operational
  Changesets" section (env targeting, dry-run, auto-vs-gated apply mode,
  blast-radius limits); auto-vs-gated default flagged OPEN, leaning gated.
- reference: softened "no dominant Rust crate" → "this research did not find…".
- Trimmed EOF blank lines from 9 raw files (whitespace only, no provenance
  impact) for a clean first commit; `git diff --check` now clean.

Pages affected:
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (created)
- decisions/schema-and-change-management.decision.md
- decisions/messaging-scope-and-receive-model.decision.md
- plans/first-poc-outbox-publisher.plan.md
- references/rust-kafka-outbox-ecosystem.reference.md

## [2026-06-20] update | operational-changeset apply mode resolved → auto-apply

Closed the OPEN guardrail question: operational changesets **auto-apply** like
structural ones (rationale: reaching prod means convergence through lower envs).

## [2026-06-20] create | configuration & environment decision

After discussion (and reverting an earlier premature retention→config edit),
settled the config/env model and recorded it as its own decision. Model: a
changeset is `apply(env, builder)`; `env` may select values, not branch
structure. One flat `kafkaman.toml` (no profiles) is rendered per environment by
CI/CD, injecting values/secrets from the vault; the app reads one resolved file,
validated at startup. Tunable settings (retention, batch sizes) are **runtime
config re-read each boot**, not changesets — so they are tunable via config +
redeploy without authoring a changeset (a run-once changeset could not be
re-tuned). Schema decision updated to match (points 6, 7, 9, +11, guardrails);
`SetRetention`-as-changeset removed.

Pages affected:
- decisions/configuration-and-environment-model.decision.md (created, Accepted)
- decisions/schema-and-change-management.decision.md (aligned)
- proposals/01-kafkaman-objectives.proposal.md (change-engine bullet)
- plans/first-poc-outbox-publisher.plan.md (out-of-scope)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (config section)
- index.md (Decisions section)

## [2026-06-20] update | operational-changeset guardrails finalized

Made the safety nets explicit and mandatory — bulk-op batching/rate-limiting
(since lower-env testing validates correctness but not prod volume), dry-run
visibility in deploy logs, and env-targeting as the per-operation opt-out.
Residual risk (large op at prod scale + replay downstream amplification) recorded
as accepted.

Pages affected:
- decisions/schema-and-change-management.decision.md (guardrails section finalized)

## [2026-06-20] update | migrate() runs at app startup

Clarified where `migrate()` runs: **at app startup** (in `main`, before
`Runtime::start()`), mirroring the reference's `sqlx::migrate!` in `main` —
advisory-locked and idempotent. Standalone pre-deploy invocation kept as an
option. To keep startup non-blocking under auto-apply, operational changesets do
only fast bounded state changes (`Replay` flips status / bumps epoch); the heavy
rate-limited draining is the runtime subsystems' job, after boot. (Retention is
runtime config, not a `SetRetention` changeset — see the later config &
environment decision entry, which supersedes any earlier `SetRetention` mention.)

Pages affected:
- decisions/schema-and-change-management.decision.md (phase 7 sharpened)
- proposals/01-kafkaman-objectives.proposal.md (migrate bullet)

## [2026-06-20] lint | external review fixes (config decision, round 3)

Acted on a third external review (config & environment decision). Fixes:
- Secrets hygiene: `.gitignore` now ignores the resolved `kafkaman.toml` and
  keeps `!kafkaman.example.toml` tracked (previously only `.env*` was covered, so
  a rendered config with injected secrets was one `git add` from leaking).
- Config-API contradiction resolved: the decision said kafkaman *loads*
  `kafkaman.toml` while the alternatives said it *consumes a property bag the host
  produces*. Settled on a **thin loader by convention** (sqlx-style); reworded the
  rejected alternative to "a full config *framework* (profiles/layering/
  precedence)", which is what is actually rejected.
- "Values not structure" made contract-enforced, not policy: changesets now
  receive a resolved **config bag** (`apply(&self, cfg, b)`) exposing typed values
  with **no env identity** to branch on — so the rule is guaranteed by the API,
  not left to discipline. Aligned the schema decision (points 9, 11) and the raw
  design note.
- Created the committed `kafkaman.example.toml` the decision asserted exists
  (illustrative/pre-implementation; documents the intended keys).
- Log hygiene: restored a heading on an orphaned guardrails entry; corrected a
  superseded `SetRetention`-as-changeset mention in the migrate-at-startup entry.

Pages affected:
- .gitignore
- kafkaman.example.toml (created)
- decisions/configuration-and-environment-model.decision.md
- decisions/schema-and-change-management.decision.md
- raw/design/2026-06-20-kafkaman-architecture-discussion.md
- wiki/log.md (orphaned heading + supersede note)

## [2026-06-20] create | runtime composition & topology decision (draft)

Drafted the runtime/topology decision resolving objectives OQ1. Schedulers are
spawnable units (`runtime.run(shutdown)` over all subsystems, or `into_tasks()`
per subsystem) honoring a `CancellationToken`; kafkaman owns neither a process
nor the Tokio runtime. Topology (embedded vs worker-role) is a host choice via
`.subsystems(...)`; no standalone daemon since handlers are compiled-in Rust.
Request-path concerns are Axum-native (`CorrelationLayer`, admin/DLQ routes,
`serve().with_runtime()` shutdown helper); background loops are never middleware.
Send UX is opinionated around `axum-sqlx-tx`: a `Sender` extractor enqueues into
the host's ambient auto-committing tx (no manual `commit()`; business write +
outbox row commit atomically), with `send_now` as the fire-and-forget opt-out;
core `enqueue(&mut tx)` stays generic in kafkaman-sqlx. Receive/consumption side
deferred to the next discussion. Status: Draft.

Pages affected:
- decisions/runtime-composition-and-topology.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (OQ1 resolved; kafkaman-axum bullet)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (runtime composition section)
- index.md (Decisions section)

## [2026-06-20] create | message consumption & handler model decision (draft)

Drafted the receive-side decision. Two schedulers so user code never locks Kafka:
an ingest scheduler consumes → writes the received row → commits the offset
immediately; a dispatch scheduler polls (`SKIP LOCKED`) → runs the handler. Dedup
is a log (`ON CONFLICT DO NOTHING`); failures accumulate in a bounded `errors`
JSONB array (most-recent-N ring; `attempts` is the authoritative counter). Handler
model is axum-shaped but its own thing, not HTTP: a `MessageRouter` that *is* a
`tower::Service<Message>`, keyed by `message_type`, explicit `.handler::<T>(fn)`
registration, kafkaman `FromMessage` extractors; generic Tower middleware is
inherited, http-bound axum/tower-http pieces are not (no http masquerade). The
receive tx relocates to kafkaman (Ok → business write + mark Processed commit
together). Hybrid wiring = config → migrate → consumer tower → axum tower → one
server. Retry/backoff/DLQ taxonomy deferred (fields reserved).

Consistency fixes: Core Promise #2 corrected ("offset after the handler" →
"offset after the durable receive write"); Execution Model bullet rewritten for
the two-scheduler model; objectives OQ3 (polling vs CDC) resolved → polling, CDC
not pursued for v1.

Pages affected:
- decisions/message-consumption-and-handler-model.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (Core Promise #2; Execution Model; OQ3 resolved)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (consumption section)
- index.md (Decisions section)

## [2026-06-20] create | testing decisions — library strategy + consumer tooling (drafts)

Split the test story into two decisions, per request. (1) Library test strategy:
how kafkaman tests itself — a three-tier pyramid (unit Docker-free / Postgres
integration / Postgres+Redpanda full-loop via testcontainers), crash-injection
gates and property tests for the core invariants (effective-once, no loss,
bounded errors ring, idempotent migrate), determinism via `dispatch_once()` + an
injected `Clock` (kafkaman dogfoods its own consumer tooling), containers started
once per test binary with schema/topic isolation. (2) Consumer test tooling: a
dedicated `kafkaman-test` dev-dependency crate — `tower` `oneshot` handler tests,
a deterministic `Harness` (ephemeral schema + migrate + capturing sender +
`dispatch_once()` + Clock + row assertions) against a caller-provided connection
string, and a `#[kafkaman::test]` macro (sqlx::test-style injection, Docker-free
by default, containers/broker opt-in, per-binary containers, never auto-starts
schedulers, optional sugar over explicit `Harness::connect`). Transport stance
respects OQ2: Postgres-only fast tests + real Redpanda full-loop behind an
optional `testcontainers` feature; in-memory fake/seam deferred. Recorded a
standing principle: macros are opt-in sugar over explicit APIs, never
load-bearing.

Pages affected:
- decisions/library-test-strategy.decision.md (created, Draft)
- decisions/consumer-test-tooling.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (kafkaman-test crate added to shape)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (testing section)
- index.md (Decisions section)

## [2026-06-20] update | dogfooding-first elevated to the primary test principle

Per discussion, sharpened the library test strategy: kafkaman's own suite uses
the consumer toolkit (`kafkaman-test`) *wherever a test sits at or above the
toolkit's abstraction*, making our suite the toolkit's primary consumer. Recorded
the boundary (layers beneath the toolkit — Harness/macro internals, SQL/DDL
builders, dedup query, ring logic, rdkafka edges — stay white-box to avoid
circularity) and the consequence (`kafkaman-test` is an early deliverable built
with core/sqlx; toolkit-using library tests live in a separate workspace test
member to avoid the `core ⇽ test` dev-dependency cycle).

Pages affected:
- decisions/library-test-strategy.decision.md (dogfooding-first as primary principle + boundary + build order)
- decisions/consumer-test-tooling.decision.md (early-deliverable consequence)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (dogfooding sharpened)

## [2026-06-20] update | PoC seeds the test Harness; V1 roadmap drafted

Two related steps. (1) Adjusted the first-PoC plan so dogfooding-first holds from
line one: added a minimal `kafkaman-test` `Harness` seed (ephemeral-schema
`migrate()`, enqueue helper, `relay_once()` one-step driver, row assertions) as
the PoC's first test-facing deliverable, routed the crash/integration gates
through it, added `-test` to the workspace stubs, and marked the full toolkit
out of scope. (2) Drafted the V1 roadmap: six milestones (M1 durable send/PoC →
M2 change-engine + config → M3 durable receive + toolkit maturity → M4
retry/backoff/DLQ → M5 observability → M6 hardening), `kafkaman-test` as a
cross-cutting track (seeded M1, matured M3, completed M6), and the explicit notes
that M4 opens with its own retry/DLQ decision and the four Draft decisions flip to
Accepted at the M1-entry review.

Pages affected:
- roadmaps/path-to-v1.roadmap.md (created, Draft)
- plans/first-poc-outbox-publisher.plan.md (Harness seed; steps + gates; out-of-scope)
- index.md (Stage line; Roadmaps section)

## [2026-06-20] create | M1 durable-send implementation review

Created a sourced review page that verifies the post-implementation M1
durable-send review against the current Rust code, plan, spec, tests, and
examples. Confirmed the main gaps around status-string centralization, database
clock ownership, Redpanda/full-loop scope, idempotency durability, worker
resilience, Harness migration races, facade usage, and index-name collisions.
Added additional findings for migration concurrency, example worker failure
visibility, duplicate descriptors, and unused worker publish-error surface.

Pages affected:
- wiki/reviews/m1-durable-send-implementation-review.reference.md
- wiki/index.md
- wiki/log.md
