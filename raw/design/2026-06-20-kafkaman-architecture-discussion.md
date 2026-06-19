# Design Discussion: kafkaman Architecture (2026-06-20)

Curated record of the human + agent design conversation on 2026-06-20 that
produced the messaging-scope and schema/change-management decisions. Captured as
a `raw/` source so those decisions have durable provenance for their rationale.
This is a synthesis, not a verbatim transcript. Treated as **append-only
provenance**: sections are added as the conversation progresses and existing text
is not rewritten. It is the transcript record (the "source" behind the decisions),
distinct from the compiled knowledge in `wiki/`.

## Participants and framing

- The reference project (cqrs-fullstack / RepForge) is a **discussion starting
  point**, not a blueprint. Design is driven by "what do we want kafkaman to be."

## Messaging scope (→ messaging-scope-and-receive-model decision)

- Considered: Kafka-command-first (A), propagation-first (B),
  durable-execution-first (C).
- Decisive evidence: the reference *built* commands-over-Kafka
  (`commands_inbox`/`processed_commands`) and then **dropped** them (migration
  `010`), replacing them with a durable HTTP `mutation_jobs` engine — i.e. the
  same durable-execution machinery with a different dispatch verb.
- Outcome: **C, durable-execution-first**, Kafka-only in v1. HTTP/commands and
  synchronous outcomes deferred. Working thesis: a reliable enough Kafka async
  story may make non-Kafka inter-service messaging unnecessary.
- Receive model: fire-and-forget with durable status. `correlation_id` /
  `causation_id` ride in the envelope so the outcome story can be added later
  without re-shaping the envelope; the wait-for-outcome mechanism (durable
  waiter store) is deferred and, when built, is provided as primitives — policy
  (timeouts, batching, synchronous responses) stays in the host.

## Execution model (→ objectives proposal)

- kafkaman is a **durable message runtime**: durable table → scheduler →
  annotated handler (`#[kafkaman::handler(...)]`). Realizes the original "a
  scheduler runs the code and handles retries" intent.
- The transaction relocates rather than disappears: on receive kafkaman owns the
  tx and hands it to the handler; on send the user owns the tx and kafkaman
  joins it. Send offers transactional and fire-and-forget enqueue.

## Table layout (→ schema-and-change-management decision)

- Journey: generic single table (reference's approach) → considered per-type
  tables → considered partitioning → **chose distinct per-type tables** in a
  dedicated `kafkaman` schema, generated from one uniform template.
- Why per-type over partitioning: operational clarity + scoped reconsume + a
  *simpler* idempotency story (per-table `UNIQUE`, avoiding the
  partition-key-in-unique-constraint trap, which is especially hostile to
  time-range partitioning of a dedup table). Partitioning reserved as a per-table
  scale optimization if one type's volume makes DELETE-based purge painful.
- Schema vs prefix: dedicated `kafkaman` Postgres schema (configurable),
  not a `kfkmm_` table-name prefix.

## Change management (→ schema-and-change-management decision)

- Rejected SQL stored functions (DB lock-in, untestable, undebuggable).
- Chose a **Rust, Flyway/Liquibase-style change engine** (`refinery` is the Rust
  precedent): one ordered changelog of versioned changesets, tracked with
  audit/checksums, applied once per environment.
- Versioned changesets cover structural (`CreateMessageTable`) and *one-shot*
  operations (`Replay`) — driven by the team's CI/CD-runs-migrations-on-all-envs
  reality ("a CLI does not cut it"). **Tunable settings (retention, batch sizes)
  are NOT changesets** — they are per-env runtime config (see Configuration
  below); a run-once changeset can't be re-tuned without authoring a new one.
- Two phases: `kafkaman::migrate()` = DB convergence (at app startup,
  advisory-locked); `Runtime::start()` = subsystems (consumers, retries, purge
  enforcer). The purge enforcer re-reads retention config each boot.

## Configuration / environment (→ configuration-and-environment decision)

- `apply(cfg, b)`: changesets receive the resolved config bag (typed values) and a
  `ChangeBuilder`. Rule: config may select *values*, not branch *structure*
  (branching schema by env → drift + untested prod path). Settled that `cfg`
  exposes values, not an environment identity — so the rule is enforced by the
  contract (nothing to branch on), not just convention.
- One flat `kafkaman.toml` property bag, **no profiles**. CI/CD renders it per env
  and injects values/secrets from the vault at deploy time; the app reads one
  resolved file. Secrets never in git; a committed `kafkaman.example.toml`
  documents the keys. Typed access, validated at startup (fail fast).
- Why flat-file-per-env over in-app profiles: simpler and safer — identical code
  path everywhere, only the injected value differs. Matches the reference's
  existing `config/runtime/{env}/...`.
- Versioning: **sequential integers**, not timestamps — same-number merge
  collisions are a deliberate reconciliation forcing function.
- Authoring: explicit ordered `changelog!` in its own module (out of `main`),
  one changeset per file. Annotations are for handlers, not changesets.
  Directory-derived changelog (`embed_changelog!`) deferred.
- Plain tables (no logic hidden in DB functions) keep break-glass manual prod
  fixes possible.

## Runtime composition & send-side UX (→ runtime-composition-and-topology decision)

- kafkaman owns neither a process nor the Tokio runtime. It builds a `Runtime`
  (in the host crate, where compiled-in `#[handler]`s are visible) that yields
  spawnable units: `runtime.run(shutdown)` (one future over all subsystems) or
  `into_tasks()` (one per subsystem). Shutdown is a `CancellationToken` the host
  trips; subsystems drain on cancel.
- Topology is a host choice via `.subsystems(...)`: embedded (next to Axum) or
  worker-role (no HTTP), same crate. No standalone daemon — handlers are
  compiled-in Rust, so a generic binary can't dispatch them.
- Request-path vs background split: request-scoped concerns are Axum-native (a
  `CorrelationLayer`, admin/DLQ routes, a `serve().with_runtime()` shutdown
  helper); continuous loops are spawnables, never middleware (middleware would
  re-couple them to HTTP and break worker-role).
- Send UX is opinionated around `axum-sqlx-tx`: a `Sender` extractor (request Ctx
  + producer handle, no tx of its own) enqueues into the host's ambient
  auto-committing transaction — `sender.enqueue(&mut tx, evt)` with no manual
  `commit()`, so the business write + outbox row commit atomically on a success
  response and roll back together on failure. Fire-and-forget `send_now` opts out
  (dual-write risk). Core `enqueue(&mut tx)` stays framework-agnostic in
  kafkaman-sqlx.
- Chose opinionated (a real `axum-sqlx-tx` dependency in kafkaman-axum) over
  compatible-by-genericity, to make atomic outbox the default and delete the
  forgettable commit. Documented assumption: the host's commit-on-success policy
  now also governs whether messages are sent.

## Message consumption & handler model (→ message-consumption-and-handler-model decision)

- Two schedulers, so user code never locks Kafka ("messages must flow"): an
  *ingest* scheduler consumes → writes the received row → commits the offset
  immediately; a *dispatch* scheduler polls the DB (`SKIP LOCKED`) → runs the
  handler. Offset commits after the durable write, not the handler.
- Per-type received tables. Dedup is a log: `INSERT … ON CONFLICT DO NOTHING` on
  identity; a redelivery is a logged no-op. Failures accumulate in an `errors`
  **JSONB array** (full history, not a single last_error), bounded to the most
  recent N so a poison message can't grow it unbounded; `attempts` is the
  authoritative counter. `Failed` + `errors` is the remediation surface.
- Handler model is axum-shaped but its **own** thing, not HTTP: a `MessageRouter`
  that *is* a `tower::Service<Message>`, keyed by `message_type` (like axum keys
  by path). Explicit `.handler::<T>(fn)` registration (no discovery macro); the
  payload type carries `topic`/`message_type` via a derive. Extractors mirror
  axum via a kafkaman `FromMessage` trait (not axum's http-bound one), enabling
  e.g. an injected HTTP client to make a REST call inside a handler.
- Tower reuse: generic middleware (Timeout, ConcurrencyLimit, Retry, Buffer,
  LoadShed, RateLimit) works on a Message unchanged; axum's http extractors and
  tower-http layers do not (http-bound) and are deliberately not faked. ~90%
  inherited, small extractor trait re-implemented, no http masquerade.
- Receive tx relocates to kafkaman: dispatch runs the stack inside a
  kafkaman-owned tx handed to the handler (`Rx`); Ok → business write + mark
  Processed commit together (effective-once); Err / layer short-circuit → append
  to errors, attempts++, next_attempt_at, row re-driven later.
- Hybrid wiring: config → migrate → build the kafkaman consumer tower → build
  axum's tower → one hybrid server under a shared shutdown (embedded topology);
  worker-role drops the axum tower. Retry/backoff/DLQ taxonomy deferred to a
  follow-up; the row fields reserve the seams.

## Testing (→ library-test-strategy + consumer-test-tooling decisions)

Two separate concerns, kept as two documents:

- **Testing kafkaman itself:** a three-tier pyramid — unit (Docker-free, pure
  logic), integration (Postgres via testcontainers: migrate convergence, SKIP
  LOCKED claim, dispatch commit, dedup), full-loop (Postgres + Redpanda: both
  broker edges). Crash-injection gates (no loss; at-least-once republish window;
  dispatch re-drive) and property tests for the core invariants (effective-once,
  no loss, bounded errors ring, idempotent migrate) are first-class. Determinism:
  kafkaman dogfoods its own consumer tooling — manual `dispatch_once()` + injected
  `Clock`, no sleeps. Containers start once per test binary with schema/topic
  isolation for parallel safety.
- **Tools for consumers:** a dedicated `kafkaman-test` dev-dependency crate. Two-
  audience principle — consumers test their handlers/sends/business logic against
  Postgres only, deterministically; the broker is only for full-loop tests.
  Handlers are `tower::Service<Message>` so `oneshot` tests them (layers included).
  A `Harness` gives ephemeral schema + migrate + a capturing sender +
  `dispatch_once()`/`tick()` + a controllable `Clock` + row-state assertions, all
  against a caller-provided connection string. A `#[kafkaman::test]` macro wraps
  tokio + ephemeral schema + migrate and injects the Harness (sqlx::test-style):
  Docker-free by default, containers/broker opt-in via attribute args, containers
  per-binary, never auto-starts schedulers, and is optional sugar over an explicit
  `Harness::connect`. Transport stance respects OQ2: Postgres-only fast tests +
  real Redpanda for full-loop (behind an optional `testcontainers` feature); an
  in-memory fake/seam is deferred until proven necessary.
- Standing principle recorded: macros are opt-in sugar over explicit APIs, never
  load-bearing (also why `#[kafkaman::handler]` was dropped for explicit
  `.handler::<T>()`).
- `#[kafkaman::test]` follows the precedent of `#[sqlx::test]` (ephemeral DB +
  injected pool) and `#[tokio::test]` (runtime wrapper).
- **Dogfooding-first (sharpened):** kafkaman's own suite should use the consumer
  toolkit *as much as possible* — wherever a test sits at or above the toolkit's
  abstraction. Boundary: the layers beneath it (Harness/macro internals, SQL/DDL
  builders, dedup query, ring logic, rdkafka edges) stay white-box, since the
  toolkit is built on them (circularity). Consequence: `kafkaman-test` is an early
  deliverable (built with core/sqlx); toolkit-using library tests live in a
  separate workspace test member to avoid the dev-dependency cycle.

## Open items noted during discussion

- Governance of operational changesets inside the deploy stream (env targeting,
  dry-run, approval gating, blast-radius limits) — see the decision.
- Whether to also accept config-file/TOML changesets (default Rust).
- topic-per-type vs shared topics; Postgres polling vs Debezium CDC.
