# Consumer Test Tooling (`kafkaman-test`)

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-20
- Category: Quality and testing
- Scope: The test tooling kafkaman *ships to library consumers* so they can test their handlers, sends, and business logic — the `kafkaman-test` crate, its `Harness`, the determinism contract, and the `#[kafkaman::test]` macro. How kafkaman tests *itself* is a separate concern (see the library test-strategy decision).
- Sources:
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
- Related:
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/proposals/01-kafkaman-objectives.proposal.md

## Ratification, 2026-08-31: accepted as direction, with the ergonomics layer deferred

The V1 roadmap held this decision at `Draft` behind a revisit gate that ratifies
only what a milestone validates. `kafkaman-test` was seeded in M1, matured in M3,
and completed in M7, so the gate has passed and the status moves to `Accepted`.

But ratifying the text as written would put an API in the design record that no
adopter can call. What V1 ships is the **principle** layer of this decision, not
its **ergonomics** layer. Verified against the crate at the V1 boundary:

Shipped — points 1, 2, 7's transport stance, and 8. A dedicated `kafkaman-test`
dev-dependency crate rather than a `testing` feature on the production crates;
the two-audience boundary that lets a consumer test handlers and enqueues
against Postgres alone; no production transport trait and no in-memory broker
fake; and the standing principle that every macro must have an explicit path.
`Harness` provides `connect`/`connect_with_config`/`connect_redpanda`,
`enqueue`, `relay_once`, `insert_received`, `outbox_row`, `assert_status`,
`published_on`, and the `CapturingPublisher` that point 4 calls the capturing
sender.

Not built, and deferred rather than rejected:

1. **Point 3's `tower::ServiceExt::oneshot` handler tests.** Neither
   `kafkaman-test` nor `kafkaman-sqlx` depends on `tower`, and `MessageRouter`
   is a registry rather than a `Service`. Handlers are exercised through
   `dispatch_once` / `dispatch_once_with_hooks` against Postgres instead.
   `wiki/specs/m3-durable-receive.spec.md` already records the Tower,
   `FromMessage`, and extractor ergonomics as deferred; this is the same gap
   seen from the toolkit side.
2. **Point 4's injectable `Clock`, `tick()`, `ingest(TestMessage)`, the
   `TestMessage` builders, the ephemeral-schema setup, and the
   `assert_processed` / `assert_failed_with` / `errors_len` / `attempts`
   assertion family.** The shipped surface is the smaller explicit one listed
   above. Retry eligibility is computed from the *database* clock
   (`now() + interval`), so an application-side `Clock` would not have governed
   it; time-dependent tests set `next_attempt_at` directly.
3. **Point 5's determinism contract, partially.** Schedulers are still stepped
   explicitly rather than auto-started, which is the half that matters. The
   `sleep`-free promise does not hold across the whole suite: the full-loop and
   telemetry tiers poll for convergence with real sleeps.
4. **Point 6's `#[kafkaman::test]` macro and its `kafkaman-test-macros` crate.**
   Point 8 is what makes this harmless — the explicit `Harness` path that point 6
   names as the fallback is complete, and is simply the only path.
5. **Point 7's optional `testcontainers` feature on `kafkaman-test`.** The
   feature is `redpanda`, which gates `kafkaman-rdkafka`; `testcontainers`
   itself is a dependency of the internal suites in `tests/`, so the container
   requirement is not exported to adopters at all. The intent of the point — never
   force Docker on a consumer — holds by a different mechanism than it specifies.

## Decision

1. **A dedicated `kafkaman-test` crate** (consumers add it as a **dev-dependency**),
   *not* a `testing` feature flag on the production crates. Test-only helpers,
   builders, and macros never bloat or leak into the production API surface.

2. **Two-audience principle — consumers test what's *theirs*, without Kafka.** The
   design isolates the broker to two thin edges, so a consumer can test handler
   logic, business writes, send-enqueues, and idempotency against **Postgres only**,
   **deterministically**. The broker edges are for full-loop integration tests,
   not for testing the consumer's own logic.

3. **Handler unit tests via `tower` `oneshot`.** Because a handler stack is a
   `tower::Service<Message>` ([consumption decision](message-consumption-and-handler-model.decision.md)),
   consumers test it — *including its layers* — with `tower::ServiceExt::oneshot(msg)`:
   build a `Message`, drive the stack, assert the outcome. No scheduler, no broker,
   no sleep. (This is the axum `Router`→`oneshot` story, inherited.)

4. **The `Harness` — the deterministic durable-path tester.** Provides:
   - an **ephemeral schema** + `migrate()` against a **caller-provided connection
     string** (works with any Postgres: the consumer's container, `#[sqlx::test]`,
     or a local instance);
   - `ingest(TestMessage)` to write a received row directly;
   - **`dispatch_once()` / `tick()`** — run the dispatch scheduler exactly one step;
   - a **capturing sender** that records enqueued rows so a test asserts "this
     path enqueued `OrderPlaced{..}` with key `o-1`" with no relay or broker;
   - an injectable **`Clock`** to advance time for backoff/`next_attempt_at`;
   - **row-state assertions** (`assert_processed`, `assert_failed_with`,
     `errors_len`, `attempts`).
   - Message/envelope **builders** (`TestMessage::new::<T>(payload).key(..).correlation(..)`).

5. **Determinism contract (the core promise of the toolkit): never `sleep`.** The
   harness does **not** auto-start the schedulers; tests step them explicitly via
   `dispatch_once()` and advance the injected `Clock`. This is what makes async,
   background-scheduler tests reproducible instead of flaky.

6. **`#[kafkaman::test]` macro — optional sugar over the explicit `Harness`.**
   Mirrors `#[sqlx::test]`/`#[tokio::test]`: sets up the Tokio runtime + ephemeral
   schema + `migrate()`, and **injects what the signature asks for** (`Harness`,
   `PgPool`, or both) by analyzing the test fn. Constraints:
   - **Docker-free by default:** uses a provided `DATABASE_URL` + ephemeral schema,
     like `#[sqlx::test]`. Containers/broker are **opt-in** via attribute args:
     `#[kafkaman::test]` (Postgres-only), `#[kafkaman::test(containers)]` (spin
     Postgres), `#[kafkaman::test(broker)]` (+ Redpanda full loop).
   - **Containers per test *binary*, not per test**, with schema + topic-prefix
     isolation (parallel-safe) — same rule as the library test strategy.
   - **Does NOT auto-start schedulers** — preserves the `dispatch_once()`
     determinism contract.
   - **Optional, not load-bearing:** it desugars to a plain
     `kafkaman_test::Harness::connect(url).await?`; the explicit form is always
     available for custom setups (multiple pools, pre-seeded data, macro-averse
     teams). The proc-macro lives in an internal `kafkaman-test-macros` crate,
     re-exported from `kafkaman-test`.

7. **Transport stance (middle path — respects [OQ2](messaging-scope-and-receive-model.decision.md)).**
   No production transport trait and no in-memory broker fake in v1. Fast tests are
   **Postgres-only**; full-loop tests use **real Redpanda via testcontainers**,
   behind an **optional `testcontainers` feature** on `kafkaman-test` so the
   dependency and Docker requirement are never forced. An in-memory transport (and
   the seam it implies) is deferred until the Redpanda round-trip is proven too
   slow — only *then* does it justify walking back OQ2.

8. **Standing principle: macros are opt-in sugar over explicit APIs, never
   load-bearing magic.** Applies to `#[kafkaman::test]`, `#[derive(KafkaMessage)]`,
   and `changelog!`; it is also why `#[kafkaman::handler]` was dropped in favor of
   explicit `.handler::<T>()` registration. Every macro must have a documented,
   ergonomic non-macro path.

## Why

- The architecture already isolates Kafka to thin edges, so **Postgres-only,
  deterministic tests cover ~90% of what consumers write** — the toolkit just
  makes that easy and `sleep`-free.
- **`tower` `oneshot` for handlers** is a large win we get for free from the
  consumption decision; not exposing it would waste it.
- **Docker-free default + opt-in containers** keeps the fast path usable in
  Docker-less CI while making the full-loop path one attribute away.
- **kafkaman's own suite is the toolkit's primary consumer** (dogfooding-first,
  per the [library test strategy](library-test-strategy.decision.md)). This makes
  `kafkaman-test` an **early deliverable** built alongside `kafkaman-core`/`-sqlx`
  — not a V1-hardening afterthought — and guarantees its gaps surface in our own
  tests before a consumer hits them.

## Dependencies / Assumptions

- The Postgres-only path needs only a reachable Postgres (provided URL); Docker is
  required **only** for the opt-in `containers`/`broker` tiers.
- Interops with `#[sqlx::test]` rather than replacing it: `sqlx::test` (or
  testcontainers) provides the server; the `Harness` carves the isolated schema and
  runs kafkaman's `migrate()` + bookkeeping.

## Alternatives Considered

- **A `testing` feature on the production crates** instead of a separate crate:
  rejected — leaks test helpers into the prod API and dependency graph.
- **Macro-only ergonomics** (no explicit `Harness`): rejected — violates the
  sugar-over-explicit principle and blocks custom setups.
- **An in-memory broker fake / transport trait in v1:** rejected for now — it
  walks back OQ2 for a speed gain we have not yet shown we need; revisit if the
  full-loop tier gets slow.
- **Auto-running schedulers in the harness:** rejected — reintroduces the
  nondeterminism the two-scheduler model and `dispatch_once()` exist to remove.

## Consequences / Tradeoffs Accepted

- kafkaman maintains a public test API + a proc-macro crate (ongoing cost), bought
  with consumer adoption and our own dogfooding.
- Full-loop consumer tests require Docker (opt-in), so some consumer CI runs only
  the Postgres-only tier — acceptable, mirrors our own strategy.

## Revisit When

- The retry/DLQ follow-up lands — extend the harness with backoff/DLQ assertions.
- Full-loop tests prove too slow → reconsider an in-memory transport (and the OQ2
  seam) for a fast end-to-end tier.
- A second transport/backend appears → the harness may need backend selection.
