# Library Test Strategy (testing kafkaman itself)

- Document Class: Decision
- Status: Draft
- Date: 2026-06-20
- Category: Quality and testing
- Scope: How kafkaman is tested internally — the test pyramid, infrastructure, the durability/idempotency invariants that must be covered, and CI gates. The consumer-facing test *toolkit* kafkaman ships is a separate concern (see the consumer test-tooling decision).
- Sources:
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/plans/first-poc-outbox-publisher.plan.md
- Related:
  - wiki/decisions/consumer-test-tooling.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Decision

**Dogfooding-first (the primary principle).** kafkaman's own test suite uses the
[consumer toolkit](consumer-test-tooling.decision.md) (`kafkaman-test`: the
`Harness`, `tower` `oneshot`, `dispatch_once()`, the injected `Clock`,
`#[kafkaman::test]`) **wherever a test sits at or above the toolkit's abstraction
level** — handler behavior, dispatch semantics, send capture, `migrate()`
convergence as observed through the harness, and full-loop tests. Our suite is
therefore the toolkit's largest and most demanding consumer, so any gap in the
public test API surfaces as pain in our own tests before a consumer ever hits it.

**The boundary.** Tests of the layers *beneath* the toolkit do **not** use it and
stay white-box: the `Harness`/macro internals themselves, the SQL/DDL builders,
the dedup `ON CONFLICT` query, the bounded-`errors`-ring logic, the state-machine
transitions, and the rdkafka publish/consume edges. The toolkit is *built on*
those primitives, so depending on it to test them would be circular. Rule of
thumb: **prefer the toolkit; drop to white-box only for what the toolkit is made
of.**

**Placement & build order (a consequence of dogfooding).** Because the suite
depends on `kafkaman-test`, the toolkit is an **early deliverable built alongside
`kafkaman-core`/`-sqlx`**, not a late add-on. Toolkit-using tests live in a
dedicated workspace test member so the `kafkaman-core` ⇽ `kafkaman-test`
dev-dependency does not form an awkward cycle with `kafkaman-test` →
`kafkaman-core`; pure white-box unit tests stay inline in their own crate.

1. **A three-tier pyramid** (each tier uses the toolkit where the assertion is at
   or above its abstraction, per the principle above).
   - **Unit (Docker-free):** pure logic with no I/O — envelope encode/decode, the
     outbox/received status state machines, the dedup-identity key, the bounded
     `errors` JSONB ring (never exceeds N, drops oldest), `attempts` monotonicity,
     sequential changeset-version ordering/collision detection, retry/backoff math.
   - **Integration (Postgres via testcontainers):** everything durable that needs
     no broker — `migrate()` idempotency + convergence (run twice → same state;
     each version recorded once), advisory-lock behavior under concurrent
     replicas, enqueue-in-caller-tx, the `FOR UPDATE SKIP LOCKED` claim, the
     dispatch receive-tx commit (business write + mark `Processed` atomic), and
     dedup (`INSERT … ON CONFLICT DO NOTHING` → logged no-op).
   - **Full-loop (Postgres + Redpanda via testcontainers):** the two broker edges
     and the end-to-end path — send → relay publish → topic; consume → ingest row
     → offset commit → dispatch → handler.

2. **Infrastructure: `testcontainers`-rs**, with **containers started once per test
   binary** (not per test) and **per-test logical isolation** — a unique schema +
   a unique topic prefix — so `cargo test` parallelism is safe without N container
   boots. The unit tier requires no Docker; integration and full-loop tiers do.

3. **Crash-injection gates are first-class** (carried from the PoC plan):
   crash-between-commit-and-publish (no loss), crash-after-Kafka-ack-before-mark
   (republish — documents the at-least-once duplicate window), and
   crash-during-dispatch (row left claimable → re-driven; no double-effect thanks
   to dedup). These prove the durable promises, so they are required, not optional.

4. **Property/invariant tests for the core guarantees.** The non-negotiable
   invariants get randomized coverage, not just example tests:
   - **Effective-once:** random redelivery orderings/duplicates converge to exactly
     one applied effect (dedup + per-table `UNIQUE`).
   - **No loss:** every committed send eventually appears on the topic.
   - **Bounded `errors`:** under a retry storm the ring stays ≤ N while `attempts`
     keeps counting.
   - **Idempotent migrate:** concurrent `migrate()` across replicas applies each
     version once.

5. **Determinism (no `sleep`) — an application of the dogfooding principle.**
   Scheduler and time-dependent tests (backoff, `next_attempt_at`, retention)
   advance the injected `Clock` and step `dispatch_once()` rather than sleeping.
   This keeps the suite stable and is exactly the determinism contract the
   [consumer toolkit](consumer-test-tooling.decision.md) promises consumers — so
   our hardest scheduler cases are themselves the proof that contract holds.

6. **CI gates:** `cargo check --workspace`, `clippy -D warnings`, `fmt --check`,
   and the three test tiers. The unit tier runs everywhere; integration/full-loop
   run where a Docker-compatible runtime is available. A coverage floor is tracked
   for `kafkaman-core`/`-sqlx`, but coverage % is a signal, not the gate — the
   gate is that the invariants in points 3–4 are exercised.

## Why

- The product *is* the durability/idempotency guarantees, so the test strategy is
  organized around **proving those invariants** (crash gates + property tests),
  not around line coverage.
- **Containers-per-session + logical isolation** is the difference between a suite
  people run and one they disable; per-test container boots are too slow.
- **Dogfooding-first** is the cheapest way to keep the public test surface honest:
  making our own suite the toolkit's primary consumer means its gaps are *our*
  problem first, not a consumer's. It also pins `kafkaman-test` as an early
  deliverable rather than an afterthought.

## Consequences / Tradeoffs Accepted

- Integration and full-loop tiers require Docker, so a subset of CI/dev
  environments can run only the unit tier; that is acceptable because the unit
  tier is Docker-free and the heavier tiers run in CI.
- Container-per-session sharing means tests must be disciplined about isolation
  (schema/topic prefixes) rather than assuming a pristine broker each test.

## Revisit When

- A second transport or backend appears (the full-loop tier multiplies per
  backend).
- The retry/DLQ follow-up lands — add its terminal→DLQ and backoff-timing gates.
- Suite wall-clock becomes a problem → consider an in-memory transport for a fast
  full-loop tier (cross-reference the consumer-tooling transport stance and OQ2).
