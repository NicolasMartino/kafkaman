# Messaging Scope: Kafka-Command-First vs Durable-Execution-First

- Document Class: Proposal
- Status: Accepted
- Date: 2026-06-20
- Category: Architecture scope
- Scope: Resolves whether kafkaman pushes service commands over Kafka, supports Kafka's propagation role only, or abstracts the durable execution model above transport.
- Promoted To: wiki/decisions/messaging-scope-and-receive-model.decision.md
- Sources:
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/research-summary.md
  - raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/manifest.md
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/references/rust-kafka-outbox-ecosystem.reference.md

## The Tension

kafkaman's original statement says services should communicate **through Kafka
instead of REST**. The local reference architecture (RepForge / cqrs-fullstack,
distilled in research-summary.md §2) has moved the *other* way: its current ADRs
make authoritative mutation completion happen through a **BFF durable mutation
queue plus internal HTTP**, while Kafka is kept for **propagation** (events,
cache invalidation, projection rebuilds, cross-service fan-out).

So kafkaman's founding idea and the host architecture's current practice point
in different directions. This is not a blocker, but it must be made explicit
before crate boundaries and v1 scope are fixed.

## Options

### Option A — Kafka-command-first
kafkaman intentionally explores authoritative service **commands over Kafka**,
diverging from current RepForge practice.
- Pro: fully realizes the original "Kafka instead of REST" vision.
- Con: directly contradicts the host architecture's current ADRs; highest
  integration friction; bets on a path the host already moved away from.

### Option B — Propagation-first
kafkaman supports only the Kafka role the host already accepts: events, cache
invalidation, projection rebuilds, cross-service propagation.
- Pro: zero friction with current practice; immediately useful.
- Con: abandons the original command-transport ambition; smaller niche.

### Option C — Durable-execution-first (recommended)
kafkaman abstracts the **durable ledger + retry + handler model** so the same
core supports Kafka publication now and internal HTTP dispatch later. Transport
becomes a pluggable backend under a stable durable-execution API.
- Pro: most compatible with the host architecture while preserving the original
  Kafka goal; keeps the command-over-Kafka door open without forcing it in v1;
  matches the proposed `kafkaman-core` (transport-agnostic) + `kafkaman-rdkafka`
  (transport impl) split.
- Con: more upfront abstraction; risk of over-generalizing before one transport
  is proven.

## Recommendation

**Option C, durable-execution-first.** The research bundle concludes this is the
most compatible position: it lets kafkaman ship Kafka outbox/inbox value now,
keeps the original "commands over Kafka" ambition reachable later, and aligns
with the `core` vs `rdkafka` crate boundary already proposed in
[01-kafkaman-objectives](01-kafkaman-objectives.proposal.md).

Concretely for v1: focus on **outbox events + idempotent consumers** over Kafka,
behind a transport-agnostic core. Defer "authoritative commands over Kafka" to a
later, explicitly-scoped iteration.

## Decision Status

**Ratified 2026-06-20.** Accepted as Option C (durable-execution-first), built
Kafka-only in v1 with HTTP deferred. The receive model is fire-and-forget with
durable status; `correlation_id`/`causation_id` ride in the envelope so the
wait-for-outcome story can be added later as primitives, not core. Full rationale
and revisit conditions:
[messaging-scope-and-receive-model.decision.md](../decisions/messaging-scope-and-receive-model.decision.md).

## What Would Revisit This

- The host architecture re-adopts Kafka for authoritative commands.
- A v1 user need requires command semantics that propagation-only cannot serve.
- The transport abstraction proves to leak (the "more upfront abstraction" con
  materializes) and a single concrete transport is cheaper.
