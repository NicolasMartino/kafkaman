# Topic Convergence

- Document Class: Plan
- Status: Draft
- Date: 2026-08-24
- Category: Delivery execution
- Scope: Executes the accepted topic-convergence decision: `TopicSpec` on the
  descriptor, boot-time verify/create/off, authorized cache-origin invalidation,
  and an `examples/provision` binary that builds the demo environment in Rust.
- Sources:
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - wiki/proposals/13-topic-convergence-and-environment-provisioning.proposal.md
  - crates/kafkaman-sqlx/src/lib.rs
- Related:
  - wiki/plans/entity-first-propagation.plan.md
  - wiki/compatibility/m5-entity-first-cache-api.compat.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## What this executes

The three items left under **Deferred** in the M5 compatibility notes are one
story, not three: boot-time compact-topic validation, the topic/partition
mismatch invalidation path, and the rebuild that makes a partition-count change
recoverable. This plan lands them in dependency order and ends with the example
demonstrating the model it documents.

## Sequencing, and one dependency worth stating first

The provisioner is the visible goal, but it is **Phase 5**, because it is a thin
caller of `create` mode. Building it first would mean reimplementing topic
creation outside the library and then deleting that work. Phases 1–3 are the
provisioner.

### Phase 1 — Declaration and config, no I/O

- `CleanupPolicy` and `TopicSpec` in `kafkaman-core`. `TopicSpec::default()` is
  `compact` alone. Data only; no new dependencies.
- `MessageDescriptor.topic_spec`, defaulted by `MessageDescriptor::new` so
  existing constructions keep compiling.
- `[topics] mode` in `kafkaman-config`: `verify` (default) / `create` / `off`,
  fail-fast on an unknown value like every other config key.

Fully unit-testable with no broker. Nothing observable changes yet.

### Phase 2 — `verify`

- `AdminClient` topic-config read in `kafkaman-rdkafka`.
- A `verify` entry point called from service boot **after config resolution and
  before any loop is spawned**.
- Error variants that name the topic, the expected policy, and the found policy.
  `compact,delete` is rejected as loudly as `delete`.
- Bounded retry on an unreachable broker, then boot failure.
- Fixtures that relied on auto-creation now provision compacted topics:
  `kafkaman-test::Harness`, `tests/durable-send`, `tests/distributed-cache`.

The fixture work is the bulk of this phase and the most likely source of
surprise, since every integration test currently depends on auto-creation.

### Phase 3 — `create`

- Create missing topics to spec; still fail on an existing topic whose policy is
  wrong.
- Refuse to guess a partition count: `create` with none declared fails boot,
  naming the missing setting.
- An `CreateTopics` ACL denial reports as an authorization failure, not a generic
  broker error.

### Phase 4 — Authorized cache-origin invalidation

- Extend `classify_skipped_cache_apply`: when the incoming record's topic is the
  **declared** topic for its type and the cache's `applied_topic` /
  `applied_partition` differ, treat it as a migration — reset the guard, accept
  the new origin, and record that it happened.
- Every other origin change keeps today's behaviour: `Error::CacheOriginMismatch`,
  terminal, blast radius one row.
- The distinction must be tested in both directions; an invalidation path that
  accepts too much is worse than none.

### Phase 5 — `examples/provision`

- New binary: creates the two databases, then calls topic convergence in `create`
  mode.
- `compose.yaml` gains it as a one-shot service; `order` and `product` gate on
  `condition: service_completed_successfully`.
- `examples/initdb/10-databases.sql` is deleted.

## Out of scope

- **Cutover tooling for a rebuild.** Phase 4 lands the primitive that makes a
  rebuild survivable; a supported end-to-end procedure (dual publish, consumer
  switch ordering, dropping v1) is open question 1 on the proposal and is not
  executed here.
- **`Replay::outbox` rejection for entity types**, the third Deferred item. It
  belongs to the same story but is a separate slice.
- **Cache bootstrap and readiness typestate** (proposal 10).
- **Table provisioning.** Explicitly rejected by the decision; `migrate()` keeps
  it.
- **Changing `examples/contracts`.** It should end this plan byte-identical.

## Verification gates

Per phase, and none of them deferred to the end:

1. **Phase 1** — unit tests for `TopicSpec` defaults and `[topics] mode` parsing,
   including an unknown mode failing fast. `cargo test --workspace` green.
2. **Phase 2** — *deliberately misconfigure and confirm the failure.* Pre-create
   `products` with `cleanup.policy=delete`, boot `order` in `verify`, and require
   a boot failure naming the topic and policy. Separately, a topic configured
   `compact,delete` must also be rejected. Catching this class is the entire
   justification for the phase, so it is demonstrated, not assumed.
3. **Phase 3** — `create` with no declared partition count fails, naming the
   setting. `create` against a broker denying `CreateTopics` reports the ACL
   cause.
4. **Phase 4** — a republish of every entity to a v2 topic at a different
   partition count converges every consumer cache, with zero
   `CacheOriginMismatch` rows. Conversely, a record arriving from an
   *undeclared* topic still fails terminally.
5. **Phase 5** — `just demo` green, and Redpanda Console reports
   `cleanup.policy=compact` on both `products` and `orders` where it reports
   `delete` today.
6. **Throughout** — `just lint` clean and `cargo test --workspace --all-features`
   green. The suite stood at 153 passing before this plan.

## Evidence to record

- The Console reading before and after Phase 5, since the "before" is the
  evidence that motivated the whole plan.
- The Phase 2 and Phase 4 deliberate-break results, quoted, in the completion
  note. A validation that has never been seen to fail is not known to work.
- Whether the fixture migration in Phase 2 changed test wall-clock materially.

## Wiki pages to update on completion

- `wiki/compatibility/m5-entity-first-cache-api.compat.md` and
  `m5-entity-first-outbox-supersede.compat.md`: move boot-time topic validation
  and the topic/partition mismatch invalidation path out of **Deferred**.
- `wiki/plans/entity-first-propagation.plan.md`: its 2026-08-13 note says
  "Boot-time broker topic validation remains pending"; that stops being true.
- A new compatibility note for the `MessageDescriptor` public-field change.
- `README.md`: the compacted-topic claim becomes enforced rather than aspirational.
- `wiki/index.md`, `wiki/log.md`.

## What closes the plan

All five phases landed with their gates demonstrated, the example running on
compacted topics, and the two Deferred items struck from the M5 compatibility
notes. Status flips to `Active` at the first Phase 1 slice.
