# Topic Convergence

- Document Class: Plan
- Status: Completed
- Date: 2026-08-24
- Category: Delivery execution
- Scope: Executes the accepted topic-convergence decision: `TopicSpec` on the
  descriptor, boot-time verify/create/off, authorized cache-origin invalidation,
  and an `examples/provision` binary that builds the demo environment in Rust.
- Sources:
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - wiki/proposals/23-topic-convergence-and-environment-provisioning.proposal.md
  - crates/kafkaman-sqlx/src/lib.rs
- Related:
  - wiki/plans/entity-first-propagation.plan.md
  - wiki/compatibility/m5-entity-first-cache-api.compat.md
  - wiki/plans/two-service-distributed-cache-example.plan.md

## Progress

**2026-08-24 — phases 1–4 implemented.** `TopicSpec` / `CleanupPolicy` /
`TopicMode` / `reconcile` in `kafkaman-core`, `[topics] mode` in
`kafkaman-config`, `TopicAdmin` and `converge_topics` in `kafkaman-rdkafka`, and
the authorized cache-origin migration in `kafkaman-sqlx`. Workspace is green at
175 passing with clippy clean.

**Phase 4's rule was wrong on the first attempt, and an existing test caught it.**
The decision as written would have authorized an *in-place* partition change,
which is the one operation the rebuild decision exists to forbid;
`cache_apply_halts_when_an_entity_changes_partition` failed and the rule was
narrowed to topic changes only. The decision has been amended to match. This is
the second time on this workstream that an existing assertion was worth more than
the reasoning that preceded it.

**2026-08-25 — phase 5 implemented; all five phases are landed.** The
`example-provision` package (library plus a `provision` binary) creates the two
databases and converges both entity topics in `create` mode; both services call
`converge_topics` after config resolution and before the pool is opened;
`examples/initdb/10-databases.sql` is deleted and `compose.yaml` gates
`product` and `order` on `provision` with
`condition: service_completed_successfully`. `ResolvedConfig` now carries
`topics: TopicMode`, so a mistyped mode is reported with the rest of the config
rather than at the call site.

**The provisioner is the reason the services can be strict.** They verify and
refuse to start on a missing topic, which is only a workable posture because
something else creates the topic first. The two halves are one design, not a
tool plus a check.

**Phase 5's gate was demonstrated, not assumed.** Setting `mode = "off"` in
`examples/order/kafkaman.toml` made
`provisioning_precedes_boot_and_is_the_only_thing_that_creates_a_topic` fail at
its first assertion, confirming the boot check is what that test observes.
`just examples demo` passes from a clean slate and again on a re-run, and `rpk topic
describe` reports `cleanup.policy compact DYNAMIC_TOPIC_CONFIG` on both
`products` and `orders` where it reported `delete` before this workstream.

**Phase 2's deliberate-break gate passed against a real broker.** Disabling the
cleanup-policy comparison failed both live tests; so did swapping the metadata
strategy for the naive one, on `verifying a topic must not create it`. The second
is the one worth keeping: with broker auto-creation on, asking whether a topic is
compacted is itself enough to create it with the `delete` default — the check
would have manufactured the very misconfiguration it exists to find.

**Still outstanding:** the ACL-denied create path has no coverage (Redpanda's
dev-container mode has no ACLs to deny with), and the decision's bounded broker
retry at boot is not implemented — `TopicAdmin` fails on the first attempt after
a flat 10s timeout. Neither blocked phase 5: compose gates the provisioner on
`redpanda: service_healthy` and the services on the provisioner, so nothing in
this stack dials a broker that is not already serving. A deployment without that
ordering would want the retry.

## What this executes

Three things are one story, not three: boot-time compact-topic validation, the
topic/partition mismatch invalidation path, and the rebuild that makes a
partition-count change recoverable. This plan lands them in dependency order and
ends with the example demonstrating the model it documents.

Only the first is still listed under **Deferred** in the M5 compatibility notes.
The closeout (`332721a`) recorded the mismatch path as shipped — it fails as
`CacheOriginMismatch` — so this plan *supersedes* that behaviour rather than
completing a deferral, and the rebuild was never tracked as an M5 item at all.

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

**As built, three things were added that the sketch did not name.**

A *library* beside the binary, because the two-service test fixture has to build
its environment the same way compose does. `tests/distributed-cache` calls
`ensure_databases` and `provision_topics` directly, so a change to what
provisioning means reaches the test rather than passing it by.

Database-name validation. The names arrive from the environment and reach
`CREATE DATABASE "..."` as quoted identifiers, so the alphabet is a boundary
rather than a style rule — and the 63-byte cap is refused rather than truncated,
because Postgres truncates silently and the service's connection string would
then name a database that does not exist.

`provision` sits in the `services` compose profile, not the default one, so
`just examples up` keeps its promise of starting in seconds with no image build.
That flow provisions with `cargo run -p example-provision` instead, sharing the
build the developer is about to pay for anyway.

## Out of scope

- **Cutover tooling for a rebuild.** Phase 4 lands the primitive that makes a
  rebuild survivable; a supported end-to-end procedure (dual publish, consumer
  switch ordering, dropping v1) is open question 1 on the proposal and is not
  executed here.
- **A positive state-sourced republish API.** The rejection half already ships —
  `Replay::outbox` returns `Err(UnsafeOutboxReplay)` unconditionally — but the
  resync surface a rebuild would actually use does not exist, and is a separate
  slice.
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
5. **Phase 5** — `just examples demo` green, and Redpanda Console reports
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
  out of **Deferred** — but only once Phase 5 wires it into boot, since until
  then the capability exists and nothing calls it. Both notes are release records
  and are otherwise left alone.
- `wiki/specs/entity-first-propagation.spec.md`: already amended 2026-08-25 for
  the cache-origin outcome table; its topic-validation limitation gets a further
  revision when Phase 5 lands.
- `wiki/roadmaps/path-to-v1.roadmap.md`: its post-M5 deferral list names
  boot-time broker topic validation.
- A new compatibility note for the `MessageDescriptor` public-field change.
- `README.md`: the compacted-topic claim becomes enforced rather than aspirational.
- `wiki/index.md`, `wiki/log.md`.

## What closes the plan

All five phases landed with their gates demonstrated, the example running on
compacted topics, and boot-time topic validation struck from the Deferred list in
both M5 compatibility notes.

**Met 2026-08-25.** `cargo test --workspace --all-features` is green at 228
passing across 41 suites, `just lint` is clean including the rustdoc gate, the
redpanda-gated suites pass (`topic_convergence` 2, `redpanda_full_loop` 11), and
`just examples demo` passes from a clean slate and on a re-run. Both M5 compatibility
notes now strike the deferral. Status flips to `Complete`.
