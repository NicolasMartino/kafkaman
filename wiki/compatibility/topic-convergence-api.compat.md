# Topic Convergence API Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-24
- Revised: 2026-08-25 (phase 5: boot wiring and `examples/provision`)
- Category: Topic configuration and cache-origin API compatibility
- Scope: Records the public API and runtime behaviour changes from implementing
  boot-time topic convergence, the authorized cache-origin invalidation path,
  and the environment provisioner that the boot check depends on (phases 1–5 of
  the topic-convergence plan).
- Sources:
  - crates/kafkaman-core/src/topics.rs
  - crates/kafkaman-rdkafka/src/topics.rs
  - crates/kafkaman-sqlx/src/resolved_config.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - examples/provision/src/lib.rs
- Related:
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - wiki/plans/topic-convergence.plan.md

## Public API Changes

**Breaking.**

- `MessageDescriptor` gains a public field, `topic_spec: TopicSpec`. Code that
  constructs the struct with a literal no longer compiles.
  `MessageDescriptor::new` is unchanged and defaults the field, which is the
  supported constructor and the one every in-tree caller uses.
  `MessageDescriptor::with_topic_spec` overrides it. The field carries
  `#[serde(default)]`, so descriptors serialized before this change still
  deserialize.
- `CacheApplyOutcome` gains a variant, `Migrated`. Exhaustive matches on it
  no longer compile.

**Additive.**

- `kafkaman_core::topics`, re-exported at the crate root and therefore through
  the `kafkaman` facade: `TopicSpec`, `CleanupPolicy`, `ObservedTopic`,
  `PartitionDrift`, `TopicMode`, `TopicAction`, `TopicOutcome`, and `reconcile`.
- `kafkaman_core::Error` gains `InvalidTopicSpec`, `TopicPolicyMismatch`,
  `TopicMissing`, and `TopicPartitionsUndeclared`.
- `kafkaman_config`: a `[topics]` section with `mode`, and `TopicsSection`.
  `TopicMode` is re-used from `kafkaman-core` rather than redeclared, matching
  how `RelayConfig` and `PurgeConfig` already live there.
- `kafkaman_rdkafka`: `TopicAdmin` and `converge_topics`, plus an `Error::Core`
  and `Error::TopicAdmin` variant.
- **Added 2026-08-25.** `ResolvedConfig` gains a public field, `topics:
  TopicMode`, and a `with_topics` builder. Additive rather than breaking: the
  struct has a private `messages` field and so was never constructible by
  literal. `from_config` resolves `[topics] mode` alongside every other section,
  so an unknown mode is reported in the same error as the rest of the config
  rather than surfacing later at the call site.

## Runtime Behavior Changes

- **`[topics]` absent now means `verify`, not "skip".** Opting out is
  `mode = "off"`, which warns at boot.
- **Revised 2026-08-25: the check is now wired into boot.** Both example
  services call `converge_topics(&admin, cfg.topics, cfg.messages())` after
  config resolution and before the pool is opened, so a missing or wrongly
  configured topic fails the process instead of being discovered later. The
  capability is still opt-in for a library consumer — kafkaman does not call it
  on anyone's behalf — but it is no longer a capability nothing exercises.
- **A verified deployment now needs its topics provisioned first.** Since the
  check refuses to start on an absent topic and will not let the broker
  auto-create one, something must create them. `examples/provision` is that
  something for the example stack; a real deployment provisions out of band and
  keeps `mode = "verify"`.
- **`create` never repairs an existing topic.** A topic whose `cleanup.policy` is
  wrong fails boot in every mode except `off`; `create` only fills in a topic
  that is absent.
- **Amended 2026-08-31: `create` verifies what it just created.** After
  `CreateTopics` returns, kafkaman observes the topic again and applies the
  `verify` rules before boot continues, so broker-side create drift fails at the
  same boundary as any pre-existing mismatch.
- **`create` refuses to guess a partition count**, failing with
  `TopicPartitionsUndeclared` rather than defaulting.
- **A cache-origin change across topics is no longer always terminal.** When a
  record arrives on the topic its message type declares and the cache holds a
  different topic, the guard is reset and the new origin adopted
  (`CacheApplyOutcome::Migrated`, logged at `info`). A same-topic partition move
  is still terminal, and a record from a topic the cache has already migrated off
  is now ignored as stale rather than failed.
- **Metadata reads cannot auto-create a topic.** `TopicAdmin` sets
  `allow.auto.create.topics=false` and fetches full cluster metadata rather than
  naming a topic on the wire, because a metadata request that names a topic is
  itself enough to make a broker create it — with the `delete` policy this code
  exists to detect.

## Verified

- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features` — 177 passing, 28 suites, exit 0.
- `cache_apply_halts_when_an_entity_changes_partition` still passes unchanged,
  which is what caught the first, too-permissive form of the invalidation rule.
- New cache-origin tests:
  `a_rebuilt_topic_migrates_a_cache_stranded_on_the_retired_one`,
  `a_straggler_from_a_retired_topic_is_dropped_quietly`,
  `an_origin_change_to_an_undeclared_topic_still_fails`.
- New live-broker tests in `tests/durable-send/tests/topic_convergence.rs`
  (`--features redpanda`), covering a missing topic, `delete`, `compact,delete`,
  a correct topic, `off`, create-refuses-to-guess, create-is-idempotent,
  create-does-not-repair, and partition drift warning rather than failing.

### Both live-broker gates were demonstrated, not assumed

Disabling the cleanup-policy comparison in `TopicSpec::check` failed both broker
tests (`delete retention must fail boot: []`).

Replacing the metadata strategy with the naive one — `allow.auto.create.topics`
left on and `fetch_metadata(Some(topic))` — failed on
`verifying a topic must not create it`. That is the subtle bug this module is
shaped around: on a broker with auto-creation enabled, *asking whether a topic is
compacted is itself enough to create it with the `delete` default*, manufacturing
the exact misconfiguration the check exists to detect. The guard is load-bearing
and is now known to be.

### Phase 5's gate, also demonstrated

Setting `mode = "off"` in `examples/order/kafkaman.toml` made
`provisioning_precedes_boot_and_is_the_only_thing_that_creates_a_topic` fail at
`a service must not start against a broker with no entity topics`. The boot
check is what that test observes, not an incidental failure on the way to it.

Against the running stack, `rpk topic describe` reports
`cleanup.policy compact DYNAMIC_TOPIC_CONFIG` on both `products` and `orders`,
where before this workstream it reported `delete`. `DYNAMIC_TOPIC_CONFIG` is the
part worth reading: the value was set deliberately, not inherited from a broker
default that happened to agree.

## Deferred

- An ACL-denied `CreateTopics` path. `TopicAdmin::admin_error` translates
  `TopicAuthorizationFailed` into an explanatory message, but Redpanda's
  dev-container mode has no ACLs, so nothing exercises it.
- Bounded broker retry at boot. The decision calls for it; `TopicAdmin` currently
  applies a flat 10s admin timeout and fails on the first attempt.
- Cutover tooling for a rebuild, and a positive state-sourced republish API. The
  rejection half already ships (`Replay::outbox` is unconditionally rejected); the
  resync surface a rebuild would use does not exist.
