# Runtime Builder and Axum Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-26
- Category: Service assembly and HTTP composition API
- Scope: Records the new public builder surface on the `kafkaman` facade, the
  `axum` feature, generated changeset identity, and what adopting the builder
  means for a database already migrated by a hand-written changelog.
- Sources:
  - crates/kafkaman/src/runtime/builder.rs
  - crates/kafkaman/src/runtime/context.rs
  - crates/kafkaman/src/runtime/subsystems.rs
  - crates/kafkaman/src/runtime/tasks.rs
  - crates/kafkaman/src/runtime/error.rs
  - crates/kafkaman/src/axum.rs
  - crates/kafkaman-axum/src/lib.rs
  - crates/kafkaman-sqlx/src/roles.rs
  - crates/kafkaman-sqlx/src/generated_changelog.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
- Related:
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/compatibility/dispatch-handler-ordering.compat.md
  - wiki/plans/runtime-builder-and-axum-composition.plan.md

## Public API Changes

**Additive.** Nothing existing is removed, renamed, or re-gated. Every low-level
primitive stays public on its current feature terms — `migrate`,
`converge_topics`, `OutboxTable`, `ReceivedTable`, `CacheTable`, the worker
loops, the publishers, the consumers, and the test harnesses.

### New on `kafkaman`, behind the existing `rdkafka` feature

- `RuntimeBuilder`, `Runtime`, `RuntimeContext`, `RuntimeTasks`, `HandlerCtx`,
  `Subsystems`.
- `BuildError` and `RuntimeError`, both `#[non_exhaustive]` `thiserror` enums.
  This is the facade's first error type and its first `thiserror` dependency.
- `CancellationToken`, re-exported from `tokio-util`, because `Runtime::run` and
  `Runtime::into_tasks_with` take one and a caller should not have to add a
  dependency to name a type this API requires of them.

The builder is gated with the transport rather than beside it. Assembling a
runtime means constructing a publisher, a consumer, and a topic admin, so a
builder without librdkafka would be a builder that cannot build anything.

### New `axum` feature

`kafkaman::axum::{serve, Serve, RunningService}`. The feature implies `rdkafka`:
there is nothing to compose with otherwise.

Amended 2026-08-31: the separate `kafkaman-axum` crate remains the low-level
home for admin routes and correlation middleware, but its older direct
supervision items (`RuntimeTask`, `RuntimeServer`, `serve`,
`DEFAULT_DRAIN_TIMEOUT`, and `RuntimeError`) were first deprecated and then, the
same day and before the V1 tag, **removed**. Shipping a deprecation in the first
release would have meant an adopter's first encounter with kafkaman being two
supervision surfaces, one already obsolete. `kafkaman-axum` is now HTTP-only.
Through the facade, `kafkaman::axum::serve`, `RuntimeError`, and
`DEFAULT_DRAIN_TIMEOUT` are the canonical runtime composition API — the only
one, rather than the one that shadows another. See
[v1-legacy-removal](v1-legacy-removal.compat.md).

### New on `kafkaman-sqlx`

- `Role`, `RoleRegistry` — the declaration model, and `RoleRegistry::changelog`,
  which is how a caller can generate the same changelog the builder does without
  using the builder.
- `TableKind`, `band`, `band_of`, `changeset_version`, `BAND_WIDTH`,
  `RESERVED_CEILING` — the generated version scheme, public so it can be
  inspected and reasoned about rather than only trusted.
- `OUTBOX_TEMPLATE_VERSION`, `RECEIVED_TEMPLATE_VERSION`,
  `CACHE_TEMPLATE_VERSION`.
- Two `Error` variants: `ConflictingRole` and `ChangesetBandCollision`.

## Operational Data Changes

### Generated changeset versions

A builder-migrated database records changeset versions derived from
`(table kind, message type, template version)`:

```text
band    = first 40 bits of sha256("kafkaman:changeset-band:v1\0" || kind || "\0" || message_type)
version = 1_000 + band * 1_024 + template_version
```

Versions below `1_000` stay reserved for library singletons and hand-written
changelogs; `InitSchema` keeps its hardcoded `1` and sorts first. The widest
possible version is below `2^51`.

Identity keys on the **table kind**, not the registration role. `cache::<T>()`,
`handle::<T>()`, and `handle_before::<T>()` are three ways to state one schema
fact — this service consumes `T` — and differ only in what application code runs
during dispatch. Keying on the registration role would give identical DDL a
different version depending on which was written, so adding a handler to a type
already being consumed would present as a new changeset against an existing
table.

The digest is part of the durable contract. Changing the `v1` prefix, the table
kind names, or the width renumbers every generated changeset in every database:
a breaking migration change, not a refactor. A checked-in test pins the expected
versions against an independent SHA-256 computation for exactly that reason.

Band collisions are detected rather than assumed away. A collision fails
`build()` with an error naming both colliding `(kind, message type)` pairs and
the band, never a bare `DuplicateChangesetVersion`, which would read as a library
bug to the one user who ever hits it.

### Adopting the builder on an existing database

Safe, and untidy. A database migrated by a hand-written changelog keeps its
original history rows and gains the generated ones, because the version integers
differ. Every generated create is `CREATE TABLE IF NOT EXISTS`, so the second set
applies cleanly against tables that already exist and no data moves — but
`changelog_history` afterwards carries both sets, and the hand-written rows will
never match a changeset again.

Nothing needs to be done about it. It is recorded here so the extra rows are not
mistaken for corruption.

**Observed, not predicted.** `just examples demo` was run against a Postgres
volume whose `order_service` database had been migrated by the hand-written
changelog before this work. Both services booted, all seven smoke steps passed,
and `kafkaman.changelog_history` afterwards read:

```text
     version      |         name
------------------+-----------------------
                1 | init_schema
                2 | create_outbox_table
                3 | create_received_table
                4 | create_cache_table
  292728960218088 | create_outbox_table
  734279107563496 | create_cache_table
 1038071105556456 | create_received_table
```

Seven rows for four tables: the four hand-numbered ones, now permanently inert,
and the three generated ones. `init_schema` is not duplicated because its version
is hardcoded to `1` in both paths.

### `template_version` and the never-edit rule

`schema_sql.rs` now carries a template version per table kind, all at `0`, and
the standing rule that a shipped template is never edited in place. This is not
style: `checksum_material` is `version;name;message_type;topic` and covers no SQL
at all, so an in-place edit changes what a fresh database gets and changes
nothing about an existing one — the version is already in `changelog_history`,
the checksum still matches, and the migration engine reports success while the
two databases diverge permanently.

The evidence is in the tree: `create_outbox_table_sql` already declares the
columns `AddIdempotencyKey`, `AddIdempotencySource`, and `AddOutboxEntityKey`
exist to add. Those alters are the repair for three in-place edits. They are
**not** generated and remain available for hand-written changelogs.

## Behaviour Notes for Adopters

- **`build()` starts nothing.** It validates, resolves config, converges topics,
  migrates, and assembles. `into_tasks` and `run` start the loops. This keeps
  `build()` callable from tests that never run one, and keeps the point at which
  OpenTelemetry instruments would bind adjacent to a call the host writes.
- **Convergence runs in the host's configured mode.** `build()` never defaults to
  or upgrades to `create`, and exposes no builder-level knob for it.
  `TopicMode::Verify` remains the default, so a service running the shipped
  configuration creates nothing.
- **Connection demand.** One pool, shared by the loops and by whatever the host
  does with `RuntimeContext::pool`. Steady state is roughly one connection per
  running loop — one relay per published type, one ingester and one dispatcher
  per consumed type, plus a purger per published type when `[retention]` is
  configured — held only while a batch is in flight. `migrate` additionally
  requires `max_connections >= 2`, because it holds an advisory-lock connection
  alongside the changeset connection. Size the pool for the loops plus the
  request path, not for the loops alone.
- **The purger is now wired.** `[retention]` has always been parsed and never
  spawned anything. The builder starts a purger per published type when the
  section is present. It stays opt-in: absent config deletes nothing.
- **Worker-role topology is explicit.** `RuntimeBuilder::subsystems(Subsystems)`
  filters only the loops this process starts; roles still drive schema, topic
  convergence, migration, and handler installation. The default is
  `Subsystems::all()`, preserving the original builder behavior, including
  purgers when both `Subsystems::PURGE` and `[retention]` are present.
  `Subsystems::PIPELINE` is the no-purge worker preset: relay, ingest, dispatch,
  and queue metrics. Disabling `INGEST` also removes the consumer-group
  requirement, because the process never constructs a Kafka consumer.
- **Publishing and consuming one type is legal and is a loop.** A service that
  declares `publish::<T>()` and `handle::<T>` where the handler republishes `T`
  will feed itself forever. The example's `product` avoids this only because it
  consumes `OrderSnapshot` and republishes `ProductSnapshot`.
- **No `.meter(..)` yet.** The accepted decision requires the builder to take an
  explicit `Meter` and never resolve a global. That is not implemented here: the
  observability work is on an unmerged branch and still calls
  `global::meter("kafkaman")`, so threading one now would be guessing at a
  signature. Builder setters are additive, so it lands without a break — but
  until it does, a builder-started loop's telemetry behaviour is whatever that
  branch settles on, and the no-global-fallback rule is accepted and untested.
- **The Tower layer surface does not exist and is not foreclosed.** Role
  registration yields a plain `MessageRouter`, verified by a test that hand-wraps
  the built one, so a future wrapping hook needs no change to roles.

## Verification

- `crates/kafkaman/src/runtime/tests.rs` — builder validation with no database
  and no broker, including that every failure names the offending message type
  or role and the fix.
- `tests/distributed-cache/tests/runtime_builder.rs` — against real Postgres and
  Redpanda: convergence running before the migration and leaving no tables behind
  when it fails, the generated changelog creating exactly the tables the roles
  imply and nothing else, building twice being idempotent, `build()` starting no
  loops, subsystem selection starting only selected loops, an empty selector
  starting no loops, a pre-cancelled runtime draining promptly, and the router
  being an ordinary `MessageRouter`.
- `crates/kafkaman-sqlx/src/tests/{roles,generated_changelog}.rs` — conflict
  rules, order independence, pinned versions, template slot arithmetic, and a
  forced band collision driven through the real code path.
- `tests/distributed-cache` — the end-to-end suite against the builder path and
  against `examples/product/src/service_manual.rs`, which boots the same service
  from the low-level primitives.
- Feature combinations: the crate is warning-free with no features, with
  `rdkafka` alone, and with `axum`.
