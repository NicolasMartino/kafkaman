# Runtime Builder and Axum Composition

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-25
- Revised: 2026-08-26 (implementation: identity keys on table kind; meter deferred)
- Category: Developer experience
- Scope: Adopt a role-driven kafkaman runtime builder as the blessed service
  assembly surface, keep HTTP integration in an optional facade feature, and fix
  the boundary between kafkaman-owned assembly and host-owned concerns.
- Sources:
  - examples/order/src/service.rs
  - examples/product/src/service.rs
  - examples/order/src/main.rs
  - crates/kafkaman/src/lib.rs
  - crates/kafkaman-sqlx/src/changesets.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
  - crates/kafkaman-sqlx/src/router.rs
  - crates/kafkaman/Cargo.toml
  - wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - `implementation/m6-observability` worktree at `~/Documents/rust/kafkaman-m6`
    (`crates/kafkaman-worker/src/metrics.rs`), for every claim about
    `global::meter` and `RelayMetrics` — none of that code exists on this
    branch, and every telemetry statement below is sourced from there
- Related:
  - wiki/plans/runtime-builder-and-axum-composition.plan.md
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/schema-and-change-management.decision.md

## Decision

1. **A role-driven runtime builder becomes the blessed assembly API.** Service
   authors declare what they publish, cache, and handle. kafkaman derives the
   descriptors, generated kafkaman changelog, topic convergence, worker loops,
   and shutdown wiring from those roles.

2. **Core kafkaman remains HTTP-free.** The core builder returns a runtime,
   context, and spawnable or runnable tasks. It does not bind sockets, own an
   Axum router, or define HTTP state.

3. **Axum composition ships as a facade feature, not a separate crate.**
   `kafkaman::axum`, behind an `axum` feature, composes an already-built runtime
   with an already-built router and graceful shutdown. This supersedes the
   original draft's `kafkaman-axum` crate: the facade already gates `rdkafka` — a
   heavier, C-linking dependency — behind a feature specifically "so an
   application never has to name a second kafkaman crate", and a pure-Rust
   optional dependency does not warrant weaker treatment.

   This retires the crate name. `kafkaman-axum` still appears as a planned crate
   in `wiki/decisions/runtime-composition-and-topology.decision.md` and in the
   wiring sketch at `wiki/decisions/message-consumption-and-handler-model.decision.md`
   (`kafkaman_axum::serve(...)`), and as deferred scope on several M1/M2 pages.
   Both decisions carry an amendment note pointing here; the historical M1/M2
   plans and specs are left as written, because rewriting shipped-milestone
   documents to use a name chosen afterwards would falsify the record.

4. **Roles are repeatable and deduplicated.** A service may call
   `publish::<T>()`, `cache::<T>()`, `handle::<T>(f)`, and
   `handle_before::<T>(f)` many times. The builder deduplicates descriptors,
   rejects conflicting definitions before any I/O, and allows a service to
   publish and consume the same message type when that is explicit.

5. **Role semantics define the generated kafkaman schema.** `publish::<T>()`
   creates an outbox table and relay path. `cache::<T>()` creates received and
   cache tables plus ingest/dispatch paths with a no-op handler. `handle::<T>`
   and `handle_before::<T>` create the same received/cache path and add an
   application handler at the position their names state. Handler positions and
   their guarantees are owned by
   `wiki/decisions/dispatch-handler-ordering.decision.md`; the builder only
   exposes them.

6. **Generated changeset identity is `(role, message_type, template_version)`,
   where "role" means the *table kind*.**

   **Amended 2026-08-26 during implementation.** Read literally against the four
   registration roles, this clause is wrong and would have shipped a defect.
   `cache::<T>()`, `handle::<T>()`, and `handle_before::<T>()` are three ways to
   state one schema fact — this service consumes `T`, so it needs a received
   table and a cache table — and differ only in what application code runs
   during dispatch, which is not a schema fact at all. Keying identity on the
   registration role would give identical DDL a different version depending on
   which of the three the author wrote, so adding a handler to a type already
   being consumed would present as a brand-new changeset against an
   already-created table.

   The three schema roles are therefore the three table kinds — `outbox`,
   `received`, `cache` — and the four registration roles map onto them:
   `publish` to `outbox`, and any consuming role to `received` plus `cache`.
   Everything below is unchanged and applies with that reading.
   Registration order may never be a durable identity. `template_version` is a
   library-owned constant per table kind, bumped when that template's DDL
   changes; the numeric version is a sparse band derived from a stable hash of
   role and message type, with template version as the intra-table slot. Two
   supporting facts made this necessary rather than merely tidy: the migration
   engine requires unique ascending versions and includes the version in the
   checksum, and `checksum_material` does not cover the DDL at all, so an
   in-place template edit is otherwise undetectable. A wire-schema version on
   the contract type is explicitly *not* the input; it moves for reasons that
   require no DDL, and it is shared across services whose migration timelines
   are independent.

   The allocation is normative, because the version is a durable primary key in
   the migration history table and a wrong choice is unfixable in place:

   - **Width.** Versions are `i64`. The band base is
     `1_000 + (stable_hash(role, message_type) mod 2^40) * 1_024`, leaving 1024
     intra-table slots and a maximum below `2^51`. `template_version` indexes
     the slot, so a table's upgrade changesets always sort after its create.
   - **Reserved range.** Versions below `1_000` are reserved for library
     singletons and hand-written changelogs. `InitSchema` is hardcoded to `1`
     and must keep sorting first.
   - **Hash.** A fixed, explicitly-versioned digest of the ASCII bytes of the
     role name and `MessageType` — never `DefaultHasher`, whose output
     `std` does not guarantee across releases. The digest is part of the
     durable contract; changing it is a breaking migration change, not a
     refactor.
   - **Collisions.** At 2^40 bands a collision is vanishingly unlikely and
     nonetheless possible, so it is detected rather than assumed away.
     Generated changesets are sorted by version and checked for duplicate bands
     *before* `assert_changelog_order` sees them, and a collision fails
     `build()` with an error naming both colliding `(role, message_type)` pairs
     and the band. It must not surface as a bare
     `Error::DuplicateChangesetVersion`, which would look like a library bug to
     the one user who ever hits it.
   - **Ordering.** `assert_changelog_order` validates that the *slice* is
     ascending, not that the set is unique, so the builder sorts before
     asserting. Inter-table order is arbitrary and must not be relied on; only
     intra-table slot order carries meaning, and that invariant is asserted.

   The alter changesets that used to sit beside these — `AddOutboxEntityKey`,
   `AddIdempotencyKey`, `AddIdempotencySource`, `AddReceivedEntityKey` and five
   more — were *not* generated. They existed to catch up databases created
   before their columns were added to the templates in place, which is precisely
   the practice `template_version` ends. **Removed at the V1 tag** (see
   [v1-legacy-removal](../compatibility/v1-legacy-removal.compat.md)): no such
   database was ever created outside development, so the catch-up had nothing to
   catch up. Generated schema starts from the current template at slot 0 and only
   ever gains slots going forward, which is now the *only* way a kafkaman table
   changes shape.

7. **The builder wraps task management, not the runtime.** It owns the boring
   assembly now repeated in the examples: cancellation token propagation,
   named-loop construction, first-failure supervision, and drain on shutdown.
   It does not call `#[tokio::main]`, create a Tokio runtime, or require a
   specific process topology.

8. **`build()` assembles; `run()` and `into_tasks()` start loops.** Loop
   construction — and therefore OpenTelemetry instrument construction — happens
   at run, not at build. This keeps `build()` callable in tests that never start
   a loop, and keeps the telemetry ordering contract adjacent to the call the
   host already writes.

9. **The builder takes a `Meter` explicitly and never resolves a global.** Loops
   started through the builder do not call `global::meter()`. Omitting a meter
   means no-op instruments — definitely off — rather than instruments
   permanently bound to whatever provider happened to be installed. This is a
   deliberate divergence from the OpenTelemetry instrumentation-library
   convention and requires amending the M6 telemetry pipeline ownership
   decision.

   **Deferred in the first implementation, 2026-08-26.** The builder ships with
   no `.meter(..)`. `implementation/m6-observability` is unmerged and still
   constructs instruments through `global::meter("kafkaman")` inside the loops,
   so threading a meter now would mean guessing at a signature that branch has
   not settled. The decision stands as the target; what changed is only when it
   lands. It is safe to defer because a builder setter is purely additive —
   `.meter(..)` can be added without breaking a single caller — and the cost is
   named honestly: until it lands, the no-global-fallback rule is accepted and
   untested, and a builder-started loop's telemetry behaviour is whatever M6
   settles on.

10. **Host-owned concerns are enumerated and stay with the host.** The Tokio
    runtime, the `PgPool` and its sizing, business schema, signal handling,
    process exit, telemetry provider and subscriber installation, config
    discovery, *the topic-creation decision*, database creation, and executor
    choice. The builder neither performs nor offers any of them, and the list is
    normative rather than illustrative. Nine of the ten are absolute; the
    topic-creation entry is the one that needs its wording read exactly.

    "The topic-creation decision" is precise, and an earlier draft was not.
    `build()` does run `converge_topics`, and `TopicMode::Create` does create
    topics — so a builder that ran convergence while claiming it "neither
    performs nor offers" creation would be contradicting itself the moment a
    host set `mode = "create"`. What stays with the host is the *authority*: the
    builder executes exactly the mode the host's resolved config selects, never
    defaults to `create` (`TopicMode::Verify` is the `Default`), never upgrades
    a mode, and exposes no builder-level knob to create topics. A service
    running the shipped default creates nothing; `examples/provision` is what
    runs in `create` mode.

11. **Low-level primitives stay public, on their current feature terms.**
    `migrate`, `converge_topics`, `OutboxTable`, `ReceivedTable`, `CacheTable`,
    worker loops, publishers, consumers, and test harnesses remain available.
    The builder is the default service UX, not a closed framework — and the
    escape hatch is proven by running the end-to-end suite against a hand-wired
    boot module, not merely documented.

    `axum` is the only feature this work adds. The facade re-exports
    `kafkaman-sqlx` and `kafkaman-worker` unconditionally today and continues
    to; retro-fitting `sqlx` or `worker` features would break every existing
    consumer to solve a problem nobody has reported.

12. **The Tower layer surface is out of scope.** `MessageRouter` exposes `new`
    and `handler` and nothing else, and
    `wiki/decisions/message-consumption-and-handler-model.decision.md` records
    that "the larger Tower/extractor/Rx surface remains future design space".
    The builder must not foreclose it, which is a constraint on the builder's
    shape — role registration produces a `MessageRouter`, so a wrapping hook can
    be added later without disturbing roles — but it does not license building
    it here. No `.layer(..)` is designed, shipped, or gated on in this work.

## Why

The current example is strong evidence that the low-level pieces are correct and
composable. It is also evidence that the public experience is too close to the
internals. A user should not have to remember that publishing means an outbox
table plus a relay, or that caching means both a received table and a cache table
plus two loops. Those are kafkaman's invariants and should be derived from the
declared roles.

Keeping HTTP separate preserves the original runtime-topology decision. The same
builder can power an embedded Axum service, a worker-role binary, or a custom
host.

The host-owned list exists because a builder is precisely where convenience
creeps in. Each entry is something a well-meaning future change would add — a
signal handler, a `database_url` shortcut, a `tracing_subscriber` default, a
config discovery fallback — and each would take something away from the caller
that the current example deliberately leaves with them.

## Consequences

- The examples become the main developer UX proof instead of a tour of
  internals, while one service keeps a hand-wired boot module so both paths stay
  exercised.
- The builder API becomes semver-sensitive public surface and needs a
  compatibility note when it lands.
- Generated migration identity becomes a design constraint. The implementation
  must prove stability before replacing the manual changelogs.
- `template_version` introduces a standing rule for the library: never edit an
  existing table template; add an upgrade changeset and bump the version.
- First implementation can use one relay per published type and one
  ingester/dispatcher pair per consumed type. The API must not expose that
  topology as a guarantee.
- Explicit-meter injection requires a signature change in `kafkaman-worker`'s
  metrics constructors and run loops. That change lands on the M6 branch. It was
  a coordination dependency for this work until the meter was deferred; it is now
  M6's integration step instead. The constructors are `pub(crate)`, so threading
  a `Meter` is internal to `kafkaman-worker` and only `worker::run`'s public
  signature changes.
- Taking a `PgPool` makes it visible that HTTP and the loops share one pool, and
  makes separate pools expressible for the first time.
- Documentation must keep the escape hatch visible, because some users will
  need manual migrations, custom supervision, or lower-level tests.
- Two accepted decisions need amendment notes rather than rewrites:
  `runtime-composition-and-topology` (which plans a `kafkaman-axum` crate) and
  `message-consumption-and-handler-model` (whose wiring sketch calls
  `kafkaman_axum::serve`).
- `schema-and-change-management` gains a standing rule it does not currently
  carry: never edit an existing table template in place.
- `wiki/specs/entity-first-propagation.spec.md` states the current handler
  ordering as validated truth and becomes wrong the moment the reorder ships.
  It is the promotion target of the handler-ordering decision, not this one, but
  the two land together.

## Alternatives Considered

- **Keep explicit wiring as the example UX:** rejected because the example now
  repeats library invariants in application code.
- **Make `RuntimeBuilder` include HTTP:** rejected because worker-role and
  non-Axum hosts are first-class.
- **Ship `kafkaman-axum` as its own crate:** rejected as inconsistent with the
  facade's own treatment of `rdkafka`.
- **Start with macro sugar:** rejected until an ordinary Rust builder has
  stabilized the semantics.
- **Take a `database_url` and build the pool:** rejected; it drags pool options
  into the builder and removes the host's ability to centralize or separate
  pools.
- **Keep a `business_schema` hook:** rejected; with the pool supplied by the
  host, the caller can run its own migrator before or after `build()` without
  the builder fixing a position for it.
- **Fall back to `global::meter()`:** rejected; it restores the silent-binding
  failure the explicit meter prevents.
- **Derive changeset identity from a contract-side version:** rejected; the wire
  schema and the table template move for unrelated reasons.
- **Have a central provisioner create tables:** rejected by the topic
  convergence decision. Databases and topics are environment; tables are
  service-owned schema converged by each service.
- **Require one publish and one cache role:** rejected because multiple entity
  types are normal service shape.
- **Forbid the builder from running convergence in `create` mode:** rejected.
  It would force a host that legitimately wants create-on-boot — a single-node
  development stack, a test fixture — back onto the low-level path for a policy
  the config already expresses, and it would make `build()` behave differently
  from the same convergence call written by hand.
- **Ship a Tower layer surface alongside the builder:** rejected. It is
  unbuilt future design space in its own decision, and pulling it in would put
  an unshipped API on the critical path of a UX refactor.

## Revisit When

- Another HTTP framework needs the same blessed integration as Axum.
- A service needs per-role process isolation that the builder's task model makes
  awkward.
- Generated changelog identity cannot be made stable without weakening the
  migration engine's existing guarantees.
- The OpenTelemetry API gains instrument rebinding, which would remove the
  reason for rejecting a global-meter fallback.
- Cache bootstrap/readiness introduces startup phases that require a different
  runtime context shape.
- The Tower/extractor surface is actually designed, at which point the builder
  needs a wrapping hook rather than a replacement setter.
- A generated-version band collision is ever observed in the wild, which would
  mean the hash width is wrong rather than the scheme.
