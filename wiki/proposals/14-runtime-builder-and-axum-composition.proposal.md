# Runtime Builder and Axum Composition

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-25
- Revised: 2026-08-26 (review pass: topic-creation authority, generated-version
  collision policy, Tower scope, and telemetry provenance)
- Category: Developer experience
- Scope: Replace the example's manual kafkaman boot wiring with a small,
  role-driven runtime builder, while keeping HTTP composition and every
  host-owned concern outside the core assembly path.
- Sources:
  - examples/order/src/service.rs
  - examples/product/src/service.rs
  - examples/order/src/changelog.rs
  - examples/product/src/changelog.rs
  - examples/order/src/main.rs
  - crates/kafkaman/src/lib.rs
  - crates/kafkaman-sqlx/src/changesets.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
  - crates/kafkaman-sqlx/src/router.rs
  - crates/kafkaman-core/src/topics.rs
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - `implementation/m6-observability` worktree at `~/Documents/rust/kafkaman-m6`,
    for the telemetry section — `global::meter`, `RelayMetrics`, and
    `opentelemetry` appear nowhere under `crates/` on this branch
  - wiki/decisions/topic-convergence-and-rebuild.decision.md
  - wiki/plans/two-service-distributed-cache-example.plan.md
- Related:
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - wiki/plans/runtime-builder-and-axum-composition.plan.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/schema-and-change-management.decision.md

## Context

The two-service example now works end to end, but it exposes an adoption problem.
`examples/order/src/service.rs` (229 lines) and `examples/product/src/service.rs`
(201 lines) are doing the library's boot choreography by hand:

- resolve `kafkaman.toml` against every message descriptor the service touches;
- verify compacted topics before any loop starts;
- open the Postgres pool;
- run the service's business schema hook;
- hand-build a kafkaman changelog with `CreateOutboxTable`,
  `CreateReceivedTable`, and `CreateCacheTable`;
- run `migrate`;
- construct one outbox table, publisher, and relay task for the published type;
- construct one consumer, received table, and dispatcher task for the consumed
  type;
- compose all of that with Axum graceful shutdown and first-failure supervision.

The two files are near-identical, and `examples/product/src/service.rs`
explicitly justifies not sharing them: "an example that factors the similarity
away stops showing what a service actually has to wire up." That justification is
the tell. The duplication is no longer teaching a reader anything about their own
service; it is the shape of an API that does not exist yet.

The blessed developer UX should be closer to:

```rust
let runtime = kafkaman::RuntimeBuilder::new()
    .config(config)
    .pool(pool)
    .brokers(brokers)
    .consumer_group("order")
    .publish::<OrderSnapshot>()
    .cache::<ProductSnapshot>()
    .build()
    .await?;
```

HTTP is a second concern. The service still owns routes, state, bind address, and
domain handlers. The builder should make the kafkaman runtime small to assemble;
it should not turn kafkaman into an HTTP framework.

## Proposal

Introduce a role-driven runtime builder in the core kafkaman facade. A service
declares the contract types it publishes, caches, or handles; the builder derives
the kafkaman-owned config coverage, topic convergence, table migrations, worker
loops, and shutdown plumbing.

The core API is intentionally about runtime assembly, not HTTP:

```rust
let runtime = kafkaman::RuntimeBuilder::new()
    .config(config)
    .pool(pool)
    .brokers(brokers)
    .consumer_group("product")
    .meter(meter)
    .publish::<ProductSnapshot>()
    .handle::<OrderSnapshot>(derive_availability)
    .build()
    .await?;

runtime.run(shutdown).await?;
```

For Axum hosts, ship the composition helper as a facade feature that wires an
already-built runtime to an already-built router:

```rust
let runtime = /* as above */;
let app = build_router(AppState::from_runtime(&runtime)?);

kafkaman::axum::serve(listener, app)
    .with_runtime(runtime)
    .run(shutdown)
    .await?;
```

`kafkaman::axum` composes HTTP graceful shutdown with the runtime drain. It does
not own the router, the Tokio runtime, the bind address, the business state
model, or the shutdown signal.

### Axum ships as a facade feature, not a separate crate

The original draft proposed a `kafkaman-axum` crate. That contradicts the
facade's own stated rationale. `crates/kafkaman/src/lib.rs` gates `rdkafka`
behind a feature and re-exports it "so an application never has to name a second
kafkaman crate" — and that is for a dependency that links a C library. Axum is a
pure-Rust optional dependency, strictly lighter, so a separate crate would impose
a second version to track and a second dependency line for less isolation
benefit than `rdkafka` already gets from a feature.

`kafkaman::axum` behind an `axum` feature keeps the boundary intact — core stays
HTTP-free, the helper is just a module — while staying consistent with how the
workspace already handles a heavy optional dependency.

## Roles

Roles are repeatable. A realistic service may publish several entity snapshot
types and cache or handle several others:

```rust
RuntimeBuilder::new()
    .publish::<OrderSnapshot>()
    .publish::<InvoiceSnapshot>()
    .cache::<ProductSnapshot>()
    .cache::<CustomerSnapshot>()
    .handle::<ShipmentSnapshot>(handle_shipment);
```

Each role has concrete semantics:

- `publish::<T>()` registers `T::descriptor()`, converges the compacted topic,
  creates the outbox table, and starts the relay path for that type.
- `cache::<T>()` registers `T::descriptor()`, converges the compacted topic,
  creates the received and cache tables, starts the ingester, and starts a
  dispatcher with a no-op business handler so the cache is still transactionally
  advanced through the normal dispatch path.
- `handle::<T>(f)` is `cache::<T>()` plus an application handler that runs
  *after* the cache upsert inside the received-row transaction.
- `handle_before::<T>(f)` is `cache::<T>()` plus a handler that runs *before* the
  upsert, for the rare case that needs the entity's previous version.

Handler positions, the prohibition on suppressing the upsert, the `Ignored`
skip, and the tombstone signature constraint are settled in
`wiki/decisions/dispatch-handler-ordering.decision.md`. This proposal only
adopts them as the builder's registration surface.

The builder deduplicates descriptors across roles and rejects conflicting
registrations with an error that names the message type and the roles involved.
`handle_before::<T>` plus `handle::<T>` on one type is the single legal
two-handler registration. It is valid for one service to both publish and consume
the same type; that simply means the service owns both the outbound and inbound
tables and loops for that type, and it is also the shape that makes an
accidental self-republishing loop possible, so it warrants an explicit note in
the documentation.

### Tower layers must stay *reachable later*, and are not built now

An earlier draft of this proposal said the handler-model decision "establishes"
an axum-shaped Tower stack and specified a `.with_router(..)` hook that calls
`.layer(..)`. Both halves were wrong, and the correction matters because it
removes work from this proposal's critical path.

`MessageRouter` today exposes `new` and `handler`, and nothing else
(`crates/kafkaman-sqlx/src/router.rs`). No `layer` method exists anywhere in
`crates/`. And `wiki/decisions/message-consumption-and-handler-model.decision.md`
does not establish the stack — its *Accepted Reconciliation* says the opposite:
M3 accepts the shipped closure-based surface, and "the larger
Tower/extractor/Rx surface remains future design space; it is not required for
M3 acceptance." The layer stack is an aspiration on a decision page, not an API.

So the requirement on this work is only that roles do not foreclose it. That is
satisfied structurally: role registration produces an ordinary `MessageRouter`,
so whenever the layer surface is designed, a wrapping hook of the form

```rust
.with_router(|router: MessageRouter| /* ...layers... */ router)
```

can be added additively, and it should be a wrapping hook rather than an
either/or `.dispatch_router(router)` — the latter would force a user who wants
one timeout layer to abandon roles entirely and hand-build every handler.

Designing, shipping, or gating on that hook is out of scope here. A UX refactor
should not be blocked on an unbuilt API.

The initial implementation may run one relay per published type and one ingester
plus one dispatcher per consumed type. That topology is an implementation detail:
future loop consolidation must not change the role-level API.

## Generated Changelog

The builder generates only kafkaman-owned changesets:

- `InitSchema`;
- one outbox-table changeset per published type;
- one received-table changeset per cached or handled type;
- one cache-table changeset per cached or handled type;
- future kafkaman-owned additive upgrade changesets when a table template
  changes.

Business schema is not generated, not hooked, and not the builder's concern. See
*Host-Owned Concerns* below.

### Changeset identity comes from a per-template version, not registration order

Generated changesets must not derive numeric versions from mutable registration
order. `assert_changelog_order` requires unique, strictly ascending versions, the
history table stores the version as its primary key, and
`descriptor_changeset!`'s `checksum_material` includes the version — so a builder
that renumbered applied changesets would fail every existing database.

Identity is therefore `(role, message_type, template_version)`:

- `role` and `message_type` are already available from the role registry;
- `template_version` is a library-owned constant per table kind — outbox,
  received, cache — bumped by whoever edits that template's DDL.

The numeric version is allocated as a sparse band. Because the version is a
durable primary key in the migration history table, the allocation is normative
rather than illustrative:

```
base    = 1_000 + (stable_hash(role, message_type) mod 2**40) * 1_024
version = base + template_version
```

- **Width.** Versions are `i64`. 1024 slots per table, a maximum below `2**51`,
  and no possibility of overflow.
- **Reserved range.** Everything below `1_000` belongs to library singletons and
  hand-written changelogs. `InitSchema` is hardcoded to version `1` and must
  keep sorting first.
- **Hash.** A fixed, explicitly-versioned digest over the ASCII bytes of the
  role name and the `MessageType`. Not `DefaultHasher` — `std` makes no
  stability guarantee across releases, and this digest is part of the durable
  contract.
- **Collisions.** 2**40 bands makes a collision vanishingly unlikely and not
  impossible, so it is detected rather than assumed away. The builder sorts
  generated changesets by version and checks for a duplicate band before
  `assert_changelog_order` runs, and fails `build()` with an error naming both
  colliding `(role, message_type)` pairs. It must not reach the user as a bare
  `Error::DuplicateChangesetVersion`, which reads as a library bug.
- **Sorting.** `assert_changelog_order` validates that the *slice* is ascending,
  not merely that versions are unique, so generated changesets must be sorted
  before they are asserted.

Reordering role registrations moves none of the hash inputs, so existing
identities survive. Slots within a table remain ascending, so a template's
upgrade changeset always sorts after its create.

The invariant this relies on must be stated and asserted, not assumed: generated
changesets for distinct `(role, message_type)` pairs are mutually
order-independent. Inter-table order is arbitrary; only intra-table order
carries meaning.

The four existing alter changesets — `AddOutboxEntityKey`, `AddIdempotencyKey`,
`AddIdempotencySource`, `AddReceivedEntityKey` — are not generated and do not
get bands. They exist to catch up databases created before their columns entered
the templates in place, which is the practice `template_version` ends. Generated
schema starts from the current template at slot 0.

### Why the version cannot come from the contract crate

A version annotation on the contract type is the obvious alternative and is the
wrong input. Two different versions exist:

- the **wire schema** version of the payload, which moves when `ProductSnapshot`
  gains a field or `ProductStatus` gains a variant — owned by the contract
  author;
- the **table template** version, which moves when kafkaman edits
  `schema_sql.rs` — owned by the library.

Cache payloads are stored as JSONB, so a new contract field requires no DDL at
all. Driving changeset identity from the wire version would generate three no-op
migrations for such a change, and because `examples/contracts` is shared by both
services, one bump would move table identity in `order` and `product`
simultaneously — coupling migration timelines that are otherwise independent.
The converse also fails: when kafkaman adds a column to the cache template, the
contract version does not move, so no upgrade changeset would be generated for
the one change that needs one.

A wire schema version remains worth having for forward-compatibility
diagnostics — reporting "consumed a v3 payload with a v2 build", which
`ProductStatus::Unrecognized` currently absorbs in silence. That is a separate
proposal, belongs with message identity and header namespacing, and must not be
folded into changeset identity.

### The checksum does not cover DDL

`descriptor_changeset!`'s `checksum_material` is
`version;name;message_type;topic` — no SQL. Editing a template in place is
therefore invisible to the migration engine: existing databases keep the old
shape, fresh ones get the new shape, and the checksum matches either way.

This is not hypothetical. `create_outbox_table_sql` already contains
`idempotency_key`, `idempotency_source`, and `entity_key` — the columns that
`AddIdempotencyKey`, `AddIdempotencySource`, and `AddOutboxEntityKey` exist to
add. The template was edited in place, and those changesets exist to catch up
databases created before the edit.

An explicit `template_version` is the only mechanism that would make such an
edit visible. The discipline it enforces: never edit an existing template; add
an upgrade changeset and bump the template's version. A fresh database then
replays create-then-alter, which costs a few extra statements and is always
correct.

Adopting the builder on a database migrated by a manual changelog leaves the
manual versions in history as orphans and re-runs the generated creates, which
are `CREATE TABLE IF NOT EXISTS` and therefore harmless. kafkaman has no
released version, so no tooling is warranted; the compatibility note records the
behavior in one sentence.

Manual changelog override remains available for services that need explicit
version control, non-standard table upgrades, or a conservative migration
process. The builder is the default path, not the only path.

## Host-Owned Concerns

Everything below stays with the host, and the documentation must say so, because
a builder is exactly where a convenience shortcut would be added later. For all
but one entry the rule is absolute: the builder neither performs nor offers
them. The exception is item 8, where what the host owns is the *decision* rather
than the mechanism — `build()` does run convergence, but only ever in the mode
the host chose. Read that item carefully rather than by analogy with the others.

1. **The Tokio runtime.** The builder never calls `#[tokio::main]` and never
   creates a runtime.
2. **The connection pool.** The builder takes a `PgPool`. It does not size one,
   and it exposes no pool options. The example's `max_connections(10)` is
   currently shared silently between HTTP handlers and three background loops;
   taking a pool makes that explicit and lets a host that wants isolation pass
   the runtime a different pool from the one in its HTTP state. The builder
   documents its steady-state demand — roughly one connection per loop plus
   transients — so a host can size deliberately.
3. **Business schema.** Not generated, and not hooked. With the pool supplied by
   the host, a service already holds everything it needs to call its own
   migrator before `build()`, or after `build()` if its schema references
   kafkaman-owned tables. Offering a hook would only fix its position in the
   boot sequence and force a decision that belongs to the caller.
4. **Signal handling.** The runtime takes a shutdown token. Neither the core
   builder nor the Axum helper installs a `SIGTERM`/`SIGINT` handler, because
   that breaks any host with its own shutdown coordinator and breaks in-process
   tests. A `kafkaman::shutdown_signal()` helper may exist for a host to *call*.
5. **Process exit.** `run()` returns after drain and never exits the process.
   The host needs control back to flush its telemetry exporter after the loops
   have stopped.
6. **Telemetry pipeline.** The builder never installs a `MeterProvider`, never
   sets a global, and never configures a `tracing` subscriber. It takes a
   `Meter` explicitly; see below.
7. **Config discovery.** `build()` requires a resolved `Config`. Discovery is
   never a silent fallback. `examples/order/src/main.rs` already calls
   `Config::discover()?` in the binary and passes the result down, while
   `service.rs` treats `None` as a boot failure: "a service with no config file
   must not silently start on defaults." The builder preserves that split rather
   than re-absorbing discovery.
8. **The topic-creation decision.** The *authority* is host-owned, not the
   mechanism — an earlier draft conflated the two and contradicted itself.
   `build()` does run `converge_topics`, and `TopicMode::Create` does create
   topics, so "the builder never creates topics" would be false the moment a
   host sets `mode = "create"`. What is true, and what this proposal commits to:
   the builder executes exactly the mode the resolved config selects, never
   defaults to `create` (`TopicMode::Verify` is the `Default`, because
   application principals are routinely denied `CreateTopics` by ACL), never
   upgrades a mode, and offers no builder-level knob for creation. A service on
   the shipped default creates nothing; `examples/provision` is the thing that
   runs in `create` mode.
9. **Database creation.** Unchanged; environment provisioning owns it.
10. **Executor choice.** `into_tasks()` stays alongside `run()`, so a host can
    spawn the loops on its own runtime under its own supervision.

## Telemetry: An Explicit Meter, No Global Lookup

M6 constructs instruments inside the run loops — `RelayMetrics::new` calls
`global::meter("kafkaman")` at loop start — and an OpenTelemetry instrument binds
permanently to whichever provider is installed at that moment. The API offers no
rebinding, so an instrument created before the host installs its provider stays a
no-op for the life of that loop.

That contract is currently visible because the example spells out
`tasks.spawn(worker::run(...))`. Moving loop construction inside a builder call
would hide it, and violating it would be silent.

The builder therefore takes a `Meter` explicitly and threads it into every loop.
Loops started through the builder never call `global::meter()`, so there is no
install-order race to lose. Omitting `.meter(...)` means instruments are no-ops —
definitely off, rather than possibly bound to whatever happened to be installed.

This diverges from the OpenTelemetry convention for instrumentation libraries,
which the M6 telemetry pipeline ownership decision adopts. The divergence is
deliberate: kafkaman's loops start at times the host does not control, the API
cannot rebind, and "you did not ask for metrics" is a better failure than
"silently no-op forever." That decision needs amending on the M6 branch, with
this reasoning recorded alongside it.

## Boot Sequence

`build()` performs the same sequence the example now spells out, with the
load-bearing order preserved:

1. collect descriptors from roles and reject an empty runtime unless explicitly
   allowed;
2. validate the role registry — conflicts, duplicates, and illegal
   two-handler registrations fail here, before any I/O;
3. converge topics before the service starts any loop;
4. run the generated kafkaman migration against the supplied pool;
5. construct typed tables, publishers, consumers, and dispatch routers;
6. return a runtime with a context the host can use to build its HTTP state.

`build()` does not start loops. Loop construction — and therefore instrument
construction — happens in `run()` / `into_tasks()`. Keeping them separate leaves
the host one clear place to complete any setup that must precede a running
loop, and keeps `build()` safe to call in tests that never run the runtime.

## Boundaries

- Core kafkaman does not know about Axum, Hyper, routes, bind addresses, or HTTP
  state.
- `kafkaman::axum` may compose HTTP graceful shutdown with the runtime drain.
- The builder does not infer domain behavior from message types. A type can be
  cached without business logic, or handled with an explicit function.
- The low-level APIs stay public for tests, migrations, unusual supervision, and
  debugging.
- Everything in *Host-Owned Concerns* stays with the host.

## Alternatives Considered

### Leave the example explicit

Rejected as the blessed UX. It was valuable while proving topic convergence and
distributed caches, because tests could break individual wiring steps. As a
library example, it now teaches consumers to manually assemble internals that the
library should derive from roles. The low-level path survives as an escape hatch
and as a tested equivalence claim, not as the headline.

### Put HTTP into the core builder

Rejected. The existing runtime decision already separated background loops from
request-path integration. Core kafkaman should be usable in worker-role binaries
and non-Axum hosts; putting HTTP in the core would collapse that boundary.

### Ship Axum composition as `kafkaman-axum`

Rejected in favour of a facade feature, for the reasons above.

### Hide everything behind a macro first

Rejected for the first implementation. Macro sugar can come later, but the
builder should be ordinary Rust first so errors, tests, and user customization
stay visible.

### Take a database URL instead of a pool

Rejected. It forces pool options into the builder's surface, hides the fact that
HTTP and the loops share a pool, and makes host-side pool centralization
impossible. A URL-based convenience constructor may be added later; it is not
the primary API.

### Fall back to `global::meter()` when no meter is supplied

Rejected. It reintroduces exactly the silent-binding failure the explicit meter
exists to prevent.

### Infer business schema and handlers

Rejected. kafkaman owns the entity propagation ledger and cache tables; it does
not own the service's business schema or domain model.

### Require one publish and one cache

Rejected. Real services can publish many entity types, consume many entity
types, or publish only. The builder API must be repeatable from the start even if
the first loop topology is one task per type.

## Verification

- Refactor the two-service example so the service boot files no longer reference
  `CreateOutboxTable`, `CreateReceivedTable`, `CreateCacheTable`, `OutboxTable`,
  `ReceivedTable`, `TopicAdmin`, `converge_topics`, `RdkafkaConsumer`,
  `RdkafkaPublisher`, `JoinSet`, or direct worker loop spawning — enforced by a
  test that greps the example sources, so it cannot regress.
- Keep one service on the low-level path in a second boot module and run the
  distributed-cache suite against both, so the escape hatch is a tested
  equivalence claim rather than a file that rots.
- Add unit tests that generated changelog identity is stable when role
  registrations are reordered.
- Add role tests for multiple published types, multiple cached types, duplicate
  registrations, the legal `handle_before` + `handle` pair, and publish-plus-
  handle of the same type.
- Add a test that a post-upsert handler does not run when the cache apply
  outcome is `Ignored`.
- Keep `cargo test --workspace --all-features` and `just lint` green.
