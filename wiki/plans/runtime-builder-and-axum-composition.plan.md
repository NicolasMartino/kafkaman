# Runtime Builder and Axum Composition Implementation

- Document Class: Plan
- Status: Completed
- Date: 2026-08-25
- Revised: 2026-08-26 (review pass: `Migrated`, tombstone reopened, Tower
  descoped, collision and savepoint gates added — the reopened tombstone question
  was then settled by Phase 0 the same day; see *Resolved Design Questions* 7)
- Completed: 2026-08-26 (all seven phases; see *Outcome* below)
- Category: Delivery execution
- Scope: Implement the accepted runtime-builder and dispatch-handler-ordering
  decisions, and refactor the two-service example onto the new developer UX
  while keeping a hand-wired boot path under test.
- Sources:
  - wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - examples/order/src/service.rs
  - examples/product/src/service.rs
  - examples/order/src/lib.rs
  - examples/product/src/lib.rs
  - examples/order/src/changelog.rs
  - examples/product/src/changelog.rs
  - crates/kafkaman-sqlx/src/changelog.rs
  - crates/kafkaman-sqlx/src/changesets.rs
  - crates/kafkaman-sqlx/src/schema_sql.rs
  - crates/kafkaman-sqlx/src/dispatch_cache.rs
  - crates/kafkaman-sqlx/src/migration_runner.rs
  - crates/kafkaman-sqlx/src/router.rs
  - crates/kafkaman-sqlx/src/dispatch.rs
  - crates/kafkaman/Cargo.toml
  - tests/distributed-cache/src/lib.rs
- Related:
  - wiki/plans/two-service-distributed-cache-example.plan.md
  - wiki/plans/topic-convergence.plan.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/missing-handler-dispatch-policy.decision.md
  - wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
  - wiki/specs/entity-first-propagation.spec.md
  - wiki/compatibility/topic-convergence-api.compat.md

## Outcome

All seven phases shipped on 2026-08-26. Boot files: `examples/order/src/service.rs`
229 → 51 lines, `examples/product/src/service.rs` 201 → 55 — both under the 60-line
target, with the option struct and the running handle moved to a sibling `boot.rs`
so each file is boot logic and nothing else. Every listed gate passes, and
`tests/distributed-cache` runs the lifecycle against the builder path and against
`examples/product/src/service_manual.rs`.

Three things implementation settled differently from this plan, all recorded in
`wiki/log.md` and in the compatibility notes:

1. **Generated identity keys on the *table kind*, not the registration role.**
   Taken literally, the plan's `(role, message_type, template_version)` would give
   identical DDL a different version depending on whether the author wrote
   `cache::<T>()` or `handle::<T>()`. Amended onto the decision.
2. **`handle_before` gets no example.** The role mapping below assigns `product` a
   pre-image hook that skips the recompute when fulfilled-ness has not changed.
   That would have hung the end-to-end suite: `two_service_cache.rs` waits for the
   product cache offset to advance after an order is *placed*, and that advance is
   `product`'s republish. The republish is what makes propagation observable, so
   suppressing it deletes the signal a consumer waits on. This plan's own fallback
   was taken — focused tests in
   `tests/durable-send/tests/entity_first_propagation/dispatch_ordering.rs` cover
   the skip — and the finding is a sharper statement of the "cost guard, not
   correctness guard" note than the plan had.
3. **`.meter(..)` is deferred**, and `HandlerCtx` moved forward from Phase 4 into
   Phase 3 rather than shipping a signature that would be rewritten a commit later.

One item was added beyond the plan: the builder spawns a purger when `[retention]`
is configured. That section has always been parsed by `kafkaman-config` and never
spawned anything, so a blessed path that ignored it would have made a documented
config section silently inert.

### Known follow-up: `examples/order/src/boot.rs` exists for a line count

`examples/order/src/boot.rs` holds 34 lines — a type alias, a re-export, and
`ServiceOptions` — and was created solely to push `service.rs` from 73 lines to
51 so it would clear this plan's "under 60 lines" gate. That was gaming a metric,
and it is recorded here rather than in a backlog because this plan is what
created it.

It should be folded back into `examples/order/src/service.rs` (~83 lines), and
the line-count gate retired. `examples/product/src/boot.rs` is different and
should stay: 148 lines of `BootMode`, the `RunningService` enum over both boot
paths, and `ManualService`, all of which two boot paths genuinely need to share.

The gate was a poor proxy in any case. The property it stood for — boot code
declares roles instead of assembling internals — is directly asserted by
`tests/distributed-cache/tests/boot_surface.rs`, which greps the blessed boot
files for kafkaman internals and is not satisfied by moving a struct to a
sibling module.

## What this executes

**Everything from here down is the as-planned record, written before
implementation and left in the future tense deliberately. Where implementation
diverged, *Outcome* above is authoritative.**

This plan turns the manual service wiring exposed by the two-service example
into a builder API that services can actually copy. The target observable result
is that `examples/order` and `examples/product` still prove the same lifecycle,
but their boot code declares roles rather than assembling kafkaman internals —
and that a hand-wired boot module still produces an observably identical runtime.

The expected shape is:

```rust
let runtime = kafkaman::RuntimeBuilder::new()
    .config(config)
    .pool(pool)
    .brokers(brokers)
    .consumer_group("order")
    .meter(meter)
    .publish::<OrderSnapshot>()
    .cache::<ProductSnapshot>()
    .build()
    .await?;

let app = build_router(AppState::from_runtime(&runtime)?);

kafkaman::axum::serve(listener, app)
    .with_runtime(runtime)
    .run(shutdown)
    .await?;
```

The code above is illustrative, not an API freeze. The proof is behavioral:
manual changelog construction and manual loop spawning disappear from the blessed
path while the distributed-cache test stays green against both boot paths.

## Resolved Design Questions

Items 1-4 were the first draft's explicit *Open Questions* and are now settled.
Items 5-6 were never open questions — they are decisions the review added — and
they are listed here because implementation needs them in the same place, not
because they were ever unresolved. Item 7 was the one thing still genuinely
undecided when this plan was written; Phase 0 settled it on 2026-08-26 and the
item now records that outcome rather than the open question.

1. **Name.** `RuntimeBuilder`, producing `kafkaman::Runtime`. `ServiceBuilder`
   collides with `tower::ServiceBuilder` in a Tower-adjacent codebase, which is
   worse than the Tokio-runtime ambiguity. Always path-qualify in docs and
   examples.
2. **Workspace sequencing.** No new crate. `kafkaman::axum` behind an `axum`
   feature on the facade, landing after the core runtime context shape is real
   (Phase 4).
3. **Generated version allocation.** Sparse band:
   `base = stable_hash(role, message_type)` scaled, plus `template_version` as
   the intra-table slot. Settled in the decision; Phase 1 implements and tests
   it.
4. **Pool ownership.** The builder takes a `PgPool`. No `database_url`, no pool
   options, no business-schema hook.
5. **Meter.** Explicit `.meter(...)`, no global fallback. Omitted means no-op.
6. **Handler positions.** `handle` is post-upsert, `handle_before` is pre-upsert,
   neither may suppress the upsert, `handle_before` may skip the post-upsert
   handler, and a post-upsert handler is skipped on `CacheApplyOutcome::Ignored`
   (the variant is `Migrated`, not `Adopted`, for the third case). The
   `MissingHandler` lookup stays ahead of the upsert and is unchanged.
7. **The tombstone representation — settled by Phase 0, 2026-08-26.** The
   handler-ordering decision requires the signature to be able to carry a
   tombstone before publication and deliberately did *not* choose the shape — an
   earlier draft of that decision claimed this plan had settled it, which was
   false in both directions. Phase 0 weighed
   `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`'s direction —
   the deletion flag travels in `ReceivedMeta` rather than changing the shape of
   `P`, "which avoids forcing every payload type into an enum wrapper and keeps
   the M3 handler signature stable" — against an `Option<T>`-style payload, and
   kept the former. `ReceivedMeta` is now `#[non_exhaustive]` and carries a
   reserved `deleted: bool` read through `is_deleted()`; the outcome is recorded
   on proposal 07.

## In Scope

- Core role model and builder API, including `handle_before`.
- Dispatch handler reordering in `dispatch_once` and the `Ignored` skip.
- Generated kafkaman changelog with `template_version` identity.
- Config resolution and topic convergence inside `build()`.
- Runtime context exposing the pool, resolved config, typed table accessors, and
  send/dispatch handles needed by the host.
- Runtime supervision, cancellation, first-failure reporting, and drain.
- Explicit meter threading into every loop.
- `kafkaman::axum` composition behind a feature.
- Refactoring `examples/order` and `examples/product` to use the builder, plus a
  hand-wired `service_manual.rs` in `examples/product`.
- Typed `thiserror` error for the builder.

## Out of Scope

- Creating databases, topics, or other environment resources.
- Creating business tables or replacing service-owned migrations.
- Implementing tombstones — only the handler signature must accommodate them.
- Wire schema versioning on contract types.
- Cache bootstrap/readiness typestate.
- Rebuild cutover tooling and state-sourced republish API.
- Macro sugar over the builder.
- The Tower/extractor layer surface on `MessageRouter`. It does not exist
  (`router.rs` has `new` and `handler`), the handler-model decision calls it
  "future design space", and this plan must not be blocked on it. Roles must
  leave it addable; that is a shape constraint, not a deliverable.
- A standalone generic kafkaman daemon.
- Restoring or replacing `apps/axum-outbox`, which this branch removed.

## Phase 0 - API Target and Failure Spec

- Write the desired order/product boot code first, either as compile-only tests,
  doc tests, or commented target snippets in a focused test module.
- List the exact symbols that should disappear from the examples:
  `CreateOutboxTable`, `CreateReceivedTable`, `CreateCacheTable`,
  `OutboxTable`, `ReceivedTable`, `TopicAdmin`, `converge_topics`,
  `RdkafkaConsumer`, `RdkafkaPublisher`, direct `worker::run*`, and `JoinSet`.
  Encode the list as a test that greps the example sources so it cannot regress.
- Record the current boot-file sizes as the baseline to beat: `service.rs` is
  229 lines in `order` and 201 in `product`. Target under 60 lines each.
- Decide the handler signature, including how a future tombstone is represented,
  before any handler code is written. This is the most semver-sensitive surface
  in the plan, and it is genuinely undecided — see Resolved Design Questions
  item 7. Evaluate proposal 07's `ReceivedMeta` flag against an `Option<T>`
  payload, pick one, and write the outcome back onto proposal 07 so the decision
  does not live only in a plan that will later be marked Completed.
- Preserve the escape hatch by keeping the low-level API exercised, which
  Phase 5 turns into a full second boot path.

Gate: the target API can express both current services, including the product
service's `handle::<OrderSnapshot>` path that republishes availability, and the
handler signature can represent an absent entity.

## Phase 1 - Roles and Generated Changelog

- Add a `RoleRegistry` or equivalent internal model with `publish`, `cache`,
  `handle`, and `handle_before` roles.
- Deduplicate descriptors and reject conflicts with errors that name the message
  type and roles involved. `handle_before::<T>` + `handle::<T>` is the one legal
  pair.
- Add a `template_version` constant per table kind next to its DDL in
  `schema_sql.rs`, with a comment stating the standing rule: never edit an
  existing template; add an upgrade changeset and bump the version.
- Generate kafkaman-owned changesets from roles using the sparse-band scheme.
- Assert the order-independence invariant for generated changesets across
  distinct `(role, message_type)` pairs, rather than relying on it silently.
- Sort generated changesets by version before `assert_changelog_order` sees
  them: it validates that the *slice* is ascending, not merely that versions are
  unique.
- Detect band collisions explicitly and fail with an error naming both colliding
  `(role, message_type)` pairs. A bare `Error::DuplicateChangesetVersion` reads
  as a library bug to the one user who ever hits it.

Gates:

- generated changelog for the current order service equals the manual semantic
  set: `InitSchema`, order outbox, product received, product cache;
- generated changelog for the current product service equals the manual semantic
  set: `InitSchema`, product outbox, order received, order cache;
- role registration order changes do not change existing generated identities or
  checksums;
- bumping a `template_version` produces a new changeset that sorts after that
  table's create and leaves every other generated identity untouched;
- duplicate and conflicting registrations fail before any database or broker I/O;
- `handle_before` + `handle` on one type is accepted; every other duplicate is
  rejected;
- generated versions never land below the reserved `1_000` boundary and never
  collide with `InitSchema`'s hardcoded version `1`;
- a forced band collision — injected by stubbing the hash — fails `build()` with
  an error naming both message types, not with a raw duplicate-version error;
- the hash is stable across processes and releases, which rules out
  `DefaultHasher`; a checked-in fixture pins the expected versions for a known
  role set.

## Phase 2 - Dispatch Handler Ordering

Independent of the builder and landable first. It changes `dispatch_once` for
low-level users too, so it carries its own compatibility note.

- Move the application handler to run after `upsert_cache_from_received`.
- Move `create_dispatch_handler_savepoint` so it opens *before* the upsert and
  covers both hooks. This is the one part of the reorder that is not
  mechanically safe: leaving the savepoint where it is would let a failed
  post-upsert handler commit an advanced cache row, after which the retry yields
  `Ignored` and the handler is skipped permanently.
- Leave the `MissingHandler` router lookup where it is, ahead of the upsert.
  An unregistered type must keep parking its row without advancing the cache.
- Add the `handle_before` position ahead of the upsert.
- Give `handle_before` a control-flow return that may skip the post-upsert
  handler and may never skip the upsert.
- Skip the post-upsert handler when the outcome is `CacheApplyOutcome::Ignored`;
  run it for `Applied` and `Migrated`. (`Migrated` is the real variant name in
  `crates/kafkaman-sqlx/src/dispatch_cache.rs`. Earlier drafts of this plan and
  of the decision said `Adopted`, which is prose from the log line, not an
  identifier, and does not compile.)
- Simplify `apply_order_snapshot`: drop the `entity_key <> $3` exclusion and the
  `this_order` add-back, and replace the "exclusion that is not obvious" doc
  section with a short note that the ordering is deliberate.

Gates:

- availability still converges in the distributed-cache test after the
  simplification;
- a replayed or out-of-order `OrderSnapshot` produces `Ignored` and the
  post-upsert handler does not run;
- a `handle_before` that returns skip does not run the post-upsert handler and
  does not prevent the cache row from advancing;
- no API allows suppressing the upsert;
- handler failure still rolls back upsert, handler writes, and enqueue together
  — asserted by failing a post-upsert handler and then reading the cache row,
  not by inspecting the savepoint code;
- an unregistered message type still parks its received row `Retryable` and
  still leaves the cache row untouched.

This phase is landable ahead of the builder, but not landable *alone*:
`examples/product/src/lib.rs` compensates for the current ordering with the
`entity_key <> $3` exclusion and the `this_order` add-back, and both become
double-counting bugs the moment the reorder lands. The example simplification
below ships in the same commit as the reorder, not in Phase 5.

## Phase 3 - Core Runtime Builder

- Add `RuntimeBuilder` to the kafkaman facade. `axum` is the only feature this
  work adds: the facade re-exports `kafkaman-sqlx` and `kafkaman-worker`
  unconditionally today, and retro-fitting `sqlx` or `worker` features would
  break existing consumers to solve a problem nobody has reported.
- Require an explicit `Config`; never fall back to discovery.
- Accept `PgPool`, broker list, consumer group, optional `Meter`.
- `build()`: validate roles, converge topics, run the generated migration,
  construct typed tables, publishers, consumers, and dispatch routers, and
  return a runtime plus context.
- `run(shutdown)` / `into_tasks()`: construct loops and instruments, supervise,
  report first failure, drain, and return — never exit the process.
- Thread the `Meter` into every loop so no builder-started loop calls
  `global::meter()`.
- Add a typed `thiserror` error for `build()`, consistent with the workspace's
  existing use of `thiserror` in six of its seven crates. The exception is
  `crates/kafkaman` itself, which has no error type and therefore no `thiserror`
  dependency yet — so this adds one, rather than following one. Prefer an
  options/params struct over positional arguments on the low-level loop
  signatures so future telemetry knobs do not each add a parameter.

Gates:

- unit tests for builder validation that do not require Docker;
- Postgres-backed tests for generated migrations;
- Redpanda-backed tests for topic convergence being invoked by `build()`;
- pre-cancelled shutdown does not publish, ingest, or dispatch a batch before
  noticing cancellation;
- `build()` starts no loops — provable by building a runtime and asserting no
  task is running and no instrument has been constructed;
- every `build()` failure names the offending message type or role *and* the
  fix, checked by asserting on message text, not just variant;
- `run()` returns after drain rather than exiting.

## Phase 4 - Handler Context, Cache Ergonomics, and Axum

- Define the handler context: typed access to cache tables, resolved config, and
  consume-then-produce through the same received transaction using existing
  `enqueue_on_connection` semantics.
- Make `cache::<T>()` install the no-op handler needed for cache-only consumers.
- Keep handler errors classified through the existing receive failure policy.
- Keep role registration producing an ordinary `MessageRouter`, so a wrapping
  `.with_router(|MessageRouter| -> MessageRouter)` hook stays addable later. Do
  not build it now: `router.rs` has only `new` and `handler`, there is no
  `.layer` anywhere in `crates/`, and the handler-model decision explicitly
  leaves that surface as future design space.
- Add `kafkaman::axum` behind an `axum` feature:
  `kafkaman::axum::serve(listener, app).with_runtime(runtime).run(shutdown)`.
- Compose Axum graceful shutdown with the kafkaman drain under the host's
  shutdown token. Install no signal handler.
- Preserve host ownership of the router, routes, state, and bind address, and
  surface the bound address for tests that bind to port `0`.

Gates:

- product availability derivation still republishes in the same transaction as
  the order dispatch;
- cache-only `ProductSnapshot` consumption in the order service advances the
  product cache without a custom handler;
- missing handler cannot occur for a builder-registered `cache::<T>()` role,
  because the role installs a no-op handler;
- role registration yields a plain `MessageRouter`, verified by a test that
  hand-wraps the built router — the structural proof that a future layer hook
  needs no change to roles;
- order and product service tests can start in-process on ephemeral ports;
- first kafkaman loop failure causes the Axum server to stop accepting traffic;
- core `RuntimeBuilder` remains usable with the `axum` feature disabled.

## Phase 5 - Refactor the Examples

### Role mapping

The two services have exactly two consumer slots between them, so not every
handler path gets an example. The assignment below is deliberate, and the
reasoning belongs in the example docs.

| Path | Where | Motivation |
| --- | --- | --- |
| `publish::<T>()` | both services | unchanged |
| `cache::<T>()` | `order` consumes `ProductSnapshot` | the most common role; kept deliberately uncomplicated as the headline simple case |
| `handle::<T>` post-upsert | `product` consumes `OrderSnapshot` | availability derivation; the reorder deletes the exclusion and the add-back |
| `handle_before::<T>` + `handle::<T>` | `product`, same type | pre-image tells whether the order's fulfilled-ness changed; when it did not, skip the `SUM` recompute |
| `Ignored` skip | test only | out-of-order snapshot asserts the handler did not re-run |
| tombstones | none | not implemented; the example models withdrawal as `ProductStatus::Discontinued` rather than a delete, which is why it cannot demonstrate them |
| role multiplicity, conflicts, self-loops | unit tests only | resisting example inflation is a conscious choice, not an oversight |

Record honestly in the example docs that in this domain the pre-image is a cost
guard and not a correctness guard: every candidate use is expressible
post-upsert, idempotently, at higher cost. That is what tells a reader
`handle_before` is the rare choice.

If the `handle_before` control-flow return slips out of the first
implementation, cover it with a focused test instead and leave the examples at
pure-cache plus post-upsert derivation.

### Work

- Delete `examples/order/src/changelog.rs` and
  `examples/product/src/changelog.rs`.
- Shrink both `service.rs` files to option parsing, builder configuration,
  router construction, and Axum composition.
- Add `examples/product/src/service_manual.rs`: the hand-wired boot path, with
  the identical `start(ServiceOptions) -> RunningService` signature. Product
  rather than order, because it exercises the hard path — `handle`,
  consume-then-produce, and the availability derivation — whereas order's
  `cache::<T>()` is trivially equivalent.
- Move the load-bearing comments currently in `service.rs` — why partition drift
  warns instead of failing, "supervision, not cleanup" — into library
  documentation. They must not evaporate with the code they annotate.
- Restate, do not transplant, the convergence-ordering comment. It currently
  reads "Topic convergence, before the pool is opened and before any loop is
  spawned", and the first half stops being true when the host opens the pool and
  hands it to `build()`. The load-bearing claim is convergence *before any loop
  starts* — boot is the last moment refusing to start is cheap. Document that,
  and drop the pool ordering.
- Parameterize `Services::start(cluster)` in `tests/distributed-cache/src/lib.rs`
  over boot mode and run the suite against both.
- Keep existing Swagger endpoints and the example README current.
- Preserve the deliberate-break posture: a missing role should fail loudly and
  observably.

Gates:

- `tests/distributed-cache` passes against the builder path;
- `tests/distributed-cache` passes against `service_manual.rs`, proving the
  builder produces an observably identical runtime;
- the symbol-absence test from Phase 0 passes on the builder path and is not
  applied to `service_manual.rs`;
- both `service.rs` files are under 60 lines;
- `just examples demo` still creates a product, rejects over-ordering, accepts a
  valid order, fulfills it, and shows converged state;
- Swagger still shows `GET /products`, `GET /orders`, and the create/order
  workflow.

If running the full suite twice proves too slow against real containers, reduce
the manual-mode run to a single boot-plus-round-trip smoke case rather than
dropping it.

## Phase 6 - Docs, Compatibility, and Closeout

- Add a compatibility note for the new public builder and Axum surface.
- Add a compatibility note for the dispatch handler reordering, which affects
  low-level users independently of the builder.
- Record in one sentence that adopting the builder on a database migrated by a
  manual changelog leaves orphan history rows and re-runs idempotent creates.
- Update README and example docs to show the builder path first and the
  low-level path as an escape hatch.
- Document the builder's steady-state connection demand so hosts can size pools.
- Document the self-republishing loop hazard for publish-plus-handle of one type.
- Document that a post-upsert handler is not guaranteed to run once per received
  row, and that auditing belongs in `handle_before`.
- Update the runtime/topology decision if implementation details tighten the
  older draft.
- Promote validated behavior into specs only after implementation and tests.
  Concretely: `wiki/specs/entity-first-propagation.spec.md` states "Receive
  dispatch still runs the handler first" in its *Receive-side convergence*
  section. That sentence is the promotion target of the handler-ordering
  decision and must be rewritten — with a `Revised:` line — once Phase 2 is
  green. A spec left asserting the old order is a contradiction, not a
  historical note.
- Amend `wiki/decisions/schema-and-change-management.decision.md` with the
  never-edit-a-template rule, and add the `kafkaman-axum` amendment notes to
  `runtime-composition-and-topology` and `message-consumption-and-handler-model`.
- Record the tombstone signature outcome onto
  `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`.
- Mark this plan Completed when all gates pass.

Final gates:

- `cargo test --workspace --all-features`;
- `just lint`;
- `just examples demo`;
- deliberate-break evidence recorded here or in the closeout log for: generated
  changelog stability under reordering, band-collision detection, missing-role
  detection, the `Ignored` skip, the savepoint covering the upsert, and a
  builder-started loop never touching `global::meter()`.

## Cross-Worktree Coordination

`implementation/m6-observability` (worktree `~/Documents/rust/kafkaman-m6`) is
active in parallel. It instruments the library crates — `relay.rs`,
`dispatcher.rs`, `metrics.rs`, `queue_metrics.rs`, `kafkaman-core/src/trace.rs`,
`kafkaman-rdkafka/src/publisher.rs` — and does **not** touch either example
`service.rs`. Phase 5 therefore destroys none of that work, and the factoring is
already correct: spans and metrics belong in the library loops, not in example
boot code.

Three items need agreement between the two branches.

1. **Explicit meter injection.** `RelayMetrics::new` and its two siblings
   currently call `global::meter("kafkaman")`, and `worker::run` constructs them
   at loop start. They need to accept a `Meter`, with the no-op twins mirroring
   the signature. This lands on the M6 branch and is a dependency for Phase 3;
   it is cheaper to agree now than after that code settles. The M6 telemetry
   pipeline ownership decision, which mandates global resolution, needs amending
   with the reasoning from the accepted decision here.
2. **`apps/` is being removed.** This branch deleted `apps/axum-outbox` in
   commit `147a08c` and dropped it from workspace members; the removal is
   intentional and the two-service example replaces it. M6 currently treats
   `apps/` as one of only two places exporter dependencies may live, and its
   plan wires an SDK pipeline into `apps/axum-outbox/src/main.rs`. Both need
   retargeting to `examples/` — specifically to `examples/*/src/main.rs`, which
   already owns `tracing_subscriber` installation and stays separate from the
   `service.rs` files Phase 5 rewrites.
3. **Config growth.** M6 adds roughly 39 lines of telemetry keys to
   `kafkaman.example.toml`. `Config` and `ResolvedConfig` grow sections that
   `build()` must pass through untouched.

## What Closes the Plan

The plan closes when the two-service example uses the builder as its primary
boot path, `examples/product/src/service_manual.rs` proves the low-level path
still produces an equivalent runtime under the same end-to-end suite, all listed
verification gates pass, and the wiki records the compatibility impact and
validated behavior.
