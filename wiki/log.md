# Wiki Log

## [2026-08-27] review | third pass over the OTel example port

Line-by-line review of the staged example port, then the fixes. Two findings
were defects; the rest were gaps the port left behind.

**A `?` inside a `select!` arm skipped both shutdowns.** `examples/{order,
product}/src/main.rs` supervised `service.wait()` with `result?` inside the
arm, and `?` there returns from the enclosing function on the spot. When a
kafkaman loop died with an error — the case the supervision exists for — neither
`service.shutdown()` nor `telemetry.shutdown()` ran, so the spans and log
records explaining the death were dropped with the providers. The previous
entry's claim that the drain-then-flush ordering covered shutdown was true only
of the clean path. Both binaries now split the service lifecycle into a `run`
function and keep the telemetry lifecycle in `main` around it, so a boot
failure, a dead loop, and Ctrl-C all return through the same flush. The env
reads and `Config::discover` moved ahead of `init`, where there is nothing yet
to lose.

**The facade's enumerated re-export dropped four public items.** Replacing
`pub use kafkaman_axum as axum` with a hand-written list resolved the `E0255`
collision against the local module, but silently made `RuntimeTask`,
`RuntimeServer`, `DEFAULT_DRAIN_TIMEOUT`, and `kafkaman_axum::RuntimeError`
unreachable through the facade, with nothing to catch the next omission. The
module now globs `kafkaman_axum::*` and keeps only the composing `serve`
explicit, which shadows the glob's plain one legally and by intent.

**The two telemetry modules were byte-identical.** Extracted to a shared
`examples/telemetry` crate, which now also carries the comments the house style
expects: why the provider shutdown order is load-bearing, why a blocking HTTP
client is safe inside a Tokio runtime, and why blank counts as unset. The
instrumentation scope was `"kafkaman"` for the whole application including the
example's own Axum handlers; it is the service name now.

**Compose and docs.** The services gained `elasticsearch: {condition:
service_healthy, required: false}` — verified with `docker compose config` not
to drag Elasticsearch into the plain `services` profile, while closing the
30-60s window in which they logged failed exports at a backend still starting.
`stop_grace_period` went 15s to 30s, because shutdown is now drain-then-flush
and a `SIGKILL` there loses the window the flush exists to deliver. Documented:
the 9.2 floor for the native `/_otlp` endpoint, the trial licence's 30-day
expiry, that export is plaintext HTTP only (the pinned `reqwest` resolves with
no TLS backend and the runtime image ships no `ca-certificates`), and that
Kibana opens empty because no data view ships yet. `just examples demo` now pins
`OTEL_EXPORTER_OTLP_ENDPOINT` empty rather than inheriting a developer's own.

**Coverage.** The feature matrix compiled `axum,rdkafka` but never `axum`
alone, a combination `[features]` advertises and nothing else builds. Added to
both `justfile` and `.github/workflows/ci.yml`.

Verification:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`
- `just features` (all seven combinations)
- `just opt-out` — still holds with the new `dep:axum` and the SDK-carrying
  `examples/telemetry`, which is outside `crates/`
- `cargo check --workspace --all-features --all-targets`
- `cargo test --workspace --lib` (238 passed)
- `docker compose ... --profile services [--profile observability] config`

Not run: the Docker-backed integration suite, and the `just examples observe`
end-to-end Kibana check. Phase 4's exit criterion stays pending.

Pages affected: `crates/kafkaman/src/lib.rs`, `crates/kafkaman/src/axum.rs`,
`crates/kafkaman/Cargo.toml`, `examples/telemetry/` (new),
`examples/order/`, `examples/product/`, `examples/compose.yaml`,
`examples/README.md`, `justfile`, `.github/workflows/ci.yml`, `Cargo.toml`,
`wiki/plans/opentelemetry-completion.plan.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-08-27] implementation | OTel port onto the two-service examples

The M6 observability branch was rebased onto the two-service distributed-cache
example and the deferred example wiring was applied where it now belongs.

**The facade conflict was resolved around the runtime-builder API.**
`kafkaman::axum` is now one namespace rather than a crate re-export colliding
with a local module: admin/correlation types are re-exported from
`kafkaman-axum`, while the facade's `serve(...).with_runtime(...).spawn()` path
stays available for the examples.

**The example binaries own the SDK pipeline.** `order` and `product` now install
an SDK-backed metrics provider, tracer provider, log provider,
`tracing-opentelemetry` layer, and OTel log bridge before constructing any
kafkaman runtime loops. With no OTLP endpoint environment variable, or a blank
one, they install only the ordinary `tracing_subscriber` formatter and no OTel
provider. On shutdown, the service drains first and then providers are shut down
so the final export window is flushed.

**Compose gained the opt-in reference backend.** The existing
`examples/compose.yaml` has an `observability` profile with Elasticsearch and
Kibana, and the services accept `OTEL_EXPORTER_OTLP_ENDPOINT`. `just examples
observe` sets that endpoint to `http://elasticsearch:9200/_otlp`, matching
Elasticsearch's native OTLP/HTTP base path.

**Rebase fixes carried through the examples.** `RuntimeBuilder` and the manual
product boot path now pass the resolved per-message `LifecycleEmission` into the
dispatcher. The shipped example config's per-message retry and observability
overrides both name `order_snapshot`, matching the two-service contracts.

Verification:
- `cargo check --workspace --all-features --all-targets`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `just lint` with loopback permission, because `kafkaman-axum` tests bind
  ephemeral ports
- `docker compose -f examples/compose.yaml --profile services --profile observability config --quiet`
- `cargo test --workspace --lib` (238 passed)
- after stopping and removing all Docker containers: `cargo test --workspace
  --all-features -- --test-threads=1 --nocapture`

The first full-suite retry hung in
`retry_and_redrive::concurrent_dispatch_of_two_states_converges_to_newer`; the
test still blocked in a post-upsert handler even though the runtime-builder work
made `handler` run after the cache upsert. That held the older row's cache lock
while the test waited for the newer row to advance the same entity. The test now
uses `handler_before`, preserving the intended artificial interleaving without
contradicting the shipped dispatch order, and the full all-features workspace
suite passes after a Docker stop/remove-all cleanup. Full-stack Kibana
visibility remains pending until that stack is run end to end.

Pages affected: `crates/kafkaman/src/lib.rs`, `crates/kafkaman/src/axum.rs`,
`crates/kafkaman/src/runtime/builder.rs`, `crates/kafkaman/Cargo.toml`,
`examples/order/`, `examples/product/`, `examples/compose.yaml`,
`examples/README.md`, `justfile`, `kafkaman.example.toml`,
`crates/kafkaman-sqlx/src/tests/resolved_config.rs`,
`tests/durable-send/tests/entity_first_propagation/retry_and_redrive.rs`,
`wiki/plans/opentelemetry-completion.plan.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-08-27] update | review corrections and one verified enqueue finding

Second review pass over the staged 2026-08-26 change. Two of its findings were
stale-text defects the first pass introduced by fixing one half of a claim and
not the other; the third is new and came out of verifying something proposal 16
had recorded as unverified.

**Proposal 10 said `get()` twice.** The revision establishes that kafkaman ships
no typed key-value getter and that readiness therefore gates the qualified table
name, but the original *Readiness surface* section still read "with `get()`
available only on `Ready`" — reintroducing the API shape the revision exists to
remove. Amended in place rather than rewritten, since the argument around it is
untouched by which capability is gated.

**The builder plan's item-7 lead-in outlived its item.** Item 7 was rewritten to
record the tombstone representation as settled by Phase 0; the paragraph
introducing the list still announced it as "the one thing that remains genuinely
undecided". Fixed.

**Proposal 16's open finding is now settled, and it inverts.** The page recorded
that both example contracts declare `partition_key` returning the same value as
`entity_key` and asked whether the documented fallback makes those four lines per
type redundant. Verified against the enqueue path: redundant for routing —
`record_key` is `partition_key.or(entity_key)` and the publisher keys from
nothing else — but **not** redundant overall.
`crates/kafkaman-sqlx/src/outbox_enqueue.rs:54` attaches the
`kafkaman-entity-key` header only when the *declared* partition key differs from
the entity key, so declaring them equal suppresses it while returning `None` does
not. A derive that omitted `partition_key` would therefore start emitting a
header on every record from both example types, stating what the record key
already says.

The condition is the real defect: it tests the declared key where the header's
purpose is about the effective one, and the fallback makes those different
things. `RegionalOrder` is the case the header exists for — declared `eu-west`,
entity `order-77`, header present and asserted in the full-loop test. Narrowing
the condition to the effective record key is a **prerequisite** for the derive's
`partition_key` default, not a follow-up: land the derive first and it is a
silent wire change for every type that declares the two keys equal.

No code changed.
Pages affected: wiki/index.md, wiki/log.md,
wiki/plans/runtime-builder-and-axum-composition.plan.md,
wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md,
wiki/proposals/16-message-contract-derive.proposal.md

## [2026-08-26] design | next dev-UX increment scoped; merge and OTel deferred

A design session over what follows the runtime-builder workstream. No code
changed. Five findings came out of it, two of which contradicted live wiki pages
and one of which is a latent trap in shipped code — recorded here because they
existed only in conversation.

**Proposal 10 was written for the wrong cache shape.** It assumes a per-process
cache, reaches for the Kafka Streams GlobalKTable distinction, and prescribes
per-instance consumer groups reading all partitions. kafkaman's cache is a
Postgres table every replica of a service queries —
`entity-first-propagation-model.decision.md:111` states it as load-bearing: "all
instances share one database". The failure the proposal is built around, a second
pod holding half the data, cannot happen; that pod holds no data. Per-instance
groups, group naming and group cleanup all dissolve, which *dissolves* its first
open question rather than answering it.

The hazard it misses is worse than the one it describes: **a fresh database under
a surviving consumer group.** A restored database booting with the same
`consumer_group` starts at that group's committed offset, and every entity below
it is missing from the cache permanently and silently. Nothing recovers from it.
The replay consumer survives, reframed as assign-based and non-committing,
triggered by what the database says rather than where the group's offsets are.

The readiness typestate survives too, with one change: there is no `get()` to
gate, because kafkaman deliberately ships no typed getter — "the table is the
API". So `Cache<Bootstrapping>` → `Cache<Ready>` gates the *qualified table name*,
which is the narrowest thing that can be withheld and the one every query has to
interpolate. Open questions 2-5 are answered on the page rather than left open.

**Concurrent dispatch would silently corrupt derived state.** New proposal 15.
The claim layer has been ready for a long time — `received_rows.rs:38` uses
`FOR UPDATE SKIP LOCKED` and says so in a comment — so only the ability to *ask*
is missing. But `examples/product/src/lib.rs::recompute_availability` reads a
`SUM` and writes it back, so two orders for one product dispatched concurrently
under READ COMMITTED both compute a partial sum and the last writer wins. It is
correct today *only* because dispatch is serial, not because anyone reasoned
about it. Hence: opt-in, default 1, documented as a handler-contract change, and
the example fixed with `SELECT … FOR UPDATE` so the demonstration ships with the
knob.

**The Tower question is settled, with reasons.** Deferred three times across the
handler-model, receive-handler-surface and runtime-builder decisions. Rejected on
two independent grounds. *Types:* handlers borrow the transaction connection, so
`HandlerFuture<'a>` is lifetime-bound while `tower::Service::Future` is a
lifetime-free associated type — and `retry` needs `clone_request` while `buffer`
needs `Req: 'static`, bounds a unique mutable borrow can never satisfy. *Fit:*
`poll_ready` addresses backpressure from uncontrolled inbound load, and the
dispatcher pulls — the work waits durably in a table, so there is no shedding
decision to express. Making dispatch event-driven via `NOTIFY` does not change
that; push-versus-poll is not the axis, *whether you can refuse work* is. Kafka
could not push anyway: it is pull-based at the protocol level and librdkafka's
bounded prefetch queue already implements the backpressure below our code. What
survives contact is `timeout` and `rate-limit`. So: borrow Tower's vocabulary, not
its trait — a `Layer` carrying `'a`, plus proposal 14's `with_router` hook.

**`RuntimeTasks::wait()` treats a completing loop as a shutdown signal.**
`crates/kafkaman/src/runtime/tasks.rs:98-107`, line 104 maps
`Some(Ok((_, Ok(()))))` to the same `Ok(())` a clean shutdown returns, after
which `Runtime::run` drains everything. Correct today because every kafkaman loop
runs until cancelled — and a trap for the first one that does not. A cache
bootstrap loop would be exactly that, making a *successful* bootstrap kill the
service seconds after boot, presenting as a broker fault. Filed as Class Q in the
deep-durability catalog, and as a blocking prerequisite on proposal 10.

**`#[derive(KafkaMessage)]`.** New proposal 16, for the 18 hand-written impls.
Two rules matter more than the macro: `message_type`/`topic` stay explicit,
because inferring them from the type name makes a struct rename a silent
wire-contract change; and the derive stays sugar, per the standing principle that
dropped `#[kafkaman::handler]`. Carries an open finding — both example contracts
define `partition_key` returning the same value as `entity_key`, which the trait's
documented fallback may make redundant.

### Sequencing: merge and OTel first, deliberately

None of the above is being built yet. The runtime-builder branch merges to
`main` and `implementation/m6-observability` integrates *before* any of it,
because every one of these slices touches the dispatcher loop, the builder, or
the examples — precisely the files M6 instruments. Landing them first would make
M6 rebase onto changed code or instrument code about to change underneath it.

The delay has a measurable cost already: the cross-worktree coordination note
recorded three `global::meter("kafkaman")` call sites; on
`implementation/m6-observability` there are now six — two in
`crates/kafkaman-rdkafka/src/metrics.rs`, three in
`crates/kafkaman-worker/src/metrics.rs`, and one in
`crates/kafkaman-worker/src/queue_metrics.rs`. None of them exist on this branch,
where `crates/` still contains no `opentelemetry` at all. Every week unmerged is more
instrumentation written against the global that clause 9 of the runtime-builder
decision says must be injected — the one accepted clause the builder knowingly
shipped against.

Also recorded: `examples/order/src/boot.rs` exists only to satisfy the builder
plan's 60-line gate and should be folded back into `service.rs`. Noted on the plan
that created it, not in a backlog.

Pages affected: `wiki/proposals/15-dispatch-concurrency-and-middleware.proposal.md`
(new), `wiki/proposals/16-message-contract-derive.proposal.md` (new),
`wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md`,
`wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md`,
`wiki/proposals/05-deep-durability-testing.proposal.md`,
`wiki/plans/runtime-builder-and-axum-composition.plan.md`, `wiki/index.md`,
`wiki/log.md`. No code changed.

## [2026-08-26] implement | runtime builder shipped, dispatch handlers reordered

All seven phases of
[plans/runtime-builder-and-axum-composition.plan.md](plans/runtime-builder-and-axum-composition.plan.md)
landed. The observable result: `examples/order/src/service.rs` went from 229
lines to 51 and `examples/product/src/service.rs` from 201 to 55, and neither
now names an outbox table, a changeset, a topic admin, a publisher, a consumer,
or a `JoinSet`. A test asserts that, because it is invisible to the compiler and
to every behavioural test — the services work identically whether the wiring is
declared or hand-rolled.

**The changelog is derived from declared roles.** Identity is
`(table kind, message type, template version)`, allocated as a sparse band from
an explicitly-versioned SHA-256 and pinned in a test against an independent
computation, because the numbers end up as primary keys in a user's
`changelog_history`.

**Identity keys on the table kind, not the registration role** — an amendment
implementation forced onto the decision. `cache::<T>()`, `handle::<T>()`, and
`handle_before::<T>()` are three ways to state one schema fact, so keying on the
registration role would have given identical DDL a different version depending on
which was written, and adding a handler to an already-consumed type would have
presented as a new changeset against an existing table.

**Handlers now run after the cache upsert.** `examples/product/src/lib.rs`
documented the trap it was working around — a handler deriving from its own cache
saw the entity one version stale and had to exclude its own `entity_key` and add
the incoming value back. Both halves are deleted. `handle_before` is the explicit
pre-image opt-in; neither position can suppress the upsert.

**The savepoint moved ahead of the upsert, and that was verified by breaking
it.** Left where it was, a failed post-upsert handler commits an advanced cache
row, the retry yields `Ignored`, and the handler is skipped forever. Moving the
savepoint back makes
`a_failing_post_upsert_handler_unwinds_the_cache_upsert_too` fail with the cache
holding the failed handler's value, which is the failure the reorder would
otherwise have shipped silently.

### Three places implementation contradicted the plan

1. **`handle_before` has no example, and the reason is worth keeping.** The plan
   assigned `product` a pre-image hook that would skip the `SUM` recompute when
   an order's fulfilled-ness had not changed. That is arithmetically correct and
   would have hung the end-to-end suite: `two_service_cache.rs` waits for the
   product cache offset to *advance* after an order is placed, and that advance
   comes from `product` republishing. The republish is what makes propagation
   observable, so suppressing it for "no availability change" deletes the signal
   a consumer waits on. The plan's own fallback was taken — focused tests in
   `dispatch_ordering.rs` cover the skip instead — and the finding is recorded in
   `derive_availability`'s documentation, because it is a sharper statement of
   "the pre-image is a cost guard, not a correctness guard" than the plan had.
2. **The meter is deferred.** `implementation/m6-observability` is unmerged and
   still calls `global::meter("kafkaman")`, so threading one would have been
   guessing at a signature. Safe because builder setters are additive; the cost
   is that the no-global-fallback rule stays accepted and untested.
3. **`HandlerCtx` was pulled forward from phase 4 into phase 3**, rather than
   shipping a handler signature that would be rewritten one commit later.

### Two smaller things

The purger is now wired. `[retention]` has always been parsed by
`kafkaman-config` and never spawned anything; the builder starts one per
published type when the section is present, and still deletes nothing when it is
absent.

`ReceivedMeta` became `#[non_exhaustive]` and gained a reserved `deleted` flag,
settling the tombstone question onto
[proposals/07-tombstone-and-deletion-semantics.proposal.md](proposals/07-tombstone-and-deletion-semantics.proposal.md).
The deciding argument against `Option<T>`: kafkaman emits soft deletes as full
entity states, so on any topic kafkaman produces the payload is never absent, and
`Option<T>` would make every handler match on a `None` its own producer cannot
emit. The `#[non_exhaustive]` half had to happen before first publication — the
struct had sixteen public fields, so *adding* the flag later would itself have
been the breaking change.

`just examples demo` was run against a Postgres volume already migrated by the
old hand-written changelog, so the demo doubled as a live test of the adoption
path: both services booted, all seven smoke steps passed, and
`changelog_history` ended up with seven rows for four tables — the four
hand-numbered ones, now permanently inert, alongside the three generated ones.
That is recorded in the compatibility note as observed rather than predicted.

The escape hatch is a tested claim. `examples/product/src/service_manual.rs`
boots the same service from the low-level primitives, `Services::start_with` is
parameterised over the boot mode, and `tests/distributed-cache` runs against
both.

### Deliberate-break evidence, as the plan asked for it

Stated with the distinction the plan's list elides — a *forced break* is a change
made to the code to watch a test fail; a *property test* asserts the invariant
without one. Both are worth having; conflating them overstates the second.

| Gate | Kind | Evidence |
| --- | --- | --- |
| Savepoint covers the upsert | **Forced break** | Savepoint moved back after the upsert; `a_failing_post_upsert_handler_unwinds_the_cache_upsert_too` fails with the cache holding the failed handler's value. Restored and re-verified. |
| Band-collision detection | **Forced break** | `build_changelog_with_forced_band` stubs the allocator to a constant and drives the real generation path. The 40-bit digest cannot be made to collide on demand, so the seam exists for this. |
| Generated changelog stability under reordering | Property test | `declaration_order_changes_neither_versions_nor_checksums` and its `RoleRegistry` twin compare versions *and* checksums across a reordered declaration. |
| The `Ignored` skip | Property test | `a_replayed_record_is_ignored_and_the_post_upsert_handler_does_not_run`, counting handler invocations across a replay. |
| Missing-role detection | Partial | Conflicting and duplicate roles are covered, and `an_unregistered_type_parks_its_row_without_advancing_the_cache` covers the runtime consequence. What is *not* separately forced is a service omitting a role it needs: `ResolvedConfig` rejects the unregistered type at `build()`, so it fails before any loop starts, and the existing test proves the runtime half. Recorded as partial rather than claimed as done. |
| A builder-started loop never calls `global::meter()` | **Not applicable** | The meter is deferred; `crates/` contains no `opentelemetry` on this branch, so there is nothing to break. This gate returns when M6 merges. |

Pages affected: `crates/kafkaman/src/**` (new `runtime` module),
`crates/kafkaman-core/src/**`, `crates/kafkaman-sqlx/src/**`,
`examples/order/**`, `examples/product/**`, `tests/distributed-cache/**`,
`tests/durable-send/tests/entity_first_propagation/**`, `README.md`,
`examples/README.md`, `Cargo.lock`,
`wiki/compatibility/runtime-builder-and-axum.compat.md`,
`wiki/compatibility/dispatch-handler-ordering.compat.md`,
`wiki/decisions/runtime-builder-and-axum-composition.decision.md`,
`wiki/decisions/runtime-composition-and-topology.decision.md`,
`wiki/plans/runtime-builder-and-axum-composition.plan.md`,
`wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`,
`wiki/specs/entity-first-propagation.spec.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-08-26] update | runtime builder pages reviewed twice and corrected

Reviewed the accepted builder proposal against the code it claims to replace and
revised all three pages plus added one decision. The review changed six things
that had been wrong or under-specified in the first draft.

**Axum stops being a crate.** `crates/kafkaman/src/lib.rs` gates `rdkafka`
behind a feature and re-exports it "so an application never has to name a second
kafkaman crate" — and that is a C-linking dependency. A pure-Rust optional
dependency does not warrant weaker treatment, so `kafkaman-axum` becomes
`kafkaman::axum` behind an `axum` feature.

**Changeset identity is settled.** The scheme is
`(role, message_type, template_version)` allocated as a sparse hash band. Two
code findings forced it: `descriptor_changeset!`'s `checksum_material` includes
the version, so renumbering breaks every existing database — and it does *not*
include the DDL, so an in-place template edit is completely invisible to the
migration engine. That is not hypothetical: `create_outbox_table_sql` already
carries the columns `AddIdempotencyKey`, `AddIdempotencySource`, and
`AddOutboxEntityKey` exist to add. A contract-side wire version was considered
and rejected — cache payloads are JSONB, so a new contract field needs no DDL,
and `examples/contracts` is shared, so one bump would couple two independent
migration timelines.

**Handler ordering became its own decision.** `examples/product/src/lib.rs`
documents a trap it calls "a genuine footgun": the handler runs before the cache
upsert, so a derivation sees its own entity one version stale and must exclude
its own `entity_key` and add the incoming value back. `handle::<T>` now runs
after the upsert, `handle_before::<T>` is the explicit pre-image opt-in, neither
may suppress the upsert, and a post-upsert handler is skipped on
`CacheApplyOutcome::Ignored` — a distinction a pre-upsert handler cannot make.
`apply_order_snapshot` loses the exclusion and the add-back.

**Host-owned concerns became a normative list.** Pool, business schema, signal
handling, process exit, telemetry install, config discovery, topic creation,
executor choice, process topology. The builder takes a `PgPool` rather than a
URL, and the `business_schema` hook was dropped entirely — with a host-supplied
pool the caller can run its own migrator before or after `build()` without the
builder fixing a position for it.

**Telemetry takes an explicit meter with no global fallback.** (Sourced from
the `implementation/m6-observability` worktree at `~/Documents/rust/kafkaman-m6`,
not from this branch — `global::meter`, `RelayMetrics`, and `opentelemetry`
appear nowhere under `crates/` here.) M6 constructs instruments inside the loops
via `global::meter("kafkaman")`, and an OTel
instrument binds permanently to whatever provider is installed at that moment.
Hiding loop construction inside `build()` would hide that contract, so `build()`
now assembles and `run()` starts loops, and the meter is passed in. Omitting it
means no-op, not global lookup. This is a deliberate divergence from the
instrumentation-library convention the M6 pipeline-ownership decision adopts.

**The escape hatch became a tested equivalence claim.** Rather than splitting
builder and hand-wired styles across the two services — which would compare
different workloads, since `order` only caches while `product` derives and
republishes — both services use the builder and `examples/product` gains a
`service_manual.rs` with an identical signature. `Services::start` is
parameterized over boot mode so the distributed-cache suite runs against both.

The plan is now implementation-ready: all four original open questions are
resolved and recorded, phases carry concrete gates including a symbol-absence
grep test and a sub-60-line target for both `service.rs` files, and the example
role mapping is written down with the honest finding that in this domain the
pre-image is a cost guard rather than a correctness guard. A Cross-Worktree
Coordination section records the three items owed to
`implementation/m6-observability`: meter injection into `RelayMetrics::new` and
the run loops, retargeting the `apps/` exporter rule and the SDK wiring to
`examples/` now that `apps/axum-outbox` has been removed here, and the telemetry
config keys `build()` must pass through.

### Second pass, 2026-08-26: every code claim checked against the tree

The pages above were then reviewed line by line against the code they cite.
Most claims held — the `examples/product/src/lib.rs` quotes are verbatim,
`checksum_material` really is `version;name;message_type;topic` with no SQL,
`assert_changelog_order` really is unique-and-strictly-ascending,
`create_outbox_table_sql` really does already carry the columns the three
`Add*` changesets exist to add, `TopicMode::Verify` is the default, and the
229/201 boot-file line counts are exact. Seven things did not.

1. **`CacheApplyOutcome::Adopted` does not exist.** The variant is `Migrated`
   (`crates/kafkaman-sqlx/src/dispatch_cache.rs`). `Adopted` is prose from the
   log line it drives. Corrected in the decision and the plan.
2. **Topic ownership contradicted itself.** The decision listed "topic creation"
   as host-owned and said the builder "neither performs nor offers" it, while
   the proposal and plan put `converge_topics` inside `build()` — and
   `TopicMode::Create` creates topics. Reworded: the host owns the
   *authority*, the builder executes the configured mode and never defaults to
   or upgrades to `create`.
3. **The tombstone shape was claimed settled and is not.** The decision pointed
   at the plan; the plan said decide it later; and
   `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md` already
   proposes a different answer — put the flag in `ReceivedMeta` rather than
   change `P`. Reopened explicitly, with proposal 07's direction named as the
   leading candidate and the outcome owed back to that page.
4. **The Tower layer surface was pulled into scope on a false premise.** The
   proposal said the handler-model decision "establishes" a `.layer(..)` stack.
   It does the opposite — its *Accepted Reconciliation* calls the larger
   Tower/extractor surface "future design space" — and `router.rs` has only
   `new` and `handler`. Descoped to a shape constraint: roles yield a plain
   `MessageRouter`, so a wrapping hook stays addable later.
5. **The generated-version scheme was under-specified for a durable primary
   key.** Width, reserved range, hash stability, sort-before-assert, and
   collision behavior are now normative in both the proposal and the decision.
6. **`MissingHandler` and the dispatch savepoint were unaddressed.** The router
   lookup short-circuits ahead of the upsert today and stays there; the
   savepoint must *move* ahead of the upsert, or a failed post-upsert handler
   commits an advanced cache row and the retry is skipped as `Ignored` forever.
7. **Smaller corrections.** `thiserror` is in six of seven crates, not all
   seven — and the exception is `crates/kafkaman`, the crate that will host the
   builder error. Two of the six "resolved" design questions were never open
   questions. `wiki/specs/entity-first-propagation.spec.md` asserts the old
   handler ordering as validated truth and is now named as the promotion
   target. Telemetry claims are sourced to the M6 worktree explicitly, since
   `global::meter`, `RelayMetrics`, and `opentelemetry` appear nowhere under
   `crates/` on this branch. Cross-links and amendment notes were added to
   `runtime-composition-and-topology`, `message-consumption-and-handler-model`,
   `missing-handler-dispatch-policy`, `schema-and-change-management`, and
   proposal 07, none of which previously pointed at the new pages. This entry's
   own heading had been inserted above `# Wiki Log`, displacing the H1 to line
   73; the file structure is restored.

Pages affected: `wiki/decisions/dispatch-handler-ordering.decision.md` (new),
`wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md`,
`wiki/decisions/runtime-builder-and-axum-composition.decision.md`,
`wiki/plans/runtime-builder-and-axum-composition.plan.md`,
`wiki/decisions/runtime-composition-and-topology.decision.md`,
`wiki/decisions/message-consumption-and-handler-model.decision.md`,
`wiki/decisions/missing-handler-dispatch-policy.decision.md`,
`wiki/decisions/schema-and-change-management.decision.md`,
`wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-25] create | runtime builder and Axum composition

*Superseded in part by the 2026-08-26 entry above, which records what two review
passes changed. This entry describes the first draft as written and is kept for
the provenance chain, not as a current description of the pages.*

Recorded the developer-UX follow-up from dogfooding the two-service example.
The example now proves the distributed-cache model, but its service boot files
manually repeat kafkaman invariants: config coverage, topic convergence,
kafkaman changelog construction, migrations, relay, ingester, dispatcher,
shutdown, and Axum composition.

Accepted proposal 14 and promoted it to a decision. The chosen split is a core
role-driven runtime builder plus optional Axum composition. Services declare
`publish::<T>()`, `cache::<T>()`, and `handle::<T>(handler)` repeatedly; the
builder derives kafkaman-owned changesets, verifies or creates compacted topics
through the existing topic mode, starts the worker paths, and owns cancellation
and supervision. HTTP stays outside core: the Axum helper composes an
already-built runtime with an already-built router and graceful shutdown.

The plan is active and starts with the migration hazard the builder introduces:
generated changelog versions cannot come from mutable registration order,
because existing databases store numeric versions and checksums. The first
implementation phase therefore has to prove stable generated identities before
the examples can drop their manual changelog files.

Pages affected: `wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md`,
`wiki/decisions/runtime-builder-and-axum-composition.decision.md`,
`wiki/plans/runtime-builder-and-axum-composition.plan.md`, `wiki/index.md`,
`wiki/log.md`.

## [2026-08-25] update | example collection reads

Added collection read endpoints to the two-service example so the Swagger UIs
show the current owned state without needing copied IDs: `GET /products` on
`example-product` lists product-owned rows, and `GET /orders` on
`example-order` lists order-owned rows. Both are local database reads only; they
do not enqueue snapshots or touch Kafka. The handler tests now assert both the
HTTP response and the generated OpenAPI path entries.

Pages affected: `examples/product/src/http.rs`,
`examples/order/src/http.rs`, `examples/product/tests/derive_availability.rs`,
`examples/order/tests/service.rs`, `examples/README.md`, `wiki/log.md`.

## [2026-08-25] implement | topic convergence phase 5: provisioning and boot wiring

Closed the last phase of the topic-convergence plan. Two halves of one design:
the services now *verify* their entity topics at boot, and a new
`example-provision` package *creates* them beforehand — the first is only a
workable posture because of the second.

**Boot wiring.** Both example services call
`converge_topics(&admin, cfg.topics, cfg.messages())` after config resolution
and before the pool is opened, so a missing or non-compacted topic fails the
process rather than being discovered by whoever eventually needs a rebuild.
Every registered type is checked, inbound and outbound alike: a topic carrying
entity snapshots has to be compacted whichever end of it a service is on.

**`ResolvedConfig` carries `topics: TopicMode`.** Reading `[topics]` at the call
site would have meant a mistyped mode surfacing separately from every other
config problem, after a pool was already open — which is the one thing that
crate promises not to do. Additive rather than breaking: the struct has a
private field and was never constructible by literal.

**The provisioner is a library with a binary on top**, not a binary alone,
because `tests/distributed-cache` has to build its environment the same way
compose does. The fixture calls `ensure_databases` and `provision_topics`
directly, so a change to what provisioning means reaches the test instead of
passing it by. `examples/initdb/10-databases.sql` is deleted: the environment
was previously half a SQL file and half nothing, and only the SQL half ran on
the host-side flow.

**Two things the plan's sketch did not name.** Database names arrive from the
environment and reach `CREATE DATABASE "..."` as quoted identifiers, so the
alphabet is a boundary rather than a style rule — and the 63-byte cap is
*refused* rather than truncated, because Postgres truncates silently and the
service's connection string would then name a database that does not exist.
Duplicate detection is the `CREATE` itself rather than a `pg_database` check,
since check-then-create has a window a concurrent provisioner can win.

**`provision` sits in the `services` compose profile, not the default one.**
Putting it in the default set would have made `just examples up` build an image,
breaking that flow's promise of starting in seconds. It provisions with
`cargo run -p example-provision` instead, sharing the build the developer is
about to pay for anyway. Compose's `--wait` was measured against a one-shot
service before committing to the shape: it treats a clean exit as satisfied and
re-runs are clean, both verified on v2.31.

**The gate was demonstrated, not assumed.** Setting `mode = "off"` in
`examples/order/kafkaman.toml` made
`provisioning_precedes_boot_and_is_the_only_thing_that_creates_a_topic` fail at
its first assertion, confirming the boot check is what that test observes rather
than an incidental failure on the way to it. That test also asserts the load-
bearing negative: after a failed boot, neither topic exists. Redpanda's
dev-container mode has auto-creation on, so a check that named the topic in its
metadata request would have manufactured the exact `delete` topic it was looking
for.

`rpk topic describe` now reports `cleanup.policy compact DYNAMIC_TOPIC_CONFIG`
on both `products` and `orders`, where it reported `delete` before this
workstream — which was the observation that started it, contradicting the
compaction claim in `README.md` with nothing anywhere reporting it.
`DYNAMIC_TOPIC_CONFIG` is the part worth reading: set deliberately, not
inherited from a default that happened to agree.

**The example recipes collapsed into one.** `examples-up`, `demo`,
`examples-ui`, `examples-logs` and `examples-down` are now
`just examples [demo|up|ui|logs|down]`, following the shape `test arg="all"`
already had — a stack this small should not own five of the seven top-level
verbs. `demo` is the default, so the headline "one command runs the whole thing"
survives as `just examples`. Entries above this one name the old recipes,
because that is what they were called at the time.

Two unrelated recipes were fixed while there: `just --list` shows only the
comment line immediately above a recipe, so `lint` and `clean-containers` were
advertising the last sentence of their rationale paragraphs. Their summaries
moved into `[doc(...)]` attributes, which decouples the listing from the prose.

Verified: `cargo test --workspace --all-features` 228 passing / 41 suites (up
from 220); `topic_convergence` 2 and `redpanda_full_loop` 11 against a real
broker; `just lint` clean including the rustdoc gate; the demo green from a
clean slate and again on a re-run; the infrastructure-only flow plus a host-run
service booting against the provisioned stack.

Still open from this workstream: an ACL-denied `CreateTopics` path (Redpanda's
dev-container mode has no ACLs to deny with), bounded broker retry at boot,
cutover tooling for a rebuild, and a positive state-sourced republish API.
Neither of the first two blocked phase 5 — compose gates the provisioner on a
healthy broker and the services on the provisioner — but a deployment without
that ordering would want the retry.

## [2026-08-25] update | merge the module refactor into topic convergence

Merged `e8c7e9a` (147 files, +16k/-15k: crate roots split into modules, durable
tests exploded into directories) into this branch. Merged rather than rebased —
main already carries `Merge branch 'implementation/*'` commits, and a merge
resolves each conflict once instead of replaying it across three.

Git's conflict markers were close to useless for the four crate roots, because
main *moved* the code our hunks were anchored to. The resolution was to take
main's version of each `lib.rs` wholesale and hand-place our additions into the
module it had carved out: `topics.rs` beside its siblings, error variants into
`error.rs`, `MessageDescriptor::topic_spec` into `message.rs`, `TopicsSection`
into `sections.rs`, `Config::topics()` rewritten over main's new `section()`
helper, and the cache-origin logic into `dispatch_cache.rs` / `dispatch.rs`.
Unit tests moved into the `src/tests/<module>.rs` layout with absolute crate
paths.

The refactor also removed work rather than adding it:

- `topic_convergence.rs` lost ~45 lines. Main promoted the Redpanda fixture to
  `durable_send_tests::redpanda()`, which reserves an OS port and retries the
  bind-then-map race — strictly better than the F16 fix this branch carried, so
  ours was discarded rather than ported.
- The three cache-origin tests joined `entity_first_propagation/cache_apply.rs`,
  next to the partition-change test that constrains them, and adopted
  `start_harness()`.

Two failures worth recording, both caught by main's tests rather than by
review:

- `the_shipped_example_config_resolves_and_covers_every_section` failed twice.
  First because this branch renamed the example's message type to
  `order_snapshot` while main's test still registered `order_created`. Then
  because the test asserts the per-type retry override actually differs from the
  defaults, and a second `order_created` reference had been missed. That test
  exists to make `kafkaman.example.toml` executable documentation, and it worked.
- It also surfaced a gap this branch left independently: `[topics]` was added as
  a config section and never documented in `kafkaman.example.toml`. Now
  documented, and the test extended to assert it — which is the point of a test
  whose stated purpose is catching "a section added without being documented".

One merge artifact rejected: the auto-merge carried main's swap of
`testcontainers` for the shared `durable-send-tests` helper into
`examples/order/Cargo.toml`. That change was made for `apps/axum-outbox`'s test
file, which this branch deletes; an example should not reach into the library's
test-support crate to run, so the direct dependency was restored.

Green at 220 passing, clippy clean, the new `cargo doc -D warnings` gate passing
(one intra-doc link needed qualifying), and `just demo` walking the lifecycle.
Pages affected: `wiki/log.md`, `kafkaman.example.toml`, `crates/**`,
`tests/durable-send/**`, `examples/order/Cargo.toml`.

## [2026-08-25] update | rebase onto main; reconcile the M5 closeout

Rebased the topic-convergence work onto `332721a` (the M5 closeout) and restored
the two-service example from stash. Only `wiki/log.md` conflicted, in both
operations — the log is newest-first, so every prepended entry lands on the same
anchor. That will recur on every rebase; it is a property of the format.

The mechanical rebase was the easy half. Main's closeout rewrote the
documentation of behaviour this branch then changed in code, and **none of those
files conflicted** — they applied clean and silently became wrong. Reconciled on
the principle that a spec describes what is true now while a compatibility note
records what one release shipped:

- `wiki/specs/entity-first-propagation.spec.md` **amended**. It enumerated only
  `Applied` and `Ignored`, said a topic or partition mismatch is reported as
  `CacheOriginMismatch` and is never a normal ignored record, and listed broker
  topic validation as unimplemented. All three are now false in part. It carries
  the four-case outcome table, and states that topic validation exists as a
  library capability but is not yet called from any boot sequence — landed rather
  than delivered.
- `wiki/roadmaps/path-to-v1.roadmap.md` **amended**, because a roadmap is live
  orchestration rather than history and its post-M5 deferral list named an item
  that has since landed.
- The three M5 compatibility notes and the now-Completed entity-first plan were
  **left alone** as release records, even though they contain statements this
  branch supersedes. `m5-code-audit-remediation.compat.md` is the one to watch: it
  still tells operators to treat `CacheOriginMismatch` as the signal to
  re-bootstrap, which stays correct for an in-place repartition and is wrong for a
  rebuilt topic.

Corrections to this branch's own documents, which mattered more than main's:

- Proposal 13 and the topic-convergence decision **misquoted** the entity-first
  plan. They cited "Boot-time broker topic validation remains pending"; the
  closeout rewrote that to "remains deferred after M5". The 2026-08-24 `create`
  entry below carries the same misquote and is left as written, since this log is
  append-only.
- Both leaned on a Deferred bullet — "Topic/partition mismatch invalidation path"
  — that the closeout deleted, having reframed that behaviour as shipped. The
  proposal's main premise survives, because boot-time validation is still Deferred
  in both notes; what changed is that this work *supersedes* a closed M5 behaviour
  rather than *completing* an open deferral.
- Three documents listed `Replay::outbox` **rejection** as deferred. It was
  already implemented at the merge base — `Replay::outbox` returns
  `Err(UnsafeOutboxReplay)` unconditionally. What is actually missing is the
  positive resync surface. That error predated the rebase and was ours.
- `wiki/index.md` still showed the topic-convergence plan as `Draft` with phases
  1-4 landed.
Pages affected: `wiki/specs/entity-first-propagation.spec.md`,
`wiki/roadmaps/path-to-v1.roadmap.md`,
`wiki/proposals/13-topic-convergence-and-environment-provisioning.proposal.md`,
`wiki/decisions/topic-convergence-and-rebuild.decision.md`,
`wiki/plans/topic-convergence.plan.md`,
`wiki/compatibility/topic-convergence-api.compat.md`, `wiki/index.md`,
`wiki/log.md`.
## [2026-08-26] fourth review response | four findings, all upheld

A fourth review of the OTel branch, filed after the third pass landed. Every
claim was reproduced before anything moved.

**The migration's own safety claim was false.** The failure backfill documented
that "a timestamp that does not parse costs only itself", and the shape test it
relied on was a prefix regex. `2026-99-99T10:00:00Z` matches it and still raises
`datetime_field_overflow` on the cast — reproduced directly against
PostgreSQL 16, where it aborted the whole `UPDATE` and left every well-formed row
beside it unrecovered. A shape test cannot be a parser: no pattern rules out
`2026-02-30` either. The backfill is now two statements — a kind pass that cannot
raise, and a timestamp pass in a `DO` block that tries one set-based `UPDATE` and
falls back to one row at a time, each cast in its own subtransaction, only when
that raises. `a_date_shaped_non_date_costs_only_its_own_row` pins it.

**The migration test asserted against the one query that still worked.** It
proved the changeset mandatory by requiring an unfiltered `received_failed_count`
to fail on a table without the columns — but that query names only `status`, so
it succeeded and the test was red. Reasserted against `received_failed_rows` and
a kind-filtered count, which do name the columns, with the surviving unfiltered
count now pinned as *the* reason the mismatch is quiet: the number beside the DLQ
page goes on counting rows the page cannot render.

**Two documentation claims had drifted from the code.** The metric schedule in
both the decision and the compatibility note still listed the three `ingest.*`
counters without `messaging.destination.name`, which the code adds and the same
pages' amendments describe; the schedule rows are the table people read, so they
were corrected rather than only amended around. And the sampler's guarantee was
stated as `round(N × sample_success)` in five places when it computes
`⌊N × sample_success⌋` — the owed count is floored after the rate is rounded to
six decimals. Restated as the floor everywhere, with the real property named: the
count never runs ahead of the rate and never falls a whole event behind it.

Pages affected: `crates/kafkaman-sqlx/src/schema_sql.rs`,
`crates/kafkaman-sqlx/src/changesets.rs`,
`crates/kafkaman-sqlx/src/tests/schema_sql.rs`,
`crates/kafkaman-core/src/lifecycle.rs`,
`tests/durable-send/tests/durable_receive/failure_metadata_migration.rs`,
`tests/observability/tests/lifecycle_events.rs`, `kafkaman.example.toml`,
`wiki/compatibility/m6-observability-operability-api.compat.md`,
`wiki/decisions/metric-instrument-and-attribute-schema.decision.md`,
`wiki/decisions/observability-operability-policy.decision.md`,
`wiki/specs/m6-observability-operability.spec.md`,
`wiki/plans/opentelemetry-completion.plan.md`, `wiki/log.md`.

## [2026-08-26] third review response | seven findings, five upheld, one misread, one declined

A third implementation review of the OTel branch, filed after the second pass
landed. Seven findings, each checked against the code before anything moved.

**The best finding is better than it knew.** The review noticed that
`AddReceivedFailureMetadata` adds the two failure columns without filling them,
and that this splits the DLQ in half: `/dlq` renders the newest audit entry's
`type`, every filter reads the column, so an operator who copies a visible
failure kind into a redrive matches nothing and is told it succeeded. True — and
the code comment justifying the omission says *"nothing can recover **when** a
row failed from an audit entry the column never saw"*, which is false.
`ReceivedError` carries `occurred_at`. Both columns were recoverable the whole
time; the omission rested on a claim about the data that the data contradicts.
Backfilled now, from both spellings a stored kind can have, with the RFC 9557
annotation stripped in the one place that trade is right — a migration runs once,
a query runs always.

Three consequences the review did not follow through to. `ORDER BY last_failed_at`
sorts NULLs last and `/dlq` pages at fifty per type, so un-backfilled rows fell
off the end while the count beside them still counted them — a page that
disagrees with its own total. The `occurred_after` filter missed the same rows for
the same reason. And the migration test asserted the false premise as though it
were a property.

**The misread.** The review reported that the compatibility note claims trace
builds no longer link OpenTelemetry's `futures`, contradicted by `cargo tree`.
The note says a ***metrics*-only** build does not, and
`cargo tree -p kafkaman --no-default-features --features metrics -i opentelemetry`
returns exactly `metrics`. The document was right; the reading was not. Both
axes' feature sets are now written out in full so the sentence cannot be read
that way again, and the half of the finding that was fair — that the guard
asserts only forbidden pairs — is taken: `just opt-out` now also pins the metrics
axis to exactly `{metrics}`. Caught a widening the pair check would have missed,
on the first teeth-check. The traces set stays documented rather than asserted;
it is upstream's optional-dependency list, and pinning it makes their patch
release our red build.

**The decline.** Repeated `tracestate` Kafka headers are first-wins, and the
review wanted them joined per W3C. The behavioural claim is correct and the
prescription does not transfer: that rule exists because HTTP lets one logical
field be split across lines, so joining restores what the sender wrote. Kafka
headers are a genuine multimap — two records are two values. Documented as a
deviation instead, which is where a knowing departure belongs.

**Three more found while fixing theirs.** `tracestate` had no duplicate-key
check, so `a=1,a=2` was accepted and forwarded — every production satisfied, and
still not a valid list. Parsing was unbounded, and every member costs a
uniqueness check against every key before it. And the tab the review did find had
a sibling worth stating: the same character is legal *between* members and never
inside one, which is why the fix removes it from the value charset rather than
from the trimming.

The DLQ index took the cheaper of the two available fixes. The review proposed a
second partial index; the existing one is new on this branch and unreleased, so
`last_failure_kind` became a fourth key column *after* the ordering chain. A
kind-filtered page keeps the index ordering and rejects other kinds inside the
index; leading with the kind would have inverted that trade.

Two documentation findings upheld without argument: a stale consequence in the
metric decision still describing gauge callbacks as querying on the SDK's
collection interval, and a CI comment promising it ran exactly `just check` while
also running a coverage job.

## [2026-08-26] second review response | eleven findings checked, ten upheld, five more found

A second implementation review of the OTel branch raised eleven findings and
scored every file. Each was checked against the code before anything changed.
**Ten held up. One did not, and the way it failed is the useful part.**

**The one that did not.** The review's only High finding — "existing received
tables are not migrated for `last_failed_at` / `last_failure_kind`" — attributed
the gap to this branch: "dispatch/query/redrive *now* depend on them." They do
not *now*. `git diff main -- schema_sql.rs` is twenty-six lines and adds only
`traceparent` and `tracestate`; `git grep last_fail main` finds the dependency
already in `queries.rs`, `replay.rs` and `received_rows.rs`. The columns and
every consumer of them are on `main`. By the review's own stated assumption —
"M6 must upgrade tables created by current main" — there was nothing here to
migrate.

The gap is real one commit further back. `git log -S` puts the columns in
`e8c7e9a`, which is `main`'s tip, with no additive changeset beside them. So the
finding was true about the repository and false about the branch, and the
difference decides who fixes it. Fixed here anyway, because it is twenty lines
and idempotent: `AddReceivedFailureMetadata` and `AddReceivedFailedIndex`, with a
legacy-table test that asserts the sharp version of the failure — before the
changeset, `received_failed_count` *raises* rather than degrades. That is what
makes this the one additive changeset kafkaman ships that is not optional.

**Five the review did not find.** Recorded because each sits one door along from
something it did:

- `TraceContext` derived `Default`, which builds one with an empty
  `traceparent`. The review caught the derived `Deserialize` as a second
  constructor that skips validation; `Default` was a third. Nothing used it.
- The `traceparent` field count was unbounded. The review's finding next door —
  that only the first appended field was checked for emptiness — is real and
  fixed, and while fixing it the larger version showed up: this header is stored
  in a column and rewritten on every hop, so an unbounded field count is an
  unbounded header travelling under kafkaman's name. Capped at twelve.
- The ingest counters carried no topic. `kafkaman.kafka.ingest.records` and
  `kafkaman.kafka.publish.records` therefore had no attribute in common beyond
  the constant `messaging.system`, so the obvious operator question — are we
  consuming this topic as fast as we publish to it — could not be asked without
  mapping message types to topics outside the telemetry. All three ingest
  instruments now carry `messaging.destination.name`.
- The queue gauges' binding rule was undocumented and untested. They register
  once per process, so they bind to whichever `MeterProvider` is installed when
  the *first* sampler starts — permanently; OpenTelemetry 0.32 has no way to
  unregister an observable gauge. A host that samples before wiring its pipeline
  sees every other kafkaman series arrive and the queue series simply absent,
  which reads as "the sampler is not running". Now stated on
  `run_queue_metrics` and pinned by a binary of its own.
- The spec's "Example application" section described `apps/axum-outbox` mounting
  `admin_router`, applying `CorrelationLayer`, and supervising through
  `serve().with_runtime()`. None of it is on this branch — `apps/` is
  byte-identical to `main`, and that example's own README says it demonstrates
  the M1 path *without* `kafkaman-axum`. The section survived the revert of the
  work it described, leaving the spec claiming an end-to-end exercise that does
  not exist. The plan's test table had the matching problem one layer down: its
  `trace_propagation` row still promised "one trace id spans enqueue → … →
  dispatch", the shape corrected in the previous review pass.

**The narrowing that has no runtime symptom.** The workspace `opentelemetry`
dependency carried default features, so a `metrics`-only build linked the trace
API, the log API, `futures` and `thiserror` as well. Nothing breaks; the graph is
just wider than it says it is. It matters because **Cargo features are additive
and a host can never subtract one** — an axis a library enables without using is
permanent in every adopter's graph, while an axis it omits is one line for a host
that wants it. Each crate now enables exactly its own half, and `just opt-out`
asserts it with `cargo tree -e features`, which is the only thing that can: a
wrongly-wide build compiles perfectly.

**Why `tracestate` needed validating at all**, since nothing in this process
reads it. Because kafkaman does not merely hold these values — it stores them in
a column and writes them back onto the wire as standard headers at the next hop,
under its own name. Forwarding an unparseable `tracestate` makes kafkaman a
laundering step: whatever is downstream then has to deal with a header this
process chose to pass on. It is now checked against the W3C list grammar and
normalized, and — separately from the `traceparent` beside it — discarded when it
breaks. The asymmetry is deliberate: the trace id correlates, the vendor list
decorates, and losing the second must never cost the first.

**`otlp_wire` was searching protobuf for substrings.** The bytes were decoded
with `from_utf8_lossy` and then string-matched, which happened to work because
instrument names are ASCII inside length-prefixed fields — and could not tell a
metric named `kafkaman.scheduler.cycles` from a log line that mentioned one. It
now decodes `ExportMetricsServiceRequest`, `ExportTraceServiceRequest` and
`ExportLogsServiceRequest` properly, which buys assertions the old form could not
express at all: that the counter is exported as a *Sum* and the latency as a
*Histogram*, and that `kafkaman.relay.publish` is exported as a child of
`kafkaman.enqueue` in the same trace. That last one is the durable gap, asserted
on the wire rather than in memory — and it is invisible to a byte search, because
both span names appear either way.

**Two tests were betting on the clock.** A freshly inserted row was asserted to
breach a 1ms `max_queue_age`, which is true almost always and a flake the rest of
the time — on a suite whose flakes teach people to rerun rather than to read. The
rows are now backdated five minutes and the threshold is sixty seconds: the same
property with no clock in it. The other four `from_millis(1)` uses in those files
were checked and left alone; they are evaluated against an explicitly shifted
"now" and are already deterministic.

## [2026-08-26] review response | ten findings against the OTel branch, verified and fixed

An implementation review of the branch raised ten findings. Every one was checked
against the code before anything was changed; all ten held up, and the two that
were most defensible on paper turned out to be the two worth acting on hardest.

**The OpenTelemetry opt-out was false, and nothing could have caught it.**
`kafkaman --no-default-features` still linked `opentelemetry`, because
`kafkaman-config` and `kafkaman-axum` depended on `kafkaman-core` without
`default-features = false`. Cargo unifies features across the graph, so one
sibling asking for the default `traces` re-enables it for everyone — and every
build still succeeds, which is exactly why a `cargo check` gate had been passing
over it for the life of the branch. The manifests are fixed, and `just opt-out`
now asserts the *dependency graph* rather than the build: `cargo tree -i` exits
non-zero when a package is absent, so absence is the pass. The same recipe
enforces the other half of the ownership boundary — no crate under `crates/` may
reach an SDK or exporter at any feature combination — and a `just features`
matrix compiles all six `metrics`/`traces` combinations, four of which nothing
else ever built. Both run in CI.

**`LifecycleSampler` silently rounded the operator's rate.** It emitted every
`n`-th success with `n = round(1/rate)`, so `sample_success = 0.75` emitted
*everything* and `0.66` emitted half. Both errors are invisible from the config
file and one of them is expensive. It now tracks successes seen against events
owed, as an exact integer ratio: a float accumulator was tried first and rejected
for a reason worth recording — adding 0.1 ten times gives 0.9999999999999999, so
a one-in-ten rate would emit its tenth event on the eleventh message and stay a
message behind forever.

**Redrive could double-claim.** The candidate CTE selected without
`FOR UPDATE SKIP LOCKED` and the `UPDATE` matched on `message_id` alone, so two
concurrent redrives — an impatient second click, two people on one incident —
both claimed the same rows, both reported the full count, and a `clear_history()`
from one erased the failure evidence the other was preserving. Fixed with row
locking and the filter repeated in the `UPDATE`. The test was confirmed to fail
without the fix, by removing it: the old code does not merely over-report, it
*blocks*, and the assertion is a timeout.

**The `traceparent` parser was looser than the specification.** It accepted
uppercase hex and trailing fields on version `00`. Both matter more than they
look: the grammar is `HEXDIGLC`, and an uppercase trace id is a different string
to every backend that compares ids as bytes, so accepting one means kafkaman
stores it, forwards it, and hands another service an id that does not match what
its own SDK would have produced. Version 00 is a closed format; higher versions
still get the forward-compatibility rule, which is the half that costs something
if it is wrong.

**A relay with `traces` off stripped the `traceparent` from every message.**
`managed_headers` emitted only the captured current span context, so a build that
opted out of *producing* spans also opted its downstream neighbours out of
tracing — which is precisely what the feature documentation promised would not
happen. The row's stored context is now the fallback: downstream sees a link to
the enqueue rather than to the publish, one hop coarser and still the same trace.

**Two more log-to-span correlation gaps, of the kind `lifecycle_events` was
written to catch.** The dispatcher sampled its success events after
`dispatch_once` returned, by which time the `kafkaman.dispatch` span had closed —
the same defect the relay had, and the entry below predicted the fix. The sampler
moves into `kafkaman-sqlx` as `dispatch_once_sampled`. The Kafka consumer logged
each record's classification from the loop rather than from inside
`kafkaman.ingest`, and now reports from within it. Separately, the dispatch span
covered only the handler call, so a row whose message type had no registered
handler — the most common deployment-order failure there is — produced no span at
all; it now opens the moment a row is claimed and covers the failure accounting
too.

**`[observability]` was optional in one code path and required in another.**
`ResolvedConfig` guarded it with `contains("observability")` and defaulted it;
every other caller of `Config::observability()` got `MissingKey` for a section
the example config marks OPTIONAL. It now returns `Option` like `retention()`
does, and the defaulting lives in one place.

**The admin router mixed reads with a destructive write.** `admin_router` is now
read-only and `redrive_router` carries the redrive `POST` alone, so mounting the
destructive surface is a decision rather than a side effect and a deployment can
put a stricter policy on writes than on reads. A test asserts the read-only
router answers `404` for redrive, because that is the property a single added
`.route` line would quietly undo.

**Queue gauges re-registered on every sampler start.** OpenTelemetry 0.32 has no
way to unregister an observable-gauge callback and dropping the handle does not
remove it, so a same-process restart left the stopped sampler's callback
observing a frozen snapshot forever — every depth reported twice, once live and
once stale. Registration is now once per process with the snapshot repointed, a
second *concurrent* sampler is refused with a named error rather than silently
overwriting the first, and the test was confirmed to catch the leak.

**The trace-shape diagram described a trace that does not exist.** The decision
drew `kafkaman.ingest` inside the caller's trace and asked verification for "one
trace id" spanning enqueue through dispatch — contradicting Decision 6 in the
same document, which specifies a link, and a link starts a new trace. The
implementation and its test were right; the picture was wrong. Recorded as an
amendment rather than quietly corrected, because the wrong version is the one an
operator would assume, and a dashboard built on "follow the trace id end to end"
silently stops at the broker.

**Two smaller corrections.** Stuck rows reported a single `age_ms` that measured
from `created_at` on the send side and from `due_at` on the receive side — one
field name, two answers, on one operator screen. Both types now carry `age_ms`
(how long the message has existed) and `stuck_for_ms` (how long the fault has
lasted), which differ by however long the row queued legitimately first. And the
publish-side `outcome` attribute said `acknowledged` on one instrument and
`published` on another for the same event, which breaks the first query that
spans them.

**Waits in the observability suite are bounded.** Bare `recv().await` on a loop
that stops signalling hangs until the binary is killed, and what that produces is
a timeout with no failing assertion and no indication which wait it was.

Coverage added alongside: fractional and edge sampling rates, the W3C grammar on
both the parser and the Kafka header path, duplicate and future-version trace
headers, trace-column round trips including corrupt stored values, concurrent
redrive, absent and empty `[observability]` sections, unknown enum values and
unknown keys, relay config boundaries, the read-only router, sampler restart, and
the receive-side lifecycle event's trace correlation.

Findings judged not to need code changes: the reserved `[observability]` knobs
are validated and documented as reserved deliberately, and rejecting them would
be the breaking config change reserving them was meant to avoid.

Pages affected: `Cargo.toml`, `crates/kafkaman/Cargo.toml`,
`crates/kafkaman-axum/Cargo.toml`, `crates/kafkaman-axum/src/lib.rs`,
`crates/kafkaman-config/Cargo.toml`, `crates/kafkaman-config/src/config.rs`,
`crates/kafkaman-config/src/sections.rs`,
`crates/kafkaman-config/src/serde_enum.rs`,
`crates/kafkaman-core/src/lifecycle.rs`, `crates/kafkaman-core/src/trace.rs`,
`crates/kafkaman-rdkafka/src/consumer.rs`,
`crates/kafkaman-rdkafka/src/publisher.rs`,
`crates/kafkaman-sqlx/src/dispatch.rs`,
`crates/kafkaman-sqlx/src/operability.rs`, `crates/kafkaman-sqlx/src/replay.rs`,
`crates/kafkaman-sqlx/src/resolved_config.rs`,
`crates/kafkaman-worker/src/dispatcher.rs`,
`crates/kafkaman-worker/src/queue_metrics.rs`, `justfile`,
`.github/workflows/ci.yml`, `tests/durable-send/`, `tests/observability/`,
`wiki/decisions/metric-instrument-and-attribute-schema.decision.md`,
`wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`,
`wiki/specs/m6-observability-operability.spec.md`,
`wiki/compatibility/m6-observability-operability-api.compat.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-26] implementation | OTel Phases 5 and 6, and the example wiring taken back out

Closes the OpenTelemetry completion plan except for the compose profile, which is
deferred deliberately.

**`apps/axum-outbox` is back to its `main` state, exactly, and the branch history
was rewritten so it never left it.** The example is being replaced wholesale by
the one under construction in a separate worktree, so telemetry wiring written
against it would be written twice — and a capability demonstration belongs in the
example that survives. The revert is total: the branch's `apps/` tree is
byte-identical to `main`, including the M6-era admin wiring, because the whole
directory is going. The four commits on this branch touch `crates/`, `tests/`,
`wiki/`, and the workspace manifest, and nothing else.

That cost one thing worth keeping, so it was moved rather than lost.
`telemetry_export` proved that OTLP bytes actually leave a process, which is the
one property an in-memory exporter cannot show — an exporter that never posts
looks exactly like a silent instrument from inside the process. It now lives in
`tests/observability/otlp_wire`, where the ownership decision already permits
exporter dependencies, and it got stronger in the move: it runs a real relay loop
against a real database and asserts kafkaman's *own* instrument names, span
names, and log lines on the wire, rather than a synthetic probe metric in an
example.

**Phase 5 is complete**, at twelve binaries. The plan named six; the extra six
each exist because building the feature turned up a property that needed pinning
— `single_cycle_silence`, `queue_gauge_staleness`, `trace_absent`,
`trace_root_enqueue`, `ingest_disjointness`, `otlp_wire`. The one binary the plan
named that does not exist is `metrics_disabled`: a test binary cannot assert a
`--no-default-features` build from inside a default build, so `just lint` now
runs `cargo check -p kafkaman --no-default-features` and the no-op twins are
compiled by the same gate as everything else.

**`lifecycle_events` found a real defect, which is why it exists.** The sampled
success events were emitted from the relay loop after each cycle, outside any
span — so they reached the log signal with no `trace_id`, and the log-to-trace
pivot that `sample_success` exists to provide did not work. The test was written
to assert the pivot and failed on it.

The cause is an ecosystem detail worth recording, because it is invisible from
either side alone: `opentelemetry-appender-tracing` stamps a log record from the
*OpenTelemetry* context current at emission and never reads the `tracing` span
stack, while `tracing-opentelemetry` bridges spans without attaching them to the
OpenTelemetry context. The two stacks simply do not meet. Version 0.32 of the
appender has no feature to make them.

So success events now emit per row, inside that row's `kafkaman.relay.publish`
span, with the span's context attached for the duration of the emission —
`kafkaman_core::attach`, whose guard is scoped so it never crosses an `await`.
The sampling rate is unchanged and still carries across cycles; what changed is
that each event is attributable to the message it describes. The receive-side
event was left uncorrelated at the time and recorded as such; the review pass
below closed it by taking the first of the two options named here — the sampler
moves into `kafkaman-sqlx` as `dispatch_once_sampled`.

**`admin_http`** covers every operator route over a real socket: health,
readiness, both depth summaries, stuck rows, DLQ, redrive with its 404 for an
unregistered type and its 400 for an unbounded request, and the correlation
header round trip in both directions. The existing coverage called handlers
through an in-process `Router`, which skips status codes, decodable JSON,
router-resolved path parameters, and the correlation hop entirely — and that
surface is the one that answers an operator's questions with no telemetry backend
at all.

**Phase 6.** The M6 spec now records that M6 shipped instrumentation rather than
a pipeline, and names the two statements in it that became wrong rather than
merely partial: instruments are no longer cached process-wide, and the ingest
partition is no longer guarded only by a `debug_assert` that release builds
delete. The compatibility note carries the metric schedule, the queue metrics
loop, the trace-context schema, API and wire format, and the lifecycle emission
change. The three reserved `[observability]` fields were re-examined now that the
log signal exists and **left reserved**, with the reasoning recorded so it is not
re-opened: `level` would duplicate and fight the host subscriber's filter, and
`payload`/`headers` still have nothing to govern — the metric-schema decision
forbids deriving any attribute from message content, which makes reserved the
permanent answer on that surface.

Evidence: `just lint` clean, including the disabled-twin check; `cargo test
--workspace --all-features` (225 passed, 4 ignored).

Pages affected:
- `crates/kafkaman-core/src/trace.rs`, `lib.rs`
- `crates/kafkaman-worker/src/relay.rs`
- `tests/observability/` — `Cargo.toml`, and the `otlp_wire`,
  `lifecycle_events`, `admin_http` binaries
- `wiki/specs/m6-observability-operability.spec.md`
- `wiki/compatibility/m6-observability-operability-api.compat.md`
- `wiki/plans/opentelemetry-completion.plan.md`
- `wiki/index.md`, `wiki/log.md`

## [2026-08-25] implementation | OTel Phases 2 and 3: traces across both durable gaps, and logs that point at them

The half M6 did not build. Metrics answer "how much"; a trace answers "what
happened to *this* message", and kafkaman puts two gaps in the middle of that
question that no in-memory context survives.

**The problem, stated once.** The outbox pattern separates enqueue from publish
in time — different task, possibly different process, after a crash, after a
lease expiry. A producer span opened at publish time is therefore orphaned from
the transaction that created the row, and the trace an operator most wants is
exactly the one the pattern breaks. The receive side has the same gap: ingest
stores a row, dispatch claims it later. Trace context must be durable, stored
beside the row, restored when the row is acted on — which is the journey
`correlation_id` already makes.

**The schema change, which is the irreversible part.** Two nullable columns,
`traceparent` and `tracestate`, on every per-type outbox table *and* every
received table, added by `AddOutboxTraceContext` and `AddReceivedTraceContext`.
Existing rows keep null, and null is a state the runtime already treats as
ordinary, so a deployment that applies the changesets and rolls the binary back
keeps working.

**Four spans**: `kafkaman.enqueue` inside the caller's transaction,
`kafkaman.relay.publish` parented from the row's stored context,
`kafkaman.ingest` linked to the producer, `kafkaman.dispatch` parented from the
received row's context. The consumer links rather than parents because it polls a
batch that may hold records from many unrelated traces; dispatch parents rather
than links because by then exactly one row has been claimed.

**A third header namespace.** `traceparent` and `tracestate` carry no
`kafkaman-` prefix — the entire value of the standard is that a consumer which
has never heard of kafkaman still reads them. `RecordHeaders` grew a third
bucket: matched case-insensitively, first-wins like the reserved namespace,
stripped from the user headers a handler sees. A producer sending them is the
normal case, not an error.

**Logs** are `opentelemetry-appender-tracing`, stamping every record with the
ids of the span it was emitted in. The appender is a host-side crate, so this
half was demonstrated in the example and reverted with the rest of the example
wiring on 2026-08-26; `tests/observability/otlp_wire` and `lifecycle_events`
carry the proof instead. That stamping is the whole feature and the
reason logs had to follow traces rather than precede them: the lifecycle events
`LifecycleSampler` already emits become trace-correlated records with no further
work, which is what `sample_success` was for.

**Five things the plan had wrong, all found by building it.**

1. *The received table needs the columns too.* The decision persisted context on
   the outbox row only, and drew dispatch as a child of ingest with no gap
   between them. There is a gap, and it is the same gap.

2. *A tracer with no caller span still produces context.* The decision said a row
   enqueued outside any span has none. True only with no tracer installed —
   `enqueue` opens its own span, so a background job gets a root span, the row
   carries its context, and the publish descends from it. That is better than the
   rule described, and both cases are now tested: `trace_absent` for an untraced
   process, `trace_root_enqueue` for a traced one with no caller span. The test
   that found this was written asserting the documented behaviour and failed.

3. *Outgoing user headers named `traceparent` must be dropped.* An application
   forwarding an incoming request's headers wholesale is doing something
   ordinary; publishing its stale copy would put it on the wire ahead of the real
   one, and first-wins on the consumer side would then link the wrong span.
   Dropped at publish, logged at debug, kept in the row for triage. Rejecting at
   enqueue was considered and refused: it turns a reasonable pattern into a
   runtime error.

4. *The W3C format is implemented by hand.* The propagators live in
   `opentelemetry_sdk`, which no `crates/` manifest may touch. `traceparent` is
   four fixed fields; `kafkaman-core::trace` parses and formats it directly,
   accepting unknown versions with trailing fields per the Recommendation's
   forward-compatibility rule and rejecting the all-zero ids and reserved `ff`
   version.

5. *`tracing-opentelemetry` is now allowed in library crates*, which amends the
   ownership decision's "the API crate and nothing more". Reading the ambient
   span's context has no other route — `tracing` spans live in the subscriber's
   registry, not on the OpenTelemetry context stack. Its published manifest was
   checked rather than assumed: `opentelemetry`, `tracing`, `tracing-core`,
   `tracing-subscriber`, and **no SDK**. The substance of the boundary holds.

**`traces` is a default-on feature with a no-op twin**, plumbed through every
library crate and the facade, matching `metrics`. Turning it off drops the
OpenTelemetry dependencies and stops this process producing context — but stored
context is still read and still forwarded, so a service with tracing compiled out
does not break propagation for its neighbours. `just lint` now compiles the
disabled twins (`cargo check -p kafkaman --no-default-features`), because nothing
else in the gate ever did and a twin that stops compiling is otherwise discovered
by an adopter.

`tests/observability/trace_propagation` is the gate: one message, a real
Redpanda broker, both durable gaps, and assertions on every parent and link in
the shape the decision draws.

Evidence: `just lint` clean; `cargo test --workspace --all-features` (223 passed,
4 ignored, up from 213).

**Phase 4 is deliberately incomplete.** The Elasticsearch/Kibana compose profile
is not built. It was specified as an extension of the two-service example's
compose file, which is not on this branch, and `apps/axum-outbox` is being
replaced by the example under construction in a separate worktree. Writing it
here would be writing it twice. `tests/observability/otlp_wire` — three
providers, installed before any kafkaman component, flushed on shutdown, asserted
on the wire — stands as the reference for that port.

Pages affected:
- `crates/kafkaman-core/src/trace.rs` (new), `rows.rs`, `lib.rs`, `Cargo.toml`
- `crates/kafkaman-sqlx/src/outbox_enqueue.rs`, `received_storage.rs`,
  `dispatch.rs`, `schema_sql.rs`, `changesets.rs`, `outbox_mark.rs`,
  `queries.rs`, `lib.rs`, `Cargo.toml`
- `crates/kafkaman-rdkafka/src/publisher.rs`, `ingest_record.rs`, `consumer.rs`,
  `tests.rs`, `Cargo.toml`
- `crates/kafkaman-worker/src/relay.rs`, `Cargo.toml`
- `crates/kafkaman/Cargo.toml`
- `tests/observability/` — `src/lib.rs`, `Cargo.toml`, and the
  `trace_propagation`, `trace_absent`, `trace_root_enqueue` binaries
- `Cargo.toml`, `Cargo.lock`, `justfile`
- `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`
- `wiki/decisions/telemetry-pipeline-ownership.decision.md`
- `wiki/compatibility/m6-observability-operability-api.compat.md`
- `wiki/plans/opentelemetry-completion.plan.md`
- `wiki/index.md`, `wiki/log.md`

## [2026-08-25] implementation | OTel Phase 1: the metrics surface, and three things the plan had wrong

Phase 1 of `wiki/plans/opentelemetry-completion.plan.md`, with its Phase 5 test
binaries landing beside it rather than trailing. M6 shipped counters only —
"how many" without "how long" or "how much is waiting", which are the two
questions an operator actually opens a dashboard for.

**Latency.** Three histograms, in seconds. `kafkaman.relay.publish.duration` is
broker-side only: the claim has committed and the mark has not run, so it is
separable from queue delay. `kafkaman.dispatch.duration` spans the whole
`dispatch_once` call — claim, handler, commit — because that is what one dispatch
costs and so what sizes a dispatcher pool; isolating the handler would mean
putting an OpenTelemetry dependency in the crate that owns the SQL, which is a
worse trade. `kafkaman.outbox.time_to_publish` measures `occurred_at` to broker
acknowledgement, and is the one instrument here no general-purpose messaging
library would have: the outbox pattern's premise is that enqueue and publish are
separated in time, that separation is the library's core latency, and only
kafkaman can see both ends.

**Depth.** Four observable gauges over `outbox_status_summary` and
`received_status_summary` — already written, already integration-tested, already
index-served, and until now reachable only by remembering to curl a route. A
gauge callback is synchronous and cannot await a database round trip, so
`run_queue_metrics` owns the queries on its own interval and the callbacks read
the snapshot it maintains. The refresh interval is deliberately the loop's rather
than the SDK's: scraping faster must not query harder. Empty statuses report an
explicit zero so a draining queue draws a line to zero instead of stopping
mid-chart, while `oldest_age` reports nothing for them, because the age of no
rows is not zero.

**Three things this slice found, all now amendments to the schema decision.**

1. *Bucket boundaries are part of the compatibility surface.* The OpenTelemetry
   defaults run 0 to 10,000 and are millisecond-scaled; against a `s` unit every
   realistic observation lands in the first bucket. That is not a coarse
   histogram, it is an empty one. Two explicit sets: 5ms–10s for work kafkaman
   performs in one step, 50ms–15min for latency that includes queue time.

2. *Semconv attributes belong on every series keyed by topic.* The schedule
   listed `messaging.system`/`messaging.destination.name` only on
   `kafkaman.kafka.publish.records`, while the decision states the rule
   generally. Read as an oversight rather than an exception: a dashboard
   grouping by `messaging.destination.name` would otherwise find kafkaman's
   publish counts and miss its publish latency.

3. *Gauge staleness cannot be a gap.* The decision asked callbacks to "report
   staleness rather than hanging", which was implemented as: stop observing once
   the snapshot is too old, so a stalled sampler shows as a hole rather than a
   flat line at a number that quietly stopped being true. **It does not work.**
   An asynchronous gauge under cumulative temporality republishes its last
   recorded value every collection cycle whether or not the callback observed
   anything. The test written to prove the gap proved the opposite, and it now
   pins the real behavior. `kafkaman.queue.sample_age` is the answer instead: a
   depth of 2 means nothing alone, a depth of 2 beside a sample age of 400
   seconds says exactly how much to trust it. Temporality is the host's to choose
   and a library must not force a view on it, so this is an added instrument
   rather than a configuration demand. `stale_after` is gone from
   `QueueMetricsConfig` — a knob that cannot do what it claims is worse than no
   knob.

**Tests, five binaries, one concern each.** `metrics_surface` asserts names,
kinds, units, attribute keys and values from a real relay loop, restating the
schedule literally rather than deriving it from the code under test.
`single_cycle_silence` pins that calling the public `relay_once` helper records
nothing — the separation that keeps a hand-driven cycle out of a deployment's
series, one line from being undone by accident every time an instrument is added.
`queue_gauges` covers depth, age, and the zero-fill. `queue_gauge_staleness`
covers finding 3 above. `ingest_disjointness` runs a real Redpanda broker behind
`--features redpanda` and asserts that the ingest outcome counters sum to exactly
the records consumed — the invariant whose violation shipped the double-count
defect, and which `debug_assert` guards in precisely the builds nobody runs.

`relay_once` grew a private inner form taking the loop's instruments, because
per-publish latency can only be measured inside it while the scheduler counters
must stay out of it. `SchedulerMetrics` is now composed into `RelayMetrics` and
`DispatchMetrics` rather than carrying relay-specific methods; the purger, which
needs neither histogram, still uses it directly.

The sampler was wired into the example beside the relay, and started only when a
telemetry endpoint was configured — without a pipeline the callbacks never run
and it would be querying Postgres on an interval for nobody. That wiring was
reverted with the rest of the example's telemetry on 2026-08-26; the condition is
worth carrying into the incoming example, which is why it is recorded here.

`opentelemetry-semantic-conventions` joins `kafkaman-worker` and
`kafkaman-rdkafka` as an optional dependency of their `metrics` features. It is
attribute-key constants only — no runtime, no SDK — so the ownership boundary
stands. Its messaging keys sit behind `semconv_experimental` upstream, which is
enabled; the constants are still better than hand-typed keys that drift.

Evidence: `just lint` clean; `cargo test --workspace --all-features` (213 passed,
4 ignored, up from 208).

Pages affected:
- `crates/kafkaman-worker/src/metrics.rs`, `queue_metrics.rs` (new), `relay.rs`,
  `dispatcher.rs`, `lib.rs`, `Cargo.toml`
- `crates/kafkaman-rdkafka/src/metrics.rs`, `Cargo.toml`
- `tests/observability/` — `src/lib.rs`, `Cargo.toml`, and the
  `metrics_surface`, `queue_gauges`, `queue_gauge_staleness`,
  `single_cycle_silence`, `ingest_disjointness` binaries
- `Cargo.toml`, `Cargo.lock`
- `wiki/decisions/metric-instrument-and-attribute-schema.decision.md`
- `wiki/compatibility/m6-observability-operability-api.compat.md`
- `wiki/plans/opentelemetry-completion.plan.md`
- `wiki/index.md`, `wiki/log.md`

## [2026-08-25] implementation | OTel Phase 0 step 5: host pipeline prototyped in the example, then reverted

Closes Phase 0 of `wiki/plans/opentelemetry-completion.plan.md`.

**Read this entry as a design record, not a change record.** The wiring described
below was built in `apps/axum-outbox` and then reverted on 2026-08-26, because
that example is being replaced wholesale by the one under construction in a
separate worktree — telemetry wiring belongs in the example that survives rather
than being written twice. The branch carries no example wiring at all, and the
history was rewritten so it never did. What survives is the reasoning, which the
incoming example inherits, and `tests/observability/otlp_wire`, which does the
same thing in a test process.

The example was the phase's own warning made flesh: it installed a
`tracing_subscriber` and no `MeterProvider`, then started a relay — precisely the
ordering that binds every instrument to the no-op provider for the life of the
process. Every adopter copying it inherited a silent metric surface.

`apps/axum-outbox/src/telemetry.rs` is the host half of the pipeline: an OTLP/HTTP
metrics exporter behind a `PeriodicReader` at a 15-second interval, installed as
the global provider, tagged `service.name=axum-outbox`. `main` calls it above the
pool, the migrations, and the relay, which is the ordering contract stated by
placement as well as by comment. Metrics only — no traces, no logs, no compose
changes; Phase 4 completes it.

**Two judgment calls the plan did not settle.**

*No endpoint means no provider.* With neither `OTEL_EXPORTER_OTLP_ENDPOINT` nor
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` set, `init_metrics` returns `Ok(None)` and
installs nothing, rather than defaulting to `localhost:4318` as the OTel
specification does. The example's telemetry backend is optional by design — it
sits behind a compose profile — so the common case is running without one, and a
pipeline aimed at a closed port reports a failed export every interval, forever.
An example that logs an error every fifteen seconds out of the box teaches
adopters to ignore export failures. The skip is logged, because "no metrics
configured" and "metrics configured but invisible" are indistinguishable
otherwise. A whitespace-only value counts as unset, which is what an unresolved
`${VAR}` in a compose file produces.

*Flush before propagating.* `main` holds the `serve` result rather than applying
`?` to it, shuts the pipeline down, and only then returns the error. A process
exiting because something failed is exactly the process whose last metrics window
matters, and `?` would have discarded it. `SdkMeterProvider::shutdown` performs a
final collect-and-export, so it is the flush as well as the teardown; failures
there are logged, not returned, so a telemetry problem cannot replace the reason
the process is actually exiting.

**Proving it, without a backend.** `apps/axum-outbox/tests/telemetry_export.rs`
binds a loopback socket, points the exporter at it, records through the global
meter, and asserts the resulting `POST /v1/metrics` carries protobuf, the
instrument name, and the service name. It runs in milliseconds and needs no
container, because a socket is enough to prove the bytes left the process — and
without it, "the example exports" would rest on the same kind of unobserved
assertion that this entire plan exists to retire.

Two unit tests sit beside it: endpoint precedence resolved through an injected
lookup rather than by mutating a shared environment, and pipeline construction
inside a Tokio runtime. The second is not ceremony. `main` is `#[tokio::main]`,
and the default OTLP HTTP client is `reqwest`'s *blocking* client, which cannot be
constructed on a thread already driving a runtime; `opentelemetry-otlp` sidesteps
that by building it on a spawned thread. That is an implementation detail of a 0.x
dependency on which the example's only startup path depends, so it is pinned.

The workspace `opentelemetry_sdk` entry dropped the `testing` feature —
`tests/observability` adds it locally now, so the example does not carry a
test-only feature. `opentelemetry-otlp` joins the workspace with `http-proto` and
`reqwest-blocking-client` only. All three are dependencies of `apps/axum-outbox`;
no `crates/` manifest gained any of them.

The example README, which still described the app as demonstrating M1 "without
`kafkaman-axum`" after M6 mounted the admin routes, was corrected while
documenting the telemetry surface and the ordering contract.

Evidence: `just lint` clean (fmt, clippy `-D warnings`, rustdoc `-D warnings`,
lib tests); `cargo test --workspace --all-features` (208 passed, 4 ignored, up
from 205).

Pages affected (as landed after the 2026-08-26 revert; the `apps/` files this
entry describes are not on the branch):
- `Cargo.toml`, `Cargo.lock`, `tests/observability/Cargo.toml`
- `wiki/plans/opentelemetry-completion.plan.md`
- `wiki/compatibility/m6-observability-operability-api.compat.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-08-25] implementation | OTel Phase 0: instruments owned by the loop, and a suite that can see them

The first slice of `wiki/plans/opentelemetry-completion.plan.md`. Phase 0 steps
1-4; step 5 (example wiring) follows separately.

**The defect.** `worker_metrics()` and `kafka_metrics()` cached their counters in
process-wide `OnceLock`s built from `global::meter("kafkaman")` at first use. An
OpenTelemetry instrument binds to whichever `MeterProvider` is installed when it
is *created* and cannot be rebound, so the provider present at the first record
was the provider every later component reported to for the life of the process.
A host that started a relay before finishing its telemetry pipeline got a
permanently silent metric surface — no error, no warning, no diagnosis. It also
made the surface untestable, because an integration test binary is one process
with one global provider.

**The fix.** Instruments are built by the component that owns them:
`SchedulerMetrics` at run-loop start (relay, dispatcher, purger), `PublishMetrics`
at `RdkafkaPublisher` construction, `IngestMetrics` at ingest-loop start. The
`enabled`/`disabled` twins still live in one file each so the pair cannot drift,
and every call site kept its shape — the free functions became methods on the
owned struct, so `record_scheduler_cycle(&attrs)` is now `metrics.cycle()`.
`SchedulerAttrs` was folded into `SchedulerMetrics` rather than kept beside it:
the pre-built `[scheduler, message_type]` attribute array exists to keep the
allocation off the hot path, and it has the same lifetime as the instruments now.

The publisher binds earlier than the loops do, so the adopter contract is
*install the provider before constructing kafkaman components or starting loops*,
not merely before starting loops. Recorded in
`wiki/compatibility/m6-observability-operability-api.compat.md`.

**Units, since the instruments were open anyway.** Every counter now declares one
(`{cycle}`, `{row}`, `{error}`, `{record}`, `{commit}`) — Phase 1 step 3, taken
early because it is one line per instrument. No name or attribute changed.

**`tests/observability/`**, a new workspace member mirroring `durable-send`'s
layout: a shared `src/` harness plus one binary per concern. It brings its
Postgres containers and message fixtures from `durable-send-tests` rather than
copying them; if a third suite appears, that scaffolding should move into
`kafkaman-test` instead of being depended on sideways. `MetricPipeline` installs
an `SdkMeterProvider` over an `InMemoryMetricExporter` and holds both, because
dropping the provider shuts the reader down and a flush after that collects
nothing. `SignallingPublisher` acknowledges and announces, so loop tests wait on
a message the loop itself sent rather than sleeping on a guess.

The first binary, `provider_ordering`, reproduces the adopter's ordering exactly:
run a relay, *then* install the SDK, run the relay again, assert the second run's
`kafkaman.scheduler.cycles` reached the exporter. Against the old `OnceLock` it
fails with an empty collection. It is worth a real database because what is under
test is *where kafkaman constructs its instruments* — a property of the run loops,
not of the OpenTelemetry API, which reproduces the underlying hazard in twenty
lines without one.

`opentelemetry_sdk = { version = "0.32", features = ["metrics", "testing"] }` is a
workspace **dev-dependency**. No `crates/` manifest gained an SDK or exporter
dependency; the host still owns the pipeline.

Evidence: `cargo test --manifest-path tests/observability/Cargo.toml --tests`
(1 passed), `cargo test --workspace --all-features` (205 passed, 4 ignored),
`cargo check --workspace --all-features --all-targets` clean.

Pages affected:
- `crates/kafkaman-worker/src/metrics.rs`
- `crates/kafkaman-worker/src/relay.rs`
- `crates/kafkaman-worker/src/dispatcher.rs`
- `crates/kafkaman-worker/src/purger.rs`
- `crates/kafkaman-rdkafka/src/metrics.rs`
- `crates/kafkaman-rdkafka/src/publisher.rs`
- `crates/kafkaman-rdkafka/src/consumer.rs`
- `crates/kafkaman-axum/src/lib.rs`
- `tests/observability/` (new)
- `Cargo.toml`, `Cargo.lock`
- `wiki/compatibility/m6-observability-operability-api.compat.md`
- `wiki/log.md`

## [2026-08-25] review | external OpenTelemetry readiness review, and two plan corrections

An external readiness review of the post-M6 OpenTelemetry state, scored 6.5/10,
is filed at `wiki/reviews/m6-opentelemetry-readiness-review.reference.md`.

**All twelve of its line citations were verified against `d9389d4` and every one
resolves to exactly what it claims.** That is recorded because it determines how
much of a review can be trusted without re-deriving it, and it is not the common
case. Two further claims were checked past their citations: the four-live /
three-reserved observability config split holds, and `LifecycleSampler` is
genuinely wired into both loops (`relay.rs:103`, `dispatcher.rs:40`) rather than
merely declared. All five of its gaps stand, and match an independent pass over
the same code.

Assessment recorded in the reference page: the review **understates** one thing —
"likely inert unless the host wires an SDK" is conditional where the fact is not,
since no SDK exists in the repository at all and no metric has ever been observed
— and is **generous** in another, citing a decision page written the same day as
evidence of established practice. Its 6.5 also averages two different
measurements: a strong foundation and a near-zero working-OTel surface. The split
is actionable where the average is not.

**Two findings adopted, both gaps in the review's own coverage:**

1. **Logs were absent from it.** Traces got a dedicated gap entry; logs appeared
   once, inside its final step. For the Elastic target that is a third of the
   value — trace-correlated records are the Kibana log→trace pivot and the reason
   `sample_success` exists. The plan's sequencing now names three signals in a
   table.

   This surfaced **an error in the plan itself**, now corrected. An earlier
   revision advised cutting Phase 3 before Phase 2 under pressure. That is
   backwards: Phase 3 is cheap *because* Phase 2 has run, so cutting it afterward
   forfeits most of the log-side value for a small saving. The correct cut is
   Phases 2 and 3 together — traces without logs is a coherent product, logs
   without traces is just the Filebeat path.

2. **Example wiring moved from last to Phase 0.** The review scheduled it as step
   7, which leaves the example without a `MeterProvider` through steps 3–6 —
   every instrument added along the way verifiable only in tests, and a contract
   about application startup never exercised by an application. A metrics-only
   provider is now Phase 0 step 5; Phase 4 completes the pipeline.

**Added independently of the review:** Phase 2 now flags that it holds the plan's
only irreversible step. Persisting trace context is a migration on every per-type
outbox table; everything else in the plan is additive code that can be revised,
and a migration adopters have run cannot.

The review's recommended order otherwise maps onto the filed phases exactly —
two independent passes producing the same sequence, differing only on where
example wiring belongs.

Pages affected:
- wiki/reviews/m6-opentelemetry-readiness-review.reference.md (new)
- wiki/plans/opentelemetry-completion.plan.md
- wiki/index.md
- wiki/log.md

## [2026-08-25] decide | telemetry pipeline direction and four decisions

Documented the OpenTelemetry completion work ahead of implementation: one
proposal, four decisions, a revised plan, and two corrections to existing pages.

**Proposal 13** frames the question proposal 04 did not ask. Proposal 04 settled
*what observability policy kafkaman should have* and M6 implemented it. Nobody
had asked whether any of it reaches a backend. It does not: the workspace holds
the `opentelemetry` API crate and no SDK, so every counter is inert by design of
the OpenTelemetry API, in every build and every test.

The four decisions:

- **telemetry-pipeline-ownership** — host owns the SDK, kafkaman uses the API
  only, no exporter under `crates/`. The substantive change is reversing M6's
  process-wide `OnceLock` for instruments in favour of per-run-loop construction.
  The `OnceLock` binds every instrument to whichever provider existed at first
  record, so a host that starts a relay before building its pipeline gets a
  permanently silent metric surface with no diagnostic — and a test binary, being
  one process with one global provider, cannot observe metrics at all. That is
  the mechanical reason no test here has ever asserted one.
- **metric-instrument-and-attribute-schema** — the instrument schedule becomes a
  public compatibility surface. Keep `kafkaman.*` names, add standard messaging
  attributes beside our own so the series are joinable in a mixed deployment, add
  latency histograms and queue-depth gauges, declare units, and pin the ingest
  disjointness invariant with a real test. `kafkaman.outbox.time_to_publish` is
  called out as the one instrument no general-purpose messaging library would
  have: the enqueue-to-ack gap is the outbox's core latency and is invisible to
  both the database and the broker.
- **trace-context-propagation-and-w3c-headers** — the heaviest of the four,
  because it amends a ratified decision. Detail below.
- **telemetry-backend-and-example-topology** — Elastic as reference backend,
  chosen because it takes all three signals natively over OTLP/HTTP and its APM
  view is built on the trace-and-log correlation the plan produces. The example
  exports direct with no collector; the collector is documented separately as the
  production topology, because a compose file is the most-copied artifact in any
  repository.

**The amendment, and a correction to how it was previously described.** Earlier
notes in this log and in the plan described the W3C header question as settling
OQ5 "which M7 is scheduled to ratify". That was wrong in both halves. The roadmap
records OQ5 as *already ratified* — opaque headers, reserved `kafkaman-*`
namespace — and `message-identity-and-header-namespace.decision.md` is the
Accepted decision fixing it at exactly two namespaces. So this work does not
resolve an open question; it amends a ratified one, which is treated as heavier
and kept as narrow as possible: a third namespace containing exactly
`traceparent` and `tracestate`, every other rule untouched.

**A contradiction found while checking that.** The roadmap simultaneously
recorded OQ5 as ratified in its status section and listed "ratifying OQ5" as an
M7 deliverable. Both cannot be true. Corrected: OQ5 is ratified, its amendment is
noted, and nothing about it remains for M7.

**A scope reduction found while writing the backend decision.** The two-service
example already ships `examples/compose.yaml` with postgres, redpanda, console,
product, and order, plus a Dockerfile, README, and smoke.sh. The Elastic work is
adding two services to a working stack behind a profile, not building one.

**One page deliberately not written.** A reference page documenting the Elastic
deployment as a runnable procedure is deferred. The example it extends is
mid-merge with unresolved conflicts, being reconciled against the same module
refactor the M6 port was replayed onto, and cannot currently build. Run
instructions written against it would be unverifiable. The design is captured in
the backend decision; the procedure is filed once it has been executed.

Pages affected:
- wiki/proposals/13-telemetry-pipeline-completion.proposal.md (new)
- wiki/decisions/telemetry-pipeline-ownership.decision.md (new)
- wiki/decisions/metric-instrument-and-attribute-schema.decision.md (new)
- wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md (new)
- wiki/decisions/telemetry-backend-and-example-topology.decision.md (new)
- wiki/decisions/message-identity-and-header-namespace.decision.md
- wiki/plans/opentelemetry-completion.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-25] plan | OpenTelemetry completion

Filed `wiki/plans/opentelemetry-completion.plan.md` after establishing that M6
shipped OpenTelemetry instrumentation without an OpenTelemetry pipeline.

The finding that drives the plan: the workspace depends on `opentelemetry
= "0.32"` — the API crate — and no `opentelemetry_sdk` exists anywhere in the
repository. `global::meter("kafkaman")` therefore resolves to the no-op provider
in every build and every test, so no metric M6 added has ever been observed.
`apps/axum-outbox` installs a `tracing_subscriber` but no `MeterProvider`, so an
adopter copying the example gets no metrics either. Traces, a log bridge, and
trace-context propagation are absent entirely.

Two defects the plan must fix, both found while scoping it:

1. `OnceLock`-cached instruments bind to whatever meter provider exists at first
   record. A host that starts a relay before building its OTel pipeline gets a
   permanently silent telemetry surface with no error — and it makes the surface
   untestable, since a test binary is one process with one global provider.
2. `record_ingest_stats`'s disjointness invariant is a `debug_assert`, compiled
   out of release. It is the only guard against a repeat of the double-count
   defect found in the M6 review, which was caught by reading rather than by a
   test.

Two decisions the plan defers rather than pre-empts: whether to add standard
messaging semantic-convention attributes alongside the `kafkaman.*` instrument
names, and how W3C `traceparent` fits a header model that today has exactly two
namespaces. The second is the same boundary question as OQ5, which the roadmap
schedules M7 to ratify; resolving it in Phase 2 settles it with a concrete case.

Verified while scoping: `opentelemetry_sdk` 0.32.1, `opentelemetry-otlp` 0.32.0,
`tracing-opentelemetry` 0.33.0, `opentelemetry-appender-tracing` 0.32.0, and
`opentelemetry-semantic-conventions` 0.32.1 are all published and version-aligned
with the `opentelemetry` 0.32 already in the workspace. Elasticsearch ingests
OTLP/HTTP natively — no collector required — which is what makes the ELK target
reachable from this plan without a translation layer.

The plan does not block the M6 merge on its own terms: M6's stated exit criterion
is the queue-depth/DLQ/stuck surface, which is integration-tested against real
Postgres. Phase 0 plus the `metrics_surface` test binary are the minimum that
makes the spec's existing OpenTelemetry claim verifiable.

Pages affected:
- wiki/plans/opentelemetry-completion.plan.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-25] correct | two rebase follow-ups closed

The M6 rebase entry below left two items open. Both are now resolved against the
code, and one of them was wrong.

**The stale compatibility claim was real and is fixed.**
`wiki/compatibility/m6-observability-operability-api.compat.md` credited M6 with
the dynamic Redpanda host port and named the helper `start_redpanda`. On this
base the change came from the module-and-test-separation refactor, and the
helper is `redpanda()` (`tests/durable-send/src/containers.rs:81`), with
`start_redpanda_on` binding and `available_host_port` picking. The described
*behavior* was accurate; only the provenance and the name were wrong, so the
section is rewritten with an explicit attribution correction rather than
deleted. The page's `Sources` list was stale for the same reason — it still
pointed at the five crate `lib.rs` roots the refactor dissolved — and now names
the modules M6's code actually lives in.

**The dropped-test finding was a false alarm and is retracted.** The refactor did
not drop `migration_context_and_replay_guardrails_are_explicit`; it split it in
two and kept every assertion:

- `crates/kafkaman-sqlx/src/tests/changelog.rs:36`
  `migration_context_selects_by_declared_context` keeps `with_context`,
  `with_applied_by`, and `applied_by()`.
- `crates/kafkaman-sqlx/src/tests/replay.rs:47`
  `a_replay_without_a_row_cap_refuses_to_build` keeps `dry_run_preview`, the
  `InvalidReplay` build rejection, and the `max_rows(0)` rejection verbatim.

The only assertion not carried across literally is the direct `ctx.matches(..)`
pair. That is not a coverage loss: `matches` is now `pub(crate)`
(`crates/kafkaman-sqlx/src/changeset.rs:90`) and is exercised through
`MigrationStepReport::context_skip`, which asserts the operator-visible skip
report rather than the predicate underneath it. Testing through the reported
behavior is stronger than testing the private predicate, so nothing is restored.

Pages affected:
- wiki/compatibility/m6-observability-operability-api.compat.md
- wiki/log.md

## [2026-08-25] rebase | M6 replayed onto the module-separation base

`main` fast-forwarded from `332721a` to the module-and-test-separation branch
(`e8c7e9a`), and M6 observability was replayed on top of it.

A `git rebase` was not used. The refactor dissolved every file M6 built on —
`kafkaman-sqlx/src/lib.rs` alone lost 4,456 lines to ~30 modules — so a 3-way
merge would have had the monolith as base, a `mod` stub as one side, and
monolith-plus-M6 as the other, with rename detection unable to bridge a 1-to-30
split. M6 is 97% additive (+1,431/-45 across the five crate roots, four in-place
modification sites in total), so the work was placing 47 hunks into named
modules, not reconciling conflicts. Each hunk was routed by the enclosing scope
its diff header names.

Where M6's code now lives:

- `kafkaman-core`: `lifecycle.rs` (new) for `LifecycleEmission`/`LifecycleSampler`;
  `is_terminal` on `status.rs` as explicit impl blocks, since `sql_enum!`
  generates no impl to extend; the `Option` timestamp adapter nested in `rfc9557`.
- `kafkaman-config`: `observability.rs` (new), mirroring `retry.rs` and its
  identical `Retry{Config,Policy,PolicyOverride}` shape; `ObservabilitySection`
  with the other sections; the shared `string_enum` helper in `serde_enum.rs`.
- `kafkaman-sqlx`: `operability.rs` (new) for the depth and stuck-row queries
  with their row types; `redrive_received` in `replay.rs`, which owns the private
  statement it reuses.
- `kafkaman-worker` and `kafkaman-rdkafka`: `metrics.rs` each, holding both cfg
  twins in one file because their purpose is that call sites read identically.

Two M6 changes were dropped as redundant, both cases where the refactor had
independently made the same improvement:

1. M6 replaced `DlqMode`'s hand-written `Deserialize` visitor with a
   `string_enum` call. The refactor had already replaced it with a
   `rename_all = "lowercase"` derive. Both accept exactly `"table"`; the only
   test on that path asserts `is_err()`.
2. M6 replaced the hard-coded Redpanda port 19092 with a free-port pick and a
   five-attempt retry. The refactor had already done this in the shared
   `containers.rs` as `redpanda()`/`available_host_port()`.

Two defects found and corrected while porting:

- A doc comment M6 attached to `Replay::RUNTIME_VERSION` actually described
  `received_descriptor`. Split correctly; no behaviour change.
- `wiki/compatibility/m6-observability-operability-api.compat.md` claims M6
  introduced dynamic Redpanda test ports and names `start_redpanda`. On this
  base that came from the refactor and the helper is `redpanda()`. **Corrected
  2026-08-25** — see the entry at the top of this log.

One finding **not** caused by this rebase: the refactor dropped
`migration_context_and_replay_guardrails_are_explicit`, which existed on
`332721a` and covered `MigrationContext` context/`applied_by`/`matches` plus the
`Replay` guardrails (`dry_run_preview`, `InvalidReplay`, `max_rows(0)`). It is
absent from `main` independently of M6 and has not been restored here.
**Retracted 2026-08-25 — this was a false alarm.** The refactor split the test
rather than dropping it; every assertion survives. See the entry at the top of
this log.

Verification, on the rebased branch:
- `cargo fmt --all -- --check` - clean.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` - clean.
- `cargo test --workspace --all-features` - 204 passed, 0 failed, 4 ignored.
- `cargo llvm-cov --workspace --all-features --fail-under-lines 80` - 82.40%.

The count reconciles against both parents: the refactor's 172 passing, plus 19
`kafkaman-axum`, 8 `kafkaman-core`, 2 `kafkaman-config`, 1 `durable_send`, and
2 `durable_receive` from M6.

Pages affected:
- wiki/index.md
- wiki/log.md

## [2026-08-25] update | external review follow-ups

Five findings from an independent review of the staged refactor, all confirmed
against the code and all fixed. Two were introduced by this branch.

`RecordHeaders` resolved duplicate Kafka header keys with `or_insert` for both
namespaces. That matches `main` for reserved headers but inverts it for user
headers, which `main` stored with `BTreeMap::insert` — last wins. A producer
sending a repeated header key would have had a different value persisted than
before the branch. User headers are back to last-wins, and both directions are
now pinned by a test that was verified to fail against the regression.

`kafkaman_worker::run` checked for shutdown only after a cycle, so a relay handed
an already-cancelled token still claimed and published one batch;
`run_dispatcher` and `run_purger` both guard at the top of the loop. Added the
guard and a test that runs the loop against a pre-cancelled token.

`apps/axum-outbox` still declared a `testcontainers` dev-dependency that nothing
referenced once the container helpers moved into `durable-send-tests`.
`kafkaman_rdkafka::Error::TestHook` was the last test vocabulary in a production
error surface and is now `Observer`. And `Harness::ensure_message` hand-rolled a
duplicate check and then called the infallible `with_message`, silently keeping
the first topic when a test registered one message type under two — the exact
conflict `ResolvedConfig::try_with_message` exists to reject; it uses the
fallible path now.

Tests 170 → 172. Pages affected: `crates/kafkaman-rdkafka/src/ingest_record.rs`,
`crates/kafkaman-rdkafka/src/error.rs`, `crates/kafkaman-rdkafka/src/tests.rs`,
`crates/kafkaman-worker/src/relay.rs`, `crates/kafkaman-test/src/harness.rs`,
`apps/axum-outbox/Cargo.toml`,
`tests/durable-send/tests/durable_send/relay_and_publish.rs`,
`tests/durable-send/tests/redpanda_full_loop/ingest_dedup.rs`,
`wiki/compatibility/module-test-separation-internal-hooks.compat.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-25] update | doc-link gate and generated enum lists

Second review pass over the module refactor, fixing what the first one missed.

Splitting the crate roots into modules broke seven `[`item`]` doc links that had
resolved while everything shared one namespace. `fmt` and `clippy` say nothing
about those, so this is the `rustfmt` hole again in a different gate: `just lint`
and the CI lint job now run `cargo doc --workspace --all-features --no-deps`
under `RUSTDOCFLAGS=-D warnings`. Verified the gate bites by reintroducing a
broken link (exit 101) before repairing all seven.

`ReceivedFailureKind` and `ReceivedIngestFailureKind` still carried hand-written
`ALL: [Self; N]` arrays with hardcoded lengths — the exact hazard `sql_enum!`
was written to remove for `OutboxStatus` and `ReceiveStatus` in the sibling file.
Every other accessor on them is a compiler-checked `match`, but `ALL` is what
`from_discriminant` searches, so a missed variant surfaced as a stored row that
no longer read back. Both now come from `discriminant_enum!`, sharing one
`count_idents!` with `sql_enum!` in `kafkaman-core`'s private `enum_macros`.
`ALL`'s type and length are unchanged.

`migrate` needs two pooled connections since the advisory-lock fix and now says
so before blocking: a single-connection pool returns `MigrationPoolTooSmall`
rather than a pool timeout that names nothing. Also collapsed the sqlx hook
plumbing — two identical type aliases and four near-identical runners became one
alias, one `DispatchHookSlot`, and one runner — and replaced the last
`use super::` in production code.

Pages affected: `crates/kafkaman-core/src/enum_macros.rs`,
`crates/kafkaman-core/src/failure_kind.rs`, `crates/kafkaman-core/src/status.rs`,
`crates/kafkaman-sqlx/src/hooks.rs`,
`crates/kafkaman-sqlx/src/migration_runner.rs`,
`crates/kafkaman-sqlx/src/tests/migration_runner.rs`, `justfile`,
`.github/workflows/ci.yml`,
`wiki/compatibility/module-test-separation-internal-hooks.compat.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-25] update | module refactor completion and migration lock fix

Completed the module separation started on 2026-08-24, and fixed one latent bug
it surfaced.

`migrate` took a session-scoped `pg_advisory_lock` on a pooled connection. A
dropped future skipped the unlock and returned the connection to the pool still
holding it, leaving the schema permanently unmigratable. Both entry points now
use `pg_advisory_xact_lock` inside a transaction, which a queued rollback
releases on cancellation. Lock keys are unchanged, so a rolling deploy still
serializes against an old binary.

`use super::*` was removed from all 49 production module files, so each module
declares what it depends on and the crate roots hold only a module graph and a
named public surface. Grab-bag files were split along the seams their names
admitted: `config_tables_router.rs` into four, `error_identifier_message.rs`
into three, `outbox_relay.rs` (which holds no relay) into three.
`kafkaman-worker` and `kafkaman-test` got the same treatment.

Duplication was removed rather than relocated: eight changesets now come from one
`descriptor_changeset!` macro; the four pool/transaction twins in
`dispatch_rows.rs` became two functions over `&mut PgConnection`; the test
suite's 79 repetitions of the Postgres-and-harness prologue became
`start_harness()`. `RetryPolicyOverride::apply_to` is a struct literal, so a new
field is a compile error rather than a silently ignored override.

Also fixed: a TOCTOU race in the Redpanda port picker (now retried), a dead
`#[allow]` in `config/schema.rs`, two `\n`-escaped SQL one-liners, and a
`create_order` double clone. Tests went 141 to 169 with no test lost; the largest
Rust file is 342 lines, from 650.

Pages affected:
- wiki/compatibility/module-test-separation-internal-hooks.compat.md
- wiki/index.md
- wiki/log.md

## [2026-08-24] remediate | M6 review remediation

A line-by-line review of the M6 branch before merge found five defects that
reach a running system, plus consistency and coverage gaps against conventions
this repository already documents. All are fixed; the closeout documents are
corrected to match.

The defects:

1. **Every admin timestamp serialized as a nine-integer array.** The workspace
   enables `time` with `serde`/`formatting`/`parsing` but not
   `serde-human-readable`, and the string branch of `impl Serialize for
   OffsetDateTime` is gated on that feature. `axum` enables it only as a
   dev-dependency, so feature unification does not rescue the build. Every
   `OffsetDateTime` on the new response structs now carries
   `kafkaman_core::rfc9557`; `rfc9557::option` was added for the nullable
   columns. Enabling the upstream feature was rejected because it would also
   change `ReceivedError`'s established wire format.
2. **`kafkaman.kafka.ingest.records` counted every record twice.** `committed`
   is `1` per consumed record *and* one of `inserted`/`duplicate`/`skipped` is
   also `1`, so recording all four as outcomes of one counter doubled the total.
   Commits moved to `kafkaman.kafka.ingest.commits`, and all recording now goes
   through `record_ingest_stats`, which debug-asserts that the outcomes partition
   `consumed`.
3. **`received_stuck_rows` used the query shape this repository documents as
   unindexable.** It filtered on `COALESCE(next_attempt_at, created_at)`, which
   the received table's `(status, next_attempt_at, created_at)` index cannot
   serve — the shape `create_outbox_retention_index_sql` records as measured
   unservable in R14, and which `claim_received_row` splits per status for
   exactly that reason. Rewritten to the split form; `due_at` remains a
   projection driving `ORDER BY`.
4. **`serve().with_runtime()` cancelled workers without draining them.** It
   returned as soon as the listener closed, cutting in-flight publishes at
   process exit. It now cancels, then awaits the tasks, bounded by
   `DEFAULT_DRAIN_TIMEOUT`; a wedged task trips `RuntimeError::DrainTimeout`
   rather than hanging. The no-task branch now cancels too, and stragglers are
   aborted rather than leaked.
5. **Six of seven observability knobs were inert while documented as live.**
   `lifecycle` and `sample_success` now drive per-message success events through
   `LifecycleEmission`/`LifecycleSampler` (deterministic every-`n`-th sampling,
   carried across cycles). `max_queue_age` now sets `over_max_queue_age` on depth
   summaries, suppressed for terminal statuses via new `is_terminal` helpers.
   `level`, `payload`, and `headers` are relabelled **reserved** in the example
   TOML, the field docs, and the spec.

Also addressed: `RedriveRequest` gained `deny_unknown_fields` and a
`MAX_REDRIVE_ROWS` cap; client-supplied correlation ids are bounded in length and
charset; `admin_router` carries a security warning at the definition;
`opentelemetry` moved behind a default-on `metrics` feature; metric attributes
are built once per loop instead of per record; `ingest.errors` gained a `reason`
label distinguishing the fatal breaker trip from transient failures; unused
`kafkaman-config` and `http-body-util` dependencies removed; `axum-outbox`
switched to the workspace `tower`; dead `Replay::message_type` removed;
`Replay::RUNTIME_VERSION` replaces the magic `0`; per-message config errors no
longer duplicate a bad default once per inheriting message type; `DlqMode`'s
hand-rolled visitor now reuses `string_enum`; every new public item in
`kafkaman-sqlx` and `kafkaman-axum` is documented; and `start_redpanda` retries
on a fresh port to close the bind race.

`apps/axum-outbox` now exercises the M6 surface end to end: it mounts
`admin_router` under `/internal/kafkaman`, applies `CorrelationLayer`, resolves
its relay lifecycle policy from `[observability]`, and replaces its hand-rolled
supervision with `serve().with_runtime()`.

Test coverage went from 146 to 173 passing. `kafkaman-axum` went from one unit
test to nineteen, including RFC 3339 assertions on every response struct — the
gap that let defect 1 ship — and five supervision tests covering the drain.

Verification:
- `rtk cargo check --workspace --all-features --all-targets`
- `rtk cargo test --workspace --all-features` - 173 passed, 4 ignored.
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings` -
  no issues found.
- `rtk cargo fmt --all -- --check`
- `rtk cargo check -p kafkaman-worker --no-default-features` and
  `rtk cargo check -p kafkaman-rdkafka --no-default-features`

Pages affected:
- wiki/specs/m6-observability-operability.spec.md
- wiki/decisions/observability-operability-policy.decision.md
- wiki/compatibility/m6-observability-operability-api.compat.md
- wiki/log.md

Code affected:
- Cargo.toml
- kafkaman.example.toml
- apps/axum-outbox/ (Cargo.toml, src/main.rs)
- crates/kafkaman/Cargo.toml
- crates/kafkaman-axum/ (Cargo.toml, src/lib.rs)
- crates/kafkaman-config/src/lib.rs
- crates/kafkaman-core/src/lib.rs
- crates/kafkaman-rdkafka/ (Cargo.toml, src/lib.rs)
- crates/kafkaman-sqlx/src/lib.rs
- crates/kafkaman-worker/ (Cargo.toml, src/lib.rs)
- tests/durable-send/tests/durable_send.rs
- tests/durable-send/tests/durable_receive.rs
- tests/durable-send/tests/redpanda_full_loop.rs

## [2026-08-24] promote | M6 observability and operability closeout

Promoted validated M6 behavior to an active spec and accepted observability
decision. The closeout records runtime observability config with per-message
overrides, direct OpenTelemetry metrics through the global `kafkaman` meter, SQL
queue depth/age/stuck inspection APIs, sanitized `kafkaman-axum` admin/health
routes, `CorrelationLayer`, runtime task supervision via
`serve().with_runtime()`, and descriptor-driven received DLQ redrive.

The implementation deliberately keeps payload bodies and arbitrary user headers
out of admin responses. Admin DLQ summaries expose identifiers, source
coordinates, timestamps, attempts, and structured failure summaries only. Host
applications still own tracing subscribers, OpenTelemetry exporters, and access
control around mounted admin routes.

The all-features gate initially failed because the existing Redpanda full-loop
test helper hard-coded host port `19092`, which was already occupied by the
local example Redpanda container. The helper now allocates a free host port and
advertises that same port, so the full-loop suite can coexist with local example
infrastructure.

Verification:
- `rtk cargo check --workspace --all-features`
- `rtk cargo test --workspace --all-features` - 146 passed, 3 ignored.
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings` -
  no issues found.
- `rtk cargo fmt --all -- --check`

Pages affected:
- wiki/decisions/observability-operability-policy.decision.md (new)
- wiki/specs/m6-observability-operability.spec.md (new)
- wiki/compatibility/m6-observability-operability-api.compat.md (new)
- wiki/proposals/04-observability-logging-policy.proposal.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

Code affected:
- Cargo.toml
- Cargo.lock
- kafkaman.example.toml
- crates/kafkaman/src/lib.rs
- crates/kafkaman-axum/
- crates/kafkaman-config/src/lib.rs
- crates/kafkaman-rdkafka/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- crates/kafkaman-worker/src/lib.rs
- tests/durable-send/tests/durable_send.rs
- tests/durable-send/tests/durable_receive.rs
- tests/durable-send/tests/redpanda_full_loop.rs

## [2026-08-24] update | module and test separation refactor

Split large production crate roots into focused modules and split the large
integration tests into named test modules. Moved consumer-facing test-hook
ergonomics to `kafkaman-test`, leaving production crates with hidden
`internal-hooks` observer gates only. No schema or runtime behavior changes.

The fragments are declared with `mod` and re-exported from each crate root with
`pub use`, so public paths are unchanged. An interim `include!`-based split was
replaced because `rustfmt` only walks module declarations: it left 9,083 of the
repo's 15,386 Rust lines outside the `cargo fmt --all -- --check` gate that CI
runs, while CI still reported success. Cross-module uses that the compiler
flagged were narrowed to `pub(crate)` rather than made public, and
`kafkaman-sqlx`'s `dispatch.rs` was further split into `dispatch`,
`dispatch_cache`, `dispatch_failure`, and `dispatch_rows`. The four integration
tests are now `tests/<name>/main.rs` targets with sibling case modules. Largest
remaining Rust file: 594 lines, down from 4,382.

Pages affected:
- wiki/compatibility/module-test-separation-internal-hooks.compat.md (new)
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md

## [2026-08-25] update | rebase onto main; reconcile the M5 closeout

Rebased the topic-convergence work onto `332721a` (the M5 closeout) and restored
the two-service example from stash. Only `wiki/log.md` conflicted, in both
operations — the log is newest-first, so every prepended entry lands on the same
anchor. That will recur on every rebase; it is a property of the format.

The mechanical rebase was the easy half. Main's closeout rewrote the
documentation of behaviour this branch then changed in code, and **none of those
files conflicted** — they applied clean and silently became wrong. Reconciled on
the principle that a spec describes what is true now while a compatibility note
records what one release shipped:

- `wiki/specs/entity-first-propagation.spec.md` **amended**. It enumerated only
  `Applied` and `Ignored`, said a topic or partition mismatch is reported as
  `CacheOriginMismatch` and is never a normal ignored record, and listed broker
  topic validation as unimplemented. All three are now false in part. It carries
  the four-case outcome table, and states that topic validation exists as a
  library capability but is not yet called from any boot sequence — landed rather
  than delivered.
- `wiki/roadmaps/path-to-v1.roadmap.md` **amended**, because a roadmap is live
  orchestration rather than history and its post-M5 deferral list named an item
  that has since landed.
- The three M5 compatibility notes and the now-Completed entity-first plan were
  **left alone** as release records, even though they contain statements this
  branch supersedes. `m5-code-audit-remediation.compat.md` is the one to watch: it
  still tells operators to treat `CacheOriginMismatch` as the signal to
  re-bootstrap, which stays correct for an in-place repartition and is wrong for a
  rebuilt topic.

Corrections to this branch's own documents, which mattered more than main's:

- Proposal 13 and the topic-convergence decision **misquoted** the entity-first
  plan. They cited "Boot-time broker topic validation remains pending"; the
  closeout rewrote that to "remains deferred after M5". The 2026-08-24 `create`
  entry below carries the same misquote and is left as written, since this log is
  append-only.
- Both leaned on a Deferred bullet — "Topic/partition mismatch invalidation path"
  — that the closeout deleted, having reframed that behaviour as shipped. The
  proposal's main premise survives, because boot-time validation is still Deferred
  in both notes; what changed is that this work *supersedes* a closed M5 behaviour
  rather than *completing* an open deferral.
- Three documents listed `Replay::outbox` **rejection** as deferred. It was
  already implemented at the merge base — `Replay::outbox` returns
  `Err(UnsafeOutboxReplay)` unconditionally. What is actually missing is the
  positive resync surface. That error predated the rebase and was ours.
- `wiki/index.md` still showed the topic-convergence plan as `Draft` with phases
  1-4 landed.
Pages affected: `wiki/specs/entity-first-propagation.spec.md`,
`wiki/roadmaps/path-to-v1.roadmap.md`,
`wiki/proposals/13-topic-convergence-and-environment-provisioning.proposal.md`,
`wiki/decisions/topic-convergence-and-rebuild.decision.md`,
`wiki/plans/topic-convergence.plan.md`,
`wiki/compatibility/topic-convergence-api.compat.md`, `wiki/index.md`,
`wiki/log.md`.

## [2026-08-24] implement | topic convergence phases 1-4

Landed the declaration, config, verify/create, and cache-origin invalidation
slices of the topic-convergence plan. `TopicSpec`, `CleanupPolicy`, `TopicMode`
and the pure `reconcile` decision live in `kafkaman-core`; `[topics] mode` in
`kafkaman-config`; `TopicAdmin` and `converge_topics` in `kafkaman-rdkafka`; the
authorized migration in `kafkaman-sqlx`. Workspace green at 177 passing, clippy
clean.

`reconcile` is deliberately pure and takes the broker's answer as an argument, so
the branches that are awkward to provoke against a real broker — an ACL-denied
create, a compacted-but-wrongly-partitioned topic, an unknown cleanup policy —
are unit-tested without one.

**The invalidation rule as decided was too permissive, and an existing test caught
it.** Authorizing any origin change that arrived on the declared topic would also
have blessed an in-place partition change, which is exactly the operation the
rebuild decision forbids; `cache_apply_halts_when_an_entity_changes_partition`
failed. The rule now authorizes migration only across a *topic* change, and adds
a fourth case the decision had missed: a straggler from a topic the cache has
already migrated off is stale, not broken, so it is ignored rather than failed —
otherwise a cutover fills the error table with its own drainage. The decision was
amended to match rather than the test being adjusted to fit.

One implementation detail worth recording: `TopicAdmin` sets
`allow.auto.create.topics=false` and reads whole-cluster metadata instead of
naming a topic, because a metadata request that names a topic is itself enough to
make a broker create it — with the `delete` policy this code exists to detect.

Both live-broker gates were demonstrated rather than assumed. Disabling the
policy comparison failed the new `topic_convergence` tests; so did swapping the
metadata strategy for the naive one, on `verifying a topic must not create it` —
with auto-creation enabled, asking whether a topic is compacted is itself enough
to create it with the `delete` default.

Phase 5 (the `examples/provision` binary) is blocked on the example services
being stashed out of the tree. Still uncovered: the ACL-denied create path
(dev-container mode has no ACLs to deny with) and the decision's bounded broker
retry at boot, which is not implemented.
Pages affected: `crates/kafkaman-core/src/topics.rs`,
`crates/kafkaman-core/src/lib.rs`, `crates/kafkaman-config/src/lib.rs`,
`crates/kafkaman-rdkafka/src/topics.rs`, `crates/kafkaman-rdkafka/src/lib.rs`,
`crates/kafkaman-sqlx/src/lib.rs`,
`tests/durable-send/tests/entity_first_propagation.rs`,
`tests/durable-send/tests/topic_convergence.rs`,
`wiki/compatibility/topic-convergence-api.compat.md`,
`wiki/decisions/topic-convergence-and-rebuild.decision.md`,
`wiki/proposals/13-topic-convergence-and-environment-provisioning.proposal.md`,
`wiki/plans/topic-convergence.plan.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-08-24] create | topic convergence, rebuild over repartitioning

Redpanda Console, added to the example stack that day, showed both entity topics
running with `cleanup.policy=delete` — so the example could not support the
rebuild the entity-first model promises, and nothing reported it. The gap was
already recorded under **Deferred** in the two M5 compatibility notes and as
"Boot-time broker topic validation remains pending" in the entity-first plan;
what was new was evidence that it is load-bearing rather than theoretical. There
is no `AdminClient`, `describe_configs`, or `cleanup.policy` reference anywhere
in `crates/`.

Accepted proposal 13 and promoted it to a decision. A topic's required
configuration moves onto `MessageDescriptor`, defaulted to `cleanup.policy=compact`
alone by the model rather than declared per contract — proposal 12 already
settled that every in-purview type is a compact entity snapshot, so
`examples/contracts` does not change. Convergence happens at boot beside
`migrate()`: `verify` by default, `create` opt-in because ACLs routinely deny
`CreateTopics`, `off` as an escape hatch that warns.

The larger decision is that **repartitioning is never performed**. Because every
message is a full snapshot sourced from the owner's state, the log is a derived
artifact rather than the system of record, so a topic is rebuilt by republishing
every entity at a cost bounded by entity count, not event count. That turns
partition count from an irreversible choice into a recoverable one. It also
resolves a second Deferred item: the guarded upsert compares `applied_topic` and
`applied_partition`, and `classify_skipped_cache_apply` already raises a terminal
`CacheOriginMismatch` on a move so the row cannot freeze silently — what was
missing was authorization to resolve it, and the declared topic supplies exactly
that. Two deferred items solve each other.

Also fixed the provisioning boundary: environment (databases, topics) may be
provisioned externally, schema (tables) never is. A provisioner creating tables
would bypass the M2 change engine and force one crate to know both services'
asymmetric changelogs, recoupling what two separate databases exist to separate.
The runtime cannot live in a contracts crate either — cargo unifies features
across a workspace, so an optional `provision` feature would not stay optional.

The execution plan sequences five phases and records that the provisioner is
Phase 5, not Phase 1, because it is a thin caller of `create` mode.
Pages affected: `wiki/proposals/13-topic-convergence-and-environment-provisioning.proposal.md`,
`wiki/decisions/topic-convergence-and-rebuild.decision.md`,
`wiki/plans/topic-convergence.plan.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-08-24] implementation | two-service distributed cache example

Shipped the plan. `apps/` is now `examples/`, holding `contracts` (the two
snapshot types and nothing else), `product`, and `order`; `tests/distributed-cache`
drives both services over HTTP and never touches a database. Workspace suite green
at 153 passing — the 141 that existed, minus the 3 deleted `axum-outbox` HTTP tests,
plus 15 new — with `just lint` clean and coverage at 91.2% lines.

The verification that mattered was demonstrated rather than assumed: **both wiring
breaks fail the new test and nothing else in the workspace notices either.** Removing
`order`'s dispatcher spawn fails at the convergence deadline; removing
`CreateCacheTable` from its changelog fails in three seconds on a 500 from the cache
read. That is the entire justification for the test tier existing.

The library documentation gap is closed: `dispatch_once` and
`MessageRouter::handler` now state that the handler runs *before* the cache upsert,
so a handler deriving from its own cache sees that entity one version stale. No
public API changed, so no compatibility note.

Redpanda's ephemeral-port approach worked, so F16 is fixed — `redpanda_full_loop.rs`
reserves a port from the OS per container instead of hardcoding `19092`. Its
process-global mutex stayed, with the comment corrected: it never protected against
a second test *binary*, and what it still buys is not starting a dozen brokers at
once.

Two deliberate deviations, both recorded in the plan. Snapshot idempotency keys are
derived from a per-entity `version` column rather than from the payload, because a
payload digest would break the example's own cancellation step: restoring
availability to a previously published value would re-derive a key the consumer has
already seen, and the restoring snapshot would be dropped as a duplicate. This is an
idempotency identity, not a convergence ordinal — the ordinal is still the Kafka
offset — so the entity-first decision's argument against producer-stamped versions
does not apply. And the availability-only rejection runs *before* discontinuation,
since after it the status check fires first and the assertion would prove nothing.

One consequence the plan did not anticipate, recorded rather than reversed: the
rename undoes a deliberate 2026-06-21 move. The example was put in `apps/`
*because* `cargo llvm-cov` excludes `examples/`, so it would count toward the
workspace total. It no longer does, and the coverage number across this change is
not comparable — 91.2% now measures `crates/` alone. Kept anyway, because the
gate should describe the library rather than be propped up by demonstration code,
and the library test strategy already treats coverage % as a signal rather than
the gate. Noted in the M1 plan where the original reasoning lives.

Also worth recording: the domain status enums carry a catch-all `Unrecognized`
variant, on the same reasoning the entity-first decision applies to origin intent —
a new variant at an un-redeployed consumer is a deterministic ingest skip, and
enough of them trip the breaker topic-wide. And `.gitignore` matched
`kafkaman.toml` at every level, which is what made the original example unrunnable;
`!examples/*/kafkaman.toml` is now an explicit exception.

Pages affected:
- wiki/plans/two-service-distributed-cache-example.plan.md
- wiki/index.md
- wiki/log.md
- `examples/contracts/`, `examples/order/`, `examples/product/` (new; `order`
  renamed from `apps/axum-outbox`)
- `tests/distributed-cache/` (new)
- `crates/kafkaman-sqlx/src/lib.rs` (rustdoc only)
- `tests/durable-send/tests/redpanda_full_loop.rs`
- `Cargo.toml`, `.gitignore`, `README.md`, `kafkaman.example.toml`
- wiki/plans/m1-durable-send-implementation.plan.md (coverage note reversed)
- wiki/specs/m1-durable-send.spec.md (example path)
- wiki/compatibility/typed-idempotency-identity-api.compat.md (example path and
  idempotency derivation)
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md (example
  path; outbox retention has since shipped)
## [2026-08-24] promote | M5 entity-first propagation closeout

Promoted validated M5 behavior to an active spec and closed the execution plan.
The closeout records the implemented entity-cache surface: required
`KafkaMessage::entity_key`, per-type cache tables, received-row entity-key
persistence, offset-guarded cache upsert, per-entity outbox supersede,
claim-time collapse of stale same-entity pending rows, `Replay::outbox`
rejection as unsafe, wire-carried producer metadata, jittered receive retry
backoff, and opt-in outbox retention.

Corrected stale wiki claims found during closeout: the M4 spec now reflects
equal-jitter backoff, and the first M5 cache compatibility note no longer claims
the cache key comes from `kafkaman-entity-key` headers or falls back to
`message_id`. M5 deferrals are explicit: bootstrap/readiness, boot-time broker
topic validation, advisory origin intent, a positive state-sourced republish API,
proactive topic-lifecycle re-bootstrap, soft-delete workflow/reclamation, and
the two-service example. The roadmap now marks M6 observability/operability as
the active lane that can run in parallel with the example worktree.

Verification:
- `rtk cargo test --workspace --all-features` - 141 passed, 3 ignored.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests` - 69
  passed, 3 ignored.
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features
  redpanda --test redpanda_full_loop -- --test-threads=1` - 11 passed.
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings` -
  no issues found.
- `rtk cargo fmt --all -- --check`

Pages affected:
- wiki/specs/entity-first-propagation.spec.md (new)
- wiki/specs/m4-retry-backoff-dlq.spec.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/compatibility/m5-entity-first-outbox-supersede.compat.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-24] plan | the example app cannot start; two-service example planned

Adding a `[retention]` section to `kafkaman.example.toml` surfaced that **the
example app has never been runnable**. `Config::discover()` walks up from the
current directory looking for `kafkaman.toml`, and no such file exists anywhere in
the repo, so `ResolvedConfig::from_config(None, [descriptor])` fails at boot with
"missing config file for registered kafkaman features". Its HTTP test never caught
it because the test builds config through `Config::from_str`, bypassing discovery.
Same failure mode as the example config file itself: nothing exercised it, so it
rotted.

Two further gaps followed from looking at it. The example demonstrates roughly a
third of the product — one message type, an outbox, a relay — while the entity-first
decision states the purview as compact entity snapshots for distributed caches, and
nothing anywhere shows a cache being read or two services converging. And nothing
tests wiring: the integration tests all go through `Harness` with direct database
access, so a dispatcher never spawned, a topic mismatch, or a `CreateCacheTable`
missing from a changelog passes every test in the workspace.

Planned in response: rename `apps/` to `examples/`, split the single app into
`product` and `order` services sharing a contracts crate, and drive the whole thing
from a new HTTP-only test package.

The design choice worth recording is on the return path. product does **not**
decrement stock when an order arrives; it recomputes availability from its converged
cache of orders, filtered to fulfilled ones. That is idempotent by construction —
the same snapshot applied twenty times still converges to one cache row — where a
decrementing handler is correct only because the handler and the processed-mark
share a transaction. It also means **cancellation restores the count for free**,
which an event-accumulating design cannot do. Caching only fulfilled orders was
considered and rejected: it would strand a stale row on a Fulfilled → Cancelled
transition, and there is no ingest-time filter hook anyway.

Planning also surfaced a library documentation gap: `dispatch_once` runs the handler
*before* upserting the message into the cache, so any handler deriving state from
its own cache sees that entity one version stale. Nothing says so. It forces an
exclude-and-substitute correction in the deriving handler, and should be documented
on `dispatch_once` and `MessageRouter`.

The plan adopts the owned-container-per-test rule from the amended
library-test-strategy decision rather than the per-binary sharing it replaced, and
carries the `com.kafkaman.*` labels so the new package's containers are reachable by
`just clean-containers`. Containers being owned per test rather than shared argues
for few, fat tests that walk the whole lifecycle over many thin ones.

Pages affected:
- wiki/plans/two-service-distributed-cache-example.plan.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-24] implementation | owned Postgres testcontainers per test

Recorded the durable-send testcontainer lifecycle decision in the library test
strategy. Postgres-backed tests now prefer one owned container per test/harness
over one shared static container per test binary, because Testcontainers cleanup
is `Drop`-based and `OnceCell<ContainerAsync<_>>` prevents that drop from running
at process exit. The accepted tradeoff is slower Docker-backed runs in exchange
for avoiding leaked shared containers. Also recorded the label-scoped cleanup
fallback for interrupted runs.

Implementation replaces the durable-send shared Postgres URL with an owned guard,
keeps that guard in each test scope, labels kafkaman-managed Postgres and
Redpanda testcontainers, and narrows `just clean-containers` to those labels. The
same static Postgres holder was removed from the axum-outbox HTTP integration
test because it runs in the workspace gate.

Verification:
- `rtk just lint`
- `rtk just test all`
- `rtk just test coverage` (85.14% lines)
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send durable_send_publishes_record_and_marks_row -- --exact`
- `rtk docker ps -a --filter label=com.kafkaman.project=kafkaman --filter label=com.kafkaman.managed-by=testcontainers --filter label=com.kafkaman.test-service=postgres --format '{{.ID}} {{.Image}} {{.Status}} {{.Names}}'` returned no containers after the focused test.

Pages affected:
- wiki/decisions/library-test-strategy.decision.md
- wiki/index.md
- wiki/log.md

Code affected:
- apps/axum-outbox/tests/http.rs
- justfile
- tests/durable-send/src/lib.rs
- tests/durable-send/tests/*.rs

## [2026-08-24] implementation | outbox retention; claim-order index refuted

Measuring R1's fix surfaced two problems neither review pass had found, and
scoping the first surfaced a third.

**R14 — the claim's ordering cannot be index-served, and the obvious fix does not
work.** `claim_batch` orders candidates by `created_at`; under a 200k `Pending`
backlog that is a sequential scan plus an external merge sort spilling 7,840 kB to
disk. A partial index on the ordering column was measured and **refuted**: the plan
is byte-identical with and without it, because the candidate predicate is an `OR`
across two statuses and the planner must examine every row to decide membership.
The stage's gate was structural — the disappearance of the `Sort` node, not a
faster wall clock — and it held, so nothing shipped. The refuted index stays in the
benchmark as a recorded negative result.

Two corrections to how R14 was first written up, both now in `review.md`. It does
not cost every relay cycle: the steady-state shape is 0.3 ms on the existing index,
so it bites only when the relay is behind. And most of the backlog-shape cost is
JIT compilation triggered by the inflated cost estimate, not the scan itself.
Fixing it properly means removing the `OR` — a `UNION ALL` of separately-ordered
branches — which is a semantic change to the correctness-critical claim and hits
Postgres rejecting `FOR UPDATE` with `UNION`. Left as its own change.

**Nothing purged any kafkaman table.** No `DELETE` existed in the workspace, so the
outbox grew for the life of the application. Retention now exists and is opt-in:
`purge_outbox_once` for one bounded batch, `run_purger` for the loop, mirroring the
existing `relay_once`/`run` pairing. Scope was inherited from the restore
proposal's drop/protect/rebuild split rather than argued fresh — the outbox carries
`drop`, so nothing may depend on a historical row, while the received table carries
`protect` and its dedupe window *is* its retention window. `Failed` outbox rows are
the invalid-send audit trail and are spared unless explicitly opted in.

**Index-adding changesets block writes.** Changesets apply inside a transaction, so
`CREATE INDEX CONCURRENTLY` is unavailable and every index build takes a `SHARE`
lock. On an outbox grown without retention that is an outage, and because `enqueue`
runs inside the caller's business transaction it propagates into application
requests. Recorded in proposal 11 and flagged in the compatibility note, since an
adopter cannot infer it from the changeset.

Applying the refutation's lesson, the retention index was verified used rather than
assumed: Index Scan, no sort, 0.47 ms over 200k rows.

139 tests pass (3 ignored diagnostics). `cargo fmt --check` and
`clippy -D warnings` clean.

Pages affected:
- wiki/decisions/outbox-retention-policy.decision.md (new)
- wiki/compatibility/m5-outbox-retention.compat.md (new)
- wiki/plans/outbox-retention.plan.md (new)
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md (amended)
- wiki/index.md
- wiki/log.md
- review.md

## [2026-08-24] review | code audit remediation reviewed and corrected

Reviewed the remediation pass above by reading the diff and resulting source
rather than its own outcome table, merging a third independent review whose four
findings were all re-verified and all confirmed. Seven issues fixed, five of them
gaps the remediation itself introduced or left.

Silent-failure classes closed:

- A transient publish failure could permanently invert an entity's state. Enqueue
  supersedes only rows that are Pending at that instant, so a Publishing row
  survives and `mark_publish_failed` returns it to Pending; the newer row then
  publishes first because it is due immediately while the retry is not, and the
  older state lands at the higher offset. This is decision point 9's corruption
  reached through retry rather than replay, and all three review passes had missed
  it. `claim_batch` now collapses each entity's Pending queue to its newest row
  before selecting candidates.
- `ResolvedConfig::from_config` still used the silent `with_message`, so F15's
  duplicate-descriptor fix protected only a helper nobody production-facing
  called.
- Relaxing `parse_duration` to accept zero removed the only guard on a zero
  `initial_backoff`, which turns a permanently failing row into an unthrottled
  loop against the database. `validate_policy` now rejects it.
- `CacheOriginMismatch` and `MissingEntityKey` were retried to exhaustion despite
  being deterministic. Failure *class* and *retryability* are now separate
  concepts (`FailureDisposition`), and both are terminal on the first attempt.
  Decision point 4's consecutive-regression breaker is now explicitly declined in
  code, with the reason: a breaker halts the pipeline, and one entity's
  repartition must not stop dispatch for every other entity.
- `received_entity_key`'s `kafkaman-entity-key` tier was unreachable through every
  supported write path, yet documented as live in three places and covered by a
  passing unit test that asserted it worked — green only because the fixture built
  the row struct directly. The tier is gone and the test now pins its absence. The
  header is still published, which is its actual purpose: foreign consumers cannot
  deserialize the typed payload, kafkaman's own ingest always can.

Test gaps closed: a legacy received-table migration test for
`AddReceivedEntityKey` (public API and step 1 of the documented upgrade, exercised
by nothing) including what happens to rows already present; the first unit tests
in `kafkaman-rdkafka`, pinning the deliberate asymmetry where a malformed
idempotency source degrades but a malformed occurrence time is rejected; and a
real broker-hop test for `partition_key != entity_key`, the shape F2 broke, which
the F2 regression test never actually crossed a broker with.

Record corrections: line coverage is 84.86%, not the 88.45% first recorded — two
consecutive runs produce byte-identical counts, so the metric is deterministic and
the original figure was never produced by the documented command. Eleven plan
items had been dropped without being listed under "Not done", and two reversals
were reported as successes; both are now recorded in `review.md`. The container
reduction claim covers PostgreSQL only.

Then measured rather than reasoned. An `#[ignore]`d diagnostic
(`tests/durable-send/tests/outbox_claim_cost.rs`) seeds a 200k-row backlog and
runs `EXPLAIN (ANALYZE, BUFFERS)` on every statement a relay cycle executes. It
refuted two things about the R1 fix that had been argued from reading the SQL: the
predicted index was not the one the planner chose (it hash-joins two sequential
scans and spills to disk), and the claim that R1 had made the cycle unbounded was
wrong — `claim_batch` was already the most expensive statement in the cycle and
already scanned the whole backlog. The collapse was still reshaped to drive from
the claim's own bounded window, worth 241ms → 69ms and no temp spill, but for the
smaller reason. The numbers are recorded in `review.md`.

That measurement also surfaced R14: `claim_batch`'s `ORDER BY created_at` can use
no index, because the only index covering its predicate leads with `status` and a
range on `next_attempt_at` destroys ordered retrieval on `created_at`. Every cycle
sequentially scans and sorts — 7,840kB spilled to disk at a 200k backlog, and 368ms
to return 100 rows. Pre-existing, larger than anything this pass introduced, and
left as its own change because the fix is a schema change needing a changeset and a
compatibility note. The benchmark is in place to prove it.

Finally, reconciled `review.md`'s pass-1 outcome table with the shipped code. Three
of its rows had gone stale (F2's entity-key resolution order, F11/F12's "hex
replaced", F15's "zero durations accepted") and asserted things later sections of
the same document contradict, ~250 lines above the corrections. Each now leads with
what is true and points at the section that changed it, following the convention
the document already used for the toolchain pin and the coverage figure.

131 tests pass (1 ignored diagnostic). `cargo fmt --check` and `clippy -D warnings`
clean.

Pages affected:
- review.md
- wiki/compatibility/m5-code-audit-remediation.compat.md
- wiki/log.md
- wiki/index.md

## [2026-08-24] implementation | code audit remediation

Full-workspace line-by-line audit and remediation. Two independent review passes
were merged; every claim was re-verified against source and toolchain before
acceptance. Findings and the phased plan are recorded in `review.md`.

Closed seven silent-failure classes, all of which were gaps between a documented
invariant and its implementation:

- `kafkaman-entity-key` did not survive a Kafka round trip (ingest strips the
  reserved namespace), so any type whose partition key differed from its entity
  key cached under the wrong identity. The entity key is now resolved from the
  typed payload at ingest and stored in a new received-table column.
- `received_entity_key` fabricated a key from `message_id` when nothing else was
  available, giving unbounded non-converging cache growth. Now an error.
- A partition change froze a cache row forever with no signal. Now reported as
  `Error::CacheOriginMismatch`, per decision point 4.
- The Kafka record key ignored `entity_key`, so keyless types scattered one
  entity across partitions.
- `Replay::outbox` republished stored rows at fresh, higher offsets, contradicting
  the README and decision point 9. Now rejected.
- `occurred_at` and `idempotency_source` never crossed the wire.
- The entity advisory lock silently degraded outside a transaction.

Also: `clippy -D warnings` was failing on the branch, on a test whose spawned
task result was discarded — the assertion it was meant to make never ran.

Infrastructure: added `[workspace.lints]`, `rust-toolchain.toml` (tracking
stable), and a CI workflow running the existing `just check` and coverage gates,
whose absence is how the lint failure reached the branch. The toolchain file
initially pinned 1.90.0, which broke rust-analyzer's proc-macro expansion via an
ABI mismatch against the editor's 1.97.1 server; it now tracks stable so the two
cannot diverge. Consolidated ~61 per-test PostgreSQL
containers into one per test binary via a shared test crate; that change also
exposed real cross-test coupling on a shared business table.

Coverage 84.86% lines; 120 tests pass (45 unit, 75 integration including the
Redpanda full loop). *(Corrected 2026-08-24: this entry originally claimed 88.45%,
which is not reproducible by the documented command. See the review-pass entry
above.)*

Pages affected:
- review.md (new)
- wiki/compatibility/m5-code-audit-remediation.compat.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-14] implementation | remove delete-retention public surface

Removed the superseded public retention-class API after the compact
entity-cache purview decision. `RetentionClass`,
`KafkaMessage::retention_class`, and `MessageDescriptor.retention_class` are
gone. `KafkaMessage::entity_key()` is now required rather than defaulting to
`message_id`, so each in-purview kafkaman message supplies a real entity
identity.

Cache-table creation, guarded cache upsert, internal `kafkaman-entity-key`
header insertion, and same-entity outbox supersede now apply to every registered
kafkaman message type instead of branching on `Compact`.

Pages affected:
- README.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/compatibility/m5-entity-first-outbox-supersede.compat.md
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

Code affected:
- crates/kafkaman-core/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- crates/kafkaman-test/src/lib.rs
- apps/axum-outbox/src/lib.rs
- tests/durable-send/tests/durable_send.rs
- tests/durable-send/tests/durable_receive.rs
- tests/durable-send/tests/redpanda_full_loop.rs
- tests/durable-send/tests/entity_first_propagation.rs
- tests/durable-send/tests/entity_first_outbox_supersede.rs

Verification:
- `rtk cargo check --workspace --all-features`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

## [2026-08-14] decision | compact entity-cache purview

Recorded the user's clarified purview decision: kafkaman should be a
distributed cache for compact domain entities only. Non-entity messages such as
"send welcome email to user 123", payments, commands, analytics events, generic
jobs, `mutation_jobs`-style durable queues, and direct transport are outside the
library's product scope.

This supersedes proposal 12's 2026-08-13 option 5 selection of a public
`compact` / `delete` retention-class model. The follow-up implementation removes
the public retention-class surface instead of carrying `delete` compatibility
forward. M4 retry/backoff/DLQ remains relevant as entity-cache pipeline support,
not as a generic durable job queue promise.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/decisions/messaging-scope-and-receive-model.decision.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/compatibility/m5-entity-first-outbox-supersede.compat.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-14] implementation | M5 outbox supersede first slice

Implemented Phase 3's outbound entity ordering foundation. Outbox rows now carry
`entity_key`; `OutboxStatus` includes `Superseded`; fresh outbox tables include
the nullable `entity_key` column plus an entity-state index; and
`AddOutboxEntityKey` upgrades existing outbox tables idempotently.

For compact message types with a valid idempotency identity, enqueue now takes a
transaction-scoped advisory lock keyed by schema, message type, and entity key
before updating same-entity pending rows to `Superseded` and inserting the new
row. Relay claiming also blocks a newer pending row while another row for the
same entity is still `Publishing`, preserving the one-in-flight ordering
contract that makes Kafka offsets usable as the convergence ordinal.

Verified gates:
- `first_concurrent_enqueues_for_entity_serialize`
- `supersede_collapses_queued_updates`
- `publishing_entity_blocks_newer_pending_claim_until_published`
- `add_outbox_entity_key_upgrades_legacy_outbox_table`

Verification:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_outbox_supersede -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

Pages affected:
- wiki/plans/entity-first-propagation.plan.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/compatibility/m5-entity-first-outbox-supersede.compat.md
- wiki/index.md
- wiki/log.md

Code affected:
- crates/kafkaman-core/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- tests/durable-send/tests/entity_first_outbox_supersede.rs

## [2026-08-13] implementation | M5 concurrent cache convergence gate

Added `concurrent_dispatch_of_two_states_converges_to_newer` to the entity-first
propagation integration tests. The gate starts one dispatcher on an older entity
row and holds its transaction open, then lets a second dispatcher claim the
newer row through `FOR UPDATE SKIP LOCKED`. The newer row updates the compact
cache first; when the older row finishes last, the offset guard prevents cache
regression.

Verification:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

Pages affected:
- wiki/plans/entity-first-propagation.plan.md
- wiki/index.md
- wiki/log.md

Code affected:
- tests/durable-send/tests/entity_first_propagation.rs

## [2026-08-13] implementation | M5 cache-upsert first slice

Started M5 on branch `implementation/m5-entity-first-propagation`. Added the
defaulted entity/retention public surface: `RetentionClass`,
`KafkaMessage::entity_key`, `KafkaMessage::retention_class`, and
`MessageDescriptor.retention_class`. Existing message implementations remain
source-compatible through default methods and constructor defaults, but direct
`MessageDescriptor` struct literals must provide the new field.

Added `CacheTable`, `CreateCacheTable`, and compact-type cache table DDL. Receive
dispatch now applies a guarded cache upsert for `Compact` message types before
marking the row `Processed`; older retry/redrive rows still process but do not
regress cache state. The harness creates cache tables for compact received types.

Implemented and verified the first two M5 convergence gates:
`retry_after_newer_applied_does_not_regress_cache` and
`redrive_after_newer_applied_does_not_regress_cache`.

Verification:
- `rtk cargo check --workspace --all-features`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test entity_first_propagation -- --test-threads=1`
- `rtk cargo test --workspace --all-features`

Pages affected:
- wiki/plans/entity-first-propagation.plan.md
- wiki/compatibility/m5-entity-first-cache-api.compat.md
- wiki/index.md
- wiki/log.md

Code affected:
- crates/kafkaman-core/src/lib.rs
- crates/kafkaman-sqlx/src/lib.rs
- crates/kafkaman-test/src/lib.rs
- tests/durable-send/tests/entity_first_propagation.rs

## [2026-08-13] update | M3/M4 merged and proposal 12 accepted for M5

Merged `implementation/m3-durable-receive` into `main` as the prerequisite for
M5, then resolved the pre-M5 proposal 12 decision. Proposal 12 is now Accepted:
every type is an entity, `entity_key` is universal but defaults to `message_id`,
and each type declares a retention class (`compact` / `delete`). `compact`
drives compacted topic configuration, cache-table generation, bootstrap
eligibility, and a meaningful convergence guard; `delete` keeps existing M1-M4
work-item behavior with no cache table and no bootstrap offer.

The entity-first decision is amended accordingly: the implicit entity/non-entity
split is replaced by a declared retention class, without removing the durable
execution surface or reversing the messaging-scope decision. The roadmap now
marks M5 Active and no longer treats its shape as contested. The M5 plan now
includes retention-class declaration, `delete` defaults for compatibility,
`compact`-only cache generation, and boot-time topic validation when broker
metadata is available.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | outbound entity enqueue serialization tightened

Replaced the previous "lock latest outbox row" multi-writer mechanism with
key-level serialization on `(message_type, entity_key)`.

The latest-row lock is insufficient because first concurrent writes may have no
row to lock, and a writer that waited behind another writer can decide from
stale latest state unless it re-reads under the same key-level lock. The
accepted design now requires a transaction-scoped PostgreSQL advisory lock or a
dedicated entity enqueue lock/control row, followed by a re-read of latest
outbox state and the supersede-or-insert decision in the same transaction.

This serializes horizontally scaled instances sharing one database. It does not
coordinate two services or two databases writing the same entity type, which
remains forbidden by entity ownership. The M5 plan now carries explicit gates for
the no-existing-row race and the waiting-writer re-read race, and the roadmap's
M5 exit now states that outbound serialization is limited to the affected entity
key rather than absent. Proposal 12 also notes that direct-mode `compact` types
cannot rely on the outbox lock and must be rejected unless they provide
equivalent key-level outbound serialization.

Pages affected:
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | entity-only reselected as declared retention class

External review of proposal 12, plus a counter-review, converged on changing the
selected option. Four claims were checked against the files; two held.

**The real defect was an incomplete option set.** Proposal 12 listed three
options, all of which kept work items served by kafkaman, so "entity-only" was
selected against a field containing no strict reading of it. Added option 4
(entity-only by exclusion) and option 5 (uniform entity model with a declared
retention class), and reselected 5.

**The decisive argument against the original selection.** Entity types and
work-item types need different topic retention under *every* option — that split
already exists under entity-first, where proposal 11 states "entity topics use
compaction alone." The earlier claim that entity-only *relocated* the duality
into broker configuration overstated it. What entity-only-by-redefinition
actually does is delete the type-level declaration that proposal 11's proposed
boot-time topic validation needs as its input, leaving the same two
configurations with misconfiguration made undetectable and both failure modes
silent. That is strictly worse than entity-first on this axis and better on none.

**Hard exclusion is recorded and costed, not selected.** It argues against the
evidence the messaging-scope decision rests on — RepForge dropped the command
transport and kept `mutation_jobs` — exports the branching into the adopter's
architecture, and strands most of M4, which is work-item machinery.

**The selected option is non-breaking**, by defaulting `entity_key` to
`message_id` and the retention class to `delete`. Existing M1-M4 types compile
and behave unchanged. This closed the migration-story and required-`entity_key`
open questions, and means the proposal promotes as an amendment rather than a
revision: nothing is removed, so the entity-first decision's point 1 still holds.

**Two conflicts surfaced in proposal 11.** Its `kafkaman_cache` "rebuild —
replay is authoritative" directive is compaction-conditional, not universal; and
the resync sweep it describes under business-data restore is what the
self-healing collapse depends on entirely, so its scope must cover non-terminal
work items.

Two review findings were declined as framework noise: a Proposed page has not
superseded an Accepted one, and Accepted pages must not be rewritten to match an
unaccepted proposal. The legitimate residue was backlinking, now added — proposal
09's open question 3, proposal 11's per-type restore bullet, the entity-first
decision's Revisit When, and the roadmap's M5 section all point forward to
proposal 12 as contested, with no Accepted content rewritten.

**Second review pass, same day — two corrections, both accepted.** The revision
above reintroduced the very failure it removed: a summary bullet claiming "one
table shape, one guard, one upsert" contradicted the proposal's own
retention-class table, which generates a cache table for `compact` types and none
for `delete` types. The "What collapses" section is now an explicit accounting —
three of six forks removed, one conditionally — and records that two survivors
(cache-table generation, bootstrap eligibility) are structural rather than
policy, so "the remainder is only configuration" is not available as a defense.

The second correction qualifies the restore claim. The resync sweep is generic
only for entity types, where kafkaman owns the cache table and can enumerate it;
for work-item types "non-terminal" is a predicate over the application's own
status machine, so the sweep becomes a per-type adopter hook. The unification is
therefore **policy-level, not mechanism-level** — the same exported-complexity
cost charged against option 4, at much smaller scale. Named in both proposals for
consistency; it does not change the selection.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] ingest | multi-writer and erasure resolved; entity-only proposed

Three outcomes from the design review of the summary-of-decisions.

**Multi-writer is resolved and moves into the decision.** Enqueue takes a row
lock on the entity's most recent outbox row before checking send status, so a
concurrent writer blocks through the supersede-or-insert commit. This serializes
every instance of the owning service — the case that actually occurs, since
horizontal scaling shares one database. The previous entry claiming this had "no
proposed answer" was wrong: it conflated intra-database scaling with
cross-database writers. The latter remains unsafe and is now forbidden by entity
ownership rather than left open, and proposal 09's open question 6 is closed.

**GDPR erasure is resolved** without a topic rebuild and without real tombstones:
publish the entity with every field but the ID stripped and a deleted status, so
compaction reclaims the earlier PII-bearing records and every cache converges to
the redacted version. Two conditions recorded — the ID must be an opaque
surrogate, since it survives forever by design, and compaction timing becomes a
compliance parameter in direct tension with proposal 11's own suggestion to raise
`min.compaction.lag.ms` to retain recent history. Storage growth from unreclaimed
keys remains unresolved.

**Filed proposal 12, entity-only.** Every message type becomes an entity; work
items are entities with a key unique per item, where the convergence guard is a
harmless no-op, compaction collapses nothing meaningful, and execute-once stays
`idempotency_key`'s job. This removes per-type branching from restore policy,
audit, self-healing, and opt-out config.

Recorded against it rather than glossed: it reverses the messaging-scope
decision's refutation of propagation-only scope, it breaks the general
send/receive surface shipped in M1-M4, and — the finding that emerged while
drafting — it **relocates the duality rather than eliminating it**. Compaction
retains one record per key forever, entity key cardinality is bounded but work
item cardinality is not, so work-item topics need `cleanup.policy=delete`, which
is exactly what proposal 11's retention invariant forbids for entity topics. Two
broker configurations, chosen per type, each with a silent failure mode if
misapplied.

Also fixed index ordering: proposal 11's entry preceded proposal 10's.

Pages affected:
- wiki/proposals/12-entity-only-message-model.proposal.md (new)
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | entity-first slotted as M5; milestones renumbered

Gave entity-first propagation a milestone slot. It had an Accepted decision, a
revised proposal and an execution plan but no place in the delivery sequence,
because the propagation model was decided after the roadmap was written.

Inserted as **M5, ahead of observability**, on the argument that it adds per-type
cache tables and a `Superseded` outbox status — building dashboards, metrics and
DLQ views on the current table layout would mean rebuilding them one milestone
later. Observability moved to M6 and V1 hardening to M7.

Renumbering rather than an out-of-order insert, because four pages outside the
roadmap referenced "M5" meaning observability and would otherwise have drifted:
the M2 plan's deferred admin routes, the M4 spec's "later milestones", the
roadmap-execution-policy decision's ratification note, and proposal 04's
promotion target. All four updated.

Also refreshed the roadmap's stale "Where We Are", which still claimed only M1
code existed, and recorded that the M1-M4 work sits unmerged on
`implementation/m3-durable-receive` with all five pre-merge findings closed —
making the merge the stated prerequisite for M5.

Cache bootstrap/readiness (proposal 10) and restore/retention/schema boundaries
(proposal 11) are explicitly deferred out of M5 and noted as separately tracked,
since proposal 11's schema split breaks M2's single-schema surface.

Pages affected:
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/plans/m2-change-engine-config.plan.md
- wiki/specs/m4-retry-backoff-dlq.spec.md
- wiki/decisions/v1-roadmap-execution-policy.decision.md
- wiki/proposals/04-observability-logging-policy.proposal.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] update | replica renamed to cache

Renamed the entity-store concept from "replica" to "cache" across the wiki, on
the user's call. Two reasons: the project already calls itself "a distributed
cache library rather than a message library to build a cache on", and "replica"
collides badly with Postgres replication in a codebase whose restore policy,
PITR behavior, and physical replication are all under active discussion —
"restore the replica" was becoming ambiguous.

Schema named `kafkaman_cache` rather than `kafkaman_distributed_cache`: from any
single service the schema holds that service's local shard, so distribution is a
system property rather than a schema property.

Renamed `wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md` to
`10-cache-bootstrap-and-readiness.proposal.md` via `git mv`, and updated the five
pages linking to it. `ReplicaTable` became `CacheTable`, `replica_state` became
`cache_state`, and proposal 10's typestate is now `Cache<Bootstrapping>` →
`Cache<Ready>`.

Deliberately **not** renamed: "replica" in the deployment sense — worker
replicas, concurrent replicas in a rolling deploy, application/replica boots —
which appears in the M1 spec and review, the library-test-strategy and
schema-and-change-management decisions, proposal 01, and proposal 08. Verb forms
(replicating, replicated, replication) were preserved everywhere. `raw/` was left
untouched per the immutable-provenance rule, and prior log entries were left
as-written per append-only.

Corrected an assertion made during the discussion: it is not true that the cache
has no origin to fall back to. The compacted topic *is* the origin — that is what
makes bootstrap-from-zero work at all. What it lacks is a per-key fallback, since
Kafka has no key-based random read, so refill is necessarily bulk replay. That
asymmetry is now recorded in proposal 10 as the reason readiness must be a
typestate rather than lazy-loading.

Also repaired stale content in proposal 10 exposed by the rename: its drift-repair
option still described the outbox-sequence versioning caveat and claimed republish
"is only safe for types using a domain version". Both are obsolete; it now carries
the state-sourced republish rule.

Pages affected:
- wiki/proposals/10-cache-bootstrap-and-readiness.proposal.md (renamed from 10-replica-*)
- wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/plans/entity-first-propagation.plan.md
- wiki/index.md
- wiki/log.md

## [2026-08-13] create | restore, retention, and schema boundaries proposal

Promoted the material the same-day ordinal ingest deliberately left in `raw/`
into proposal 11, on the user's call to file it separately rather than fold it
into the entity-first pages.

Records which of the 2026-08-12 discussion's invariants survived the offset
ordinal and which died with it: the immutable-version-source and
producer-state-restore invariants are gone, and the never-restore-the-outbox rule
survives with its rationale *replaced* — republished rows now win on offset
rather than colliding with a rewound sequence.

Departures from the source discussion, each argued in the proposal rather than
asserted: the schema cut is three-way by reconstructibility (drop / protect /
rebuild) rather than inbound/outbound, because the inbound/outbound cut groups
the irreplaceable received table with the fully rebuildable replica table; the
split is justified as backup-set composition and grants rather than independent
restore policy, because PITR and physical replication are cluster-wide; and
`enqueue_on_connection` commits received-row updates and outbox inserts in one
transaction, so restoring the two schemas to different points would tear
committed transactions apart.

Also carried forward the boundary that soft-delete-first makes GDPR erasure a
topic-rebuild operation, and left proposal 08's polling-cost question explicitly
unanswered and assigned.

Pages affected:
- wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-13] ingest | offset as convergence ordinal

Ingested two design sources: the 2026-08-12 restore-policy/schema-separation
discussion and the 2026-08-13 conversation that reviewed it. The review of the
first source dismantled and rebuilt the entity-first version model, so the second
source supersedes parts of the first.

The change: **there is no producer-side entity version.** The convergence ordinal
is the Kafka offset, already persisted as `source_topic` / `source_partition` /
`source_offset` on every received row in shipped M3 code. It is trustworthy only
because of a new send-side rule — per-entity supersede of pending outbox rows —
which makes log order equal truth order and thereby answers proposal 09's
original reason for rejecting offsets.

Removed from the accepted design: the per-entity high-water table, the outbox
monotonicity constraint, the `kafkaman-entity-version` reserved header, the
per-type version-source declaration and its boot-time fail-fast, and the
domain-version override. Added: state-sourced republish (re-emitting a stored
outbox row is unsafe because it receives a new higher offset, which constrains
M2's `Replay::outbox` for entity types), and topic-lifecycle invalidation with
detection that must not key off observing offset 0.

Contradictions resolved with the user rather than silently overwritten: seven
claims in the Accepted entity-first decision, plus the newly surfaced send-side
replay hazard, which inverted an earlier claim made during the conversation that
replay is harmless under a version guard — true for a producer-stamped version,
false for an offset.

**Deliberately not promoted this pass** (user scoped it to the ordinal change):
the restore-policy and schema-separation material from the 2026-08-12 source —
inbound-ledger protection, layered idempotency, and the inbound/outbound schema
split. It remains staged in `raw/design/` as provenance and is referenced from
the new plan's Out of Scope, but has no proposal or decision page. Note that
offset-as-ordinal dissolved that source's invariant 4 and the version-rewind
rationale behind invariants 5 and 6; invariant 5 survives for a different reason
(republished rows win on offset), which is now recorded as state-sourced
republish.

Also corrected stale index bookkeeping: the M4 plan was listed Active while the
page itself reads Completed, and the stage line still said "M3/M4 pre-merge fixes
active". The M3 durable-receive plan is still marked Active while the roadmap
records M3 as Completed — left alone pending a lint pass rather than changed
here.

Two cross-page contradictions were also resolved. Proposal 07 required a foreign
tombstone's version to come from the `kafkaman-entity-version` header; its open
question 2 is now answered, because a Debezium tombstone has a Kafka offset by
virtue of arriving on the topic, so it orders against kafkaman-produced records
with no header and no per-type rule. Proposal 10 referenced proposal 09's
version-source check as a contrast case for compile-time enforcement; that check
no longer exists, and proposal 10 instead gains a second caller — forced
re-bootstrap on topic-lifecycle invalidation is the `Ready` → `Bootstrapping`
transition it already defines.

Pages affected:
- raw/design/2026-08-12-restore-policy-and-schema-separation-discussion.md (new)
- raw/design/2026-08-13-offset-as-convergence-ordinal-discussion.md (new)
- wiki/proposals/09-entity-first-propagation.proposal.md
- wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md
- wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md
- wiki/decisions/entity-first-propagation-model.decision.md
- wiki/plans/entity-first-propagation.plan.md (new)
- wiki/index.md
- wiki/log.md

## [2026-08-12] ingest | error-row rule scope and ownership recorded

Promoted the reserved-header question from an open asymmetry to a decided
boundary, extending the typed-idempotency decision rather than filing a new page
since that decision already owns the error-row rule.

The rule now has an explicit scope: it applies to a send whose envelope is
structurally safe to persist. A send rejected *because its own content must not
enter the ledger* fails before any database work. That draws the line at
"would recording it write something the ledger must not contain" rather than at
severity, and it explains the reserved-header path as consistent with an
existing house pattern — invalid config and invalid retry settings already fail
before touching the database, with tests asserting exactly that — instead of as
an oversight in the error-row rule. Concretely, an audit row for a reserved
header would persist the offending `kafkaman-*` header into the row's `headers`
JSONB, so downstream tooling reading `headers` would observe a spoofed
`kafkaman-message-id`.

Also recorded that the send/receive difference follows from transaction
ownership: on receive kafkaman owns the transaction and can roll back to a
savepoint while persisting its failure record, whereas on send the caller owns
the transaction and the business write inside it. An earlier reading of this as
a design flaw was wrong.

Consequences added: reserved-header rejections leave no ledger trace, so that
failure mode is only visible in application logs; and a caller that commits
after an invalid send holds business state with no corresponding event, which is
a deliberate election of forensics over atomicity that the caller must
reconcile. Revisit triggers added for wanting a durable record of reserved-header
rejections (which would mean persisting the envelope with the header stripped
and the removal noted) and for making identity required at construction or via
typestate, which would remove the missing-identity error row entirely.

Pages affected: wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md,
wiki/index.md, wiki/log.md.

## [2026-08-12] implementation | tests for caller-controlled invalid-send audit

Reworked the invalid-send tests to demonstrate the property they claim. Both
existing tests exercised the commit and rollback branches but wrote no business
data, so neither proved the thing that makes the choice meaningful: that the
caller's own rows share the fate of the audit row. Both now write business data
inside the same transaction and assert it survives a commit and is discarded by
a rollback.

Added the recovery path that makes rollback safe rather than lossy. A
consume-then-produce handler whose enqueue is rejected has its business write
and the audit row discarded together by the handler savepoint, leaves the
receive row `Retryable` with its failure recorded and `processed_at` unset, and
the message is delivered again — completing on the next attempt once the handler
supplies an identity. The recorded failure is asserted to be the rejected
enqueue rather than an incidental error.

Clarified during discussion that the send/receive asymmetry follows from
transaction ownership, not inconsistency: on receive kafkaman owns the
transaction and can roll back to a savepoint while persisting its failure
record, whereas on send the caller owns the transaction and the business write
inside it, so kafkaman must not decide whether that write survives. An earlier
reading of this as a design flaw was wrong.

The reserved-header path is pinned by a test rather than left implicit: a
rejection returns before the insert, so it leaves the caller nothing to commit
even if they want the record, while a missing idempotency identity does. That
behaviour was kept and is now decided rather than open — see the follow-up entry
above.

Verified: durable_send 20 passed, durable_receive 27 passed, redpanda_full_loop
10 passed, workspace 85 passed across 23 suites, clippy clean.

Pages affected: wiki/compatibility/typed-idempotency-identity-api.compat.md,
wiki/log.md.

## [2026-08-12] implementation | typed idempotency fix verified against Docker

Ran the Docker-backed gates that the typed idempotency fix plan had never
executed. The earlier `CreateContainer(RequestTimeoutError)` was a cold Docker
daemon, not a defect. First execution produced seven `durable_receive` failures
in three classes: a latent serialization defect exposed by the F3/F4 fix, two
tests querying `idempotency_key` with pre-digest plaintext, and a
diagnosability regression where a missing row was reported by its digest rather
than the caller's key.

The load-bearing finding is the first. `ReceivedError::occurred_at` was a bare
`OffsetDateTime`, so `time`'s default serde wrote a component array that
`(errors -> -1 ->> 'occurred_at')::timestamptz` cannot parse. Nothing had ever
cast that field, so the wrong format was invisible until the fix reached for it.
The deeper problem was the cast itself: deriving triage state from audit JSON
couples DLQ queries to a serialization format. Failure time and kind were
promoted to `last_failed_at` and `last_failure_kind` columns, which decouples
them, makes triage indexable, and frees the audit records to carry annotated
RFC 9557 timestamps that PostgreSQL cannot cast at all.

Confirmed empirically against PostgreSQL 16 that `timestamptz` rejects both
`...Z[UTC]` and `...+01:00[Europe/London]`, and that RFC 9557 is a strict
superset of RFC 3339, so an unannotated timestamp is valid under both. Stored
failure records were reshaped as RFC 9457 problem details (`type` as a stable
`urn:kafkaman:problem:*` URI, `title`, `detail`, `occurred_at` as an extension
member; `status` omitted as HTTP-specific), with read-side aliases so
pre-problem-detail rows stay readable and an unknown `type` degrades to the
default kind rather than failing the read.

Two further defects surfaced while fixing these. DLQ inspection and bounded
redrive ordered by failure time with a random `message_id` tiebreak, so a
`max_rows` redrive could select an unpredictable subset of rows failed by the
same dispatch pass; ordering is now `last_failed_at, created_at, message_id`.
And `apps/axum-outbox` enqueued without an idempotency identity, so the
reference example had silently stopped working under the F1 contract — its
error type renders as a 200 with a text body, which is why its test asserted a
successful status and then failed parsing JSON.

Verified: `durable_send` 19 passed, `durable_receive` 26 passed,
`redpanda_full_loop` 10 passed, workspace 83 passed across 23 suites, clippy
clean. All five pre-merge review findings are closed and the branch now meets
its own merge gate.

Pages affected: wiki/plans/typed-idempotency-identity-error-row-fix.plan.md
(Active to Completed), wiki/compatibility/typed-idempotency-identity-api.compat.md
(Draft to Active, expanded), wiki/reviews/m3-m4-pre-merge-branch-review.reference.md
(resolution section), wiki/index.md, wiki/log.md.

## [2026-08-12] ingest | entity-first propagation design discussion

Ingested the 2026-08-12 design discussion that settled kafkaman's positioning as
a reference propagation system and the identity model that positioning requires.
Central finding: dedup is not convergence. The existing `idempotency_key`
answers "have I executed this work item", while a replica needs "is this newer
than what I hold", and diff-and-upsert cannot close the gap because kafkaman's
own retry backoff, `Replay::received` redrive, redelivery, and bootstrap replay
all reorder application by design. Fixed a required `entity_version` sourced
from the outbox sequence by default and overridable by a domain version, since
direct transport mode has no outbox and republish-based drift repair corrupts
the replica under sequence versioning. Adopted entity-only messages across the
existing inbox plus a new guarded per-entity replica table, an outbox
monotonicity constraint as source-side defense-in-depth, and advisory
`#[non_exhaustive]` origin intent with a mandatory catch-all wire variant —
without which a single new variant can trip the ingest circuit breaker
topic-wide. Reversed proposal 07's emission choice to soft-delete-first, because
real tombstones are reclaimed after `delete.retention.ms` and a long-offline
replica misses the delete; proposal 07 keeps real-tombstone ingestion for
foreign producers. Filed the bootstrap/backfill gap left open by the previous
ingest as proposal 10. Implementation planning deliberately deferred until
M3/M4 merges and the typed-idempotency fix plan lands.
Contradiction resolved: proposal 07's selected option (per-type real tombstones
on send and receive) conflicted with soft-delete-first; the user resolved it in
favor of soft delete during the discussion, and proposal 07 was revised in place
with the reversal recorded rather than silently rewritten.
Pages affected: `raw/design/2026-08-12-entity-first-propagation-discussion.md`,
`wiki/proposals/09-entity-first-propagation.proposal.md`,
`wiki/proposals/10-replica-bootstrap-and-readiness.proposal.md`,
`wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`,
`wiki/decisions/entity-first-propagation-model.decision.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-12] ingest | prior-art review of cqrs-fullstack messaging

Reviewed the `cqrs-fullstack` messaging implementation in the workout2 project
snapshot as external prior art for kafkaman's durable send/receive model. The
snapshot implements the same pattern by hand: a transactional `outbox_events`
table with claim-by-`SKIP LOCKED`, stuck-row reclaim, attempt counters, and a
retention purge; and per-topic inbox tables where the consumer only enqueues and
commits offsets before a separate dispatcher executes domain work with retry and
dead-letter state. Two capability gaps were identified and filed as proposals:
Kafka tombstones are unrepresentable in kafkaman and currently quarantine as
`MissingPayload`, and both schedulers poll on a fixed interval with no
notification-driven wakeup. A third gap, bootstrap/backfill of a new reference
replica, was identified but not yet filed pending a positioning decision on
kafkaman as a reference builder.
Pages affected: `wiki/proposals/07-tombstone-and-deletion-semantics.proposal.md`,
`wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md`,
`wiki/index.md`, `wiki/log.md`.

## [2026-08-12] implement | typed idempotency identity fixes

Implemented typed SHA-256 idempotency identity with retained source JSON,
transactional invalid-send outbox audit rows, digest Kafka header parsing,
latest-failure-time DLQ/redrive filtering, and received-only replay checksum
cleanup. Added regression tests and a draft compatibility note. Verification
passed for non-Docker compile/tests and clippy; Docker-backed integration
execution is pending because testcontainer creation timed out with
`CreateContainer(RequestTimeoutError)`.

Pages affected:
- `wiki/compatibility/typed-idempotency-identity-api.compat.md`
- `wiki/plans/typed-idempotency-identity-error-row-fix.plan.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-08-12] create | typed idempotency identity and error-row symmetry

Accepted the typed idempotency identity proposal and recorded the durable
decision that kafkaman idempotency is a SHA-256 digest plus caller-provided JSON
source material. Added an active implementation plan to close the M3/M4
pre-merge review findings and to apply the send/receive rule: record invalid or
problem work transactionally, return an error, allow caller rollback, and have
workers claim only non-error rows.

Pages affected:
- `wiki/proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md`
- `wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md`
- `wiki/plans/typed-idempotency-identity-error-row-fix.plan.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-06-23] review | M3/M4 pre-merge branch

Recorded an adversarial pre-merge review of branch
`implementation/m3-durable-receive` against local `main`. Findings: send-side
idempotency remains optional while receive-side ingest requires
`kafkaman-idempotency-key`; empty idempotency keys are accepted and collapse
unrelated receive rows; DLQ inspect/redrive "failure time" semantics use business
or row time instead of latest failure time; received-only replay knobs can alter
outbox replay checksums without changing outbox SQL. Verification recorded:
workspace all-features compile/tests and clippy passed; one durable-send package
`PoolTimedOut` flake passed when rerun in isolation.

Pages affected:
- `wiki/reviews/m3-m4-pre-merge-branch-review.reference.md`
- `wiki/index.md`
- `wiki/log.md`

## [2026-06-22] promote | M4 retry/backoff/DLQ spec

Completed M4 Phase 3/4 and promoted validated behavior into an Active spec
`specs/m4-retry-backoff-dlq.spec.md`. Added `ReceivedFailureFilter` (since +
kind) narrowing on the DLQ inspect surface, `Replay::failure_kind` to scope
redrive by failure kind, and `Replay::clear_history` for clean-slate redrive
(default redrive preserves attempts and error history). Phase 4 added no Redpanda
coverage by design — retry/backoff/DLQ is Postgres-side. Updated the M4 plan to
Completed, the roadmap M4 status, the index, and the runtime-API compat note.

## [2026-06-22] add | M4 Phase 3 DLQ inspect surface

Added `received_failed_rows` and `received_failed_count` to `kafkaman-sqlx` so
operators can inspect the terminal `Failed` (DLQ) backlog before redriving with
`Replay::received`. The list is oldest-first, `limit`-bounded, and preserves
attempts and error history. Recorded under the M4 plan Phase 3 progress. Test:
`received_failed_rows_inspect_surface_lists_terminal_dlq_rows`.

## [2026-06-22] fix | Replay::received redrive targets terminal Failed rows

The M4 retry-backoff slice scheduled a `next_attempt_at` on every `Retryable`
failure, which made the prior `Retryable` + `next_attempt_at IS NULL` parked
shape unreachable and stranded the `Replay::received` operational surface.
Repointed redrive at exhausted terminal `Failed` rows, preserving attempts and
error history. Reconciled the M3 spec replay paragraph and limitation note, and
recorded the change under M4 plan Phase 3. Test renamed to
`replay_received_redrives_failed_rows_without_replaying_processed_rows`.

## [2026-06-22] promote | M3 durable receive spec

Promoted validated M3 durable receive behavior into an Active spec. The spec
captures Kafka ingest ordering, durable quarantine, consecutive-skip breaker,
idempotency-key dedup, message-id conflict quarantine, receive dispatch,
failure classification, replay semantics, production loops, and atomic
consume-then-produce evidence. Updated the project stage and marked the M3
completion plan completed while leaving remaining chaos/model cases in the
deep-durability hardening backlog.

Pages affected:
- wiki/specs/m3-durable-receive.spec.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md

## [2026-06-22] update | M3 Phase 4 consume-then-produce J3

Extended the consume-then-produce atomicity test through J3. The existing
handler surface (`&mut PgConnection`, `ReceivedMeta`, and `enqueue_on_connection`)
now proves success commits business row plus follow-up outbox, failure rolls
both back, and duplicate input redelivery deduplicates without a second business
effect or outbox row. Reconciled and accepted the central
`message-consumption-and-handler-model` decision for the shipped M3 surface.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive handler_enqueues_outbox_atomically_with_receive_transaction`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop full_loop_consume_then_produce_deduplicates_duplicate_input -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md

## [2026-06-22] update | M3 A1 real two-worker dispatch interleaving

Added `kafkaman-sqlx` `test-hooks` support for pausing after a failed handler
transaction rolls back and before failure accounting is recorded. Added the
real two-worker A1 regression: worker A fails and pauses before stale failure
accounting, worker B processes the same durable row through normal
`dispatch_once`, and worker A resumes without clobbering the processed row or
over-reporting failure stats.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md

## [2026-06-22] update | M3 G1/G2 ingest uncertainty gates

Added `kafkaman-rdkafka` `test-hooks` support for injecting a failure after
the durable receive write commits and before Kafka offset commit. Added
Redpanda tests for G1 crash-window redelivery and G2 runner-level offset commit
uncertainty; both prove same-group redelivery deduplicates to the existing
received row and then commits the broker offset.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/proposals/05-deep-durability-testing.proposal.md

## [2026-06-22] update | M3 production ingest runner

Added the production rdkafka ingest runner slice. The compatibility note now
records `IngestLoopStats` and `RdkafkaConsumer::run_ingester`, and the M3
completion plan notes the cancellable loop, transient retry backoff, and loud
poison-breaker stop semantics.

Proof:
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --tests`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

Pages affected:
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md
- wiki/plans/m3-durable-completion.plan.md

## [2026-06-22] update | M3 durable completion implementation slice

Implemented and documented the M3 durable completion slice covering Phase 1A
dispatch hardening, Phase 2 receive replay, Phase 3 receive ingest/dispatcher
loop, and the Phase 4 minimal consume-then-produce handler surface. Added
accepted decisions for MissingHandler policy, post-handler infrastructure error
classification, dispatch stats semantics, Kafka ingest identity/ordering, and
receive handler surface scope. Proof commands recorded in the plan progress:
`cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets
--all-features -- -D warnings`; `cargo test --workspace --all-features`;
`cargo test --manifest-path tests/durable-send/Cargo.toml --tests`;
`cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda
--test redpanda_full_loop`.

Pages affected:
- wiki/plans/m3-durable-completion.plan.md
- wiki/index.md
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/decisions/missing-handler-dispatch-policy.decision.md
- wiki/decisions/dispatch-infrastructure-error-classification.decision.md
- wiki/decisions/dispatch-stats-semantics.decision.md
- wiki/decisions/kafka-ingest-identity-and-ordering.decision.md
- wiki/decisions/receive-handler-surface-scope.decision.md

## [2026-06-22] update | M3 durable completion plan review fixes

Applied a plan review (all five findings verified valid). Phase 1 split into
Phase 1A blockers (C1 head-of-line, C3 poisoned-tx, M3-stats, A1 real-worker) that
gate Phase 3 and Phase 1B invariant pins (B2, D1-D3, E2, I1, K4, K5) that may
defer. Scoped Phase 3's full-loop gate to receive-only effective-once and added a
Phase 4 consume-then-produce full-loop gate so a green Phase 3 cannot read as
end-to-end. Added Phase 2 and Phase 4 specific verification gates (injected-clock
due-boundary; Replay dry-run/apply/audit/guardrail/idempotence). Added a closure
step to accept/reconcile the still-Draft `message-consumption-and-handler-model`
decision before spec promotion. Bumped the index Updated date to 2026-06-22.

Pages affected:
- wiki/plans/m3-durable-completion.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] create | M3 durable completion plan

Created a dedicated completion plan sequencing the remaining M3 work after the
first slice and its review fixes. Adopts a tests-lead strategy grounded in the
deep-durability-testing proposal: Phase 0 scaffolding (interleaving primitive +
P1 oracle fix), Phase 1 hardening of the `dispatch_once` seam against the
gap-revealing catalog (A1 real workers, C3 poisoned-tx, C1 head-of-line block,
B2 crash window, M3 stats, D/E/I/K pins) with each forced choice recorded as its
own decision doc, Phase 2 operational replay + injected clock, Phase 3 Kafka
ingest + dispatcher loop with full-loop Redpanda coverage, Phase 4 decision-gated
handler surface for consume-then-produce, and Phase 5 chaos/model + spec
promotion. Rationale: the proposal predicts real defects in current `dispatch_once`
(C1/C3) and forces ingest-shaping decisions (I2/G3/K5), so the seam is hardened
and its contracts decided before an engine wraps it.

Pages affected:
- wiki/plans/m3-durable-completion.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 review M1/L3 follow-up resolved

Landed handler metadata access (M1) and the L3 nullability note, closing the last
two M3 implementation-review findings. Added `ReceivedMeta` to `kafkaman-core`,
rethreaded `MessageRouter::handler`/`dispatch_once`/the erased handler trait to
pass `Fn(&mut PgConnection, ReceivedMeta, P)`, documented the nullable
`correlation_id`/`causation_id` columns inline, and added the
`dispatch_exposes_message_metadata_to_handler` gate (durable_receive now 11
tests). Marked the review Resolved and recorded the slice in the M3 plan Progress;
only out-of-scope M3 surface area (`FromMessage`/`Rx`/Tower, `Replay::received`,
injected clock, macros, Kafka ingest, full-loop) remains.

Pages affected:
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/plans/m3-durable-receive.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | Deep durability testing second review verification

Verified the second follow-up review against the proposal, M3 review, dispatch
SQL/control flow, handler API, received insert path, and receive tests. Updated
the proposal to remove the unsupported "incorrect review claim" framing, narrow
B1's landed status to panic-only rollback, align A2/A3 matrix status with their
landed sequential tests, and add verified gaps for stale-failure `DispatchStats`
over-reporting, consume-then-produce API support, hard-coded received
`message_version = 1`, and the load-bearing success/failure status-guard
asymmetry.

Pages updated:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | Deep durability testing review verification

Double-checked the follow-up review against `dispatch_once`,
`record_received_failure`, received-table DDL, core receive statuses, Harness
registration paths, and receive integration tests. Updated the proposal to
correct B2's attempt-accounting mechanism, strengthen C1's head-of-line
description, add the poisoned-transaction success-branch gap, track parked
retryables as already test-pinned, add reserved status variant drift coverage,
and add timestamp precision test-oracle hardening. Confirmed that parked
retryables are already covered by
`dispatch_failure_rolls_back_effect_and_parks_retryable`.

Pages updated:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | Deep durability testing proposal review

Expanded the deep durability testing proposal from the original M3 receive-focused
catalog into a reviewed durability-test roadmap. Marked landed receive
regressions, added rare-failure classes for ambiguous commits, cancellation,
Kafka offset uncertainty, idempotency/source-offset collisions,
consume-then-produce atomicity, schema/config drift, observability safety,
send-side mirrors, and model/chaos testing, and refreshed priority order.

Pages updated:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md

## [2026-06-21] create | Direct mode and observability policy proposals

Captured two proposed design directions from user discussion: an explicit non-durable direct Kafka producer/consumer mode for high-throughput or low-durability workloads, and a configurable `tracing`-based observability/logging policy with per-message-type overrides and payload-safety controls.

Pages created:
- wiki/proposals/03-direct-transport-mode.proposal.md
- wiki/proposals/04-observability-logging-policy.proposal.md

Pages updated:
- wiki/index.md

## [2026-06-21] fix | M2 change-engine + config review findings resolved

Implemented the fixes from the M2 implementation review. Code:
- H1: Axum example resolves config before opening a pool or touching the schema.
- H2: `ResolvedConfig::from_config` validates a present `[retry]` section on the
  boot path (new `Config::contains`); boot-path gate test added.
- H3: checksums are now SHA-256 hex (`sha256:<hex>`, new `sha2` dep), replacing
  the interim `fnv1a64:` format.
- H4: `migrate_dry_run` runs inside a rolled-back transaction, so it persists no
  bootstrap DDL, `applied_by` backfill, or changelog rows; gate test added.
- M1: `Replay` resets `attempts`/`last_error` on requeued rows.
- M2: `kafkaman.example.toml` labels `database.url`/`kafka.brokers` as host-owned.
- M3: dry-run uses `pg_try_advisory_lock`, returns new `Error::MigrationLockBusy`.
- M4: added boot-path retry and dry-run-legacy-history gate tests.
- L1/L2/L4/L5: explicit nullable checksum decode, `try_changelog!`, zero-duration
  rejection, `errors_limit` is `u32`. L3 (bind-carrying statements) deferred.

Verification: fmt/clippy/check clean; 13 unit + 17 durable-send + 1 axum-http
tests pass. Pages affected: wiki/specs/m2-change-engine-config.spec.md,
wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md,
wiki/reviews/m2-change-engine-config-implementation-review.reference.md,
wiki/log.md.

## [2026-06-21] promote | M2 change-engine/config implementation validated

Implemented M2 change-engine and configuration slice: dedicated `kafkaman-config`
crate, config-loaded `ResolvedConfig`, migration reports, nullable checksum and
`applied_by` changelog audit columns, checksum drift enforcement,
`changelog!`, dry-run preview, and guarded send-side `Replay`.
Pages affected: wiki/specs/m2-change-engine-config.spec.md,
wiki/compatibility/m2-change-engine-config-schema-and-api.compat.md,
wiki/roadmaps/path-to-v1.roadmap.md, wiki/plans/m2-change-engine-config.plan.md,
wiki/index.md.

## [2026-06-21] create | V1 remaining decision pass

Recorded user-selected options for the remaining V1 planning decisions:
required idempotency keys for all persisted V1 messages, rejected
user-supplied `kafkaman-*` headers, per-message retry/backoff/DLQ runtime
configuration in `kafkaman.toml`, table-backed V1 DLQ, split sub-decision vs
milestone validation status, and dependency-aware parallel worktrees.

Pages created:
- wiki/decisions/message-identity-and-header-namespace.decision.md
- wiki/decisions/retry-backoff-dlq-policy.decision.md
- wiki/decisions/v1-roadmap-execution-policy.decision.md

Pages affected:
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/decisions/configuration-and-environment-model.decision.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-06-20] promote | M1 durable-send implementation validated

Implemented the M1 durable-send slice on branch `implementation/m1-durable-send`.
The workspace now includes core types, SQLx migration/outbox primitives, claim
lease relay worker, rdkafka publisher, Docker-free Harness seed, testcontainers
Postgres integration gates, and a runnable Axum outbox example. Promoted the
validated behavior into an active spec and marked the two M1 plans completed.

Evidence:
- `cargo test --workspace` passed with 10 tests across 16 suites in 7.34s.
- Integration tests start Postgres through `testcontainers`.

Pages affected:
- specs/m1-durable-send.spec.md (created, Active)
- plans/first-poc-outbox-publisher.plan.md (Status: Completed)
- plans/m1-durable-send-implementation.plan.md (Status: Completed)
- roadmaps/path-to-v1.roadmap.md (M1 Status: Completed)
- index.md (catalog refreshed)

## [2026-06-20] update | M1 plan review fixes

Corrected the M1 implementation plan after review. The relay model now uses a
real claim lease/token flow instead of an impossible single transaction around
Kafka publish; `Publishing` rows are reclaimable after lease expiry. The plan now
uses object-safe changesets, typed message descriptors, validated SQL identifiers,
stale-claim-safe `mark_published`, `mark_publish_failed`, and a Docker-free
capturing-publisher Harness default with opt-in Redpanda/full-loop support.
Rewrote the first-PoC plan to match those mechanics and strengthened verification
gates so delivery claims require both a published/captured record and the expected
outbox row state.

Pages affected:
- plans/m1-durable-send-implementation.plan.md
- plans/first-poc-outbox-publisher.plan.md
- decisions/schema-and-change-management.decision.md

## [2026-06-20] lint | design review fixes (8 findings) + M1 implementation plan

Actioned an external design review of the four Draft decisions, the roadmap, and
the PoC plan. Fixes:
- **Receive state machine (#2):** pinned the dispatch claim predicate
  (`status IN (Pending, Retryable) AND next_attempt_at <= now()`) and the status
  set (`Pending→Processing→Processed`, `→Retryable→`, `→Failed`) in the
  message-consumption decision; only the retry *policy* stays deferred.
- **Effective-once scope (#1):** message-consumption point 6 now states the
  guarantee covers only writes through `Rx`; external side effects are
  at-least-once and need their own idempotency/outbox. Handler example comment
  corrected.
- **`send_now` footgun (#4):** runtime decision renames it
  `send_non_transactional`, quarantines it off the default `Sender` surface,
  requires a counter/span.
- **Subsystem default (#5):** runtime decision default is now the smallest
  non-destructive set (relay + consumers, never `Purge`); explicit selection.
- **Daemon vocabulary (#6):** reconciled "no standalone daemon" across the runtime
  decision and the proposal's `kafkaman-worker` note ("worker-role host binary").
- **"Full" vs bounded history (#7):** message-consumption `errors` reworded to
  "recent failure history" (bounded ring, explicitly not an audit log).
- **raw/ immutability (#8):** marked the design-discussion source as append-only
  provenance, distinct from compiled `wiki/` knowledge.
- **Roadmap (#3 + TDD):** dropped the blanket "flip four decisions to Accepted at
  M1"; now "Accepted design direction + revisit gate at each milestone's exit",
  only the send-side of runtime ratifiable at M1. Added an Outside-In TDD working
  method (the failing `Harness` test drives the API).
- Recorded the **dedup-identity open question** (idempotency key vs message_id vs
  offset) in the message-consumption decision, with a recommended default to
  ratify before M3.

Page created:
- plans/m1-durable-send-implementation.plan.md — code-level M1 plan (crate layout,
  deps, types/signatures, migrate engine + outbox DDL, enqueue/claim/mark, relay,
  Harness seed, ordered outside-in-TDD tasks → PoC gates).

## [2026-06-20] create | project bootstrap

Initialized `kafkaman` with the LLM Wiki framework.

## [2026-06-20] ingest | kafkaman objectives research bundle

Ingested `raw/research/2026-06-20-kafkaman-objectives-rust-microservice-architecture/`
(research-summary.md, manifest.md, sources 02-12). The 01-cqrs-fullstack copy is
on disk but gitignored; its distilled form (research-summary.md §2) was used.

Pages created:
- proposals/01-kafkaman-objectives.proposal.md
- proposals/02-messaging-scope-kafka-command-vs-durable-execution.proposal.md
- references/rust-kafka-outbox-ecosystem.reference.md
- plans/first-poc-outbox-publisher.plan.md
- index.md updated.

Contradictions: none in the wiki (it was empty). One real architecture tension
recorded as proposal 02 (host architecture uses Kafka for propagation + internal
HTTP for commands, vs kafkaman's original Kafka-instead-of-REST idea) — filed as
a candidate awaiting ratification, not an accepted decision.

Gaps: outbox-pattern-processor docs.rs/crates.io version recency disagreed
(0.3.6 vs 0.4.0). Five v1-scope open questions remain in proposal 01.

## [2026-06-20] promote | messaging scope ratified to a decision

Ratified proposal 02 as a decision after reviewing the cqrs-fullstack migration
history (commands-over-Kafka was built and dropped in migration 010; replaced by
the durable HTTP `mutation_jobs` engine). Choice: durable-execution-first core,
Kafka-only transport in v1, HTTP/commands/synchronous outcomes deferred, receive
is fire-and-forget + durable status with `correlation_id`/`causation_id` in the
envelope and wait-for-outcome reserved as future primitives.

Pages affected:
- decisions/messaging-scope-and-receive-model.decision.md (created, Accepted)
- proposals/02-messaging-scope-... (Status → Accepted, promoted)
- proposals/01-kafkaman-objectives (open questions 2 and 4 resolved)
- index.md (stage → "Scope decided"; Decisions section populated)

## [2026-06-20] lint | external review fixes (provenance + delivery semantics)

Acted on an external review of the bootstrap state. Fixes:
- Provenance: the decision cited the gitignored cqrs-fullstack copy. Added a
  committed verbatim excerpt
  (sources/01-cqrs-fullstack-migration-evidence.md) and repointed the decision's
  sources to it; full project remains local-only.
- Delivery semantics: PoC plan claimed "exactly-once-effectively". Corrected to
  durable at-least-once (publish-then-mark duplicate window), added a
  crash-after-ack-before-mark verification test, noted effective-once belongs to
  the (out-of-scope) consumer idempotency layer.
- Provenance hygiene: expanded abbreviated `raw/research/...` Source paths to
  full resolvable paths in the ecosystem reference.
- Config hygiene: gitignored `.llm_wiki/runtime.toml` (per-machine install state,
  absolute paths); set Serena `languages: [rust]`; trimmed AGENTS.md EOF blank
  line; staged the previously-untracked decision so the index is commit-consistent.

Pages affected:
- raw/.../sources/01-cqrs-fullstack-migration-evidence.md (created)
- decisions/messaging-scope-and-receive-model.decision.md
- plans/first-poc-outbox-publisher.plan.md
- references/rust-kafka-outbox-ecosystem.reference.md

## [2026-06-20] update | execution model + reference stance recorded

From design discussion: recorded the agreed **durable message runtime** model
(durable table → scheduler → annotated handler; receive-tx owned by kafkaman and
handed to the handler; send offers transactional + fire-and-forget enqueue;
runtime `migrate()`), and the guiding stance that the cqrs-fullstack reference is
a discussion starting point, not a blueprint. Physical table layout (per-type vs
partitioning) and purge/retention remain under active discussion, flagged as
open in the proposal.

Pages affected:
- proposals/01-kafkaman-objectives.proposal.md (Execution Model section + reference stance)

## [2026-06-20] create | schema & change-management decision

Captured the design discussion outcome on persistence and operations so the
reasoning is not lost. Decision: dedicated `kafkaman` schema; distinct per-type
tables from one template; per-table UNIQUE for idempotency; DELETE-based purge
(partitioning deferred); a Rust Flyway-style change engine with versioned
changesets (structural + operational unified) tracked in
`kafkaman.changelog_history`; `migrate()` = CI/CD convergence, `Runtime::start()`
= subsystems; retention declared by changeset, enforced at runtime; no SQL
functions; plain tables for break-glass. Alternatives (single generic table,
partitioning, SQL functions, extending host sqlx migrations) recorded with
rejection rationale.

Pages affected:
- decisions/schema-and-change-management.decision.md (created, Accepted)
- proposals/01-kafkaman-objectives.proposal.md (schema/change-management bullet;
  table-layout + purge open items resolved)
- index.md (Decisions section)

## [2026-06-20] update | changeset versioning + placement settled

Settled two changeset details: sequential integer versions (not timestamps —
merge collisions are a deliberate reconciliation forcing function), and the
changelog lives in its own module out of `main` (one changeset per file;
directory-derive macro deferred).

Pages affected:
- decisions/schema-and-change-management.decision.md (point 10 added)

## [2026-06-20] update | realigned the first-PoC plan to the decisions

Rewrote the PoC plan to match the accepted decisions: per-type outbox table in
the `kafkaman` schema provisioned by a **minimal** `kafkaman::migrate()`
(structural changesets only — operational changesets, checksums, audit, and the
`changelog!` macro explicitly deferred); envelope carries
`correlation_id`/`causation_id`; example shows the changelog in its own module
and the two-phase `main`. Delivery semantics stay durable at-least-once. Now
validates the two most expensive commitments (per-type layout + `migrate()`
entry point), not just a generic outbox.

Pages affected:
- plans/first-poc-outbox-publisher.plan.md (rewritten)
- index.md (plan summary)

## [2026-06-20] lint | external review fixes (round 2)

Acted on a second external review. Fixes:
- Provenance: created raw/design/2026-06-20-kafkaman-architecture-discussion.md
  (curated design-discussion note) and repointed both decisions' dangling
  "Design discussion" source to it.
- messaging-scope decision: "without a schema migration" → "without changing the
  existing envelope fields" (the waiter store still adds tables).
- PoC plan: softened the per-type claim (validates the template + plumbing, not
  the operational advantages) and added a second `CreateMessageTable` + a
  template-generalizes gate at near-zero cost.
- schema decision: added an "Open Refinement: Guardrails for Operational
  Changesets" section (env targeting, dry-run, auto-vs-gated apply mode,
  blast-radius limits); auto-vs-gated default flagged OPEN, leaning gated.
- reference: softened "no dominant Rust crate" → "this research did not find…".
- Trimmed EOF blank lines from 9 raw files (whitespace only, no provenance
  impact) for a clean first commit; `git diff --check` now clean.

Pages affected:
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (created)
- decisions/schema-and-change-management.decision.md
- decisions/messaging-scope-and-receive-model.decision.md
- plans/first-poc-outbox-publisher.plan.md
- references/rust-kafka-outbox-ecosystem.reference.md

## [2026-06-20] update | operational-changeset apply mode resolved → auto-apply

Closed the OPEN guardrail question: operational changesets **auto-apply** like
structural ones (rationale: reaching prod means convergence through lower envs).

## [2026-06-20] create | configuration & environment decision

After discussion (and reverting an earlier premature retention→config edit),
settled the config/env model and recorded it as its own decision. Model: a
changeset is `apply(env, builder)`; `env` may select values, not branch
structure. One flat `kafkaman.toml` (no profiles) is rendered per environment by
CI/CD, injecting values/secrets from the vault; the app reads one resolved file,
validated at startup. Tunable settings (retention, batch sizes) are **runtime
config re-read each boot**, not changesets — so they are tunable via config +
redeploy without authoring a changeset (a run-once changeset could not be
re-tuned). Schema decision updated to match (points 6, 7, 9, +11, guardrails);
`SetRetention`-as-changeset removed.

Pages affected:
- decisions/configuration-and-environment-model.decision.md (created, Accepted)
- decisions/schema-and-change-management.decision.md (aligned)
- proposals/01-kafkaman-objectives.proposal.md (change-engine bullet)
- plans/first-poc-outbox-publisher.plan.md (out-of-scope)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (config section)
- index.md (Decisions section)

## [2026-06-20] update | operational-changeset guardrails finalized

Made the safety nets explicit and mandatory — bulk-op batching/rate-limiting
(since lower-env testing validates correctness but not prod volume), dry-run
visibility in deploy logs, and env-targeting as the per-operation opt-out.
Residual risk (large op at prod scale + replay downstream amplification) recorded
as accepted.

Pages affected:
- decisions/schema-and-change-management.decision.md (guardrails section finalized)

## [2026-06-20] update | migrate() runs at app startup

Clarified where `migrate()` runs: **at app startup** (in `main`, before
`Runtime::start()`), mirroring the reference's `sqlx::migrate!` in `main` —
advisory-locked and idempotent. Standalone pre-deploy invocation kept as an
option. To keep startup non-blocking under auto-apply, operational changesets do
only fast bounded state changes (`Replay` flips status / bumps epoch); the heavy
rate-limited draining is the runtime subsystems' job, after boot. (Retention is
runtime config, not a `SetRetention` changeset — see the later config &
environment decision entry, which supersedes any earlier `SetRetention` mention.)

Pages affected:
- decisions/schema-and-change-management.decision.md (phase 7 sharpened)
- proposals/01-kafkaman-objectives.proposal.md (migrate bullet)

## [2026-06-20] lint | external review fixes (config decision, round 3)

Acted on a third external review (config & environment decision). Fixes:
- Secrets hygiene: `.gitignore` now ignores the resolved `kafkaman.toml` and
  keeps `!kafkaman.example.toml` tracked (previously only `.env*` was covered, so
  a rendered config with injected secrets was one `git add` from leaking).
- Config-API contradiction resolved: the decision said kafkaman *loads*
  `kafkaman.toml` while the alternatives said it *consumes a property bag the host
  produces*. Settled on a **thin loader by convention** (sqlx-style); reworded the
  rejected alternative to "a full config *framework* (profiles/layering/
  precedence)", which is what is actually rejected.
- "Values not structure" made contract-enforced, not policy: changesets now
  receive a resolved **config bag** (`apply(&self, cfg, b)`) exposing typed values
  with **no env identity** to branch on — so the rule is guaranteed by the API,
  not left to discipline. Aligned the schema decision (points 9, 11) and the raw
  design note.
- Created the committed `kafkaman.example.toml` the decision asserted exists
  (illustrative/pre-implementation; documents the intended keys).
- Log hygiene: restored a heading on an orphaned guardrails entry; corrected a
  superseded `SetRetention`-as-changeset mention in the migrate-at-startup entry.

Pages affected:
- .gitignore
- kafkaman.example.toml (created)
- decisions/configuration-and-environment-model.decision.md
- decisions/schema-and-change-management.decision.md
- raw/design/2026-06-20-kafkaman-architecture-discussion.md
- wiki/log.md (orphaned heading + supersede note)

## [2026-06-20] create | runtime composition & topology decision (draft)

Drafted the runtime/topology decision resolving objectives OQ1. Schedulers are
spawnable units (`runtime.run(shutdown)` over all subsystems, or `into_tasks()`
per subsystem) honoring a `CancellationToken`; kafkaman owns neither a process
nor the Tokio runtime. Topology (embedded vs worker-role) is a host choice via
`.subsystems(...)`; no standalone daemon since handlers are compiled-in Rust.
Request-path concerns are Axum-native (`CorrelationLayer`, admin/DLQ routes,
`serve().with_runtime()` shutdown helper); background loops are never middleware.
Send UX is opinionated around `axum-sqlx-tx`: a `Sender` extractor enqueues into
the host's ambient auto-committing tx (no manual `commit()`; business write +
outbox row commit atomically), with `send_now` as the fire-and-forget opt-out;
core `enqueue(&mut tx)` stays generic in kafkaman-sqlx. Receive/consumption side
deferred to the next discussion. Status: Draft.

Pages affected:
- decisions/runtime-composition-and-topology.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (OQ1 resolved; kafkaman-axum bullet)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (runtime composition section)
- index.md (Decisions section)

## [2026-06-20] create | message consumption & handler model decision (draft)

Drafted the receive-side decision. Two schedulers so user code never locks Kafka:
an ingest scheduler consumes → writes the received row → commits the offset
immediately; a dispatch scheduler polls (`SKIP LOCKED`) → runs the handler. Dedup
is a log (`ON CONFLICT DO NOTHING`); failures accumulate in a bounded `errors`
JSONB array (most-recent-N ring; `attempts` is the authoritative counter). Handler
model is axum-shaped but its own thing, not HTTP: a `MessageRouter` that *is* a
`tower::Service<Message>`, keyed by `message_type`, explicit `.handler::<T>(fn)`
registration, kafkaman `FromMessage` extractors; generic Tower middleware is
inherited, http-bound axum/tower-http pieces are not (no http masquerade). The
receive tx relocates to kafkaman (Ok → business write + mark Processed commit
together). Hybrid wiring = config → migrate → consumer tower → axum tower → one
server. Retry/backoff/DLQ taxonomy deferred (fields reserved).

Consistency fixes: Core Promise #2 corrected ("offset after the handler" →
"offset after the durable receive write"); Execution Model bullet rewritten for
the two-scheduler model; objectives OQ3 (polling vs CDC) resolved → polling, CDC
not pursued for v1.

Pages affected:
- decisions/message-consumption-and-handler-model.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (Core Promise #2; Execution Model; OQ3 resolved)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (consumption section)
- index.md (Decisions section)

## [2026-06-20] create | testing decisions — library strategy + consumer tooling (drafts)

Split the test story into two decisions, per request. (1) Library test strategy:
how kafkaman tests itself — a three-tier pyramid (unit Docker-free / Postgres
integration / Postgres+Redpanda full-loop via testcontainers), crash-injection
gates and property tests for the core invariants (effective-once, no loss,
bounded errors ring, idempotent migrate), determinism via `dispatch_once()` + an
injected `Clock` (kafkaman dogfoods its own consumer tooling), containers started
once per test binary with schema/topic isolation. (2) Consumer test tooling: a
dedicated `kafkaman-test` dev-dependency crate — `tower` `oneshot` handler tests,
a deterministic `Harness` (ephemeral schema + migrate + capturing sender +
`dispatch_once()` + Clock + row assertions) against a caller-provided connection
string, and a `#[kafkaman::test]` macro (sqlx::test-style injection, Docker-free
by default, containers/broker opt-in, per-binary containers, never auto-starts
schedulers, optional sugar over explicit `Harness::connect`). Transport stance
respects OQ2: Postgres-only fast tests + real Redpanda full-loop behind an
optional `testcontainers` feature; in-memory fake/seam deferred. Recorded a
standing principle: macros are opt-in sugar over explicit APIs, never
load-bearing.

Pages affected:
- decisions/library-test-strategy.decision.md (created, Draft)
- decisions/consumer-test-tooling.decision.md (created, Draft)
- proposals/01-kafkaman-objectives.proposal.md (kafkaman-test crate added to shape)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (testing section)
- index.md (Decisions section)

## [2026-06-20] update | dogfooding-first elevated to the primary test principle

Per discussion, sharpened the library test strategy: kafkaman's own suite uses
the consumer toolkit (`kafkaman-test`) *wherever a test sits at or above the
toolkit's abstraction*, making our suite the toolkit's primary consumer. Recorded
the boundary (layers beneath the toolkit — Harness/macro internals, SQL/DDL
builders, dedup query, ring logic, rdkafka edges — stay white-box to avoid
circularity) and the consequence (`kafkaman-test` is an early deliverable built
with core/sqlx; toolkit-using library tests live in a separate workspace test
member to avoid the `core ⇽ test` dev-dependency cycle).

Pages affected:
- decisions/library-test-strategy.decision.md (dogfooding-first as primary principle + boundary + build order)
- decisions/consumer-test-tooling.decision.md (early-deliverable consequence)
- raw/design/2026-06-20-kafkaman-architecture-discussion.md (dogfooding sharpened)

## [2026-06-20] update | PoC seeds the test Harness; V1 roadmap drafted

Two related steps. (1) Adjusted the first-PoC plan so dogfooding-first holds from
line one: added a minimal `kafkaman-test` `Harness` seed (ephemeral-schema
`migrate()`, enqueue helper, `relay_once()` one-step driver, row assertions) as
the PoC's first test-facing deliverable, routed the crash/integration gates
through it, added `-test` to the workspace stubs, and marked the full toolkit
out of scope. (2) Drafted the V1 roadmap: six milestones (M1 durable send/PoC →
M2 change-engine + config → M3 durable receive + toolkit maturity → M4
retry/backoff/DLQ → M5 observability → M6 hardening), `kafkaman-test` as a
cross-cutting track (seeded M1, matured M3, completed M6), and the explicit notes
that M4 opens with its own retry/DLQ decision and the four Draft decisions flip to
Accepted at the M1-entry review.

Pages affected:
- roadmaps/path-to-v1.roadmap.md (created, Draft)
- plans/first-poc-outbox-publisher.plan.md (Harness seed; steps + gates; out-of-scope)
- index.md (Stage line; Roadmaps section)

## [2026-06-20] create | M1 durable-send implementation review

Created a sourced review page that verifies the post-implementation M1
durable-send review against the current Rust code, plan, spec, tests, and
examples. Confirmed the main gaps around status-string centralization, database
clock ownership, Redpanda/full-loop scope, idempotency durability, worker
resilience, Harness migration races, facade usage, and index-name collisions.
Added additional findings for migration concurrency, example worker failure
visibility, duplicate descriptors, and unused worker publish-error surface.

Pages affected:
- wiki/reviews/m1-durable-send-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] create | M1 durable-send implementation re-review

Created a fresh sourced re-review after attempted fixes to the M1 durable-send
implementation. Verified the current working tree against the plan, active spec,
tests, examples, and prior review. Recorded fixed items, remaining scope gaps,
and new risks, including the missing `idempotency_key` upgrade migration for
existing outbox tables, still-missing Redpanda/full-loop Harness path, remaining
Harness registration race, strict clippy failure, and reserved Kafka metadata
header collision risk. Updated the wiki index date and Reviews catalog.

Pages affected:
- wiki/reviews/m1-durable-send-implementation-rereview.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] implement | M1 durable-send review-fix pass

Resolved both M1 durable-send reviews in code. Implemented the Redpanda
full-loop path (`Harness::connect_redpanda` + `HarnessPublisher::Redpanda`,
behind the `redpanda` feature) with a testcontainer gate that publishes through
`RdkafkaPublisher` and consumes the record back, asserting payload, key, and
`kafkaman-*` headers. Centralized status SQL via `OutboxStatus`, moved lease and
retry scheduling to the database clock, made the worker `run()` loop resilient
with `tracing`, advisory-locked `migrate()`, made `idempotency_key` durable and
forwarded (with an additive `AddIdempotencyKey` upgrade changeset and test),
rejected reserved `kafkaman-` headers at enqueue, and closed the Harness
registration race. Strict clippy and fmt pass. Added `cargo llvm-cov` coverage
with an 80% workspace line gate (`just test coverage`); current line coverage is
~93%. The example now consumes the `kafkaman` facade, is split into a testable
lib + thin bin, and was moved from `examples/` to `apps/axum-outbox` so
cargo-llvm-cov (which excludes `examples/`) counts it in the workspace total.
Added a `justfile` with `just test [all|unit|integration|coverage]` (default
`all` runs the full suite including integration).

Pages affected:
- wiki/plans/m1-durable-send-implementation.plan.md
- wiki/compatibility/m1-durable-send-schema-and-api-changes.compatibility.md
- wiki/index.md
- wiki/log.md
- justfile
- apps/axum-outbox/ (moved from examples/)

## [2026-06-21] review | M2 change-engine + config implementation

Wrote `wiki/reviews/m2-change-engine-config-implementation-review.reference.md`: a
line-by-line challenge of the M2 implementation against its plan and spec.

Key findings:
- H1: retry-config validation never invoked on any boot path (unit-test-only),
  so the spec's "retry validated" / roadmap "fails fast at boot" is unmet for retry.
- H2: checksum is FNV-1a 64-bit, not the SHA-256 the plan specified; ad-hoc
  delimiter-based canonical form.
- H3: migrate_dry_run creates schema/history table and backfills (not side-effect-free).
- M1: Replay leaves attempts/last_error stale, will fight M4 retry cap.
- Plus medium/low: noisy from_config error aggregation, NULL/decode conflation in
  history_row, example.toml advertising ignored keys, exclusive dry-run lock,
  missing concurrent-migrate and tunable-change gate tests.

Pages affected:
- wiki/reviews/m2-change-engine-config-implementation-review.reference.md (new)
- wiki/index.md
- wiki/log.md

## [2026-06-21] create | M3 durable receive implementation plan

Created the active M3 execution plan for durable receive and toolkit maturity.
The plan sequences the first Harness-level receive test, received-table schema,
deterministic dispatch, explicit handler API, receive-side test tooling, Kafka
ingest, and validation gates.

Pages affected:
- wiki/plans/m3-durable-receive.plan.md
- wiki/roadmaps/path-to-v1.roadmap.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive plan review fixes

Clarified the M3 dispatch transaction model before implementation. The plan now
chooses the held-transaction row-lock model, removes persistent receive claim
columns from M3, defines parked retryable failure accounting, requires
Clock-bound due predicates, adds property and crash gates, includes consume-side
replay, includes `#[kafkaman::test]`, states crate topology, and adds
greenfield `idempotency_key NOT NULL` plus bounded-errors ring verification.

Pages affected:
- wiki/plans/m3-durable-receive.plan.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive first implementation slice

Recorded the first M3 durable-receive implementation slice: received-table core
types and DDL, deduplicating received insert, Harness receive helpers, minimal
message router, and held-transaction `dispatch_once()` success/failure behavior.
Proof commands recorded in the active M3 plan.

Pages affected:
- wiki/plans/m3-durable-receive.plan.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] review | M3 durable receive implementation review

Wrote a sourced post-implementation review of the first M3 durable-receive slice
against the active plan. Static review plus re-run of the cheap gates
(`cargo fmt --check`, `cargo check`, `cargo clippy`, `kafkaman-sqlx` lib tests —
all clean); Postgres integration suite not re-executed (Docker/testcontainers).
Headline finding (H1, high): the failure-path accounting update runs outside the
held transaction with an unguarded `WHERE message_id = $1`, so under concurrent
dispatch it can overwrite a row another worker committed as `Processed`,
producing a double effect — an effective-once hole the single-call tests cannot
catch. Also flagged the minimal handler surface vs plan scope, a Harness
send/receive registration conflict, the missing property/crash/ring gates, and
low-severity polish.

Pages affected:
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive review follow-up fixes

Resolved the H1 stale failure-accounting race by guarding receive failure
accounting to `Pending`/`Retryable` rows, fixed Harness send/receive migration
registration for same-type send-after-receive use, removed the transient
in-transaction `Processing` write, and changed missing receive lookups to report
the missing idempotency key. Added receive integration gates for stale failure
interleaving, crash redrive, randomized duplicate redelivery convergence,
bounded 20-entry error retention, and missing received-row error reporting.
Serialized the Docker-backed durable receive integration file with an in-process
async mutex to avoid local Postgres container pool flakiness under the default
parallel test runner.

Verification passed:

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-features`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo test -p kafkaman-sqlx --lib`
- `cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`

Pages affected:

- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] update | M3 durable receive review double-check

Double-checked the sourced M3 durable-receive implementation review against the
landed code and tests. Re-ran the cheap gates (`cargo fmt --all -- --check`,
`cargo check --workspace --all-features`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, `cargo test -p kafkaman-sqlx
--lib`) and the Docker/testcontainers-backed receive integration suite
(`cargo test --manifest-path tests/durable-send/Cargo.toml --test
durable_receive`); all passed after escalating the integration run for Docker
access. Confirmed H1 by exact failure-path SQL/control-flow inspection and
confirmed M2 with a temporary regression test that failed with PostgreSQL
`42P01` missing outbox relation after receive-side registration. Corrected the
review and index to state that the bounded error-ring SQL is inspected as
correct, but the 20-entry cap is not yet test-pinned.

Pages affected:
- wiki/reviews/m3-durable-receive-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-21] create | Deep durability testing proposal

Added a living deep-testing proposal cataloguing adversarial concurrency, crash,
and durability test designs for kafkaman's durable-execution paths, plus the
invariant-escape technique that surfaced the M3 review's H1. Seeded with 13
test designs across concurrency, crash, robustness, atomicity, bounded-resource,
clock, and ingest classes, a priority order, a status-tracking table, and open
design questions (crash-durable receive `attempts`; parking on `MissingHandler`;
the controlled-interleaving primitive for `kafkaman-test`). Predicts A1, B2, and
C1 reveal real defects today.

Pages affected:
- wiki/proposals/05-deep-durability-testing.proposal.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] review | M3 durable-completion implementation

Adversarial in-depth review of the M3 durable-completion slice against landed
code, focused on failure modes the deep-durability catalog does not anticipate.
Findings: F1 ingest poison stalls the partition (offset never advances on any
pre-commit error); F2 PK-vs-`ON CONFLICT`-target mismatch is a second stall
vector; F3 the dispatcher loop does not drain (one row per poll interval, against
the plan's stated "drain"); F4 parked failure/missing-handler rows are
unrecoverable through the shipped API (`Replay::received` is Processed-only and
dispatch never reclaims Retryable/NULL); F5 `Replay::received` re-executes handler
side effects on already-processed rows. Also recorded plan-accuracy gaps: the
Phase 0 interleaving primitive, the real two-worker A1 test, and the Phase 4 J3
consume-then-produce full-loop are claimed in Progress but not present in code.

Pages affected:
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 durable-completion review fixes

Recorded implementation follow-up for the M3 durable-completion review fixes.
F1-F9 are now closed by deterministic ingest poison skips with offset commit,
receive insert conflict hardening, dispatcher backlog drain and mid-dispatch
shutdown tests, retryable-only `Replay::received`, structured receive failure
causes, and Redpanda poison/redelivery/topic-provenance coverage. The active
plan still keeps broader M3 closure gates open: controlled interleaving, real
two-worker A1, G1/G2 crash/offset uncertainty, consume-then-produce Redpanda J3,
handler-model reconciliation, and M3 spec promotion.
Pages affected:
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 ingest poison quarantine policy

Implemented and documented the ingest poison quarantine policy prompted by the
G1-G5 review. Added accepted decision `ingest-poison-quarantine-policy`, durable
`received_ingest_failures` quarantine table, consecutive-skip circuit breaker,
explicit receive insert outcomes for idempotency duplicate vs message-id
conflict, handler-domain SQL failure classification, and replay history
preservation. Evidence: `rtk cargo test --manifest-path
tests/durable-send/Cargo.toml --tests` (37 passed), `rtk cargo test
--manifest-path tests/durable-send/Cargo.toml --features redpanda --test
redpanda_full_loop -- --test-threads=1` (5 passed), and `rtk cargo test
--workspace --all-features` (61 passed).
Pages affected:
- wiki/decisions/ingest-poison-quarantine-policy.decision.md
- wiki/reviews/m3-durable-completion-implementation-rereview.reference.md
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/plans/m3-durable-completion.plan.md
- wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] update | M3 durable-completion review double-check

Added follow-up verification notes to the M3 durable-completion implementation
review after checking each claim against the current code and tests. Clarified
the scope of F1/F2, confirmed F1-F6 and the plan-accuracy gaps, and added three
missed gaps: case-variant reserved headers can poison ingest before offset
commit (F7), consumed source topic is not checked or persisted from the broker
record (F8), and graceful shutdown mid-dispatch is not proven by the shipped
dispatcher cancellation test (F9/H3). Updated the index summary accordingly.

Pages affected:
- wiki/reviews/m3-durable-completion-implementation-review.reference.md
- wiki/index.md
- wiki/log.md

## [2026-06-22] rereview | M3 durable-completion fixes

Second-pass review after the review-fix slice. Confirmed F1-F5 closed and
test-pinned (ingest poison skip, untargeted ON CONFLICT, dispatcher drain,
retryable-only Replay::received). Found the fixes traded stalls for silent drops:
G1 ingest skip is silent/untraceable data loss; G2 schema skew is treated as
poison and dropped topic-wide on a bad deploy ordering; G3 untargeted ON CONFLICT
silently drops a different logical message on message_id collision; G4 failure
`kind` keys off the Rust error variant (handler SQL errors mis-bucket as
Infrastructure); G5 replay erases the error history of the parked rows it targets.
Recommended a single ingest-poison/dead-letter policy decision. Noted residual
scope: no run_ingest loop, Phase 0 primitive / real two-worker A1 / J3 still absent.

Pages affected:
- wiki/reviews/m3-durable-completion-implementation-rereview.reference.md
- wiki/index.md
- wiki/log.md
## [2026-06-22] update | M4 retry/backoff/DLQ first slice

Started M4 reliability work. Added active M4 plan and compatibility note,
marked M3 completed and M4 active in the V1 roadmap, and implemented
policy-driven receive retry scheduling: `ResolvedConfig` retains `RetryConfig`,
`ReceivedTable` carries the per-message retry policy, receive failure accounting
computes `next_attempt_at`, honors configured `errors_limit`, and moves
exhausted rows to terminal `Failed` table-backed DLQ state. Redrive/admin
surfaces remain in the active M4 plan.
Pages affected: `crates/kafkaman-config/src/lib.rs`,
`crates/kafkaman-sqlx/src/lib.rs`, `tests/durable-send/Cargo.toml`,
`tests/durable-send/tests/durable_receive.rs`,
`wiki/plans/m4-retry-backoff-dlq.plan.md`,
`wiki/compatibility/m4-retry-backoff-runtime-api.compat.md`,
`wiki/roadmaps/path-to-v1.roadmap.md`, `wiki/index.md`, `wiki/log.md`.

## [2026-06-22] update | dispatch failure accounting hardening

Changed receive dispatch failure accounting to hold the claimed row lock through
normal failure recording by using a handler savepoint, with best-effort rollback
and separate-connection fallback only when the transaction connection is already
unusable. Updated M3 spec, dispatch decisions, compatibility note, and
deep-durability proposal evidence to match the new single-flight behavior.
Pages affected: `crates/kafkaman-sqlx/src/lib.rs`,
`tests/durable-send/tests/durable_receive.rs`,
`wiki/specs/m3-durable-receive.spec.md`,
`wiki/decisions/dispatch-infrastructure-error-classification.decision.md`,
`wiki/decisions/dispatch-stats-semantics.decision.md`,
`wiki/decisions/missing-handler-dispatch-policy.decision.md`,
`wiki/compatibility/m3-durable-receive-review-fix-api.compat.md`,
`wiki/proposals/05-deep-durability-testing.proposal.md`,
`wiki/reviews/m3-durable-completion-implementation-review.reference.md`,
`wiki/index.md`, `wiki/log.md`.
