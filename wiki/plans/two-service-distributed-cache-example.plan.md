# Two-Service Distributed Cache Example

- Document Class: Plan
- Status: Completed
- Date: 2026-08-24
- Completed: 2026-08-24
- Category: Delivery execution
- Scope: Replace the single send-only example app with two services that prove
  entity-first cache convergence end to end, driven entirely by HTTP.
- Sources:
  - apps/axum-outbox/src/main.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - review.md
- Related:
  - wiki/decisions/entity-first-propagation-model.decision.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/library-test-strategy.decision.md

## Context

Three gaps, found together while adding a `[retention]` section to
`kafkaman.example.toml`.

**The example app cannot start.** `Config::discover()` walks up from the current
directory looking for `kafkaman.toml`; no such file exists anywhere in the repo. So
`ResolvedConfig::from_config(None, [descriptor])` fails at boot with "missing config
file for registered kafkaman features". `apps/axum-outbox/tests/http.rs` never
caught it because it builds config through `Config::from_str`, bypassing discovery
entirely. This is the same failure mode as the example config file itself: nothing
exercised it, so it rotted.

**The example demonstrates a third of the product.** One message type, an outbox, a
relay. The entity-first decision states kafkaman's purview as *compact entity
snapshots for distributed caches*, and nothing anywhere shows a cache being read, a
service serving data it does not own, or two services converging.

**Nothing tests the wiring.** The integration tests are thorough but go through
`kafkaman_test::Harness` with direct database access. A wiring mistake — a
dispatcher never spawned, a topic mismatch, a `CreateCacheTable` missing from a
changelog — passes every test in the workspace today. The library-test-strategy
decision calls for dogfooding at or above toolkit abstractions; an HTTP-only test
is the only tier that catches this class.

## The domain

`product` owns products: presentation, status, and stock on hand. `order` owns
orders, and owns the decision of whether a product is orderable.

That ownership split is what makes the cache legitimate rather than a proxy.
**order needs product data on its own request path** — to reject an order for a
discontinued or out-of-stock item, and to render a line without a synchronous call
to product. A cache that exists only for symmetry teaches the wrong thing, which is
why the two directions deliberately demonstrate *different* capabilities.

| Direction | Mechanism | What it shows |
|---|---|---|
| product → order | `ProductSnapshot`, **no-op handler** | Cache convergence is free. The upsert happens inside `dispatch_once` via `apply_successful_dispatch`, not in application code. |
| order → product | `OrderSnapshot`, **deriving handler** | Availability is recomputed from converged state, not decremented, and republished through `enqueue_on_connection` — consume-then-produce in one transaction, which no example currently shows. |

The loop closes and terminates: order publishes `OrderSnapshot` → product recomputes
and republishes `ProductSnapshot` → order's cache converges → order publishes
nothing further.

### Entity shapes

`ProductSnapshot { product_id, name, price_cents, status, available }` with
`ProductStatus` one of Draft, Available, Discontinued.

`OrderSnapshot { order_id, product_id, quantity, status }` with `OrderStatus` one of
Placed, Fulfilled, Cancelled.

order accepts a request only when the cached product has
`status == Available && available >= quantity`. Both conditions matter: a
discontinued product with stock on hand must still be unorderable, so the example
proves the cache carries state rather than just a counter.

`available` is published rather than `on_hand`, because order has no orders cache
and cannot derive it. product keeps `on_hand` private.

## Deriving rather than decrementing

product does **not** mutate `on_hand` when an order arrives. It recomputes:

```
available = on_hand − SUM(quantity over cached OrderSnapshots
                          for this product WHERE status = 'Fulfilled')
```

This is the entity-first thesis applied to the consumer side, and it buys a
property a decrementing handler cannot have: **idempotence by construction.** Apply
the same `OrderSnapshot` twenty times and the cache still converges to one row, so
the sum cannot double-count. A decrementing handler is correct only because the
handler and the processed-mark share a transaction — true, but a property that has
to be argued and asserted rather than being impossible to get wrong.

It is also the JOIN that justifies keeping the cache in Postgres at all, and the
reason no typed cache getter is needed: `CacheTable::qualified_name()` is the API,
and a key-value getter would be worse than SQL while encouraging N+1 reads.

### Only fulfilled orders count, and the filter belongs in the query

Caching only fulfilled orders was considered and rejected. It would break
convergence: an order going Fulfilled → Cancelled would leave a stale Fulfilled row
in the cache forever and the count would stay decremented, reintroducing the manual
cache invalidation that convergence exists to eliminate. It is also not
expressible — the upsert lives in `apply_successful_dispatch` and runs for every
consumed message regardless of handler content, so there is no ingest-time filter
hook.

Filtering at query time instead means **cancellation restores the count for free**,
with no compensating logic anywhere. That is the strongest property this example
demonstrates, and an event-accumulating design cannot do it at all.

### The constraint that is not obvious

`dispatch_once_inner` runs the handler (`crates/kafkaman-sqlx/src/lib.rs:2551`)
*before* upserting the message into the cache (`:2553`, inside
`apply_successful_dispatch`). A handler deriving from the cache therefore sees that
entity's row **exactly one version stale** — absent on the first snapshot, and
holding the previous status on every transition after that.

The correction is not "add the missing row" but *exclude and substitute*:

```sql
SELECT COALESCE(SUM(quantity), 0)
  FROM <order cache>
 WHERE product_id = $1
   AND status = 'Fulfilled'
   AND order_id <> $2          -- this entity's row is one version behind
```

then add `this_order.quantity` back only if the incoming status is `Fulfilled`.
Without the exclusion, a Placed → Fulfilled transition counts the order at both its
old and new status.

It is deterministic — a failed handler rolls the whole transaction back, so every
attempt sees the same pre-state — but it is a genuine footgun for anyone deriving
state in a handler.

## Shape

Rename `apps/` to `examples/`: the crates are demonstration code and the directory
should say so. Safe because the workspace root is a virtual manifest with no
`[package]`, so cargo will not treat `examples/` as auto-discovered example targets.

- `examples/contracts/` — new. The two snapshot types and their `KafkaMessage`
  impls, nothing else. Shared because duplicated impls drift, and a changed `TOPIC`
  or `entity_key` is a runtime mismatch with no compile error.
- `examples/order/` — renamed from `axum-outbox`. `OrderCreated` becomes
  `OrderSnapshot`: entity-first names the entity, not the event, and the example is
  what people copy. Gains the receive side, order lifecycle endpoints, a
  `GET /products/{id}` served from its cache, and a `kafkaman.toml`.
- `examples/product/` — new. Product CRUD, a deriving dispatch handler, and a
  `kafkaman.toml`.
- `tests/distributed-cache/` — new test package alongside `tests/durable-send`.

Each service runs three loops: `worker::run`, `RdkafkaConsumer::run_ingester`, and
`run_dispatcher`. Each changelog needs `CreateOutboxTable`, `CreateReceivedTable`
**and** `CreateCacheTable` — the current one has only the first, which is exactly
the wiring mistake this example exists to catch.

**Two databases, one PostgreSQL container.** Each service gets its own connection
string, so neither can read the other's tables even by accident and "distributed" is
enforced rather than asserted. No cost over two schemas.

The container is **owned by the test**, per the amended library-test-strategy
decision: Testcontainers cleans up through `ContainerAsync`'s `Drop`, and a static
never drops at process exit. The guard must outlive both services' pools. The new
package also carries the `com.kafkaman.*` labels so `just clean-containers` can
find its containers by label rather than by image.

## The test

One binary, HTTP only, no database access. The lifecycle is the point, so the
sequence walks it:

1. Create a product (Available, on hand 10) → poll until order's cache converges.
2. Order 3 → accepted; availability is **still 10**, because a placed order reserves
   nothing.
3. Fulfil → poll until availability is **7**. One HTTP write, observable two hops
   later through one HTTP read.
4. Cancel → poll until availability is **back to 10**, with no compensating logic
   anywhere. This is the test that justifies the design.
5. Discontinue the product → poll until converged, then order → rejected despite
   availability being 10.
6. Order 999 → rejected on availability alone.

Steps 2–4 must poll to a *stable* value rather than the first change observed: a
two-hop round trip passes through an intermediate republish, and asserting on the
first value seen would pass by accident.

Both services run in-process as tokio tasks on ephemeral ports. The process boundary
would add little — transport, broker and databases are all real — and in-process
keeps failures debuggable.

**Redpanda must not use a fixed port here.** `redpanda_full_loop.rs` hardcodes
`19092` behind a process-global mutex, which is per-binary and will not protect
against a second test binary under `cargo test --workspace`. Pick a free port at
runtime, map it, and set `--advertise-kafka-addr` to match. If that works cleanly it
is also the fix for the fixed-port fragility recorded as F16.

## Verification

- `just lint`, then `cargo test --workspace --all-features`. All 141 existing tests
  must still pass; the rename touches the workspace members list, CI, and the
  existing HTTP tests, and is the main regression risk.
- **Deliberately break the wiring and confirm the test fails** — comment out the
  dispatcher spawn, then the `CreateCacheTable` changeset. Catching that class is
  the entire justification for the test, so it must be demonstrated, not assumed.
- Each service must boot from its `kafkaman.toml` via `Config::discover()`, not from
  a string, or the original bug survives in a new form.
- Dispatching the same `OrderSnapshot` twice must leave availability unchanged.
- Fulfil two separate orders for one product and confirm availability reflects both;
  re-fulfil one and confirm it is not counted twice. A missing `order_id <> $2`
  exclusion passes the single-order case and only fails here.
- Cancellation restores the count with no compensating logic.

## A library gap this surfaced

`dispatch_once`'s rustdoc does not state that the handler runs before the cache
upsert. Any handler deriving state from its own cache is silently one version stale
for the entity it is processing, and nothing warns about it. Document it on
`dispatch_once` and `MessageRouter`.

This is a real documentation gap found only by trying to build a realistic consumer,
which is itself the argument for the example existing.

## Risks

- **Wall clock.** One PostgreSQL, one Redpanda, two services, six loops, multi-hop
  polling — and containers are owned per test rather than shared, so the cost is
  paid per test function. Likely the slowest binary in the suite, which argues for
  few, fat tests that walk the lifecycle rather than many thin ones.
- **Flakiness.** Eventual convergence over two hops plus container startup. The most
  likely new flake source; mitigated by deadline polling and the ephemeral port.
- **Rename churn.** `apps/axum-outbox` appears in its Cargo.toml, the workspace
  members list, `.github/workflows/ci.yml`, its own tests, and the wiki.

## Outcome (2026-08-24)

Shipped as `examples/contracts`, `examples/order`, `examples/product`, and
`tests/distributed-cache`. `cargo test --workspace --all-features` is green at
153 passing: the 141 that existed, minus the 3 deleted `axum-outbox` HTTP tests,
plus 15 new. `just lint` is clean and the coverage floor still passes at 91.2%
lines.

Every verification item held, including the one that mattered most: **both
wiring breaks were demonstrated, not assumed.** Removing the dispatcher spawn
failed the test at the convergence deadline (`did not settle past offset -1
within 90s; last observed: None`); removing `CreateCacheTable` from `order`'s
changelog failed it in three seconds on a 500 from the cache read. Nothing else
in the workspace noticed either break.

The Redpanda ephemeral-port approach worked cleanly, so F16 is fixed:
`redpanda_full_loop.rs` reserves a port from the OS per container instead of
hardcoding `19092`. Its process-global mutex was kept, with its comment
rewritten — it never protected against a second *binary*, and what it still buys
is not running a dozen brokers at once on a laptop.

### Deviations from the plan

**Two, both deliberate.**

**Snapshot idempotency keys are derived from a per-entity version column**, not
from the payload. The plan did not settle this, and payload-derived keys are the
obvious choice that quietly breaks the example's own step 4: cancelling restores
availability to a value already published, so a payload digest would re-derive a
key the consumer has already seen and the restoring snapshot would be dropped as
a duplicate — the cache would sit at 7 forever. `orders.version` and
`products.version` increment on every state change, so a retried write of one
state still deduplicates while a genuinely new state never can. This is an
idempotency identity, not a convergence ordinal; the ordinal is still the Kafka
offset, so the entity-first decision's argument against producer-stamped
versions does not apply.

**The availability-only rejection runs before discontinuation, not after.** The
plan's step 6 ("order 999 → rejected on availability alone") follows step 5,
which discontinues the product — after which any rejection proves nothing about
the quantity comparison, because the status check fires first. Reordering keeps
both assertions meaningful without a second product.

### Smaller decisions worth recording

- **Wire enums carry a catch-all `Unrecognized(String)` variant.** The
  entity-first decision makes this a hard requirement for the origin-intent enum;
  domain status enums travel identically and have the identical failure mode
  (a new variant at an un-redeployed consumer is a deterministic ingest skip, and
  enough of them trip the topic-wide breaker). Serde's `#[serde(other)]` does not
  cover externally-tagged string enums, so they round-trip through `String`.
- **`Config::discover()` is exercised by each service's own tests, not by the
  two-service one.** Discovery walks up from the process's current directory, so
  it cannot serve two services in one process. Cargo runs an integration test
  with the package root as the current directory, which is exactly where each
  binary is meant to be run from — so `examples/*/tests/` test discovery for
  real, and `tests/distributed-cache` loads the same files by path.
- **The example configs had to be un-ignored.** `.gitignore` matched
  `kafkaman.toml` at every level, which is what made the original example
  unrunnable in the first place; `!examples/*/kafkaman.toml` is now an explicit
  exception, with the reasoning in the file.
- **`product` republishes unconditionally**, without change detection. A snapshot
  repeating a value is harmless under a guarded upsert, and always republishing
  makes "this order has been accounted for" something a consumer can wait on —
  which is what lets the test assert on a *rising `applied_offset`* rather than
  on a timeout.
- **Handler-tier cases live in `examples/product/tests/`, not in the two-service
  test.** The exclusion bug and the double-count case need Postgres but no
  broker, and the case that actually distinguishes a correct exclusion from a
  missing one — two orders, one re-fulfilled — costs a fraction of a round trip
  there.
- **No compatibility note.** The library's public API is unchanged; the only
  library edit is rustdoc on `dispatch_once` and `MessageRouter::handler`.
  `kafkaman.example.toml`'s per-type retry override moved from `order_created` to
  `order_snapshot` to track the example's rename, which is documentation.

### One consequence the plan did not anticipate

**The rename reverses a deliberate 2026-06-21 decision, and the coverage number
moved for a reason that has nothing to do with test quality.** The example was
moved *out of* `examples/` into `apps/axum-outbox` back then specifically because
`cargo llvm-cov` excludes `examples/`, so keeping it there stopped it counting
toward the workspace total. Renaming back removes roughly 900 lines of example
code from the denominator: the floor still passes comfortably at 91.2% lines, but
what it now measures is `crates/` alone.

Recorded rather than reversed, because on balance it is the better number.
kafkaman's coverage gate exists to describe the library, and the library test
strategy already states that coverage % is a signal while the gate is that the
durability invariants are exercised — a percentage propped up by demonstration
code answers neither question. The example services are covered by their own
tests either way; they simply no longer flatter the total.

The thing to avoid is reading the coverage number across this change as if it
were comparable. It is not.
