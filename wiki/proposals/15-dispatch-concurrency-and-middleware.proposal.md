# Dispatch Concurrency and Middleware

- Document Class: Proposal
- Status: Proposed
- Date: 2026-08-26
- Category: Message consumption
- Scope: Proposes concurrent dispatch as opt-in runtime config, records the
  handler-contract change it forces, and settles the long-deferred Tower
  question by adopting Tower's vocabulary without its trait.
- Sources:
  - crates/kafkaman-worker/src/dispatcher.rs
  - crates/kafkaman-sqlx/src/received_rows.rs
  - crates/kafkaman-sqlx/src/router.rs
  - crates/kafkaman-sqlx/src/retry_backoff.rs
  - crates/kafkaman-rdkafka/src/consumer.rs
  - examples/product/src/lib.rs
  - Design session, 2026-08-26
- Related:
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/receive-handler-surface-scope.decision.md
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/decisions/dispatch-handler-ordering.decision.md
  - wiki/proposals/08-listen-notify-scheduler-wakeup.proposal.md
  - wiki/proposals/14-runtime-builder-and-axum-composition.proposal.md
- Promotion Target: a decision fixing `max_in_flight` semantics and the
  middleware shape, amending the handler-model decision's "future design space"
  clause.

## Context

Two questions have been deferred repeatedly and are now worth answering
together, because the second only becomes interesting once the first lands.

**Throughput.** `dispatch_once` handles exactly one row per call and
`run_dispatcher` runs one loop per message type
(`crates/kafkaman-worker/src/dispatcher.rs:20-26`). A service consuming a burst
of a thousand snapshots processes them strictly one at a time.

**Middleware.** `wiki/decisions/message-consumption-and-handler-model.decision.md`
has carried a `.layer(..)` in its wiring sketch since M3 while recording that
"the larger Tower/extractor/Rx surface remains future design space".
`wiki/decisions/receive-handler-surface-scope.decision.md` defers it again. The
runtime-builder decision descoped it a third time, while requiring that roles
leave it addable — a constraint the builder satisfied and proved with a test.

Deferring a design three times is a signal that it needs deciding, not that it
needs deferring again.

## Concurrent dispatch

### The claim layer is already built

`crates/kafkaman-sqlx/src/received_rows.rs:38` claims with
`FOR UPDATE SKIP LOCKED`, and the comment above it at `:21` says exactly why:
"`FOR UPDATE SKIP LOCKED` is what lets several dispatchers share one table".

So the hard part — several workers claiming from one table without stepping on
each other — is done and has been for some time. What is missing is only the
ability to *ask* for more than one.

### Proposed surface

```toml
[relay]
max_in_flight = 1   # default; > 1 opts into concurrent dispatch
```

The value bounds concurrent dispatch; **whether it bounds it per consumed type
or across the whole runtime is open question 1 below and is not decided here.**
The sketch that follows assumes per-type — `RuntimeBuilder` spawning
`max_in_flight` dispatcher loops per consumed type — because that matches the
existing one-loop-per-type shape at `crates/kafkaman-worker/src/dispatcher.rs:20-26`,
but nothing below depends on that choice except the loop count.

It belongs in config rather than as a builder method because it is a tuning value
that differs per environment, and
`wiki/decisions/configuration-and-environment-model.decision.md:43` already draws
that line: "tunable settings live here".

**On the section name.** `[relay]` is documented as the *send-side* outbox relay
section — `worker_id`, `batch_limit` and `lease_for` are all publisher-side and
mean nothing to a dispatcher. The reason `[relay]` is nonetheless the honest
place to put this today is that the receive dispatcher already borrows from it:
`crates/kafkaman/src/runtime/builder.rs:585` reads `cfg.relay.poll_interval` and
hands it to `run_dispatcher`. So `max_in_flight` under `[relay]` is consistent
with what ships, not with what the section claims to be. A `[dispatch]` section
owning `poll_interval` and `max_in_flight` together would be cleaner and is a
breaking config change; see open question 5.

### Per-entity ordering needs no new machinery

The cache upsert is offset-guarded, so applying records out of order is already
safe by construction: a record at or behind the entity's applied offset yields
`CacheApplyOutcome::Ignored`. Two dispatchers racing on the same entity's cache
row serialize on the row lock, and the later offset wins regardless of arrival
order.

### The finding that makes this opt-in

**Concurrency would silently corrupt derived state in code that is correct
today.**

`examples/product/src/lib.rs::recompute_availability` does:

```sql
SELECT COALESCE(SUM((payload->>'quantity')::bigint), 0)::bigint
  FROM {order_cache}
 WHERE deleted = false AND payload->>'product_id' = $1 AND payload->>'status' = $2
```

then

```sql
UPDATE products SET available = GREATEST(on_hand - $1, 0) WHERE product_id = $2
```

Two orders for the same product, dispatched concurrently under READ COMMITTED:
neither transaction sees the other's uncommitted cache row, so both compute a
sum missing the other's order, and both write. The `UPDATE` takes a row lock, so
they serialize — but the second one writes a value it computed *before* the
first committed. Last writer wins, with a stale figure.

This is correct today **only** because dispatch is serial per message type. It
is not correct because anyone reasoned about it.

Three things follow:

1. **Default `1`.** Anyone who does not opt in keeps today's contract exactly.
2. **`max_in_flight > 1` changes the handler contract**, and that must be
   documented as a contract change rather than a tuning knob. A handler doing
   read-modify-write over anything other than its own entity's cache row needs
   to say so in SQL.
3. **Fix the example and make it the teaching case.**
   `recompute_availability` should take `SELECT ... FROM products WHERE
   product_id = $1 FOR UPDATE` before the `SUM`, with a test that fails without
   it at `max_in_flight > 1`. There is precedent to mirror: the outbox side
   already serializes per entity with advisory locks
   — a transaction-scoped lock keyed by schema, message type and entity key
   (`wiki/compatibility/m5-entity-first-outbox-supersede.compat.md:38`).

The last point is why this belongs in the same slice rather than a follow-up.
Shipping the knob without fixing the example ships a documented footgun beside a
working demonstration of it.

## Middleware: adopt Tower's vocabulary, not its trait

### Why not `tower::Service`

Two independent reasons. Either alone would be sufficient; together they settle
it.

**The types do not fit.** A handler is given `&'a mut PgConnection` borrowed from
the open dispatch transaction, so its future is
`HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>`
(`crates/kafkaman-sqlx/src/router.rs:16`). `tower::Service::Future` is an
associated type with no lifetime parameter, so a Tower service cannot hold a
future that borrows from its request — the lending-service problem. It is
solvable with a lifetime-parameterised request plus HRTB, or by restructuring
`dispatch_once` so the handler owns a connection and returns it, but the second
would change the handler signature that shipped 2026-08-26, which is the most
semver-sensitive surface in the crate.

**Most Tower middleware is locked out even if the lifetime is solved.**
`tower::retry` requires `Policy::clone_request`, and our request holds a *unique
mutable borrow* — uncloneable by definition. `tower::buffer` requires
`Req: Send + 'static` because it sends the request to a worker task. Neither
bound can ever be satisfied here.

### What would actually survive

| Middleware | On kafkaman's dispatch path |
| --- | --- |
| `retry` | **Conflicts.** Retry is durable and table-backed — `crates/kafkaman-sqlx/src/retry_backoff.rs`, `next_attempt_at`, attempt budgets, terminal DLQ. An in-memory Tower retry would burn attempts invisibly. |
| `buffer`, `hedge` | Impossible — need `'static` or `Clone` requests. |
| `balance`, `discover` | Meaningless — one handler per message type. |
| `limit::ConcurrencyLimit` | Meaningless *today* (concurrency is 1); becomes meaningful if `max_in_flight` lands. |
| `load_shed` | Meaningless — nothing to shed. The dispatcher just claims fewer rows. |
| `filter` | Expressible in the handler, and the ordering decision forbids suppressing the upsert anyway. |
| tracing / metrics | M6's territory. |
| `timeout` | **Genuinely useful.** |
| `limit::RateLimit` | **Plausible** — throttling a downstream API called from inside handlers. |

Two useful layers, in exchange for the largest refactor available.

### The deeper reason: kafkaman is not a network service

Tower's distinctive idea is `poll_ready` — backpressure. It matters when
something you do not control is *pushing* work at you, so you must buffer, shed,
or fall over.

The dispatcher pulls. It claims one row, processes it, commits, and only then
goes round again; if handlers are slow it claims more slowly. The work waits
durably in a table, so nothing is lost by not claiming and there is no shedding
decision to express.

This is not changed by making dispatch event-driven. Proposal 08's `NOTIFY`
would replace the sleep with a wakeup, but the dispatcher would still claim at
its own pace. **Push versus poll is not the axis; whether you can refuse work
is.**

Nor could Kafka push even if we wanted it to: Kafka is pull-based at the
protocol level — brokers never push, consumers issue Fetch requests — and
librdkafka's bounded prefetch queue already implements that backpressure below
our code. The ingest loop's `self.consumer.recv().await`
(`crates/kafkaman-rdkafka/src/consumer.rs:125`) is a stream abstraction over a
pull, not a callback.

### Proposed surface

Borrow the vocabulary so the crate feels familiar, and keep the lifetime so it
compiles:

```rust
/// Familiar shape, working lifetime.
pub trait Layer: Send + Sync + 'static {
    fn call<'a>(
        &'a self,
        next: Next<'a>,
        conn: &'a mut PgConnection,
        meta: ReceivedMeta,
        payload: serde_json::Value,
    ) -> HandlerFuture<'a>;
}

impl MessageRouter {
    /// Wrap every registered handler. Layers nest in declaration order.
    pub fn layer(self, layer: impl Layer) -> Self;
}
```

Ship `Timeout`, and `ConcurrencyLimit` once `max_in_flight` makes it meaningful.
Handler failures inside a layer must flow through the existing receive-failure
policy rather than a parallel path.

Plus the builder hook proposal 14 already specifies:

```rust
impl RuntimeBuilder {
    pub fn with_router(self, f: impl FnOnce(MessageRouter) -> MessageRouter) -> Self;
}
```

A **wrapping** hook, deliberately not `.dispatch_router(router)` — proposal 14's
reasoning stands: the latter "would force a user who wants one timeout layer to
abandon roles entirely and hand-build every handler".

## Consequences

Positive:

- Throughput stops being fixed at one row per message type.
- The Tower question is settled with reasons, so it does not return every time
  someone notices `MessageRouter` has no `.layer`.
- A middleware surface exists without a dispatch-core refactor.

Costs and risks:

- **`max_in_flight > 1` is a handler-contract change**, and the failure it
  introduces is silent. Documentation is necessary and not sufficient; the
  example must demonstrate the correct pattern.
- A kafkaman-specific `Layer` trait is one more thing to learn, and "we invented
  our own middleware trait" is a choice that has to keep being justified. The
  justification is the table above.
- No `tower` crate interop. Users cannot drop in a third-party layer — though
  per the table, the ones they would reach for do not work here regardless.
- `ConcurrencyLimit` as a *layer* overlaps with `max_in_flight` as *config*.
  They should not both be the answer to the same question; the decision must say
  which governs.

## Alternatives Considered

- **Adopt `tower::Service` properly**, restructuring dispatch so the handler owns
  its connection. Rejected: the largest refactor available, changing the handler
  signature that just shipped, for two usable layers.
- **Ship `with_router` alone and no `Layer`.** Honest but nearly empty:
  `MessageRouter` exposes only `new`, `handler` and `handler_before`, and
  `handler_for` is `pub(crate)`, so a caller could add or replace a handler but
  not wrap one. Worth doing only as a placeholder.
- **Concurrency as a builder method rather than config.** Rejected for
  consistency with `batch_limit` and `poll_interval`, which are already runtime
  config for the same reason.
- **Guarantee per-entity handler serialization** so concurrency is safe by
  default. Rejected: the derivation that breaks spans *different* entities — a
  product's availability derived from many orders — so entity-keyed
  serialization would not prevent it, while costing real throughput.

## Open Questions

1. Does `max_in_flight` apply per message type or across the runtime? Per type
   is simpler and matches the one-loop-per-type shape; across the runtime is
   what an operator sizing a connection pool actually wants to bound.
2. Should the runtime refuse to start when `max_in_flight > 1` and the pool is
   too small to serve every loop, the way `migrate` already refuses a pool below
   two connections?
3. If `ConcurrencyLimit` ships as a layer *and* `max_in_flight` as config, which
   is authoritative, and what does setting both mean?
4. Does the `Layer` trait need a pre-upsert position too, mirroring
   `handler_before`, or is wrapping the post-upsert handler sufficient?
5. Does `max_in_flight` go under `[relay]`, alongside the `poll_interval` the
   dispatcher already borrows from there, or does the receive side get its own
   `[dispatch]` section? The second is cleaner and breaks every existing config
   that sets `[relay] poll_interval`, so it is worth deciding once rather than
   twice.

## Promotion Target

A decision fixing `max_in_flight` semantics and its handler-contract change, the
`Layer` shape, and the rejection of `tower::Service` with reasons — amending the
"future design space" clause in
`wiki/decisions/message-consumption-and-handler-model.decision.md` so it stops
reading as undecided.
