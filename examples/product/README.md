# `product` — the deriving side

Owns products. Consumes order snapshots and recomputes how much of each product
is still available, then republishes the product.

See [`../README.md`](../README.md) for how to run both services together.

## Deriving rather than decrementing

The handler does not subtract a quantity from stock. It recomputes:

```
available = on_hand − SUM(quantity over cached fulfilled orders for this product)
```

That buys a property a decrementing handler cannot have: **idempotence by
construction.** Apply the same order snapshot twenty times and the cache still
converges to one row for that order, so the sum cannot double-count. A
decrementing handler is correct only because the handler and the processed-mark
share a transaction — true, but a property that has to be argued rather than one
that is impossible to get wrong.

It is also why **cancellation restores the count for free.** A cancelled order
simply stops matching the filter. There is no compensating write in either
service.

Only fulfilled orders count, and the filter lives in the query rather than at
ingest. Caching only fulfilled orders would break convergence — an order going
`Fulfilled` → `Cancelled` would leave a stale `Fulfilled` row in the cache
forever — and it is not expressible anyway: kafkaman upserts every consumed
message into the cache, with no ingest-time hook.

## Reading its own cache is safe here

The availability query includes the order currently being dispatched, with no
exclusion and no add-back, because `dispatch_once` applies the cache upsert
*before* it calls the handler registered with `handle`. The incoming snapshot is
already the cache's current row for its entity, so the query sees the new status
rather than the previous one.

The ordering is deliberate. An earlier version ran the handler before the
upsert, which forced the query to exclude the current `entity_key` and add the
incoming order back by hand; forgetting either half double-counted transitions.
The current post-upsert handler order is recorded in
`wiki/decisions/dispatch-handler-ordering.decision.md`.

## Endpoints

| Method | Path | |
|---|---|---|
| `POST` | `/products` | Create. Enqueues `ProductSnapshot` in the same transaction. |
| `GET` | `/products` | List the owner's products. |
| `GET` | `/products/{product_id}` | The owner's view, including the private `on_hand`. |
| `POST` | `/products/{product_id}/discontinue` | Soft state, not a tombstone: the entity keeps flowing with a terminal status. |

The published snapshot carries `available`, never `on_hand`. Consumers hold no
orders cache and could not derive availability themselves, so publishing the
derived number is what lets this service keep its stock private while still
telling consumers something they can act on.

## Environment

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | — | required |
| `KAFKA_BROKERS` | — | required |
| `BIND_ADDR` | `0.0.0.0:3002` | |
| `KAFKA_CONSUMER_GROUP` | `product-service` | |

## Worker binary

`cargo run --bin product-worker` runs the same product role as a no-HTTP worker:
it declares `publish::<ProductSnapshot>()` and `handle::<OrderSnapshot>()`, then
selects `Subsystems::PIPELINE` so only relay, ingest, dispatch, and queue
metrics run. It does not read `BIND_ADDR`, bind a listener, mount `/faults`, or
serve operator routes.

Run it from this directory so `Config::discover()` finds `kafkaman.toml`:

```bash
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/product_service \
KAFKA_BROKERS=127.0.0.1:19092 \
KAFKA_CONSUMER_GROUP=product-service \
cargo run --bin product-worker
```

Telemetry is installed by the same `kafkaman-otel` helper as the HTTP binary.
Set `OTEL_EXPORTER_OTLP_ENDPOINT` or a signal-specific OTLP endpoint to export
it; leave them unset for a local worker with no provider.
