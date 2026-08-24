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

## The exclusion that is not obvious

`dispatch_once` runs the handler **before** it upserts the message into the
cache, so a handler sees its own entity's row exactly one version stale. The
availability query therefore excludes the order it is processing and adds the
incoming quantity back explicitly:

```sql
SELECT COALESCE(SUM((payload->>'quantity')::bigint), 0)::bigint
  FROM <order cache>
 WHERE deleted = false
   AND payload->>'product_id' = $1
   AND payload->>'status'     = 'Fulfilled'
   AND entity_key            <> $2   -- this order's row is one version behind
```

Without the exclusion a `Placed` → `Fulfilled` transition counts the order at
both its old and its new status. A single-order test passes either way; the case
that separates them is in `tests/derive_availability.rs`.

## Endpoints

| Method | Path | |
|---|---|---|
| `POST` | `/products` | Create. Enqueues `ProductSnapshot` in the same transaction. |
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
