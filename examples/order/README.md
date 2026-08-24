# `order` — the consuming side

Owns orders. Reads products it does **not** own out of a local kafkaman cache,
and never calls the service that owns them.

See [`../README.md`](../README.md) for how to run both services together.

## What to look at

| File | Why |
|---|---|
| `src/lib.rs` | `dispatch_router()` — a handler that does nothing at all. kafkaman writes the cache row; convergence needs no application code. |
| `src/http.rs` | `create_order` — the admission decision and the order row are taken against one snapshot of the cache, inside one transaction. |
| `src/changelog.rs` | Outbox, received *and* cache tables. Omitting the cache table is the mistake that migrates and boots cleanly and fails only at dispatch. |
| `src/service.rs` | The three loops a service actually has to spawn. |
| `kafkaman.toml` | Read by `Config::discover()`, and asserted by `tests/service.rs`. |

## Endpoints

| Method | Path | |
|---|---|---|
| `POST` | `/orders` | Accept an order, if the cached product allows it. Enqueues `OrderSnapshot` in the same transaction. |
| `GET` | `/orders/{order_id}` | |
| `POST` | `/orders/{order_id}/fulfil` | |
| `POST` | `/orders/{order_id}/cancel` | Legal from `Placed` *and* from `Fulfilled`. |
| `GET` | `/products/{product_id}` | Served entirely from the cache. This is the point of the example. |

An order is accepted only when the cached product is `Available` **and** has
enough of it. Both conditions matter: a discontinued product with stock on hand
must still be unorderable, which is why the snapshot carries state rather than
just a counter.

## Environment

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | — | required |
| `KAFKA_BROKERS` | — | required |
| `BIND_ADDR` | `0.0.0.0:3001` | |
| `KAFKA_CONSUMER_GROUP` | `order-service` | |

Neither the connection string nor the broker list is a kafkaman config key.
kafkaman's contract covers the tables and loops it owns; where they live is the
host's business.
