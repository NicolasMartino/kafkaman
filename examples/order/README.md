# `order` — the consuming side

Owns orders. Reads products it does **not** own out of a local kafkaman cache,
and never calls the service that owns them.

See [`../README.md`](../README.md) for how to run both services together.

## What to look at

| File | Why |
|---|---|
| `src/lib.rs` | The domain model and HTTP state. Product convergence needs no handler because the service declares `cache::<ProductSnapshot>()`. |
| `src/http.rs` | `create_order` — the admission decision and the order row are taken against one snapshot of the cache, inside one transaction. |
| `src/service.rs` | The role declaration. The builder derives the changelog, tables, topic checks, relay, ingester, dispatcher, queue metrics, and shutdown wiring. |
| `kafkaman.toml` | Read by `Config::discover()`, and asserted by `tests/service.rs`. |

## Endpoints

| Method | Path | |
|---|---|---|
| `POST` | `/orders` | Accept an order, if the cached product allows it. Enqueues `OrderSnapshot` in the same transaction. |
| `GET` | `/orders` | List the owner's orders. |
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
