# kafkaman Examples

Two services that share no database and never call each other, yet each serves
data the other owns. That is the whole product in one runnable picture.

```
                       topic: products
        ┌────────────────────────────────────────────┐
        │                                            ▼
┌───────┴────────┐                            ┌──────────────┐
│    product     │                            │    order     │
│                │                            │              │
│ owns products  │                            │ owns orders  │
│ cache: orders  │                            │ cache: prods │
└───────▲────────┘                            └──────┬───────┘
        │                                            │
        └────────────────────────────────────────────┘
                        topic: orders
```

| Package | What it is |
|---|---|
| [`contracts`](contracts) | The two snapshot types and their `KafkaMessage` impls. Nothing else. |
| [`provision`](provision) | Builds the environment — the two databases and the two compacted topics — then exits. Runs before either service. |
| [`product`](product) | Owns products. Publishes `ProductSnapshot`; consumes `OrderSnapshot` with a **deriving** handler. |
| [`order`](order) | Owns orders. Publishes `OrderSnapshot`; consumes `ProductSnapshot` with **no handler at all**. |

The two directions are deliberately different, because they demonstrate
different things:

- **product → order** shows that cache convergence is free. `order` declares
  `cache::<ProductSnapshot>()` and writes no handler; kafkaman creates both
  tables, runs both loops, and upserts the cache row itself.
- **order → product** shows consume-then-produce in one transaction. `product`
  declares `handle::<OrderSnapshot>`, recomputes availability from its converged
  order cache, and republishes the product on the dispatch transaction's own
  connection.

The loop closes and terminates: an order snapshot makes `product` republish,
`order`'s cache converges, and `order` publishes nothing further.

## How a service is assembled

Boot is a role declaration. `order/src/service.rs` is 51 lines and
`product/src/service.rs` is 55, and neither names an outbox table, a changeset,
a topic admin, a publisher, a consumer, or a `JoinSet`:

```rust
let runtime = RuntimeBuilder::new()
    .config(config)
    .pool(pool.clone())
    .brokers(&options.brokers)
    .consumer_group(&options.consumer_group)
    .publish::<OrderSnapshot>()
    .cache::<ProductSnapshot>()
    .build()
    .await?;

kafkaman::axum::serve(listener, build_router(state))
    .with_runtime(runtime)
    .spawn()?
```

kafkaman derives the rest: the changelog and its version numbers, the outbox,
received, and cache tables, topic convergence, the migration, the relay,
ingester, and dispatcher loops, and a cancellation token the HTTP server shares —
so the first loop to die stops the service accepting traffic.

That the boot files no longer name any of those symbols is
[asserted by a test](../tests/distributed-cache/tests/boot_surface.rs), because
it is invisible to the compiler and to every behavioural test: the services work
identically whether the wiring is declared or hand-rolled.

### The escape hatch, kept under test

`RuntimeBuilder` is the default UX, not a closed framework. Some services need
hand-written migrations, custom supervision, or a topology it does not express,
and the low-level API stays public for them.

`product/src/service_manual.rs` is that path, written out in full — three times
the size, and every line of it something the builder derives. Documenting the
escape hatch would prove nothing, so `Services::start_with` is parameterised over
the boot mode and `tests/distributed-cache` runs against both. If the low-level
path stops producing an equivalent runtime, a test says so.

## Running them

Everything runs from one compose file, in two shapes.

**The whole thing, one command.** Builds the provisioner and both services, then
walks the lifecycle against them:

```bash
just examples demo   # or just `just examples` — demo is the default
```

The first run compiles librdkafka from source and takes a few minutes; later
ones reuse the build cache. The stack stays up afterwards so you can poke at it
on `:3001` (order) and `:3002` (product). `just examples down` tears it down.

**With OpenTelemetry export**, add Elasticsearch and Kibana:

```bash
just examples observe
```

That is the same service stack plus the `observability` compose profile. The
services export OTLP/HTTP directly to Elasticsearch at
`http://elasticsearch:9200/_otlp`, and the Rust exporter appends
`/v1/metrics`, `/v1/traces`, and `/v1/logs`. The plain `demo` path pins
`OTEL_EXPORTER_OTLP_ENDPOINT` to the empty string, so the binaries install no
provider and do not log failed exports into a backend that is not running — and
so that a value you export for your own tooling is not inherited by containers
where it would point somewhere else.

Export is **plaintext HTTP only**. The workspace pins `opentelemetry-otlp` to
its blocking `reqwest` client, which resolves without a TLS backend, and the
runtime image carries no `ca-certificates`; an `https://` endpoint fails at run
time with nothing failing at build time. A real deployment swaps in the
exporter's `reqwest-rustls-client` feature and adds a root store to the image.
The pipeline itself lives in `examples/telemetry`, shared by both binaries and
written out in full so it can be copied rather than inferred.

**Infrastructure only**, when you are working on the services themselves and
want a normal `cargo` loop:

```bash
just examples up
```

That starts Postgres and Redpanda, runs the provisioner on the host — it builds
no image in this shape, and you are about to `cargo run` anyway, so it shares
that build — and prints the two commands to paste. Each service reads its own
`kafkaman.toml` through `Config::discover()`, which walks up from the current
directory, so each runs from its own directory:

```bash
cd examples/product
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/product_service \
KAFKA_BROKERS=127.0.0.1:19092 cargo run
```

```bash
cd examples/order
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/order_service \
KAFKA_BROKERS=127.0.0.1:19092 cargo run
```

Both shapes talk to the same broker, which is why it advertises two listeners:
`redpanda:9092` inside the compose network and `127.0.0.1:19092` from the host.
A Kafka client reconnects to whichever address it is *advertised*, so one
listener could not serve both.

## What builds what

The split is worth stating, because it is the one thing about this stack that is
not obvious and the one thing it got wrong for a while.

**The services build everything reachable through a connection.** Migrations
apply themselves in-process at boot under an advisory lock, so the kafkaman
tables and each service's own business tables need no setup step.

**Nothing else does.** The databases and the topics exist before a connection
can be opened, so `provision` creates them — and then the services *verify*
rather than create. That asymmetry is load-bearing:

- An entity topic has to be compacted. Every kafkaman message is a full snapshot
  of one entity keyed by that entity, so the log is what a late consumer rebuilds
  from, and a `cleanup.policy=delete` topic ages those snapshots out.
- A broker left to auto-create makes the topic on first publish with its default
  — `delete`. Silently. This stack ran that way until the topics were
  provisioned deliberately, contradicting the compaction claim in the repository
  README with nothing anywhere reporting it.
- So both services check their topics at boot, before any loop is spawned, and
  refuse to start on one that is missing or wrongly configured. `[topics] mode`
  in each `kafkaman.toml` is that switch; it ships as `verify`.

That is why the ordering is enforced rather than assumed: in compose, `product`
and `order` gate on `provision` with
`condition: service_completed_successfully`, and `tests/distributed-cache`
provisions in its fixture for the same reason. Re-running provisioning is a
no-op, so it happens on every `up`.

If you want to see the refusal, `docker compose -f examples/compose.yaml down -v`
and start a service without provisioning: it exits with
``topic `orders` does not exist`` rather than quietly creating it.

## Looking at it

Each service serves its own OpenAPI description, generated from the handlers
rather than checked in, so a renamed route cannot leave the spec behind:

| | |
|---|---|
| `product` | <http://127.0.0.1:3002/swagger-ui> |
| `order` | <http://127.0.0.1:3001/swagger-ui> |

The raw document is at `/api-docs/openapi.json` on each. `order`'s is the one
worth reading: its endpoints are split into `orders`, which it owns and writes,
and `cache`, which it serves from another service's state without ever calling
it.

Both Swagger UIs include collection reads now: `GET /products` lists every
product owned by `product`, and `GET /orders` lists every order owned by
`order`.

For the messages themselves, `just examples ui` adds Redpanda Console on
<http://127.0.0.1:8080>, alongside whatever is already running:

```bash
just examples ui
```

Topics → `products` / `orders` shows the actual snapshots the two services
exchange, one record per entity key, and the offsets that
`GET /products/{id}` reports back as `applied_offset`. It is behind its own
profile, so neither `just examples up` nor `just examples demo` pays for it unasked.

For telemetry, `just examples observe` adds Kibana on
<http://127.0.0.1:5601>. Elasticsearch needs noticeably more memory than the
plain stack; override `ES_JAVA_OPTS` if Docker Desktop is tight on RAM, and use
`KIBANA_PORT` or `ELASTICSEARCH_PORT` if the defaults are already bound.
`ELASTIC_VERSION` moves both images together, with 9.2 as the floor — the native
`/_otlp` endpoint does not exist before it, and an older tag shows up as 404s
from the exporter rather than as anything compose reports.

Kibana opens **empty**: no data view for the kafkaman signals ships yet, and
neither does a dashboard. Create one over the indices Elasticsearch's OTLP
endpoint writes to and the metrics, spans, and correlated logs are there.
Packaging that view is tracked in
`wiki/plans/opentelemetry-completion.plan.md`.

Walk the lifecycle:

```bash
# product owns this; order has never heard of it yet.
PRODUCT=$(curl -sX POST localhost:3002/products -H 'content-type: application/json' \
  -d '{"name":"widget","price_cents":1250,"on_hand":10}' | jq -r .product_id)

# ...until it propagates. Served from order's local cache, no call to product.
curl -s localhost:3001/products/$PRODUCT

# order admits against that cached state alone.
ORDER=$(curl -sX POST localhost:3001/orders -H 'content-type: application/json' \
  -d "{\"product_id\":\"$PRODUCT\",\"quantity\":3}" | jq -r .order_id)

# A placed order reserves nothing: availability is still 10.
curl -s localhost:3001/products/$PRODUCT

# Fulfil it, and availability becomes 7 — recomputed by product, two hops away.
curl -sX POST localhost:3001/orders/$ORDER/fulfil
curl -s localhost:3001/products/$PRODUCT

# Cancel it, and it goes back to 10. There is no compensating write anywhere.
curl -sX POST localhost:3001/orders/$ORDER/cancel
curl -s localhost:3001/products/$PRODUCT
```

## Tests

| Tier | Where | Needs |
|---|---|---|
| Contract shape and wire spelling | `contracts/src/lib.rs` | nothing |
| Config file, declared roles, admission rules | `order/tests/service.rs` | Postgres |
| Config file, declared roles, derived availability | `product/tests/derive_availability.rs` | Postgres |
| The whole loop, HTTP only, on both boot paths | `tests/distributed-cache` | Postgres + Redpanda |
| Boot files name no kafkaman internals | `tests/distributed-cache/tests/boot_surface.rs` | nothing |
| The compose stack itself | `smoke.sh` | a running stack |

`tests/distributed-cache` is the reason the others can stay cheap: it is the
only tier that observes *wiring*, so it starts both services exactly as their
binaries do and never touches a database. Nothing in that column needs
starting by hand — every tier brings its own containers up through
testcontainers:

```bash
cargo test --workspace --all-features    # or: just test all
```

Do not run the suite while the compose stack is up. Ports do not collide —
every fixture binds an ephemeral one — but the Docker VM does: four compose
containers plus the two each test starts is enough contention that the
convergence deadline stops being generous. Measured here, the same test binary
goes from 2 passed in 13s to a `PoolTimedOut` failure in 33s. Run
`just examples down` first.

`smoke.sh` is not a test tier. It asserts the same lifecycle over HTTP, but
against whatever stack is already running, and exists so the compose path is
exercised rather than merely documented — the example config in this repo
rotted once already because nothing read it. `just examples demo` runs it; point it
somewhere else with `ORDER_URL` and `PRODUCT_URL`.
