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

## Where to go from here

In a hurry: [Running them](#running-them) is the quickstart, and
[When it goes wrong](#when-it-goes-wrong) is the failure walkthrough. The rest
is reference.

- [How a service is assembled](#how-a-service-is-assembled) — what boot actually
  declares, and the escape hatch under it
- [Running them](#running-them) — `just examples all`, and each narrower arm
- [What builds what](#what-builds-what) — which container creates which table
  and topic
- [Looking at it](#looking-at-it) — the waterfall, and the two knobs that bound
  its volume
  - [Keeping the trace volume honest](#keeping-the-trace-volume-honest)
  - [Going deeper than the waterfall](#going-deeper-than-the-waterfall)
- [When it goes wrong](#when-it-goes-wrong) — the fault switch, the seven
  scenarios, and what each one proves
  - [The fault switch](#the-fault-switch)
  - [What each scenario shows](#what-each-scenario-shows)
  - [A panicking handler no longer takes the service with it](#a-panicking-handler-no-longer-takes-the-service-with-it)
  - [Reading it in Kibana](#reading-it-in-kibana)
- [Tests](#tests)

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

Everything runs from one compose file, in a few shapes.

**Everything, one command.** Builds the provisioner and both services, starts
the telemetry backend and the message UI beside them, creates the Kibana data
view, and walks the lifecycle against the result:

```bash
just examples all   # or just `just examples` — all is the default
```

The first run compiles librdkafka from source and takes a few minutes; later
ones reuse the build cache. Elasticsearch adds to that: a gigabyte of heap and
30-60s before it goes yellow. The stack stays up afterwards so you can poke at
it on `:3001` (order), `:3002` (product), `:5601` (Kibana) and `:8080` (Redpanda
Console). `just examples down` tears all of it down.

After the seven assertion steps it drives another twelve products through the
same path without asserting anything, so the latency histograms and queue-depth
gauges have a distribution rather than a single observation. One entity makes
Kibana look broken. Set `VOLUME_PRODUCTS` to change the count.

**Break it on purpose**, once something is up:

```bash
just examples faults
```

Six asserted failure scenarios — retries, the dead-letter queue, redrive, a
panicking handler, a poison record, a broker outage. See
[When it goes wrong](#when-it-goes-wrong). `just examples all` runs the first two
of them, so the telemetry is not uniformly green.

**Without the backend**, when you want propagation and not a gigabyte of heap:

```bash
just examples demo
```

Same walkthrough, no collector, no Elasticsearch, no Kibana, no volume phase.

**Telemetry without the message UI or the volume** is the middle arm:

```bash
just examples observe
```

That is the service stack plus the `observability` compose profile. The
services export OTLP/HTTP to `http://otel-collector:4318`, and the collector
writes all three signals to Elasticsearch. The `demo` path pins
`OTEL_EXPORTER_OTLP_ENDPOINT` to the empty string, so the binaries install no
provider and do not log failed exports into a backend that is not running — and
so that a value you export for your own tooling is not inherited by containers
where it would point somewhere else.

**Why a collector, when Elasticsearch ingests OTLP natively.** Because it does
not ingest all of it. Measured against 9.3.5 on 2026-08-27:
`POST /_otlp/v1/metrics` answers 200, while `/v1/traces` and `/v1/logs` answer
400 `no handler found for uri`. The Rust SDK surfaces that as a bare "network
error", so the direct topology this example used to ship dropped every span and
every log record with nothing reporting a cause. Even metrics were partial —
Elasticsearch wants delta temporality and the SDK emits cumulative, so the three
kafkaman latency histograms were accepted and silently discarded.

The collector answers all three, and `cumulativetodelta` converts the
histograms. That conversion lives in the collector rather than in `kafkaman-otel`
on purpose: temporality is a property of the backend, not of the
instrumentation, and the crate stays correct against a cumulative backend
unchanged. `examples/otel-collector.yaml` carries the detail.

The pipeline itself is `kafkaman-otel`, the opt-in companion crate both
binaries depend on. It is not reachable through the `kafkaman` facade, and that
is deliberate — the facade must not link an OpenTelemetry SDK, which
`just opt-out` asserts. Adopters name it directly:

```rust
let telemetry = kafkaman_otel::init("order-service")?;
// ... build the runtime, serve, drain ...
telemetry.shutdown()?;
```

Export is **plaintext HTTP only**. The exporter's client resolves without a TLS
backend and the runtime image carries no `ca-certificates`, so an `https://`
endpoint fails at run time with nothing failing at build time. To change that,
add a root store to the image and enable TLS on the exporter from your own
manifest:

```toml
kafkaman-otel = "0.1"
opentelemetry-otlp = { version = "0.32", features = ["reqwest-rustls"] }
```

Cargo unifies features across the graph, so that reaches the exporter
`kafkaman-otel` links without changing any code. It is done from the adopter's
side rather than behind a `kafkaman-otel` feature so the crypto provider stays
your choice; see the crate docs.

**Running the services on the host against the observed stack.** `just examples
observe` publishes the collector on `127.0.0.1:4318`, so the `cargo run` shape
below exports too — the endpoint is just the host address instead of the compose
one. Start the stack, stop the two containers you want to replace, and run them
yourself:

```bash
just examples observe
docker compose -f examples/compose.yaml stop order product

cd examples/order
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/order_service \
KAFKA_BROKERS=127.0.0.1:19092 \
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run
```

Leave `OTEL_EXPORTER_OTLP_ENDPOINT` unset and the binary installs no provider at
all, which is what keeps an ordinary `just examples up` loop quiet.

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
profile, so neither `just examples up` nor `just examples demo` pays for it
unasked. `just examples all` includes it.

For telemetry, `just examples all` and `just examples observe` add Kibana on
<http://127.0.0.1:5601>. Elasticsearch needs noticeably more memory than the
plain stack; override `ES_JAVA_OPTS` if Docker Desktop is tight on RAM, and use
`KIBANA_PORT` or `ELASTICSEARCH_PORT` if the defaults are already bound.
`ELASTIC_VERSION` moves both images together, with 9.2 as the floor — the
collector's `otel` mapping mode writes to the `*-*.otel-*` data streams and
relies on index templates that ship from 9.2. The collector service is Elastic
Agent running in OpenTelemetry mode, because that distribution includes the
`elasticapm` processor and connector that Kibana's Applications/APM views use.
`ELASTIC_VERSION` moves that image too, so the collector never runs a different
generation from the stack it writes to. It is a much larger image than the
contrib collector it replaced — on top of Elasticsearch's heap, this is the arm
that costs the most; `just examples demo` still pays for none of it.
`ELASTIC_AGENT_IMAGE` can override the collector image, and `OTLP_HTTP_PORT`
publishes 4318 so a service you run on the host under `just examples up` can
export to the same collector as the containers.

`just examples handoffs` is the last arm, and the only one that reads the stack
rather than running it: it joins linked-mode Kafka handoffs back into producer
and consumer APM URLs. It needs the observability profile already up, and says
so rather than failing at a socket. More on it below.

Kibana opens with APM and a **`kafkaman telemetry`** dashboard, created by
`examples/kibana-dashboard.sh`, which both `just examples all` and `just examples
observe` run for you. Start in Applications/APM: the example configs set
`observability.defaults.kafka_trace_handoff = "parented"`, so the product-create
path renders as one distributed waterfall across both services:

```text
POST /products -> db.query insert product -> kafkaman.enqueue
  -> db.query insert outbox row -> kafkaman.relay.publish
  -> kafkaman.ingest -> kafkaman.dispatch
  -> db.query apply received row to cache -> db.query mark received processed
  -> db.query mark outbox published
```

Open `kafkaman-example-product`, select the `POST /products` transaction, then
choose a trace sample timeline. You should see rows for both
`kafkaman-example-product` and `kafkaman-example-order` in the same trace. That
is the primary waterfall view. The collector also copies the resource
`service.name` onto each span as `attributes.service.name`, so span detail views
and Discover rows can identify the producing service without changing span
names.

The saved dashboard is the raw-signal companion. It contains four Discover
panels: Kafka trace handoffs, recent kafkaman waterfall spans, queue metrics,
and service logs. The handoff panel shows consumer `kafkaman.ingest` spans; in
the example's parented mode `parent_span_id` points at the producer
`kafkaman.relay.publish` span, and in linked mode the `links.trace_id` /
`links.span_id` fields point back to it. The trace panel includes
`attributes.service.name`, HTTP route spans, named `db.query ...` spans, and
`kafkaman.*` spans so one request's HTTP, Postgres, outbox, relay, ingest,
dispatch, and cache work can be searched together. The saved dashboard opens on
a four-hour window because traces and logs are produced by the startup and smoke
traffic, while queue metrics continue to arrive as long as the services run. The
split is deliberate: the underlying `kafkaman telemetry` data view spans
`*-generic.otel-*`, where the collector's `otel` mapping mode writes all three
signals —
`metrics-generic.otel-default`, `traces-generic.otel-default` and
`logs-generic.otel-default` — and a raw Discover page is dominated by recurring
metric documents.

The dashboard script also creates the shared data view and is idempotent, so you
can run it against a stack you started yourself:

```bash
examples/kibana-dashboard.sh
```

Dashboard URL:

```text
http://127.0.0.1:5601/app/dashboards#/view/kafkaman-telemetry-dashboard?_g=(time:(from:now-4h,to:now),filters:!())
```

APM URL:

```text
http://127.0.0.1:5601/app/apm/services?rangeFrom=now-4h&rangeTo=now&environment=ENVIRONMENT_ALL
```

The library default remains `kafka_trace_handoff = "linked"`, following
OpenTelemetry messaging semantics for batch-safe consumers. If you switch the
examples back to linked mode, Kibana's APM UI shows the span-link count but does
not reliably turn that count into a downstream waterfall link for these
OTel-generic documents. Use the handoff helper when you want the two APM URLs
joined for you:

```bash
just examples handoffs
```

For the exact product-create-to-order-ingest path, filter the candidate handoffs:

```bash
MESSAGE_TYPE=product_snapshot \
CONSUMER_SERVICE=kafkaman-example-order \
PRODUCER_TRANSACTION='POST /products' \
LIMIT=100 \
just examples handoffs
```

Each linked-mode row prints a producer waterfall URL and a consumer waterfall
URL. Open the producer URL to inspect `POST /products ->
kafkaman.relay.publish`, then open the consumer URL to inspect
`kafkaman.ingest -> kafkaman.dispatch -> db.query apply received row to cache`.

### Keeping the trace volume honest

Spans scale with traffic, and a demo stack left running overnight is traffic.
Three things bound it, cutting at three different points:

**`RUST_LOG` bounds what is recorded.** Everything that runs on a *timer* is
`debug`, so at the default `info` it is never created and costs nothing: the
polling spans `claim outbox batch` and `claim received row`, the claim
transactions around them, the retention sweep, and the scheduler functions that
open them. Raise it to `debug` only when you want to inspect empty polling
cycles; otherwise start from a route transaction such as `POST /products` to
debug one lifecycle. What runs *because there is a message* is `info` and is
already in the trace — see "Going deeper" below for where the line falls.

**`OTEL_TRACES_SAMPLER` bounds what is exported.** The OpenTelemetry SDK reads it
directly, so it works on these examples without any kafkaman involvement:

```bash
OTEL_TRACES_SAMPLER=parentbased_traceidratio \
OTEL_TRACES_SAMPLER_ARG=0.1 \
just examples all
```

That keeps a tenth of traces *whole*, rather than a tenth of every trace's spans
— head sampling, decided at the root and inherited across the outbox and Kafka
hops by the same propagated context the waterfall is built from. The default is
`parentbased_always_on`, which is right for a demo you are about to read and
wrong for anything with real traffic.

**The collector bounds what is stored.** Compose polls each service's `/health`
every two seconds, which would otherwise be the highest-volume span in the stack
by a wide margin, so `examples/otel-collector.yaml` drops spans whose
`http.route` is `/health` before they reach Elasticsearch. It drops by route
template rather than by raw path, which is also why an unmatched request reports
`http.route` as `<unmatched>` rather than the URL somebody typed: a transaction
name minted per URL is a Kibana page that never finishes loading.

None of the three is a retention policy. Elasticsearch keeps what it is given for
as long as it is configured to, and this stack configures nothing — `just
examples down` is the retention policy.

### Going deeper than the waterfall

The `kafkaman.*` waterfall is kafkaman's *phases*. Underneath it, kafkaman's own
functions are instrumented too, behind the target `kafkaman::internal` — and
they are split across two levels, because the two halves cost wildly different
amounts.

**The message-path half is `info`, and you already have it.** A function that
runs because there is a message to process — `dispatch_claimed_row`,
`converge_and_dispatch`, `enqueue_inner`, `publish_row_traced`, and about sixty
others — is in the default trace. That is what makes a dispatch waterfall
gapless: the time between `kafkaman.dispatch` opening and the first `db.query`
underneath it is attributed to a named frame rather than to nothing.

**The polling half is `debug`, and you have to ask for it:**

```bash
RUST_LOG=info,kafkaman::internal=debug just examples all
```

The target is the point. Plain `RUST_LOG=debug` would also switch on `sqlx`'s and
`rdkafka`'s debug logging and bury what you came for; this reaches kafkaman's
functions and nothing else.

**Why the split is where it is.** Measured on this stack, not estimated. With
the *whole* tier promoted for one service and the stack otherwise idle, that
service produced **2322 spans in two minutes** against 5 from the service without
it. A product-create request, meanwhile, went from 17 spans to 25.

Those two numbers are unrelated. The 2322 is not request work — it is
`claim_batch`, `claim_received_row`, `observe`, `refresh`, and the other loop
functions running on their intervals forever and describing having found nothing.
So the loop functions stay `debug` and the message-path ones do not.

Re-measured after the split, against this stack at `RUST_LOG=info`:

| | spans |
| --- | --- |
| one product-create request, end to end | 38 |
| ...of which promoted function spans | 16 (7 in `product`, 9 in `order`) |
| ...of which `db.query` | 16 |
| ...of which `kafkaman.*` phase | 5 |
| idle stack, both services, two minutes | **0** |

Eight promoted spans per service per request, and nothing at all when nothing is
happening. That is the whole affordability argument, and it is the same rule that
already applied to the `db.query` spans — which is why `claim outbox batch` is
invisible at `info` and `insert outbox row` is not.

Two functions are `debug` for a different reason and are called out where they
are defined: `health` and `ready`, which an orchestrator probes forever, and
`enqueue`, which is excluded so `kafkaman.enqueue` stays a *trace root* in a
service with no caller span rather than hanging under a function name.

Those span names are function names. They are **not** a stability surface and
will change whenever the functions do — build dashboards on the `kafkaman.*`
phase spans and the `db.query` summaries, which are. That the *level* of a given
function is not a stability surface either is the same statement: expect
individual functions to move between the two halves as this is tuned.

What this deliberately does not do is time every Rust method. Rust has no
runtime agent that can instrument a whole binary the way a JVM agent rewrites
bytecode at class load, and inlining means most small functions do not exist as
call frames in a release build. Spans answer "where did this request wait";
"which code burned the CPU" is a profiler's question, and
`wiki/plans/apm-waterfall-traces.plan.md` records what happened when we measured
whether continuous profiling could answer it against this stack.

## When it goes wrong

Everything above is the happy path, and for a long time that was all this stack
could show. Measured against it before any of the below existed: **227,394 spans
indexed, none carrying a failure status, no log record above `INFO`, and every
dead-letter queue empty.** The half of kafkaman that exists for when things go
wrong — backoff, the attempt budget, the DLQ, redrive, ingest quarantine — was
demonstrated by nothing and therefore proven by nothing.

```bash
just examples faults
```

Seven scenarios, each asserted, against a stack that is already up. `just
examples all` runs the first two of them, so the telemetry has a failure side to
look at without your asking.

### The fault switch

`product` carries a small control plane that makes its dispatch handler
misbehave on demand. It is in the OpenAPI spec, so it is also in
`http://127.0.0.1:3002/swagger-ui`:

```bash
curl -sX POST localhost:3002/faults -H 'content-type: application/json' \
  -d '{"mode":"error","remaining":2}'   # fail twice, then succeed
curl -sX POST localhost:3002/faults -H 'content-type: application/json' \
  -d '{"mode":"error"}'                 # fail until disarmed
curl -sX POST localhost:3002/faults -H 'content-type: application/json' \
  -d '{"mode":"panic"}'                 # panic instead of returning
curl -sX POST localhost:3002/faults -H 'content-type: application/json' \
  -d '{"mode":"constraint"}'            # fail on a constraint the database held
curl -sX POST localhost:3002/faults -H 'content-type: application/json' \
  -d '{"mode":"contention"}'            # fail on a deadlock
curl -sX POST localhost:3002/faults -H 'content-type: application/json' \
  -d '{"mode":"statement"}'             # fail on a statement that cannot run
curl -s  localhost:3002/faults          # mode, budget left, times fired
curl -sX DELETE localhost:3002/faults   # disarm
```

The last three fail *in the database*, on the dispatch transaction's own
connection, which is what makes them worth having as separate modes: each lands
in a different branch of kafkaman's SQL classifier, and each answers a different
operational question. A deadlock is nobody's bug and is worth retrying unchanged;
a constraint violation will fail identically on all eight attempts; a malformed
statement is a bug that shipped. They are produced with `RAISE … USING ERRCODE`
rather than by genuinely breaking something, because sqlx classifies on the
five-character SQLSTATE alone — a real check violation and this one are the same
value by the time kafkaman reads them — and because a deadlock cannot be
staged from one handler at all.

`remaining` is the whole difference between the two stories kafkaman tells about
failure. A bounded count is *transient*: the row retries on its backoff and
converges anyway, with nobody involved. Omitting it is *permanent*: the row
spends its budget, dead-letters, and waits for a human.

Only `product` has one, because only `product` has an application handler —
`order` declares `ProductSnapshot` with `cache::<T>()` and has no handler to
fault. The switch is in-memory and per-process, deliberately, so it can be armed
and disarmed without a restart; a restart is the very thing some of these
scenarios are checking does *not* happen.

### What each scenario shows

| | Scenario | What to look for |
| --- | --- | --- |
| 1 | Two failures, then success | The cache converges anyway. Nothing dead-letters, no operator involved — this is the common case and it is meant to be boring. |
| 2 | Failing forever | Eight attempts over ~19s, then `Failed`. The count is asserted against `kafkaman.toml`, not against the library default. |
| 3 | Redrive | `POST /internal/kafkaman/dlq/order_snapshot/redrive` puts it back to `Pending`; it converges on the next pass. |
| 4 | Three panics | The service **stays up** and keeps dispatching. See below. |
| 5 | A poison record | Quarantined into `received_ingest_failures`, and the good record produced behind it still converges. One unreadable record must not stall a partition. |
| 6 | The broker stops | Writes are still accepted, the outbox backs up, and the backlog drains when the broker returns. Nothing to replay by hand. |
| 7 | Three database failures | The row stores `infrastructure` at `stage: handler`; APM groups each under `constraint`, `contention` or `statement`. Two vocabularies, each asserted where it lives. |

Scenarios 5, 6 and 7 need Docker rather than HTTP alone — one produces a
malformed record with `rpk`, one stops and starts the broker, one reads the
service's own `received_order_snapshot` rows — and are skipped with a note when
Docker is not available. Pick a subset with
`FAULT_SCENARIOS="1 2" examples/faults.sh`.

Scenario 7 is where both of kafkaman's failure vocabularies are visible at once,
and where they deliberately disagree.

The **row** stores `type: urn:kafkaman:problem:infrastructure` and `stage:
handler`. That pair is the taxonomy and the blame axis: a database error returned
by a handler is an infrastructure failure raised in the handler's frame, not a
handler failure. Collapsing the two into one field is what made an operator
filtering for infrastructure problems miss every one a handler had touched.

The **span** stores `error.type: urn:kafkaman:problem:constraint`, and Elastic
turns its exception event into an APM error group of the same name. `type` on the
row has four permanent values because they are written into stored rows and
cannot churn, so fourteen of the eighteen problem URIs collapse onto
`infrastructure`; the fine class survives on the span, which is the only reason
that coarsening is acceptable. The scenario asserts each half against the place
that actually holds it — the row through `psql`, the error group through
Elasticsearch — and skips the second half with a note when the observability
profile is not up.

### A panicking handler no longer takes the service with it

It used to. A panic unwound out of the handler, out of the dispatcher's task, and
out of the supervised runtime, so the whole process exited — HTTP server
included. Compose restarted it, the claim transaction had rolled back so the row
was still `Pending`, it was claimed again, and it panicked again. After three
restarts compose gave up. **One bad message killed the service permanently.**

The panic is now caught at the handler call boundary and turned into an ordinary
handler error, so an isolated bad row retries on its normal budget and
dead-letters like any other failure. A bad deploy that makes every claimed row
panic is not treated as healthy background work: after ten consecutive *distinct
rows* panic, the dispatcher returns an error so runtime supervision can surface
it. Distinct rows and not panics, deliberately — one poison message spending its
whole retry budget never trips it, because absorbing that is exactly what the
budget and the DLQ are for. The limit is
`[dispatcher].max_consecutive_panicking_rows`.
Panics in kafkaman's *own* loops still fail fast: a bug in the library is not
something to swallow, and a dispatcher that dies quietly while `/health` still
answers 204 is worse than one that stops.

This is containment, not permission. A handler should still return `Err` — that
is the path with a failure class, a retry schedule, and a message an operator can
read. What changed is the blast radius when one does not.

Scenario 4 proves the containment without needing Docker, using the fault
switch's own state: `fired` lives in the panicking process's memory, so reading
`3` back after three panics is proof that the process never restarted. A restart
would report `0` and a disarmed fault.

### Reading it in Kibana

The dashboard has a **kafkaman failed transactions** panel, querying:

```
data_stream.type: traces and attributes.processor.event: transaction and (status.code: Error or attributes.event.outcome: failure)
```

That is the APM-level split: a failed receive attempt is a failed
`kafkaman.dispatch` transaction, so the APM service overview can separate
successful and failed async consumer work. Use `attributes.transaction.type` to
distinguish messaging transactions from HTTP requests, and
`attributes.kafkaman.failure.stage` to group where the failure was recorded:
`routing`, `handler`, or `bookkeeping`.

The dashboard also has a **kafkaman failure details** panel, querying:

```
data_stream.type: traces and status.code: Error
```

`status.code` is where the OTel mapping puts a span's status, and it is present
only on spans that reported one, so that query is an exhaustive list of what the
system called a failure with no exclusions to maintain. The detail view includes
both the failed `kafkaman.dispatch` transaction and the narrower span that
failed inside it, such as `kafkaman.handler`. `status.message` carries the
recorded error, `attributes.error.type` carries the stable problem URI, and
`attributes.handler.outcome` is `panicked` on the spans where the handler
panicked rather than returning. Metrics carry the same distinction as
`kafkaman.scheduler.rows{scheduler="dispatcher",status="panicked"}`; panicked
rows are also counted under `status="failed"`.

In APM, look at the service whose consumer handled the message. A product-side
handler failure caused by an order event belongs to
`kafkaman-example-product`, with `transactionType=messaging`. The
`POST /orders` request in `kafkaman-example-order` should stay successful: that
service accepted and durably published the order, and the downstream product
handler failed asynchronously after the HTTP request had already finished.
Elastic's Errors UI is a separate grouping, over reported *exceptions* rather
than over failed transactions, and this stack now populates it. Every failure on
the durable path emits an OpenTelemetry `exception` span event carrying
`exception.type` — the same permanent `urn:kafkaman:problem:*` URI the DLQ row
stores — and `exception.message`, bounded to 256 bytes. So the APM service page
groups failures by kind, counts occurrences per group, and lets you pivot from a
failed transaction to the error and back.

The grouping is finer than the DLQ's. `ReceivedFailureKind` has four permanent
values because they are written into stored rows and cannot churn; the exception
type is derived from the error's own Rust type and has eighteen, so a handler that
*panicked* reports `urn:kafkaman:problem:handler-panicked` and groups separately
from one that returned an error, while both still dead-letter as
`urn:kafkaman:problem:handler`. Reconcile the two views on
`attributes.error.type`, which both carry.

One failure produces one error document, not one per span: the event is emitted
on the innermost span that owns the failure — `kafkaman.handler` for a handler
failure, `kafkaman.dispatch` for a routing or bookkeeping one — and the spans
enclosing it carry the failure *status* without repeating the event.

The operator routes are the other half of the picture, and they answer without
any telemetry backend at all:

```bash
curl -s localhost:3002/internal/kafkaman/dlq      | jq   # depth, attempts, error history
curl -s localhost:3002/internal/kafkaman/received | jq   # depth by status
curl -s localhost:3002/internal/kafkaman/outbox   | jq   # the send side
curl -s localhost:3002/internal/kafkaman/stuck    | jq   # overdue on either side
```

> **These routes have no authentication.** `/dlq` omits message bodies and
> headers by construction, but it does return `entity_key` and each failure's
> `detail` — a business key and free text your handler wrote. Panic details can
> also carry assertion dumps or `Debug` output the handler never meant to expose.
> That is acceptable *here* and nowhere else: a disposable local stack, on a
> private compose network, with ports published to loopback. kafkaman ships reads
> and the destructive redrive as two separate routers precisely so a real
> deployment can put different policies on each — see `admin_router` and
> `redrive_router`.

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
rotted once already because nothing read it. `just examples` runs it; point it
somewhere else with `ORDER_URL` and `PRODUCT_URL`, and drive extra traffic past
the assertions with `VOLUME_PRODUCTS`.
