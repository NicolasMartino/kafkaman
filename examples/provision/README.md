# example-provision

Builds the environment the two example services boot into, then exits.

```bash
ADMIN_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
KAFKA_BROKERS=127.0.0.1:19092 \
cargo run -p example-provision
```

## Why a service cannot do this itself

Every kafkaman message is a full snapshot of one entity, keyed by that entity, so
the topic it travels on is what a late consumer rebuilds the entity from. That
only works if the topic is log-compacted. A broker with
`auto.create.topics.enable` will happily make the topic on first publish — with
its default `cleanup.policy=delete`, which ages those snapshots out and quietly
destroys the property the whole model rests on.

So the services do not create topics. They **verify** them at boot, before any
loop is spawned, and refuse to start on one that is missing or wrongly
configured. That posture is only workable because something else creates them
first, and this is that something.

## What it does

| | |
|---|---|
| Databases | `CREATE DATABASE` for each name, absorbing "already exists". |
| Topics | `converge_topics` in `TopicMode::Create` for each registered type's declared topic. |

Idempotent in both halves — compose re-runs it on every `up`.

## What it deliberately does not do

**Tables.** Not kafkaman's, not the services'. Each service migrates its own
schema at boot under an advisory lock; a second writer of the same tables would
be a second source of truth for their shape. The boundary is that things which
must exist *before* a connection can be opened belong to the environment, and
everything reachable *through* one belongs to the service that owns it.

**Repair.** Convergence creates what is absent and fails on what is present and
wrong. A topic already carrying records under the wrong retention policy is an
operational decision — the supported recovery is republishing every entity onto a
new topic, because offsets only compare within one topic-partition — and not a
call a provisioner should make on someone's behalf.

## Environment

| Variable | Required | Meaning |
|---|---|---|
| `ADMIN_DATABASE_URL` | yes | A connection string for a database that already exists, conventionally `postgres`. Not a service's own URL: `CREATE DATABASE` cannot run from the database being created. |
| `KAFKA_BROKERS` | yes | Bootstrap servers. |
| `PROVISION_DATABASES` | no | Comma-separated; defaults to `product_service,order_service`. |
| `TOPIC_PARTITIONS` | no | Partitions per entity topic; defaults to 1. |

The partition count is an argument rather than something `example-contracts`
declares, and that split is the point: how a topic is partitioned is a property
of the deployment, while its cleanup policy is a property of the model. Raising
it later is not free — changing a keyed compacted topic's partition count changes
which partition each entity hashes to, and the supported migration is
republishing every entity onto a new topic.

## Where it is covered

- `src/lib.rs` — name validation and topic declaration, no containers.
- `tests/distributed-cache/tests/provision.rs` — against a real Postgres and a
  real broker: that a service refuses to boot without it, that asking did not
  create the topic anyway, that the created topics are compacted, and that a
  re-run changes nothing.
