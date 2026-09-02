# kafkaman

kafkaman is a Rust library for building a Kafka-backed distributed cache of
domain entities.

The supported model is intentionally narrow: every in-scope message is a full
snapshot of one domain entity, keyed by that entity, published to a compacted
Kafka topic, and applied to a local Postgres cache table with an offset guard.
Services read another domain's current state locally instead of making
synchronous service-to-service calls.

## What kafkaman Owns

- transactional entity snapshot enqueue through Postgres outbox tables;
- key-serialized outbound supersede so only the newest pending state for an
  entity is published;
- Kafka ingest into durable received tables before committing offsets;
- guarded cache upsert from Kafka metadata, using topic, partition, and offset;
- boot-time verification that entity topics really are `cleanup.policy=compact`,
  under `[topics] mode`, before any loop is spawned — a broker left to
  auto-create makes them `delete`, which quietly breaks rebuild-from-log;
- retry, backoff, DLQ, and redrive as reliability plumbing for the entity-cache
  pipeline;
- state-sourced republish for repair, never replaying stale outbox rows as truth;
- assembly of all of the above from declared roles, so a service says what it
  publishes and caches rather than wiring up the tables and loops those imply.

## Assembling a Service

```rust
let runtime = kafkaman::RuntimeBuilder::new()
    .config(config)          // parsed kafkaman.toml; there is no discovery fallback
    .pool(pool.clone())      // the host owns pool sizing
    .brokers(&brokers)
    .consumer_group("order-service")
    .publish::<OrderSnapshot>()      // outbox table + relay
    .cache::<ProductSnapshot>()      // received + cache tables, ingester + dispatcher
    .build()                 // validate, converge topics, migrate — start nothing
    .await?;

runtime.run(shutdown).await?;        // start the loops, supervise, drain
```

`publish`, `cache`, `handle`, and `handle_before` are the whole vocabulary.
kafkaman derives the changelog and its version numbers, the tables, topic
convergence, the loops, and the shutdown wiring from them.

Worker-role binaries can keep the same role declaration and narrow only the
running topology with `.subsystems(...)`. `Subsystems::PIPELINE` starts relay,
ingest, dispatch, and queue metrics while leaving purge explicit; the default
remains all configured loops for compatibility.

What stays with the host is deliberate and enumerated: the Tokio runtime, the
pool, business schema, signal handling, process exit, telemetry installation,
config discovery, database creation, and the *authority* over topic creation.
`build()` converges topics in exactly the mode the config selects and never
defaults to or upgrades to `create`.

HTTP composition lives in `kafkaman::axum` behind a feature, which makes the
server one more supervised loop — so the first kafkaman loop to die stops the
service accepting traffic.

Every low-level primitive stays public and supported: `migrate`,
`converge_topics`, the table handles, the worker loops, the publishers and
consumers. `examples/product/src/service_manual.rs` boots entirely through them,
and the end-to-end suite runs against that path as well as the builder's, so the
escape hatch is a tested claim rather than a documented intention.

## What kafkaman Does Not Own

kafkaman is not a general durable job queue or command bus. Messages like
"send welcome email to user 123", payment execution, commands, analytics events,
generic jobs, mutation dispatch queues, and direct transport mode are outside
the library's purview.

Applications may still need those systems, but they should live outside
kafkaman.

## Seeing It Work

[`examples/`](examples) holds two services that share no database and never call
each other, yet each serves data the other owns: `product` publishes product
snapshots, `order` caches them and admits orders from that cache alone, and
`product` recomputes availability from its own converged cache of orders.

[`tests/distributed-cache`](tests/distributed-cache) drives both of them over
HTTP and nothing else, which is the only tier at which a wiring mistake — a
dispatcher never spawned, a cache table missing from a changelog — is visible.
It runs the whole lifecycle against the builder path and a hand-wired one.

To watch it happen rather than read about it, `just examples` builds both services,
starts them against Postgres and Redpanda, and walks the lifecycle: an order is
fulfilled on one service and availability drops on the other, two hops away,
with no call between them. Each service serves a Swagger UI describing its own
endpoints, Redpanda Console reads the snapshots actually on the wire, and Kibana
opens on the OpenTelemetry metrics, traces and logs the two services emitted
doing it. The example config sets `observability.defaults.kafka_trace_handoff =
"parented"`, so APM trace samples show the product-to-order path as one
distributed waterfall; the library default stays `linked`, which is what
messaging semantic conventions prescribe for batch-shaped consumers. `just
examples demo` is the same walkthrough without the telemetry backend, for when
Docker is tight on memory.

## Verifying It

`just check` is the gate to run before pushing: formatting, `clippy -D warnings`,
the dependency-graph assertions in `just opt-out`, `cargo doc -D warnings`, the
lockfile check, the feature matrix, and the full test suite. It mirrors CI except
for three jobs that are too slow or need their own toolchain:

| Recipe | Answers |
| --- | --- |
| `just msrv` | Does the workspace still build on the `rust-version` it advertises? |
| `just audit` | Any security advisory against `Cargo.lock`? Triage lives in `.cargo/audit.toml`, where every ignore carries a reason and a revisit condition. |
| `just test coverage` | Is the line-coverage floor still met? |

`just check-release` runs all of it, plus `just publish-order`, which records the
order the crates must be published in — publishing out of it fails with "no
matching package named ...", which reads like a missing crate rather than a
sequencing mistake.

The container-backed suites start Postgres and Redpanda through testcontainers,
so they need a running Docker daemon. `just clean-containers` removes any left
behind by an interrupted run, scoped by label so it cannot touch unrelated ones.

## Current Status

This repository is pre-v1, with the V1 acceptance envelope now documented.
M1-M7 have delivered and validated the durable send, durable receive,
retry/DLQ, entity-cache propagation, runtime composition, observability, and
ship-quality hardening surface. The remaining step is release management, not an
open M7 implementation phase.
