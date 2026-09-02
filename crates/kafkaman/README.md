# kafkaman

A Rust library for building a Kafka-backed distributed cache of domain entities.

Every in-scope message is a **full snapshot of one domain entity**, keyed by that
entity, published to a compacted Kafka topic, and applied to a local Postgres
cache table behind an offset guard. Services read another domain's current state
locally instead of making synchronous service-to-service calls.

The model is deliberately narrow. kafkaman is not a job queue, a command bus, or
an analytics event pipeline, and it will not become one.

## What it owns

- Transactional entity-snapshot enqueue through a Postgres outbox table.
- Key-serialized supersede, so only the newest pending state for an entity is
  published.
- Kafka ingest into durable received tables *before* offsets are committed.
- A guarded cache upsert keyed on topic, partition, and offset.
- Boot-time verification that entity topics really are `cleanup.policy=compact` —
  a broker left to auto-create them makes them `delete`, which silently breaks
  rebuild-from-log.
- Retry, backoff, dead-lettering, and redrive.
- State-sourced republish for repair. Row-sourced outbox replay is refused, not
  offered: republishing a stored row emits stale state at a *newer* Kafka offset,
  which every consumer's convergence guard would then correctly and permanently
  believe.

## Assembling a service

```rust,ignore
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

## What stays with the host

Deliberate and enumerated: the Tokio runtime, the `PgPool` and its sizing,
business schema, signal handling, process exit, telemetry installation, config
discovery, database creation, and the *authority* over topic creation. `build()`
converges topics in exactly the mode your config selects, and never defaults to
or upgrades to `create`.

kafkaman emits through `tracing` and the `opentelemetry` API and installs no SDK,
so it composes with a host that already exports telemetry rather than colliding
with it. [`kafkaman-otel`](https://docs.rs/kafkaman-otel) is the optional,
opt-in pipeline for hosts that do not have one.

## Features

| Feature | Default | Effect |
| --- | --- | --- |
| `metrics` | on | OpenTelemetry counters and histograms from the worker loops and Kafka transport |
| `traces` | on | W3C trace-context capture, span parenting, and links |
| `rdkafka` | off | Links librdkafka; required for `RuntimeBuilder` |
| `axum` | off | Correlation middleware, operator routes, and HTTP/runtime composition |

Turning `metrics` and `traces` off removes the `opentelemetry` dependency
entirely — a claim about the dependency graph that the repository's `just opt-out`
gate asserts with `cargo tree` rather than merely documenting.

## Status

Pre-1.0. The supported surface is described in `wiki/specs/v1-acceptance.spec.md`,
and every behavioural change carries a note in `wiki/compatibility/`.

## License

MIT OR Apache-2.0.
