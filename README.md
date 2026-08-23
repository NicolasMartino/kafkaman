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
- retry, backoff, DLQ, and redrive as reliability plumbing for the entity-cache
  pipeline;
- state-sourced republish for repair, never replaying stale outbox rows as truth.

## What kafkaman Does Not Own

kafkaman is not a general durable job queue or command bus. Messages like
"send welcome email to user 123", payment execution, commands, analytics events,
generic jobs, mutation dispatch queues, and direct transport mode are outside
the library's purview.

Applications may still need those systems, but they should live outside
kafkaman.

## Current Status

This repository is pre-v1. The active milestone is M5: entity-cache propagation.
The M1-M4 durable send, receive, retry, and DLQ machinery remains in the codebase
as the reliability substrate for entity cache propagation, not as a separate
generic messaging product.
