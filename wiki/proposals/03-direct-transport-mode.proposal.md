# Direct Transport Mode

- Document Class: Proposal
- Status: Proposed
- Date: 2026-06-21
- Category: Runtime modes
- Scope: Proposes explicit non-durable Kafka producer and consumer modes for workloads that intentionally bypass kafkaman's Postgres message ledger.
- Sources:
  - User design discussion, 2026-06-21
  - wiki/index.md
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
- Related:
  - wiki/proposals/04-observability-logging-policy.proposal.md
  - wiki/roadmaps/path-to-v1.roadmap.md

## Context

kafkaman's current project promise is a Rust library plus optional worker runtime for reliable Kafka-backed service messaging, with Postgres as the durable execution ledger. The accepted receive-side shape writes a consumed Kafka record into a per-type received table before committing the Kafka offset, so handler health does not block broker flow and operators keep a retry/remediation surface.

Some workloads may not need that guarantee. Very high-throughput streams, telemetry, analytics, cache invalidation, or other lossy events may prefer lower latency and lower database write volume over durable execution.

## Proposal

Add an explicit direct transport mode alongside the durable mode.

1. `Durable` remains the default mode.
   - Produce path uses the outbox / relay model.
   - Consume path uses ingest and dispatch schedulers, durable received tables, idempotency-key deduplication, retries, status, and DLQ.

2. `DirectProducer` publishes directly to Kafka.
   - No outbox row is written.
   - No transaction coupling with the application's business database is promised.
   - The API name should make the weaker guarantee visible, similar to the existing non-transactional send naming direction.

3. `DirectConsumer` consumes from Kafka and calls the handler directly.
   - No received row is written.
   - No kafkaman retry, durable status, DLQ, dedup-as-log, or operator redrive surface is promised.
   - Offset commit behavior must be explicit in configuration.

## Offset Policy

Direct consumers should require one of these policies rather than choosing silently:

- `CommitAfterHandler`: commits after the handler returns success. This reduces loss risk but lets slow, failing, or poison handlers block the partition.
- `CommitBeforeHandler` or `CommitOnReceipt`: commits before or independent of handler success. This maximizes broker flow but can lose work on handler failure.

The exact naming should be settled during API design, but the user-facing contract must distinguish throughput-first behavior from reliable execution.

## Consequences

This mode gives kafkaman an escape hatch for high-throughput or high-database-pressure workloads without weakening the default durable story.

The cost is semantic complexity. Documentation, type names, config, spans, and examples must make the durability boundary obvious. Direct mode must not be described as equivalent to durable mode with a storage setting turned off.

Direct mode also narrows the usefulness of some V1 features:

- idempotency keys may still be carried as headers, but kafkaman does not enforce durable deduplication;
- retry/backoff/DLQ policy does not apply unless the user implements it outside kafkaman;
- operator status pages can show transport activity, but not durable job state.

## Recommended Default

Keep durable mode as the default and require opt-in direct mode at the message type or runtime binding level.

Do not allow a global "disable DB storage" flag that quietly changes all guarantees. Prefer explicit producer and consumer constructors or per-message-type mode configuration that names the weaker behavior.

## Open Questions

1. Should direct mode be configured per message type, per runtime, or both?
2. Should direct mode live in the facade crate, or only behind lower-level `kafkaman-rdkafka` APIs?
3. Should `DirectConsumer` support middleware layers that are safe without durable state, or require a separate stack from durable dispatch?
4. What metrics and warnings should be mandatory when direct mode is enabled?

## Promotion Target

If accepted, promote this into a decision that defines direct-mode API names, configuration shape, offset policy names, and which V1 observability fields apply to direct mode.
