# Ingest Poison Quarantine Policy

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-22
- Category: Durable receive ingest safety
- Scope: Defines how Kafka ingest handles malformed records, schema/deploy mismatches, and receive identity conflicts before committing offsets.
- Sources: wiki/reviews/m3-durable-completion-implementation-review.reference.md; wiki/plans/m3-durable-completion.plan.md
- Related: wiki/decisions/kafka-ingest-identity-and-ordering.decision.md; wiki/compatibility/m3-durable-receive-review-fix-api.compat.md

## Decision

Ingest may commit past a deterministic poison record only after it has written a
durable quarantine record in Postgres.

The quarantine row records source topic, partition, offset, key, raw payload
bytes when present, headers, expected message type/topic, failure kind, and
error message. The `(source_topic, source_partition, source_offset)` identity is
unique so repeated delivery of the same bad broker record is idempotent.

Deserialize/schema failures are not trusted as isolated poison. They count
toward a consecutive-skip circuit breaker. When the breaker threshold is
reached, ingest writes the quarantine row but does not commit the Kafka offset.
This intentionally pauses the partition before a deploy-ordering or schema
break can silently skip an unbounded topic segment.

Receive `message_id` conflicts with a different idempotency key are identity
anomalies, not normal duplicates. They are quarantined and committed after the
quarantine row is durable. Idempotency-key redelivery remains a normal duplicate
and does not create a quarantine record.

## Rationale

The previous review fix solved partition stalls by committing offsets for
deterministic poison. That is correct for a single malformed external record but
dangerous for realistic schema/deploy mismatches, where every record can look
like poison. Durable quarantine preserves evidence, and the consecutive-skip
breaker bounds silent loss.

## Consequences

- Operators can query ingest failures even when offsets have advanced.
- Consumers must choose and monitor a skip threshold appropriate for the topic.
- A poison record can still block the partition after the breaker trips; this is
  deliberate and safer than unbounded committed skips.
- Message-id conflict handling is no longer indistinguishable from legitimate
  idempotency-key redelivery.
