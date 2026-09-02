# M1 Durable Send - Schema and API Compatibility

- Document Class: Compatibility
- Status: Active
- Date: 2026-06-21
- Category: Persistence and API change
- Scope: Records the persisted-schema and public-API changes made while resolving the M1 durable-send implementation reviews, and the migration/compatibility impact for anyone who built an outbox table before these changes.
- Sources:
  - wiki/reviews/m1-durable-send-implementation-review.reference.md
  - wiki/reviews/m1-durable-send-implementation-rereview.reference.md
  - crates/kafkaman-sqlx/src/lib.rs
  - crates/kafkaman-core/src/lib.rs
  - crates/kafkaman-rdkafka/src/lib.rs
- Related:
  - wiki/specs/m1-durable-send.spec.md
  - wiki/decisions/schema-and-change-management.decision.md

## Summary

The review-fix pass changed the outbox table shape and three public API surfaces.
Fresh installs are unaffected. Existing outbox tables created before these changes
need one additive changeset (provided) to stay compatible.

## Persisted Schema Changes

### New `idempotency_key` column

The outbox table now carries a nullable `idempotency_key TEXT` column so the
`Envelope::idempotency_key` a caller sets is persisted durably and forwarded to
the broker, instead of being silently dropped.

- Fresh tables get the column directly from `create_outbox_table_sql`.
- Tables created before this change do **not** get the column from a re-run of
  `CreateOutboxTable`, because that changeset uses `CREATE TABLE IF NOT EXISTS`.
  Enqueue into such a table would fail with `column "idempotency_key" does not
  exist`.

Migration path for pre-existing tables: add the additive
`AddIdempotencyKey` changeset to the changelog, after the `CreateOutboxTable`
changeset for each message type. It runs
`ALTER TABLE <outbox> ADD COLUMN IF NOT EXISTS idempotency_key TEXT`, which is a
no-op on fresh tables and additive on old ones. This preserves the
immutable-changeset rule: the original `CreateOutboxTable` changeset is not
edited in place; the column is delivered to existing tables by a new versioned
changeset.

## Public API Changes

### `mark_publish_failed` takes a retry `Duration`

`kafkaman_sqlx::mark_publish_failed` now accepts `retry_after: Duration` instead
of an absolute `retry_at: OffsetDateTime`. The next-attempt time is computed from
the database clock (`now() + make_interval(secs => ...)`), matching how claim
eligibility is evaluated, so retry scheduling no longer depends on the worker
host clock. Callers that passed an absolute timestamp must pass a duration.

### Reserved `kafkaman-` header namespace

`enqueue` now rejects any envelope header whose key is in the reserved
`kafkaman-` namespace (case-insensitive) with `Error::ReservedHeader`. This
prevents user headers from shadowing or spoofing the system metadata headers
(`kafkaman-message-id`, `kafkaman-correlation-id`, `kafkaman-causation-id`,
`kafkaman-idempotency-key`). Callers that previously set such headers will now
get an error at enqueue time and must rename them.

### New `Envelope::with_idempotency_key`

Builder added for parity with `with_message_id` / `with_correlation_id`, now that
the field is durable. Purely additive.

## Kafka Header Behavior

When publishing through `RdkafkaPublisher`, a present `idempotency_key` is emitted
as the `kafkaman-idempotency-key` record header alongside the existing
message/correlation/causation headers. Consumers may rely on these reserved
header keys carrying system-owned values only.
