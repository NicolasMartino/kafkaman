# M1 Durable Send

- Document Class: Spec
- Status: Active
- Date: 2026-06-20
- Category: Durable send implementation
- Scope: Validated M1 behavior for transactional outbox enqueue, per-type outbox tables, minimal migration, claim-lease relay, publisher boundary, Harness seed, and Axum example.
- Sources:
  - wiki/plans/first-poc-outbox-publisher.plan.md
  - wiki/plans/m1-durable-send-implementation.plan.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
- Related:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md

## Validated Behavior

- The repository now has a Rust workspace with `kafkaman-core`, `kafkaman-sqlx`,
  `kafkaman-worker`, `kafkaman-rdkafka`, `kafkaman-test`, a facade crate, an
  `axum-outbox` example, and a `durable-send-tests` integration-test package.
- `kafkaman-core` defines validated SQL identifiers, message descriptors,
  `KafkaMessage`, `Envelope`, send-side outbox status/row types, `PublishAck`,
  `PublishedRecord`, and relay config/stats.
- `kafkaman-sqlx` implements the M1 subset of `migrate()`: schema creation,
  `changelog_history`, ordered object-safe changesets, `InitSchema`, and
  `CreateOutboxTable`.
- Per-type outbox tables include message identity, status, attempts,
  `next_attempt_at`, `last_error`, claim lease fields (`claim_id`, `claimed_by`,
  `claim_expires_at`), topic/key, envelope metadata, JSON payload, timestamps, and
  a status check.
- `enqueue(&mut Transaction, &ResolvedConfig, &Envelope<P>)` inserts into the
  per-type outbox table inside the caller's transaction where `P: KafkaMessage`.
  The validated atomicity is business write + outbox row commit together.
- `claim_batch` claims due `Pending` rows and expired `Publishing` rows with
  `FOR UPDATE SKIP LOCKED`, sets a fresh `claim_id`, increments attempts, and
  stores the lease owner/expiry.
- `mark_published` and `mark_publish_failed` both require the matching
  `claim_id`; stale or missing claims do not mutate the row.
- `kafkaman-worker::relay_once` commits the claim transaction before publishing,
  publishes through a generic `Publisher`, then either marks published or requeues
  with `last_error` and `next_attempt_at`.
- `kafkaman-rdkafka` maps a claimed row to an `rdkafka` `FutureRecord`, including
  payload, partition key, headers, `message_id`, `correlation_id`, and optional
  `causation_id`. It performs no database writes.
- `kafkaman-test` provides a Docker-free Harness default using a capturing
  publisher, explicit `relay_once`, ephemeral schema migration, row assertions,
  and published-record assertions.
- `examples/axum-outbox` demonstrates host wiring: migrate with local changelog,
  start relay worker, and enqueue inside the same SQL transaction as the business
  insert.

## Verified Gates

- `cargo test --workspace` passes with 10 tests across 16 suites.
- Integration tests use `testcontainers` to start Postgres automatically.
- Migration is idempotent and two `CreateOutboxTable` changesets create two
  structurally separate per-type outbox tables.
- Normal durable send verifies both a captured published record and row status
  `Published`.
- Publish errors requeue the row as `Pending`, record `last_error`, clear claim
  fields, and retain the incremented attempt count.
- A stale `claim_id` cannot mark a row `Published`.
- A stale `claim_id` cannot mark publish failure.
- The ack-before-mark duplicate window is represented by manually publishing a
  claimed row, expiring its lease, then confirming `relay_once` republishes and
  marks the row `Published`.

## Limitations

- This is not exactly-once delivery. The ack-before-mark window intentionally
  republishes after lease expiry.
- Full retry/backoff/DLQ policy is not implemented; M1 only requeues publish
  errors with a fixed retry delay.
- Consume/inbox, handler routing, receive transactions, offset handling,
  `kafkaman-axum`, admin routes, retention/purge, operational changesets,
  checksums/audit maturity, and config loading remain future milestones.
- The automated full-loop gate uses a capturing publisher rather than Redpanda.
  `kafkaman-rdkafka` compiles and maps records, but broker-level publish/consume
  assertions are deferred behind the future full-loop feature.

## Evidence

- Last verified command: `cargo test --workspace`
- Result: `10 passed (16 suites, 7.34s)` in the local run that promoted this spec.
