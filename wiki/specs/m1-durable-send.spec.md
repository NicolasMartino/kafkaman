# M1 Durable Send

- Document Class: Spec
- Status: Active
- Date: 2026-06-21
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
  `CreateOutboxTable`. The additive `AddIdempotencyKey` upgrade changeset this
  line also named was removed at V1 (see
  [v1-legacy-removal](../compatibility/v1-legacy-removal.compat.md)).
  `migrate()` is concurrency-safe: it holds a per-schema Postgres advisory lock
  for the run so simultaneous application/replica boots cannot race the
  changelog primary key.
- All outbox status SQL (defaults, the CHECK constraint, claim/mark predicates)
  is generated from the `OutboxStatus` enum, so the database literals cannot
  drift from the Rust type.
- Per-type outbox tables include message identity, durable `idempotency_key`,
  status, attempts, `next_attempt_at`, `last_error`, claim lease fields
  (`claim_id`, `claimed_by`, `claim_expires_at`), topic/key, envelope metadata,
  JSON payload, timestamps, and a status check.
- `enqueue` rejects envelope headers in the reserved `kafkaman-` namespace so
  user headers cannot shadow or spoof system metadata headers.
- `enqueue(&mut Transaction, &ResolvedConfig, &Envelope<P>)` inserts into the
  per-type outbox table inside the caller's transaction where `P: KafkaMessage`.
  The validated atomicity is business write + outbox row commit together.
- `claim_batch` claims due `Pending` rows and expired `Publishing` rows with
  `FOR UPDATE SKIP LOCKED`, sets a fresh `claim_id`, increments attempts, and
  writes the lease expiry from the database clock (`now() + interval`). Retry
  scheduling in `mark_publish_failed` uses the same database clock, so neither
  lease nor retry eligibility depends on the worker host clock.
- `mark_published` and `mark_publish_failed` both require the matching
  `claim_id`; stale or missing claims do not mutate the row, and the relay
  reports those outcomes as distinct `stale` vs `missing` counters.
- `kafkaman-worker::relay_once` commits the claim transaction before publishing,
  publishes through a generic `Publisher`, then either marks published or requeues
  with `last_error` and `next_attempt_at`. `run()` logs and continues past a
  transient relay error rather than exiting, and rejects an unsafe `RelayConfig`
  (e.g. a zero lease).
- `kafkaman-rdkafka` maps a claimed row to an `rdkafka` `FutureRecord`, including
  payload, partition key, headers, and the `kafkaman-message-id`,
  `kafkaman-correlation-id`, optional `kafkaman-causation-id`, and optional
  `kafkaman-idempotency-key` metadata headers. It performs no database writes.
- `kafkaman-test` provides a Docker-free Harness default using a capturing
  publisher, plus an opt-in `Harness::connect_redpanda` (feature `redpanda`) that
  publishes through `RdkafkaPublisher` to a real broker. It offers explicit
  `relay_once`, ephemeral schema migration, row assertions, and published-record
  assertions, and serializes concurrent dynamic message registration.
- `examples/order` demonstrates host wiring: migrate with local changelog,
  start relay worker, and enqueue inside the same SQL transaction as the business
  insert. (Was `apps/axum-outbox` until 2026-08-24, when the examples were
  reshaped into the two-service distributed cache; the M1 send path it shows is
  unchanged, and it now also runs the receive side.)

## Verified Gates

- `cargo test --workspace --all-features` passes; `just test [unit|integration|coverage]`
  wraps the tiers and `cargo clippy --all-targets --all-features -D warnings`
  and `cargo fmt --check` are clean.
- Integration tests use `testcontainers` to start Postgres (and Redpanda for the
  full-loop gate) automatically.
- Migration is idempotent and two `CreateOutboxTable` changesets create two
  structurally separate per-type outbox tables.
- Normal durable send verifies both a captured published record and row status
  `Published`.
- Publish errors requeue the row as `Pending`, record `last_error`, clear claim
  fields, and retain the incremented attempt count.
- A stale `claim_id` cannot mark a row `Published`, nor mark publish failure.
- The ack-before-mark duplicate window is represented by manually publishing a
  claimed row, expiring its lease, then confirming `relay_once` republishes and
  marks the row `Published`.
- **M7 amendment, 2026-08-31:** the ack-before-mark duplicate window is also
  proven through a real Redpanda broker hop. The
  `ack_before_mark_republish_is_deduplicated_after_real_broker_hop` gate
  publishes a claimed row, expires its lease, republishes through the relay,
  ingests both broker records, and proves receive-side identity absorbs the
  duplicate before dispatch side effects repeat.
- A caller-set `idempotency_key` is persisted on the outbox row and forwarded as
  a Kafka header; the column is in the outbox create template.
- `enqueue` rejects reserved `kafkaman-` headers.
- Concurrent dynamic message registration on one Harness is safe (no caller
  observes a registered type before its table exists).
- The opt-in `redpanda` full-loop gate enqueues, runs the real worker against a
  Redpanda broker, consumes the record back, and asserts payload, partition key,
  and `kafkaman-*` headers.

## Limitations

- This is not exactly-once delivery. The ack-before-mark window intentionally
  republishes after lease expiry.
- Full retry/backoff/DLQ policy is not implemented; M1 only requeues publish
  errors with a fixed retry delay.
- Consume/inbox, handler routing, receive transactions, offset handling,
  `kafkaman-axum`, admin routes, retention/purge, operational changesets,
  checksums/audit maturity, and config loading remain future milestones.
- The default automated suite uses the capturing publisher; broker-level
  assertions run only under the opt-in `redpanda` feature (a Redpanda
  testcontainer), not in the Docker-free default path.
- Schema/API changes from the review-hardening pass (durable `idempotency_key`,
  the `mark_publish_failed` retry-duration signature, reserved headers) are
  recorded in
  `wiki/compatibility/m1-durable-send-schema-and-api-changes.compatibility.md`.

## Evidence

- Verified commands: `cargo fmt --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo test --workspace --all-features`,
  `just test coverage`.
- Test result: full suite green (core/sqlx unit tests, durable-send integration
  suite, the Redpanda full-loop test, and the axum-outbox HTTP test).
- Coverage: ~89% workspace line coverage, gated at 80% via `just test coverage`.
