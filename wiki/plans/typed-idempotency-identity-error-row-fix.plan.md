# Typed Idempotency Identity Error-Row Fix

- Document Class: Plan
- Status: Completed
- Date: 2026-08-12
- Category: Message identity implementation
- Scope: Executes the typed idempotency identity decision and closes the M3/M4 pre-merge review findings before merge.
- Sources:
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/reviews/m3-m4-pre-merge-branch-review.reference.md
- Related:
  - wiki/specs/m3-durable-receive.spec.md
  - wiki/specs/m4-retry-backoff-dlq.spec.md
  - wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
  - wiki/compatibility/m4-retry-backoff-runtime-api.compat.md

## Deliverable

The branch becomes mergeable by fixing identity-contract gaps, preserving
auditable invalid sends transactionally, aligning DLQ/redrive time semantics
with latest failure time, and removing outbox replay checksum drift from
received-only knobs.

## Implementation Steps

1. Add typed idempotency API in `kafkaman-core`.
   - Add `IdempotencyKey`, `IdempotencySource`, and `IdempotencyIdentity`.
   - Derive digests from namespace plus canonical JSON source material.
   - Parse/render canonical digest strings for Kafka headers.
2. Update storage and publish/ingest boundaries.
   - Persist canonical digest values as `idempotency_key`.
   - Add `idempotency_source` JSONB to outbox and received rows.
   - Publish digest headers only.
   - Parse receive digest headers and quarantine missing/invalid values.
3. Apply symmetric error-row behavior.
   - Invalid send inserts an outbox `Failed` row with `last_error`, then returns
     an error in the caller's transaction.
   - Workers continue to claim only non-error rows.
4. Fix M4 review findings.
   - Make DLQ inspect and received redrive `since` use latest failure time.
   - Order DLQ/redrive candidates by latest failure time.
   - Prevent received-only replay knobs from changing outbox replay checksums.
5. Update tests first around the review findings, then implementation.
6. After tests pass, update specs, compatibility notes, index, and log with
   validated behavior.

## Verification Gates

- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_send`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --test durable_receive`
- `rtk cargo test --manifest-path tests/durable-send/Cargo.toml --features redpanda --test redpanda_full_loop -- --test-threads=1`
- `rtk cargo test --workspace --all-features`
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings`

## Progress

- 2026-08-12: Implemented the typed idempotency identity API, digest/header
  parsing, `idempotency_source` storage, transactional invalid-send outbox audit
  rows, latest-failure-time DLQ/redrive filtering, and outbox replay checksum
  cleanup. Added regression tests and a draft compatibility note. Verification
  passed for `rtk cargo check --workspace --all-features`,
  `rtk cargo test -p kafkaman-core -p kafkaman-sqlx -p kafkaman-rdkafka -p
  kafkaman-worker -p kafkaman-test`, `rtk cargo test --workspace
  --all-features --no-run`, `rtk cargo test --manifest-path
  tests/durable-send/Cargo.toml --tests --no-run`, and `rtk cargo clippy
  --workspace --all-targets --all-features -- -D warnings`. Docker-backed
  execution of the new send regression tests failed before assertions with
  `CreateContainer(RequestTimeoutError)`.
- 2026-08-12: Ran the Docker-backed gates. The earlier container timeout was a
  cold daemon, not a defect. First execution surfaced seven `durable_receive`
  failures in three classes:
  1. **Latent serialization defect exposed by the F3/F4 fix.**
     `ReceivedError::occurred_at` was a bare `OffsetDateTime`, so `time`'s
     default serde wrote a component array (`[2026, 224, 9, ...]`) that
     `(errors -> -1 ->> 'occurred_at')::timestamptz` cannot parse. Nothing had
     ever cast that field before, so the wrong format was invisible.
  2. **Stale test expectations.** Two tests queried `idempotency_key` with
     pre-digest plaintext, including a `LIKE 'idem-replay-failed-%'` that
     silently matched zero rows and left the seeded state empty.
  3. **Diagnosability regression.** The harness reported a missing row by its
     64-character digest instead of the caller-supplied key.
  Resolved by promoting failure time and kind to `last_failed_at` /
  `last_failure_kind` columns so no SQL parses the audit JSON, reshaping
  `ReceivedError` as an RFC 9457 problem detail with RFC 9557 timestamps,
  giving DLQ/redrive a total order, deriving digests in the affected tests,
  and reporting the identity source alongside the digest in harness errors.
  Two further defects were found while fixing these: DLQ and bounded-redrive
  ordering resolved ties by random `message_id`, and `apps/axum-outbox`
  enqueued without an idempotency identity and so had stopped working under
  the F1 contract. All verification gates now pass.

## Closure Criteria

The plan closes when the review findings have regression tests, all verification
gates pass, specs/compatibility notes reflect validated behavior, and the
pre-merge review can be marked resolved or superseded by a clean follow-up.
