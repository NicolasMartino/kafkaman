# M3/M4 Pre-Merge Branch Review

- Document Class: Reference
- Status: Sourced
- Date: 2026-06-23
- Category: Code review
- Scope: Adversarial pre-merge review of branch `implementation/m3-durable-receive` against local `main` at merge base `23212b8c8b0e00d54e989e222353bd270bd6f97c`.
- Sources:
  - `crates/kafkaman-core/src/lib.rs`
  - `crates/kafkaman-sqlx/src/lib.rs`
  - `crates/kafkaman-rdkafka/src/lib.rs`
  - `crates/kafkaman-worker/src/lib.rs`
  - `crates/kafkaman-test/src/lib.rs`
  - `tests/durable-send/tests/durable_receive.rs`
  - `tests/durable-send/tests/redpanda_full_loop.rs`
  - `wiki/specs/m3-durable-receive.spec.md`
  - `wiki/specs/m4-retry-backoff-dlq.spec.md`
  - `wiki/compatibility/m3-durable-receive-review-fix-api.compat.md`
  - `wiki/compatibility/m4-retry-backoff-runtime-api.compat.md`
- Related:
  - `wiki/reviews/m3-durable-completion-implementation-review.reference.md`
  - `wiki/reviews/m3-durable-completion-implementation-rereview.reference.md`
  - `wiki/decisions/message-consumption-and-handler-model.decision.md`
  - `wiki/decisions/kafka-ingest-identity-and-ordering.decision.md`
  - `wiki/decisions/ingest-poison-quarantine-policy.decision.md`
  - `wiki/plans/m4-retry-backoff-dlq.plan.md`

## Resolution 2026-08-12

All five findings are closed and the merge gate is satisfied. The work is
recorded in
[typed-idempotency-identity-error-row-fix](../plans/typed-idempotency-identity-error-row-fix.plan.md)
and its compatibility note.

| Finding | Resolution |
| --- | --- |
| F1 send-side idempotency optional | Closed. Identity is required uniformly; an invalid send is recorded as a transactional outbox error row. `apps/axum-outbox` was updated, having silently stopped working under the new contract. |
| F2 empty keys collapse messages | Closed. Keys are SHA-256 digests validated by a `CHECK (idempotency_key ~ '^[0-9a-f]{64}$')`, so an empty key cannot be stored. |
| F3 DLQ filters by business time | Closed by a `last_failed_at` column rather than the proposed JSON cast — see below. |
| F4 redrive filters by business time | Closed the same way, and its bounded selection made deterministic. |
| F5 received-only knobs alter outbox checksums | Closed. |

Two points where the fix departed from this review's expectations, both
validated by the Docker gates:

1. This review proposed filtering on `(errors -> -1 ->> 'occurred_at')::timestamptz`.
   That is not viable: the stored timestamp is not RFC 3339, and PostgreSQL
   cannot cast the RFC 9557 format the audit records now use. Failure time and
   kind were promoted to `last_failed_at` / `last_failure_kind` columns instead,
   which also makes triage indexable.
2. Ordering by failure time alone is not a total order, and the existing
   `message_id` tiebreak is a random UUID. Bounded redrive could therefore select
   an unpredictable subset of rows failed by the same dispatch pass. Ordering is
   now `last_failed_at, created_at, message_id`.

The `PoolTimedOut` flake noted under Verification Run did not recur across the
full re-run.

## Review Target

Reviewed branch `implementation/m3-durable-receive`, clean working tree, no upstream configured. The branch contains the M3 durable receive implementation plus follow-up M4 retry/backoff/DLQ work:

- receive storage types, `ReceivedTable`, `ReceivedRow`, `ReceivedMeta`, failure kinds, and ingest failure kinds.
- received-table migrations and shared `received_ingest_failures` quarantine table.
- `insert_received_with_outcome`, receive dispatch, retry scheduling, bounded error history, terminal `Failed` state, and DLQ inspect APIs.
- `RdkafkaConsumer` durable ingest loop, deterministic ingest quarantine, offset commit ordering, and consecutive-skip breaker.
- `Replay::received`, DLQ redrive filters, and clean-slate redrive option.
- new Postgres and Redpanda integration coverage.
- associated M3/M4 wiki promotions and compatibility notes.

The review stance was intentionally adversarial: assume local tests can pass while merge-risk behavior remains inconsistent, underspecified, or operator-hostile.

## Summary Verdict

Do not merge as-is. The branch is broadly well-tested and the core receive/dispatch architecture is coherent, but it has two high-severity idempotency-contract gaps that can cause kafkaman-produced records to be quarantined or unrelated records to collapse into one durable receive row.

The M4 DLQ inspect/redrive surface also has a semantic mismatch around "failure time": APIs and comments talk about failure-time triage, but filters/orderings use business `occurred_at` or row `created_at`. That is not a compile-time problem, but it will mislead operators during incident triage and bounded redrive.

## Findings

### F1. Send-side idempotency remains optional while receive-side ingest now requires it

Severity: High.

`RdkafkaConsumer::record_envelope` requires `kafkaman-idempotency-key` and classifies a missing header as deterministic poison:

- `crates/kafkaman-rdkafka/src/lib.rs:529` calls `header_value(..., "kafkaman-idempotency-key")?.ok_or(Error::MissingIdempotencyKey)?`.
- `crates/kafkaman-rdkafka/src/lib.rs:453-456` treats `MissingIdempotencyKey` as deterministic ingest skip.
- deterministic skips are quarantined and the Kafka offset is committed.

But kafkaman's own send side still accepts and publishes messages without that key:

- `crates/kafkaman-sqlx/src/lib.rs:1706-1744` persists `evt.idempotency_key.as_deref()` as nullable outbox data in `enqueue_on_connection`.
- `crates/kafkaman-rdkafka/src/lib.rs:170-175` emits `kafkaman-idempotency-key` only when `row.row.idempotency_key` is `Some`.
- `Envelope::new` still creates an envelope with `idempotency_key: None`; `Envelope::with_idempotency_key` is optional, not required.

Impact: a kafkaman producer can enqueue and publish a record that a kafkaman receiver will quarantine as poison. That is a cross-crate contract break inside the same library. It is especially risky because the branch's M3/M4 specs now describe idempotency as required receive identity, while the public send API still makes the key opt-in.

Expected fix:

- enforce an idempotency key before enqueue, or generate a durable default key at envelope creation/enqueue time.
- make the contract uniform across core, SQLx, publisher, and receiver.
- add a regression test proving a default kafkaman send path can be consumed by `RdkafkaConsumer`, or proving enqueue without idempotency now fails before publish.

### F2. Empty idempotency keys are accepted and collapse unrelated messages

Severity: High.

The receive path checks only presence, not semantic validity:

- `crates/kafkaman-core/src/lib.rs:204-207` stores any string in `Envelope::with_idempotency_key`, including `""`.
- `crates/kafkaman-sqlx/src/lib.rs:1778-1782` rejects `None` but accepts `Some("")`.
- `crates/kafkaman-rdkafka/src/lib.rs:529` rejects a missing header but accepts an empty `kafkaman-idempotency-key` value.
- the received table has a unique index on `idempotency_key`, so every blank-key record for the message type deduplicates against the first blank-key row.

Impact: external producers, bad serializers, or misconfigured callers can send blank idempotency headers. kafkaman will treat the first blank-key message as the canonical durable row and classify later blank-key messages as duplicate redeliveries. That is silent data loss: unrelated business events converge into one handler effect.

Expected fix:

- introduce shared idempotency-key validation in core, not just in rdkafka.
- reject empty and all-whitespace keys before enqueue and ingest.
- add a database `CHECK (length(btrim(idempotency_key)) > 0)` for received tables, and consider the same for outbox if idempotency becomes required there.
- add tests for `with_idempotency_key("")`, direct `insert_received_with_outcome`, and Kafka ingest with an empty idempotency header.

### F3. DLQ "failure time" filtering and ordering use business/row time instead of latest failure time

Severity: Medium.

The DLQ inspect surface says it is failure-time oriented:

- `crates/kafkaman-sqlx/src/lib.rs:2728-2734` documents `received_failed_rows` as listing terminal `Failed` rows "oldest failure first" and says the filter narrows by failure time.
- `ReceivedFailureFilter::since` is exposed as the operator-facing way to narrow terminal rows.

The SQL does not use failure time:

- `crates/kafkaman-sqlx/src/lib.rs:2713-2726` filters with `occurred_at >= ...`.
- `occurred_at` is the business/message occurrence timestamp stored on the receive row, not the timestamp of the latest failure appended to `errors`.
- `crates/kafkaman-sqlx/src/lib.rs:2744` orders DLQ rows by `created_at, message_id`, not latest failure time.

Impact: an old business event that fails today will be excluded by a "recent failures" filter if its business `occurred_at` is old. Conversely, an old failure for a recently-created row may appear before an operationally newer failure. This undermines DLQ triage and any limited redrive workflow that expects oldest failure or recent failure semantics.

Expected fix:

- if the intended semantics are failure-time triage, filter/order by the latest error entry: `errors -> -1 ->> 'occurred_at'`, cast to `timestamptz`.
- if the intended semantics are business-time triage, rename/docs should say `business_occurred_after` or equivalent, and "oldest failure first" must be removed.
- add tests where business `occurred_at`, row `created_at`, and latest error `occurred_at` differ.

### F4. `Replay::received` also uses business time for bounded redrive selection

Severity: Medium.

The same time-semantic issue exists in received redrive:

- `crates/kafkaman-sqlx/src/lib.rs:1111-1140` builds the received redrive query.
- candidates are selected with `replay_received_filter_sql`.
- `crates/kafkaman-sqlx/src/lib.rs:1174` adds `AND occurred_at >= ...`.
- candidates are ordered by `occurred_at, message_id`.

Impact: `Replay::received(...).since(...)` appears operationally like "redrive failures since this time", but actually redrives messages by business occurrence time. An operator trying to redrive the last incident window can select the wrong terminal rows: old events that failed in the incident are missed, and newer business events with older failures may be included.

Expected fix: align `Replay::received(...).since(...)` with the same chosen semantics as DLQ inspect. If it remains business-time filtering, document it explicitly in the API, spec, compatibility note, and tests.

### F5. Received-only replay knobs can change outbox replay checksums while doing nothing

Severity: Low.

`Replay::failure_kind` and `Replay::clear_history` are documented as received-only:

- `crates/kafkaman-sqlx/src/lib.rs:922-933`.

But they are accepted for every `Replay`, and their values are included in checksum material for both targets:

- `crates/kafkaman-sqlx/src/lib.rs:965-967` still builds outbox replay with `replay_outbox_update_sql`.
- `crates/kafkaman-sqlx/src/lib.rs:986-1002` includes `failure_kind` and `clear_history` in the checksum regardless of target.

Impact: a developer can accidentally write `Replay::outbox::<P>(...).clear_history()` or `.failure_kind(...)`. The generated outbox SQL does not change, but the changeset checksum does. That creates migration history drift from a no-op option and makes future checksum mismatch investigations harder.

Expected fix:

- either reject received-only knobs when `target == Outbox`, or exclude them from outbox checksum material.
- add a unit test that no-op received-only knobs cannot silently alter an outbox replay checksum.

## Positive Observations

The branch closes many earlier M3 durable-completion review findings:

- deterministic ingest poison is now durably quarantined before offset commit decisions.
- message-id conflicts are distinguished from idempotency-key redelivery.
- receive dispatch holds row locks through normal failure accounting.
- handler effects are protected by a savepoint and rollback path.
- retry scheduling, terminal `Failed`, bounded error history, DLQ inspect, and redrive behavior are substantially covered by integration tests.
- Redpanda tests cover ingest, duplicate redelivery after crash window, consume-then-produce, poison quarantine, unexpected topic quarantine, run loop cancellation, and breaker behavior.

Those are meaningful improvements and should stay.

## Verification Run

Commands executed during review:

```bash
rtk git status
rtk git branch --show-current
rtk git merge-base main HEAD
rtk git diff --stat main...HEAD
rtk git diff --name-status main...HEAD
rtk cargo test --workspace --all-features --no-run
rtk cargo test --workspace --all-features
rtk cargo clippy --workspace --all-features -- -D warnings
rtk cargo test -p kafkaman-rdkafka
rtk cargo test -p kafkaman-sqlx
rtk cargo test -p durable-send-tests
rtk cargo test -p durable-send-tests replay_received_redrive_filters_by_kind_and_clears_history_on_request -- --nocapture
```

Results:

- `rtk cargo test --workspace --all-features --no-run` passed.
- `rtk cargo test --workspace --all-features` passed: 72 tests across 23 suites.
- `rtk cargo clippy --workspace --all-features -- -D warnings` passed.
- `rtk cargo test -p kafkaman-rdkafka` passed with no tests in two suites.
- `rtk cargo test -p kafkaman-sqlx` passed: 6 tests across two suites.
- `rtk cargo test -p durable-send-tests` failed once with `PoolTimedOut` in `replay_received_redrive_filters_by_kind_and_clears_history_on_request` after 25 tests passed.
- rerunning `replay_received_redrive_filters_by_kind_and_clears_history_on_request` alone passed.

The `PoolTimedOut` failure looks like test-container/pool pressure rather than a deterministic logic failure, but it should be watched because this suite already serializes Docker-backed durable receive tests to avoid local pool/container flakiness.

## Recommended Merge Gate

Before merge:

1. Fix F1 and F2 or explicitly decide that M3/M4 intentionally breaks kafkaman send-to-receive compatibility until callers opt into idempotency keys.
2. Add tests for missing and empty idempotency keys on both send and receive boundaries.
3. Resolve DLQ/redrive time semantics: failure-time or business-time, but not mixed docs and SQL.

After those are addressed, F5 can be fixed as a small API cleanup or tracked as a compatibility note if the team accepts no-op checksum churn.
