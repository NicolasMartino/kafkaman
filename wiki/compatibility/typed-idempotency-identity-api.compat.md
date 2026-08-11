# Typed Idempotency Identity API and Schema

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-12
- Category: Public API and schema
- Scope: Records the compatibility impact of typed idempotency identity, retained source JSON, transactional invalid-send audit rows, RFC 9457 problem-detail error records, and denormalized failure columns.
- Sources:
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/plans/typed-idempotency-identity-error-row-fix.plan.md
- Related:
  - wiki/compatibility/m3-durable-receive-review-fix-api.compat.md
  - wiki/compatibility/m4-retry-backoff-runtime-api.compat.md

## Compatibility Impact

`Envelope::idempotency_key` now carries typed idempotency identity rather than an
unstructured optional string. The public typed model is:

- `IdempotencyKey`: SHA-256 digest rendered as canonical lowercase hex.
- `IdempotencySource`: caller-provided JSON identity material.
- `IdempotencyIdentity`: digest plus optional retained source material.

`Envelope::with_idempotency_key` accepts values that convert into
`IdempotencyIdentity`; the retained legacy string-source conversion derives a
digest under a compatibility namespace. New code should prefer explicit
`IdempotencyIdentity::derive(namespace, source)` calls so the business identity
namespace is visible.

Outbox and received rows now expose typed `idempotency_key` fields and
`idempotency_source` JSON values. Fresh outbox/received table DDL includes
`idempotency_source JSONB` and validates digest-shaped keys.

Missing send-side idempotency identity is now persisted transactionally as an
outbox `Failed` row with `last_error`, then returned as an error. If the caller
rolls back the transaction, the audit row rolls back too; if the caller commits,
the relay ignores the failed row because it claims only publishable statuses.

Kafka `kafkaman-idempotency-key` carries only the digest hex. External producers
that send missing or malformed digest headers are quarantined as deterministic
ingest failures before offset commit.

## The Invalid-Send Audit Row Is Caller-Controlled

The audit row is written on the caller's connection, inside the caller's
transaction, and kafkaman then returns an error without rolling anything back.
The caller chooses:

- **Commit** — the business row and a `Failed` outbox row carrying `last_error`
  both persist. The relay claims only publishable statuses, so the audit row is
  inert and never publishes.
- **Roll back** — both are discarded. The work is recovered by retry rather than
  by the audit trail: on the consume-then-produce path the receive row is never
  marked `Processed`, so the dispatcher redelivers it.

This is deliberate, and the asymmetry with the receive path follows from
transaction ownership rather than inconsistency. On receive, kafkaman owns the
transaction and can roll back to a savepoint while still persisting its failure
record. On send, the caller owns the transaction and the business write inside
it, so kafkaman must not decide whether that write survives.

Both branches are covered by tests that write business data alongside the audit
row, plus a consume-then-produce test asserting the rejected enqueue is rolled
back, the receive row is left unacknowledged with its failure recorded, and the
message is delivered again.

**Known asymmetry.** A reserved-header rejection returns before the insert, so
unlike a missing idempotency identity it leaves the caller nothing to commit
even if they want the record. `reserved_header_rejection_leaves_no_audit_row_to_commit`
pins this behaviour so it cannot change silently; whether the two invalid-send
paths should agree is unresolved.

## Stored Failure Records Become RFC 9457 Problem Details

`ReceivedError` is now an RFC 9457 problem detail object. `kind` serializes as
the `type` member carrying a stable `urn:kafkaman:problem:*` URI, `message` is
renamed to `detail`, and a `title` member is added. RFC 9457's `status` member is
omitted: it is defined as an HTTP status code and has no meaning for a dispatch
failure. `occurred_at` is an RFC 9457 extension member.

Reads are backward compatible. `type` also accepts the legacy `kind` field and
bare variant names, `detail` accepts the legacy `message` field, and `title`
defaults when absent, so rows written before this change still deserialize. An
unrecognized `type` degrades to the default kind rather than failing the read, so
an older binary can still read an audit trail written by a newer one.

`occurred_at` is written as an RFC 9557 (IXDTF) timestamp annotated `[UTC]`, and
values are normalized to UTC before formatting. Parsing accepts annotated and
unannotated input, so bare RFC 3339 — which RFC 9557 defines as valid IXDTF — and
foreign zone annotations both round-trip.

**This format is not castable by PostgreSQL.** `timestamptz` rejects the
bracketed annotation, which is why no SQL parses this column; see below.

## Failure Time and Kind Move to Columns

Received tables gain two columns:

- `last_failed_at TIMESTAMPTZ`
- `last_failure_kind TEXT`, with a CHECK generated from `ReceivedFailureKind` so
  the database and the Rust enum cannot drift. `NULL` satisfies the CHECK, so
  rows that have never failed are unconstrained.

DLQ inspection and received redrive previously filtered and ordered by casting
`(errors -> -1 ->> 'occurred_at')::timestamptz` and comparing
`errors -> -1 ->> 'kind'`. Both now read the columns. This decouples triage from
the audit JSON's serialization format, makes it indexable, and is what allows the
audit records to adopt an annotated RFC 9557 timestamp at all.

Ordering is now `last_failed_at, created_at, message_id`. `last_failed_at` alone
is not a total order — rows failed by the same dispatch pass share an instant —
and `message_id` is a random UUID, so the previous two-key order resolved ties
arbitrarily. That affected selection, not just presentation: a redrive bounded by
`max_rows` could take an unpredictable subset of a tied group.

`clear_history()` now also nulls both columns, so a history-erased row is no
longer matched by a `since`/`kind` filter.

Both changes alter generated SQL, so `CreateReceivedTable` and received
`Replay` changeset checksums change. `ReceivedFailureKind` discriminant strings
are deliberately unchanged and are covered by a regression test, because they
feed replay checksums.

## Example Application

`apps/axum-outbox` now derives an explicit idempotency identity from the order
id under a versioned namespace. Callers that construct an `Envelope` without an
identity no longer enqueue successfully; this is the intended uniform contract
from review finding F1.

## Verification Status

Verified against Docker-backed PostgreSQL and Redpanda on 2026-08-12:

- `durable_send`: 19 passed
- `durable_receive`: 26 passed
- `redpanda_full_loop` (`--test-threads=1`): 10 passed
- `cargo test --workspace --all-features`: 83 passed across 23 suites
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: clean

The earlier `CreateContainer(RequestTimeoutError)` was a cold Docker daemon, not
a code defect.
