# Typed Idempotency Identity and Error-Row Symmetry

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-12
- Category: Message identity
- Scope: Defines typed idempotency identity and the transactional error-row rule shared by send and receive paths.
- Sources:
  - wiki/proposals/06-typed-idempotency-identity-and-error-row-symmetry.proposal.md
  - wiki/reviews/m3-m4-pre-merge-branch-review.reference.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
- Related:
  - wiki/plans/typed-idempotency-identity-error-row-fix.plan.md
  - wiki/decisions/kafka-ingest-identity-and-ordering.decision.md

## Decision

Kafkaman idempotency identity is typed. The public shape is:

- `IdempotencyKey`: a SHA-256 digest used as the durable dedupe key.
- `IdempotencySource`: caller-provided JSON identity material retained for
  audit/debug.
- `IdempotencyIdentity`: the source plus its derived digest.

`message_id` remains the physical envelope identity used for tracing and row
identity. `IdempotencyIdentity` is the logical business dedupe identity.

The Kafka header `kafkaman-idempotency-key` carries the canonical digest string.
Durable outbox and received rows store both `idempotency_key` and
`idempotency_source` when the source is available.

Send and receive paths follow a symmetric transactional error-row rule:

1. Record invalid or problem work transactionally.
2. Return an error to the crate user.
3. Let the crate user roll back the surrounding transaction to remove the
   recorded error row, or commit to preserve it for audit.
4. Have workers claim only non-error rows.

### Scope of the error-row rule

The rule applies to a send whose envelope is structurally safe to persist. A
send rejected *because its own content must not enter the ledger* fails before
any database work and produces no row.

Concretely, a missing idempotency identity is recorded as a `Failed` outbox row
carrying `last_error`, because the envelope is otherwise well formed and the row
tells an operator which business event was attempted. A reserved
`kafkaman-*` header is rejected before the insert, because the outbox row would
persist that header into its `headers` JSONB — writing exactly the value the
guard exists to keep out of kafkaman's namespace, where downstream tooling
reading `headers` would then observe a spoofed `kafkaman-message-id`.

Callers therefore cannot obtain an audit record of a reserved-header rejection,
even by committing. This is intended, not an oversight, and is pinned by
`reserved_header_rejection_leaves_no_audit_row_to_commit`.

### Ownership determines who may roll back

The send and receive paths differ in who decides the fate of the error record,
and the difference follows from transaction ownership rather than inconsistency:

- On **receive**, kafkaman owns the transaction. It rolls back to a handler
  savepoint and still persists its own failure record, so a failed handler leaves
  a durable error history even though the handler's writes are discarded.
- On **send**, the caller owns the transaction and the business write inside it.
  Kafkaman must not decide whether that write survives, so it records, returns an
  error, and leaves the choice to the caller.

Rolling back is not lossy. On the consume-then-produce path the receive row is
never marked `Processed`, so the dispatcher redelivers the message and the work
is recovered by retry rather than by the discarded audit row.

## Rationale

SHA-256 gives kafkaman a fixed canonical identity value that cannot be blank,
whitespace-only, or accidentally malformed like an unstructured string. Retained
source JSON makes the irreversible digest explainable during incident review.

The source material must remain caller-owned because only the application knows
which business fields define duplicate intent and which fields are safe to
persist. Kafkaman must not silently hash or store full payloads.

The transactional error-row rule keeps the outbox and receive ledgers useful for
audit without taking control away from the host application transaction.

Bounding the rule to persistable sends keeps it from working against the guards
it sits beside. Kafkaman already fails before touching the database for pure
input validation — invalid config and invalid retry settings both do, with tests
asserting it — and a reserved header belongs to that family. The distinction is
not "which failure is more severe" but "would recording it write something the
ledger must not contain".

## Consequences

- Public API and schema compatibility notes are required when implemented.
- Existing rows with legacy string keys need a compatibility path or migration
  posture before V1.
- External producers that only send the digest header can still participate in
  receive dedupe, but their source JSON may be absent.
- Operational tooling should treat `idempotency_source` as user-controlled data
  and avoid assuming it is redacted.
- Reserved-header rejections leave no trace in the outbox. An operator
  investigating a send that never arrived must look at application logs, not the
  ledger, for that specific failure mode.
- A caller that commits after an invalid send holds business state with no
  corresponding event. That is a deliberate election of forensics over
  atomicity, and reconciling it is the caller's responsibility.

## Revisit When

- A production integration needs a privacy policy that forbids retaining
  idempotency source JSON.
- Kafkaman adds a broker/header mechanism for carrying source material from
  external producers.
- Operators need a durable record of reserved-header rejections, which would
  require persisting the row with the offending header stripped and the removal
  noted, rather than persisting the envelope as given.
- Identity becomes required at `Envelope` construction or enforced by typestate,
  which would remove the missing-identity error row entirely by making the state
  unrepresentable.
- Digest collisions become a practical concern, which is not expected for
  SHA-256 in this use case.
