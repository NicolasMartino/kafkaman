# Typed Idempotency Identity and Error-Row Symmetry

- Document Class: Proposal
- Status: Accepted
- Date: 2026-08-12
- Category: Message identity
- Scope: Proposes typed idempotency identity, retained source material, and symmetric transactional error-row behavior for send and receive paths.
- Sources:
  - wiki/reviews/m3-m4-pre-merge-branch-review.reference.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - wiki/decisions/kafka-ingest-identity-and-ordering.decision.md
- Related:
  - wiki/decisions/typed-idempotency-identity-and-error-row-symmetry.decision.md
  - wiki/plans/typed-idempotency-identity-error-row-fix.plan.md

## Context

The M3/M4 pre-merge review found that kafkaman's send and receive identity
contract was inconsistent: receive requires `kafkaman-idempotency-key`, while
send still allows persisted rows without one, and blank keys can collapse
unrelated receive rows.

The current raw-string key shape also loses useful audit context. A digest can
deduplicate reliably, but it cannot be reversed when operators need to
understand why two messages shared identity.

## Options

1. **Raw non-empty string.**
   Simple and flexible, but it leaves blank/normalization mistakes at every
   boundary and does not communicate whether the value is a digest, UUID, or
   ad-hoc label.
2. **UUID v7 only.**
   Typed and time-sortable, but it models a generated command/envelope id better
   than a deterministic business dedupe identity.
3. **SHA-256 digest only.**
   Fixed-size, canonical, and deterministic, but opaque during incident review.
4. **SHA-256 digest plus retained source JSON.**
   Uses a fixed typed digest for dedupe and stores caller-provided source JSON
   for audit/debug.

Selected option: **4**.

## Proposal

Introduce a public idempotency identity composed of:

- a typed SHA-256 digest used for dedupe, storage indexes, and Kafka headers.
- caller-provided JSON source material retained in durable rows for audit.

Kafkaman derives the digest from a namespace plus canonical JSON source bytes.
The crate user chooses the source material and is responsible for ensuring it
contains stable identity fields that are safe to persist. Kafkaman must never
automatically hash or retain the full message payload as the source.

Send and receive should also share one operational rule: when a message cannot
be processed because of validation or identity problems, kafkaman records the
problem transactionally, returns an error, lets the caller roll back to remove
the record, and downstream workers claim only non-error rows.

## Accepted Consequences

- Idempotency becomes a public API concept rather than an unstructured string.
- Existing string-key tests and examples must move to typed identities.
- Digest source JSON may contain sensitive data if the crate user supplies it;
  this is an application responsibility and must be documented clearly.
- External Kafka producers can provide only the digest header unless a future
  explicit source carrier is added.
