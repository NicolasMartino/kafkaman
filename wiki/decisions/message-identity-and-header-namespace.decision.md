# Message Identity and Header Namespace

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-21
- Category: Messaging envelope
- Scope: Defines the required V1 message identity fields, durable deduplication key, and reserved Kafka header namespace.
- Sources:
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/reviews/m1-durable-send-implementation-rereview.reference.md
  - User direction in Codex conversation on 2026-06-21
- Related:
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/specs/m1-durable-send.spec.md
  - wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md
    (amends clause 5: adds W3C trace headers as a third namespace)

## Decision

1. **Every persisted V1 kafkaman message requires an `idempotency_key`.**
   The key is producer supplied, domain meaningful, and non-empty after
   validation. It is the durable semantic identity kafkaman uses for
   effective-once processing.
2. **`message_id` remains required, but it is not the dedup fallback.**
   `message_id` identifies the envelope instance for tracing, row identity,
   correlation, broker headers, and diagnostics. It does not replace
   `idempotency_key` when deduplicating handler effects.
3. **Received tables enforce idempotency with a per-type unique key.**
   Each per-type received table has a required `idempotency_key` column and a
   `UNIQUE(idempotency_key)` constraint. Duplicate inserts are logged as no-op
   redeliveries and are not dispatched to the handler.
4. **Outbox tables persist the same key.**
   Send-side rows carry `idempotency_key` so a produced message and any later
   received copy share the same semantic identity.
5. **The `kafkaman-*` Kafka header namespace is reserved.**
   User envelope headers are rejected before persistence if their ASCII
   case-insensitive key starts with `kafkaman-`. Kafkaman alone writes headers
   such as `kafkaman-message-id`, `kafkaman-correlation-id`,
   `kafkaman-causation-id`, and `kafkaman-idempotency-key`.

## Options Considered

1. **Dedup by `(topic, partition, offset)`.**
   This only deduplicates Kafka redelivery of the same broker record. It does
   not collapse the same logical event emitted at another offset.
2. **Dedup by `message_id`.**
   This deduplicates one envelope instance across redelivery and rebalance, but
   it does not protect handler effects when the producer emits the same logical
   event with a new envelope id.
3. **Use `idempotency_key` when present, else `message_id`.**
   This is flexible and was the previous recommended default, but it creates
   two semantic classes of messages and weakens the V1 guarantee.
4. **Require `idempotency_key` for all persisted messages.**
   This gives one explicit V1 contract: every durable message has semantic
   identity. It adds API friction, but the friction is valuable because the
   library exists to make reliable processing explicit.

Selected option: **4**.

Header namespace options:

1. **Allow all user headers and append kafkaman headers.**
   Rejected because Kafka permits duplicate header keys and consumers could see
   conflicting system metadata.
2. **Overwrite user `kafkaman-*` headers during publish.**
   Rejected because silently changing user input hides configuration mistakes.
3. **Reject reserved `kafkaman-*` headers before persistence.**
   Accepted because it is fail-fast, observable, and keeps persisted envelopes
   unambiguous.
4. **Rename conflicting user headers automatically.**
   Rejected because magic renaming creates surprising downstream behavior.

## Consequences / Tradeoffs Accepted

- V1 API ergonomics become stricter: callers must supply an idempotency key for
  every persisted message.
- M1 compatibility work may add a nullable `idempotency_key` column first to
  upgrade old-shape tables, but the V1 contract is non-null. Enforcing `NOT
  NULL` can be a later changeset once the public API rejects absent keys and
  old rows have been handled.
- The receive-side effective-once guarantee has one primary identity key,
  avoiding the ambiguity of fallback dedup behavior.
- User-provided tracing or business headers cannot use the `kafkaman-*`
  namespace. Applications must choose their own prefix.

## Revisit When

- A real integration has no domain-meaningful idempotency key and the generated
  fallback would be less error-prone than forcing the host to invent one.
- Kafkaman adds another transport whose metadata namespace is incompatible with
  the `kafkaman-*` header convention.
