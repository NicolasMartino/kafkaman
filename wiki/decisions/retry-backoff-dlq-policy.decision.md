# Retry, Backoff, and DLQ Policy

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-21
- Category: Reliability
- Scope: Defines V1 retry classification, backoff scheduling, DLQ behavior, and where per-message policy is configured.
- Sources:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/configuration-and-environment-model.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/message-identity-and-header-namespace.decision.md
  - User direction in Codex conversation on 2026-06-21
- Related:
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md

## Decision

1. **Retry/backoff/DLQ policy is runtime configuration in `kafkaman.toml`.**
   It is not a changeset and not compiled into message types. CI/CD renders the
   file per environment, so retry behavior can differ by environment without
   changing code or schema.
2. **Policies have common defaults plus per-message-type overrides.**
   A message type inherits from shared defaults and may override max attempts,
   backoff parameters, bounded error history size, and DLQ behavior.
3. **Handler failures classify as success, retryable, or terminal.**
   Success moves the row to `Processed`. Retryable failures increment
   `attempts`, append bounded error history, compute `next_attempt_at`, and move
   the row to `Retryable`. Terminal failures move directly to the DLQ surface.
4. **Exhausted retryable failures become terminal.**
   Once `attempts >= max_attempts`, kafkaman records the final failure reason as
   `attempts_exhausted` and moves the row to DLQ.
5. **V1 DLQ is table-backed.**
   The per-type durable table remains the source of truth. DLQ is represented by
   a terminal status plus durable error history and admin/redrive queries. Kafka
   DLQ topics are deferred.
6. **Backoff tests use the injected `Clock`.**
   M4 implementation must prove timing behavior without `sleep`.

Example TOML shape, names subject to implementation polish:

```toml
[retry.defaults]
max_attempts = 10
initial_backoff = "1s"
max_backoff = "5m"
multiplier = 2.0
errors_limit = 20
dlq = "table"

[retry.messages.order_created]
max_attempts = 5
initial_backoff = "500ms"
max_backoff = "30s"
```

The config file is still one flat, already-rendered environment file. These
tables are not environment profiles; CI/CD supplies the environment-specific
values before boot.

## Options Considered

1. **One global retry policy.**
   Simple, but message types often have different business urgency and poison
   tolerance.
2. **Per-message policy in Rust code or attributes.**
   Discoverable near the type, but bad for per-environment operations: changing
   production retry pressure would require a code change and redeploy.
3. **Per-message policy in `kafkaman.toml`, inheriting common defaults.**
   Accepted. It keeps operational knobs in the environment-rendered config while
   avoiding repetitive per-message configuration.
4. **Fully user-defined retry policy trait for V1.**
   Flexible, but prematurely large public API surface. A trait can be added once
   real users exceed the declarative policy.
5. **Kafka DLQ topic as the V1 DLQ.**
   Familiar from Kafka ecosystems, but it splits truth across Kafka and
   Postgres. V1 keeps durable status and redrive in the DB.

## Consequences / Tradeoffs Accepted

- M2 config loading must validate retry defaults and per-message overrides when
  the retry feature is used.
- M4 implementation should not hard-code retry timing; it should consume the
  resolved policy for the current message type.
- Operators can tune retry behavior per environment by changing rendered
  `kafkaman.toml` and redeploying.
- The first DLQ surface is operationally simple but Postgres-centric. Kafka DLQ
  topics remain possible later as an integration feature, not the source of
  truth.

## Revisit When

- A production deployment needs policy that cannot be represented with
  declarative max attempts, exponential backoff, bounded errors, and terminal
  classification.
- Operators need a Kafka-native DLQ topic for integration with existing
  enterprise tooling.
