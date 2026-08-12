# Observability and Logging Policy

Document Class: Proposal
Status: Proposed
Date: 2026-06-21
Category: Observability
Scope: Proposes configurable tracing, logging, metrics, and payload-safety policy for kafkaman, including per-message-type overrides.
Sources:
- User design discussion, 2026-06-21
- wiki/proposals/01-kafkaman-objectives.proposal.md
- wiki/decisions/configuration-and-environment-model.decision.md
- wiki/decisions/message-consumption-and-handler-model.decision.md
- wiki/decisions/retry-backoff-dlq-policy.decision.md
- wiki/roadmaps/path-to-v1.roadmap.md
Related:
- wiki/proposals/03-direct-transport-mode.proposal.md
- wiki/decisions/message-identity-and-header-namespace.decision.md

## Context

V1 already has an observability milestone: tracing spans, metrics, lag and stuck-job detection, DLQ inspection, admin/health routes, and shutdown helpers. The current receive decision also stores bounded recent failure history in the durable `errors` field, while the retry policy already uses runtime config with defaults plus message-type overrides.

The logging story is not yet a first-class policy. Library consumers should be able to decide how much detail kafkaman emits globally and, where useful, per message type.

## Proposal

Use the Rust `tracing` ecosystem as kafkaman's logging surface.

kafkaman should emit structured spans and events; it should not own the application's subscriber, formatter, or sink. Applications remain responsible for choosing stdout, JSON logs, OpenTelemetry, tracing filters, and retention.

## Default Behavior

Defaults should be quiet enough for production:

- `error` for failed terminal outcomes, DLQ writes, scheduler crashes, migration failures, and publisher/consumer backend errors;
- `warn` for retryable failures, stuck jobs, high lag/age, unsafe direct-mode configuration, and dropped/invalid messages;
- `info` for runtime start/stop, scheduler start/stop, migration completion, and optional summary-level throughput;
- `debug` for per-message lifecycle transitions;
- `trace` for verbose claim, ack, decode, middleware, and payload-adjacent detail.

Per-message success logs should not be emitted at `info` by default. Metrics and spans should carry routine throughput instead.

## Structured Fields

Events and spans should include stable fields where available:

- `message_type`
- `topic`
- `partition`
- `offset`
- `partition_key`
- `message_id`
- `idempotency_key`
- `correlation_id`
- `causation_id`
- `attempt`
- `status`
- `scheduler`
- `mode` (`durable` or direct-mode variant)
- `latency_ms`
- `error_class`
- `error_retryable`

Payload bodies and arbitrary user headers should be excluded by default.

## Configuration Shape

Follow the existing runtime-config pattern: common defaults with per-message-type overrides.

Example shape:

```toml
[observability.defaults]
level = "warn"
lifecycle = "summary"
payload = "off"
headers = "kafkaman-only"
sample_success = 0.0

[observability.messages.order_created]
level = "debug"
lifecycle = "per-message"
payload = "redacted"
sample_success = 0.05
```

Exact names are not final. The important contract is that logging verbosity is runtime config, not schema history, and message-type overrides are allowed for noisy or high-value streams.

## Payload and Header Safety

Payload logging should be opt-in and should support at least:

- `off`: never log payload bodies;
- `redacted`: call a user-provided redaction hook or derive-provided safe view;
- `sampled`: log only a configured sample after redaction;
- `full`: allowed only behind an explicit unsafe or development-oriented setting.

User headers should also be filtered. Reserved `kafkaman-*` headers are safe for routine diagnostics; arbitrary user headers may contain secrets and should be off or allowlisted by default.

## Metrics

Logging config should not be the only observability control. kafkaman should emit metrics for queue depth, age, lag, retry counts, DLQ counts, success/failure counts, publish latency, dispatch latency, and stuck-job detection.

Metrics should remain low-cardinality. Message type is acceptable; raw IDs, keys, offsets, correlation IDs, and error strings should not be metric labels.

## Consequences

This keeps kafkaman operable without forcing a logging stack on host applications. It also lets high-throughput users reduce log volume without turning off metrics or durable error state.

The tradeoff is a larger configuration surface. The config validator must fail fast on invalid levels, unsupported payload policies, or message-type overrides that do not map to registered message descriptors.

## Open Questions

1. Should observability config be global to the runtime, per scheduler, per message type, or all three?
2. Should payload redaction be trait-based, config-based, or both?
3. Which tracing spans are part of public compatibility, and which are best-effort diagnostics?
4. Should direct mode force a warning or metric so operators can see that durable guarantees are bypassed?

## Promotion Target

If accepted, promote this into an observability decision before or during M6, and update the V1 roadmap if observability policy needs earlier hooks in M2 or M3.
