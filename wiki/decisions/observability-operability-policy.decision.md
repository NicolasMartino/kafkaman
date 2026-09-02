# Observability and Operability Policy

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-24
- Category: Observability
- Scope: Accepted M6 policy for tracing, OpenTelemetry metrics, admin routes, payload/header safety, and runtime supervision.
- Sources:
  - wiki/proposals/04-observability-logging-policy.proposal.md
  - wiki/roadmaps/path-to-v1.roadmap.md
  - crates/kafkaman-config/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - crates/kafkaman-worker/src/lib.rs
  - crates/kafkaman-rdkafka/src/lib.rs
  - crates/kafkaman-axum/src/lib.rs
  - tests/durable-send/tests/durable_send/
  - tests/durable-send/tests/durable_receive/
- Related:
  - wiki/specs/m6-observability-operability.spec.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Decision

M6 accepts observability as reusable library surface, not example-only code.

kafkaman keeps `tracing` as its span/event surface and does not install or own a
subscriber. Metrics are emitted through the direct OpenTelemetry Rust API using
the global `kafkaman` meter; host applications own exporters and SDK setup.

Admin and request-path integrations live in a new optional `kafkaman-axum` crate
and are re-exported by the facade behind the `axum` feature. Core, SQLx, worker,
and rdkafka crates do not depend on Axum.

## Policy Choices

Observability config is runtime config with global defaults and per-message-type
overrides. There is no per-scheduler override in M6; scheduler identity is a
metric/log attribute, not a separate config hierarchy.

Metrics labels must remain low-cardinality. M6 uses scheduler, message type,
topic, status, outcome, and reason. Raw ids, entity keys, offsets, correlation
ids, user headers, payload bodies, and raw error strings are not metric labels.

Counter outcomes must partition their subject exactly once. A quantity that
accompanies a classification rather than replacing it — an offset commit next to
an insert/duplicate/skip — gets its own counter, not a fourth outcome value, so
that summing a counter across its outcomes yields a real total.

Metric emission is a default-on feature rather than an unconditional dependency,
matching how the project already gates `rdkafka`: a consumer that does not want
OpenTelemetry in its tree can drop it without losing the worker loops.

Payload and arbitrary user-header exposure is off by default. The config records
payload/header policy names, but the shipped admin routes return sanitized row
summaries rather than full durable rows because those rows contain payloads and
user headers.

Config fields that parse but do not yet drive behavior are labelled **reserved**
wherever an operator or caller can see them — the example TOML, the field
documentation, and the spec — rather than described in the imperative present.
Accepting them early keeps a later redaction hook from being a breaking config
change; describing them as live would be a documentation defect.

"Sanitized" is stated precisely rather than as a blanket claim. Admin DLQ output
excludes payload bodies and the user-header map, but still carries `entity_key`
and the handler's free-text failure detail, which are application-controlled and
may hold sensitive values.

DLQ redrive from admin routes uses the existing guarded received-row replay
semantics. Outbox replay remains rejected as unsafe for entity snapshots.

Shutdown is a two-phase operation, not a single signal. `with_runtime` cancels
the shutdown token and then waits for supervised tasks to finish, bounded by a
timeout. Returning at listener close would cut in-flight publishes, which is the
failure the outbox pattern exists to prevent; waiting without a bound would turn
a graceful shutdown into a hang. Both are rejected in favour of a bounded drain
that reports `DrainTimeout` when a task ignores the signal.

Inspection queries must stay index-servable. `received_stuck_rows` splits its
due-time predicate per status rather than filtering on
`COALESCE(next_attempt_at, created_at)`, matching `claim_received_row` and the
R14 finding recorded on the retention index. Ordering may use the `COALESCE`
projection, because it sorts only rows the indexed predicate already matched.

Timestamps on operational responses use the project's `rfc9557` adapter
explicitly per field rather than relying on `time`'s derived `Serialize`. The
workspace does not enable `time/serde-human-readable`, so the derived impl emits
a nine-integer array; enabling that feature instead would silently change
`ReceivedError`'s established wire format.

Destructive admin input is bounded and strict. Redrive requires `max_rows`, caps
it, and rejects unknown JSON fields, because a silently-ignored misspelling on a
mutating route means the operator believes they asked for something they did not
get. Client-supplied correlation ids are bounded in length and charset before
being echoed or logged.

## Consequences

The project now carries a direct `opentelemetry` API dependency in the worker and
rdkafka crates. This avoids a second facade and matches the explicit M6 choice,
but it makes OpenTelemetry API compatibility part of the public operational
surface.

`ResolvedConfig` gained an `observability` field, and `RelayConfig` gained a
`lifecycle` field. Both are useful because hosts and admin routes consume one
validated config object, but both are semver-sensitive for downstream code
constructing the public structs directly. `run_dispatcher` and the two status
summary functions also gained parameters; see the compatibility note.

Lifecycle sampling is deterministic and proportional rather than random: after
`N` successes exactly `⌊N × sample_success⌋` events have been emitted — the count
never runs ahead of the rate and never falls a whole event behind it. That makes
the rate exact over any window and keeps an RNG dependency out of the worker, at
the cost of being predictable — acceptable because this selects log lines, not
security material.

**Corrected 2026-08-26.** This paragraph read "every `n`-th success", which was
both the wording and the implementation, and it silently rounded the operator's
rate to the nearest reciprocal — `0.75` emitted every success. The property to
state is the proportion, not the stride.

The new Axum surface is opt-in. Applications not enabling the `axum` feature do
not build the admin route or middleware dependency path. Within it the read-only
routes and the destructive redrive route are separate routers, so mounting a
dashboard cannot hand out a re-enqueue endpoint by accident.

**Operability has a migration surface, and it is not all optional.** The DLQ
inspection and redrive routes read `last_failed_at` and `last_failure_kind` and
order by the first. A received table created before those columns existed does
not degrade on these routes — it raises, on the path an operator reaches for
during an incident.

**Amended 2026-08-31.** This paragraph named `AddReceivedFailureMetadata` as a
*required* upgrade changeset and `AddReceivedFailedIndex` as an optional one
beside it. Both were removed at the V1 tag along with the other seven, because
the tables they upgraded were only ever created by a pre-release kafkaman — the
columns and the index are in the received create template. The reasoning above
is why they were required rather than optional, and it is preserved because it
is the rule any future template bump touching these columns has to satisfy:
a changeset that adds a column the DLQ routes *read* must also fill it, or every
pre-existing dead letter becomes visible and unfilterable at once. See
[v1-legacy-removal](../compatibility/v1-legacy-removal.compat.md).

## Revisit Triggers

Revisit if hosts need scheduler-specific verbosity, if a real redaction trait is
added, if OpenTelemetry metrics API compatibility churn becomes excessive, or if
admin routes need authenticated destructive operations beyond bounded received
DLQ redrive.
