# M6 Observability and Operability

- Document Class: Spec
- Status: Active
- Date: 2026-08-24
- Category: Observability and operability
- Scope: Validated M6 behavior for observability config, OpenTelemetry metrics, queue inspection, admin routes, correlation middleware, and runtime supervision.
- Sources:
  - wiki/decisions/observability-operability-policy.decision.md
  - wiki/proposals/04-observability-logging-policy.proposal.md
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/compatibility/m6-observability-operability-api.compat.md
  - crates/kafkaman-config/src/lib.rs
  - crates/kafkaman-sqlx/src/lib.rs
  - crates/kafkaman-worker/src/lib.rs
  - crates/kafkaman-rdkafka/src/lib.rs
  - crates/kafkaman-axum/src/lib.rs
  - tests/durable-send/tests/durable_send/
  - tests/durable-send/tests/durable_receive/
  - tests/durable-send/tests/redpanda_full_loop/
- Related:
  - wiki/specs/entity-first-propagation.spec.md
  - wiki/specs/m4-retry-backoff-dlq.spec.md

## Validated Behavior

M6 makes the post-M5 table shape operable.

### Runtime config

`kafkaman.toml` supports an optional `[observability.defaults]` section plus
`[observability.messages.<message_type>]` overrides. The resolved policy is
available as `ResolvedConfig.observability`.

Validated policy fields, and whether they currently drive behavior:

| Field | Accepted values | Status |
| --- | --- | --- |
| `level` | `error`, `warn`, `info`, `debug`, `trace` | Reserved |
| `lifecycle` | `summary`, `per-message` | Live |
| `payload` | `off`, `redacted`, `sampled`, `full` | Reserved |
| `headers` | `off`, `kafkaman-only`, `all` | Reserved |
| `sample_success` | finite `0.0..=1.0` | Live |
| `stuck_after` | duration greater than zero | Live |
| `max_queue_age` | duration greater than zero | Live |

Reserved fields parse, validate, and are part of the config contract, but no
code path reads them. They are accepted now so that adding a redaction hook
later is not a breaking config change. `kafkaman.example.toml` and the
`ObservabilityPolicy` field docs both mark them, so the distinction is visible
where an operator or a caller would look.

Absent config uses quiet defaults: warn-level policy, summary lifecycle,
payload off, kafkaman headers only, zero success sampling, `stuck_after = 60s`,
and `max_queue_age = 5m`. Overrides are validated against registered message
types. Per-message overrides are validated on the fields they set, not on the
merged result, so one bad default reports once rather than once per message type
that inherits it.

`lifecycle` and `sample_success` reach the relay and dispatcher loops as
`LifecycleEmission` (`kafkaman-core`), which the loops turn into a
`LifecycleSampler`. Sampling is deterministic rather than a per-message Bernoulli
trial: after `N` successes the sampler has emitted exactly `⌊N ×
sample_success⌋` events — never ahead of the rate, never a whole event behind it
— so the rate is exact over any window and a low rate cannot go a long stretch
emitting nothing, which is precisely when an operator turned sampling on to see
something. The count carries across cycles, so the rate holds on a scheduler
handling one message per poll.

**Corrected 2026-08-26.** This paragraph specified "every `n`-th success, where
`n = round(1 / sample_success)`", and that is what shipped. It silently rounds
the operator's rate to the nearest reciprocal: `0.75` becomes every 1st (100% of
successes, at four thirds the intended volume and cost) and `0.66` becomes every
2nd. Nothing in the config file, the logs, or the metrics would show it. The
proportional form above is what the sampler does now.

`stuck_after` is the overdue threshold for the stuck-row routes. `max_queue_age`
sets `over_max_queue_age` on each depth summary bucket; terminal statuses never
set it, because a `Published` row awaiting retention is history rather than
backlog.

### Metrics and tracing

Worker loops emit OpenTelemetry counters through the global `kafkaman` meter:

- `kafkaman.scheduler.cycles`
- `kafkaman.scheduler.rows`
- `kafkaman.scheduler.errors`

Kafka transport emits:

- `kafkaman.kafka.publish.records`
- `kafkaman.kafka.ingest.records`
- `kafkaman.kafka.ingest.commits`
- `kafkaman.kafka.ingest.errors`

`ingest.records` counts each consumed record under exactly one `outcome` —
`inserted`, `duplicate`, or `skipped` — so its total is the number of records
ingested and `sum by (outcome)` partitions that total without overlap. Offset
commits are a separate counter rather than a fourth outcome: a commit accompanies
a classification rather than replacing one, so counting it alongside them would
double every record. A debug assertion in `record_ingest_stats` enforces the
partition.

`ingest.errors` carries a `reason` label separating the fatal
`consecutive_skip_limit` breaker trip from an ordinary `transient` failure.

Labels are low-cardinality: scheduler, message type, topic, status, outcome, and
reason. kafkaman still emits `tracing` events/spans but does not install a
subscriber or exporter.

**Superseded in part, 2026-08-25.** What M6 shipped was OpenTelemetry
*instrumentation*, not an OpenTelemetry *pipeline*: no `opentelemetry_sdk`
existed anywhere in the workspace, so every counter above resolved to the no-op
provider in every build and every test, and nothing had ever observed one. That
is closed. The current surface — counters, latency histograms, observable
queue-depth gauges, four spans, and trace-correlated log records — is recorded in
`wiki/compatibility/m6-observability-operability-api.compat.md` and pinned by
`tests/observability/`, including an OTLP export asserted on the wire. The
sections above describe the M6 subset and remain accurate about it; they are no
longer the whole picture.

Two M6 statements are now wrong rather than merely partial. Instruments are no
longer cached process-wide — they are built by the loop or component that owns
them, because an instrument binds permanently to whichever provider is installed
when it is created. And the debug assertion behind the ingest partition is no
longer the only thing enforcing it: `tests/observability/ingest_disjointness`
asserts it against real ingest cycles through a real broker, in a build where
`debug_assert` is compiled out.

Metric attributes are built once per run loop rather than per record, so an idle
scheduler does not allocate its message type on every poll.

Both metric surfaces sit behind a default-on `metrics` feature on
`kafkaman-worker` and `kafkaman-rdkafka`, forwarded by the facade. Building with
`--no-default-features` drops the `opentelemetry` dependency entirely while
keeping the loops, matching how the crate already gates `rdkafka`.

**Amended 2026-08-26.** That last sentence was a claim no build could check, and
for a while it was false — sibling crates pulled `kafkaman-core` with default
features and Cargo unifies across the graph. `just opt-out` now asserts it with
`cargo tree`, together with the narrower property that followed: each crate
enables only the OpenTelemetry axis it uses, and a `metrics` build links the
metrics API alone. Features are additive, so an axis a library enables without
using is permanent in every adopter's graph. See the compatibility note.

### SQL inspection

`kafkaman-sqlx` exposes read-only queue inspection APIs:

- `outbox_status_summary`
- `received_status_summary`
- `outbox_stuck_rows`
- `received_stuck_rows`

Status summaries return per-table counts plus oldest row timestamp, age in
milliseconds, and an `over_max_queue_age` flag. They are unbounded aggregates
with no time window; the routes that serve them are documented as operator-rate,
not probe-rate. Stuck-row APIs report expired `Publishing` outbox claims and due
received rows that have stayed overdue past the caller-provided threshold.

`received_stuck_rows` splits its predicate per status —
`(status = 'Pending' AND (next_attempt_at IS NULL OR next_attempt_at <= $1) AND created_at <= $1)`
`OR (status = 'Retryable' AND next_attempt_at <= $1)` — rather than filtering on
`COALESCE(next_attempt_at, created_at)`. Both select the same rows, but only the
split form is servable by the received table's
`(status, next_attempt_at, created_at)` index; the `COALESCE` shape is the one
R14 measured as unservable, and `claim_received_row` splits it for the same
reason. `due_at` remains a projected `COALESCE` driving `ORDER BY`, which sorts
only rows the indexed predicate already matched.

All timestamps in these structures serialize through `kafkaman_core::rfc9557`.
The workspace does not enable `time/serde-human-readable`, so a bare
`OffsetDateTime` would serialize as a nine-integer array rather than a timestamp
string.

`redrive_received` executes the existing guarded received-row replay update at
runtime. `Replay::received_descriptor` supports descriptor-driven redrive for
admin routes. Redrive still targets terminal received rows only and preserves
failure history unless `clear_history` is explicitly selected.

### Axum integration

The new `kafkaman-axum` crate provides:

- `CorrelationLayer`, which reads or creates `x-correlation-id`, stores it in
  request extensions, adds it to the request span, and returns it in the response
  header.
- `admin_router(AdminState)`, read-only: health/readiness, outbox summary,
  received summary, stuck rows on both sides, the DLQ summary, and — after the
  M7 hardening amendment — ingest quarantine summaries at `GET
  /ingest-failures`.
- `redrive_router(AdminState)`, carrying the one destructive route — bounded
  received-DLQ redrive. Separate from `admin_router` deliberately: a deployment
  that mounts the read-only router for dashboards must not acquire a
  re-enqueue endpoint as a side effect. Mounting both is `admin_router(state
  .clone()).merge(redrive_router(state))`.
- `serve(listener, app).with_runtime(tasks, shutdown)`, which supervises an Axum
  server plus required runtime tasks, fails if a required task exits before
  shutdown, and — after the server stops — cancels the shutdown token and
  **waits for those tasks to finish** before returning, bounded by
  `DEFAULT_DRAIN_TIMEOUT` (30s) or by
  `with_runtime_drain_timeout`. Returning at listener close would cut in-flight
  publishes, which is the failure an outbox exists to prevent. A task that
  ignores the signal trips `RuntimeError::DrainTimeout` rather than hanging the
  process, and stragglers are aborted rather than leaked.

`CorrelationLayer` adopts an inbound `x-correlation-id` only when it is
non-empty, at most 128 bytes, and printable non-space ASCII; anything else is
replaced with a generated UUID. The value is echoed into a response header and
into every log line for the request, so its length is otherwise
attacker-controlled log volume.

`RedriveRequest` uses `deny_unknown_fields` and caps `max_rows` at
`MAX_REDRIVE_ROWS` (10,000). On a destructive route a misspelled `clearHistory`
must fail rather than silently do something other than what the caller asked;
`clear_history` still defaults to false.

### What "sanitized" means

Admin DLQ output excludes the `payload` body and the `headers` map. Mounting the
route therefore cannot turn into a payload-exfiltration endpoint.

It does still carry `entity_key` — a caller-chosen business key — and
`latest_error.detail`, a free-text string produced by the host's handler.
Neither is payload, but both carry whatever the application put in them, up to
and including personal data. The response is sanitized of message bodies, not of
everything, and `DlqRowSummary`'s documentation says so at the type.

### Test infrastructure hardening

The Redpanda full-loop test helper now chooses a free host port for each test
container and advertises that same port to host-side clients. This lets the
all-features suite run while another local Redpanda, such as the example stack,
is already bound to `19092`.

Because Redpanda must advertise an address the host can dial, the container and
host ports have to be the same number, which means picking one before the
container exists. That leaves a window in which another process can take it, so
`start_redpanda` retries on a fresh port up to five times.

### Example application

**Corrected 2026-08-26.** This section described `apps/axum-outbox` mounting
`admin_router` under `/internal/kafkaman`, applying `CorrelationLayer`, and
supervising the relay through `serve().with_runtime()`. None of that is on this
branch: `apps/` is byte-identical to `main`, and the example there states in its
own README that it demonstrates the M1 durable-send path *without*
`kafkaman-axum`. The section described work that was reverted, and left the spec
claiming an end-to-end exercise that did not exist.

M6 ships no example-application change. The end-to-end exercise of every M6
surface lives in `tests/observability` instead — `admin_http` drives the routes
over real HTTP against a real database, and `otlp_wire` proves the telemetry
leaves the process. That is the better home for it anyway: it makes the proof
independent of whichever example currently ships, which is the reasoning
`wiki/decisions/telemetry-pipeline-ownership.decision.md` already gives for
keeping exporter dependencies in `tests/`.

## Evidence

Coverage added in this M6 pass includes:

- config merge/validation tests for observability defaults and per-message
  overrides;
- `kafkaman.example.toml` executable documentation coverage for the new section;
- `LifecycleSampler` rate tests: silent by default, `per-message` without a rate
  stays silent, `summary` ignores a rate, and a 5% rate emits exactly 5 events
  per 100 successes whether they arrive in one batch or a hundred;
- status terminality tests keeping the queue-age flag aligned with the retention
  predicate;
- `rfc9557::option` JSON round-trip coverage;
- Axum coverage: correlation adoption, generation, blank/oversized rejection and
  the exact-bound case; RFC 3339 serialization asserted for every timestamp on
  every admin response struct; `RedriveRequest` unknown-field rejection and
  `clear_history` default; `AdminError` status mapping and confirmation that a
  storage error's text never reaches the response body;
- Axum runtime supervision: the drain actually waits for a task to finish, an
  early task exit fails by name, a task error surfaces its message, a wedged task
  trips the bounded drain instead of hanging, and the no-task path still cancels
  the token;
- Postgres-backed outbox inspection: bucket separation across terminal and
  non-terminal statuses, a bounded age assertion rather than `is_some()`, the
  queue-age flag on both sides, and confirmation that a claim inside its lease is
  not reported stuck;
- Postgres-backed received inspection: the `Retryable` branch with a real
  `next_attempt_at`, a row inside its backoff window not reported stuck,
  descriptor-driven redrive through `Replay::received_descriptor`, failure
  history preserved by default and cleared under `clear_history`, and an
  unbounded redrive rejected;
- full all-features suite coverage with dynamic, retried Redpanda ports.

Recorded verification on 2026-08-24:

- `rtk cargo check --workspace --all-features --all-targets`
- `rtk cargo test --workspace --all-features` - 173 passed, 4 ignored.
- `rtk cargo clippy --workspace --all-targets --all-features -- -D warnings` -
  no issues found.
- `rtk cargo fmt --all -- --check`
- `rtk cargo check -p kafkaman-worker --no-default-features` and
  `rtk cargo check -p kafkaman-rdkafka --no-default-features` - the `metrics`
  opt-out builds.

## Limitations

M6 does not add authentication/authorization for admin routes. Hosts must mount
them behind their own access control. Both routers carry the warning at the call
site, and splitting the destructive route into `redrive_router` means an
unauthenticated dashboard mount is at worst a disclosure rather than a
re-enqueue endpoint — but the crate cannot enforce either.

Three policy fields are reserved rather than implemented: `level`, `payload`, and
`headers`. They parse and validate but nothing reads them. Filtering today is the
host subscriber's job — an `EnvFilter` directive on the `kafkaman` targets — and
no kafkaman code path emits payload bodies or arbitrary user headers, so
`payload`/`headers` are `off` in effect whatever they are set to. They are
accepted now so adding a redaction hook later is not a breaking config change.

**Re-examined 2026-08-25, after the log signal landed, and left reserved.** The
question was whether an OTel log appender makes any of the three live. It does
not, and the reasoning is worth recording so it is not re-opened by the next
person to notice them:

- `level` would duplicate the host's filter. A subscriber already decides which
  events are recorded, per target, before kafkaman is consulted; a second
  threshold inside the library would fight it, and the loser would be whichever
  an operator did not expect. If kafkaman ever needs its own, the honest form is
  a `Filtered` layer the host installs, not a config key.
- `payload` and `headers` still have nothing to govern. No path emits either, and
  the metric-schema decision forbids deriving any attribute from message content
  — payload, user headers, or entity keys — which makes the reserved answer the
  permanent one for the metric surface and the default one for the log surface.

Making any of them live would be a behavior change to a parsed, validated config
key, which is exactly what reserving them was meant to avoid.

Queue age and stuck thresholds are available through resolved config and route
behavior. The summary routes are unbounded aggregates executed per request, one
query per message type in sequence. Sequential is deliberate — issuing them
concurrently would take a pool connection per message type away from the relay
and dispatcher to serve an operator — but it means the routes are sized for an
operator or a slow scrape, not a probe.

**Superseded.** This section said M6 adds no asynchronous gauge callbacks that
query Postgres without an admin request. `kafkaman_worker::run_queue_metrics`
now does exactly that, and the qualification that made it worth doing is that the
callbacks do *not* query: a background loop owns the queries on its own interval
and the callbacks read the snapshot it maintains. An observable-gauge callback is
synchronous and could not await a round trip in any case, and a database queried
on the SDK's collection schedule would be queried hardest exactly when it is
already struggling. See
`wiki/decisions/metric-instrument-and-attribute-schema.decision.md`.

`/ready` verifies only that Postgres is reachable. It does not check that tables
exist, that migrations are current, or that workers are alive.

Cache bootstrap/readiness typestate remains proposal 10 work and is not closed
by the M6 readiness route.
