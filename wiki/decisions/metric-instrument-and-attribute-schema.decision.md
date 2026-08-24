# Metric Instrument and Attribute Schema

- Document Class: Decision
- Status: Accepted
- Date: 2026-08-25
- Category: Observability contract
- Scope: Fixes kafkaman's metric instrument names, kinds, units, and attribute keys as a public compatibility surface, and defines how they relate to OpenTelemetry messaging semantic conventions.
- Sources:
  - wiki/proposals/13-telemetry-pipeline-completion.proposal.md
  - wiki/specs/m6-observability-operability.spec.md
  - crates/kafkaman-worker/src/metrics.rs
  - crates/kafkaman-rdkafka/src/metrics.rs
  - crates/kafkaman-sqlx/src/operability.rs
- Related:
  - wiki/decisions/telemetry-pipeline-ownership.decision.md
  - wiki/decisions/dispatch-stats-semantics.decision.md
  - wiki/compatibility/m6-observability-operability-api.compat.md

## Decision

1. **Instrument names keep the `kafkaman.*` namespace.**
   Names are not renamed to OpenTelemetry messaging semantic conventions. A
   deployment running several messaging libraries must be able to identify
   kafkaman's series, and semconv has no vocabulary for outbox-specific concepts
   — time-to-publish, claim expiry, dispatcher backlog — which are the series
   this library exists to expose.

2. **Attributes adopt the standard messaging keys alongside our own.**
   Where a kafkaman attribute has a semconv equivalent, both are emitted.
   Correlation across libraries happens on attributes, not on instrument names,
   and a series nobody can join to the rest of their telemetry is worth less than
   the bytes it costs.

3. **Metric names, kinds, units, and attribute keys are a public compatibility
   surface.** Changing one is a breaking change to a dashboard, an alert, and a
   recording rule that we cannot see. They are recorded in the compatibility note
   and changed under the same rules as a public API.

4. **Every instrument declares a unit.** No M6 instrument calls `.with_unit()`.
   Backends use it for axis formatting and conversion; omitting it is a defect,
   not a style choice.

5. **The disjointness invariant is asserted by a test, not only by
   `debug_assert`.** `record_ingest_stats` requires that `inserted + duplicates
   + skipped == consumed`. That is the invariant whose violation shipped the
   double-count defect. A `debug_assert` is compiled out of release builds and so
   guards nothing where it matters; the invariant is pinned by a test that runs
   real ingest cycles. The `debug_assert` stays as fast local feedback.

6. **No attribute may derive from message content.** Not payload, not user
   headers, not entity keys. This extends the M6 sanitization rule — which
   governs what admin responses expose — to the metric surface, where the
   equivalent mistake is unbounded cardinality as well as a data leak.

## Instrument Schedule

Existing instruments, unchanged in name:

| Instrument | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `kafkaman.scheduler.cycles` | Counter | `{cycle}` | `scheduler`, `message_type` |
| `kafkaman.scheduler.rows` | Counter | `{row}` | `scheduler`, `message_type`, `status` |
| `kafkaman.scheduler.errors` | Counter | `{error}` | `scheduler`, `message_type` |
| `kafkaman.kafka.publish.records` | Counter | `{record}` | `topic`, `outcome`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.kafka.ingest.records` | Counter | `{record}` | `message_type`, `outcome`, `messaging.system` |
| `kafkaman.kafka.ingest.commits` | Counter | `{commit}` | `message_type`, `messaging.system` |
| `kafkaman.kafka.ingest.errors` | Counter | `{error}` | `message_type`, `reason`, `messaging.system` |

New instruments:

| Instrument | Kind | Unit | Attributes | Why |
| --- | --- | --- | --- | --- |
| `kafkaman.relay.publish.duration` | Histogram | `s` | `topic`, `outcome`, `messaging.system`, `messaging.destination.name` | Broker-side latency, separable from queue delay |
| `kafkaman.dispatch.duration` | Histogram | `s` | `message_type`, `outcome` | Handler cost; the number that sizes a dispatcher pool |
| `kafkaman.outbox.time_to_publish` | Histogram | `s` | `message_type` | `occurred_at` → broker ack. The health of the outbox itself |
| `kafkaman.outbox.depth` | Observable gauge | `{row}` | `message_type`, `status` | Backlog, scraped rather than polled |
| `kafkaman.outbox.oldest_age` | Observable gauge | `s` | `message_type`, `status` | Age of the oldest unpublished row |
| `kafkaman.received.depth` | Observable gauge | `{row}` | `message_type`, `status` | Receive-side backlog |
| `kafkaman.received.oldest_age` | Observable gauge | `s` | `message_type`, `status` | Age of the oldest undispatched row |
| `kafkaman.queue.sample_age` | Observable gauge | `s` | none | How old the snapshot behind the four gauges above is. Added 2026-08-25; see Amendments |

`kafkaman.outbox.time_to_publish` deserves its justification stated, because it
is the one instrument here that no general-purpose messaging library would have.
The whole premise of the outbox pattern is that enqueue and publish are separated
in time. That separation is the library's core latency, it is invisible to both
the database and the broker, and no counter can reconstruct it. It is the first
thing to look at when someone reports that events are arriving late.

## The Gauges Are Nearly Free

`outbox_status_summary` and `received_status_summary` already compute depth and
oldest-row age against real Postgres. Both are integration-tested. Both were
written for the admin routes and are already index-served — the M6 review
rewrote `received_stuck_rows` specifically to keep the query shape servable by
the `(status, next_attempt_at, created_at)` index.

Registering them as observable gauges is a callback wrapping existing, tested
SQL. It converts a route an operator must remember to curl into a series a
dashboard scrapes, which is the difference between data that exists and data
somebody looks at.

Two caveats, both real:

- **These are aggregate queries.** The M6 documentation already warns that the
  summary routes are built for a scrape interval measured in seconds, not for a
  per-request probe. Registering them as gauges means the *collection interval*
  now drives that query. The interval is the host's to configure, and the
  documentation must say so plainly.
- **Observable gauges fire on the SDK's schedule, not ours.** The callback must
  be cheap, cancel-safe, and must never block the collection cycle on a database
  that is already struggling — which is precisely when someone is looking at the
  dashboard. Callbacks use a bounded timeout and report staleness rather than
  hanging.

## Attribute Mapping

| kafkaman attribute | Semconv attribute emitted alongside | Note |
| --- | --- | --- |
| `topic` | `messaging.destination.name` | Same value, both keys |
| — | `messaging.system` = `kafka` | Constant |
| `message_type` | none | kafkaman-specific; no semconv equivalent |
| `scheduler` | none | kafkaman-specific loop identity |
| `status`, `outcome`, `reason` | none | Deliberately ours; semconv's `error.type` means something narrower |

The duplication in row one is intentional and its cost is understood: one extra
key-value per data point, bounded by the number of topics. What it buys is that a
Kibana dashboard grouping every messaging library by `messaging.destination.name`
includes kafkaman's series without special-casing.

`opentelemetry-semantic-conventions` 0.32.1 is version-aligned with the
`opentelemetry` 0.32 already in the workspace and provides these keys as
constants, so they do not become hand-typed strings that drift.

## Amendments

### 2026-08-25 — Bucket boundaries are part of the surface

Implementing the histograms surfaced something the schedule did not name. The
OpenTelemetry default bucket boundaries run from 0 to 10,000 and are meant for
milliseconds; against the `s` unit this decision fixes, every realistic
observation lands in the first bucket. A histogram declared in seconds with
default buckets is not a coarse histogram, it is an empty one, so boundaries are
declared explicitly and are as much a part of the compatibility surface as the
unit — changing them rewrites every percentile a dashboard draws.

Two sets, in `crates/kafkaman-worker/src/metrics.rs`:

- **Step latency**, 5ms to 10s, for `kafkaman.relay.publish.duration` and
  `kafkaman.dispatch.duration` — one broker round trip, or one
  claim-handle-commit cycle.
- **Queue latency**, 50ms to 15 minutes, for
  `kafkaman.outbox.time_to_publish` — a backlog is measured in poll intervals at
  best and in minutes when a relay has been down, and a histogram saturating at
  10s cannot say whether the queue is draining or growing.

### 2026-08-25 — Semconv attributes apply to every series carrying a topic

The schedule listed `messaging.system` and `messaging.destination.name` only for
`kafkaman.kafka.publish.records`, while Decision 2 states the rule generally:
where a kafkaman attribute has a semconv equivalent, both are emitted. The
omission was read as an oversight rather than an exception, so
`kafkaman.relay.publish.duration` carries them too — it is the other series keyed
by topic, and a dashboard grouping by `messaging.destination.name` would
otherwise find kafkaman's publish counts and miss its publish latency. The
schedule row above is updated to match.

`opentelemetry-semantic-conventions` 0.32.1 gates the messaging keys behind its
`semconv_experimental` feature, since messaging semconv is not yet stable
upstream. The feature is enabled; the constants are string literals, and using
them is still better than hand-typing keys that would drift.

### 2026-08-25 — Staleness is a series, not a gap

This decision required gauge callbacks to "report staleness rather than
hanging". The implementation first read that as: stop observing once the
snapshot is too old, so a stalled sampler shows as a gap rather than a flat line
at a number that quietly stopped being true.

**That does not work, and the reason is not visible from kafkaman's side of the
API.** An asynchronous gauge under cumulative temporality republishes its last
recorded value on every collection cycle whether or not the callback observed
anything. Observing nothing produces the flat line anyway. It was found by a test
written to prove the gap, which instead proved the opposite;
`tests/observability/queue_gauge_staleness` now pins the real behavior.

`kafkaman.queue.sample_age` is the amendment: the age of the snapshot behind
every other queue gauge, in seconds. A depth of 2 means nothing on its own; a
depth of 2 beside a sample age of 400 seconds says plainly how much to trust it,
and that age is the series to alert on. Temporality is the host's to choose and a
library must not force a view on it, which is why this is an added instrument
rather than a configuration demand.

## Options Considered

**A. Rename everything to semconv.** Rejected — see Decision 1.
**B. Keep names, keep ad-hoc attributes only.** Rejected — see Decision 2.
**C. Keep names, add semconv attributes.** *Accepted.*
**D. Emit semconv attributes only, drop `topic`.** Rejected: it breaks any
dashboard M6 adopters have already built, for no gain over emitting both.

## Consequences

- The compatibility note gains a metric schedule, and metric changes acquire the
  review weight of API changes.
- Data-point size grows by one attribute on the publish path. Bounded and
  measured; if it proves material, the mitigation is dropping our `topic` key
  rather than the standard one.
- Gauge callbacks put the summary queries on the SDK's collection interval, which
  is a new load pattern this library has not previously had. Documented, bounded,
  and configurable by the host.
- The double-count class of defect becomes testable rather than reviewable.
