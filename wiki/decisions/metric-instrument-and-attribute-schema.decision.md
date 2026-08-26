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
| `kafkaman.kafka.ingest.records` | Counter | `{record}` | `message_type`, `outcome`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.kafka.ingest.commits` | Counter | `{commit}` | `message_type`, `messaging.system`, `messaging.destination.name` |
| `kafkaman.kafka.ingest.errors` | Counter | `{error}` | `message_type`, `reason`, `messaging.system`, `messaging.destination.name` |

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

Registering them as observable gauges converts a route an operator must remember
to curl into a series a dashboard scrapes, which is the difference between data
that exists and data somebody looks at.

**Amended 2026-08-26.** This section originally described the gauges as "a
callback wrapping existing, tested SQL", with the SDK's collection interval
driving the query. That is not what was built, and the difference matters enough
to record rather than edit away — a reader who took the original at face value
would size the collection interval as though it were a database knob.

An observable-gauge callback is synchronous, so it cannot await a query at all;
and a callback that could would put the database on a schedule chosen for
telemetry, stalling the collection cycle exactly when the database is already
struggling. So `run_queue_metrics` owns the queries on its own
`refresh_interval`, maintains a snapshot, and the callbacks read the snapshot.
Scraping faster does not query harder.

Three consequences, all real:

- **These are aggregate queries.** Each refresh is a full `GROUP BY status` over
  each table. `refresh_interval` is what decides how often that runs, and it is
  the host's to configure.
- **A failed or slow refresh leaves the previous snapshot in place.** The refresh
  is bounded by a timeout; it does not clear what it could not update.
- **Staleness is a series, not a gap.** Under cumulative temporality an
  asynchronous gauge republishes its last value on every collection cycle whether
  or not the callback observed anything, so declining to observe cannot produce a
  gap. `kafkaman.queue.sample_age` reports the age of the snapshot behind every
  other series here, and that is the condition worth alerting on.

## Attribute Mapping

| kafkaman attribute | Semconv attribute emitted alongside | Note |
| --- | --- | --- |
| `topic` | `messaging.destination.name` | Same value, both keys, on the publish series |
| — | `messaging.destination.name` | On the ingest series, which has no kafkaman-side `topic` key: the consumer's topic is fixed by the message type, so a second key naming the same constant would only add cardinality |
| — | `messaging.system` = `kafka` | Constant |
| `message_type` | none | kafkaman-specific; no semconv equivalent |
| `scheduler` | none | kafkaman-specific loop identity |
| `status`, `outcome`, `reason` | none | Deliberately ours; semconv's `error.type` means something narrower |

### Every Kafka-side series names its topic

**Added 2026-08-26.** The ingest counters carried `message_type` and
`messaging.system` and no topic at all, so `kafkaman.kafka.ingest.records` and
`kafkaman.kafka.publish.records` had no attribute in common beyond the constant
`messaging.system`. The obvious operator question — is this topic backing up, are
we consuming it as fast as we publish to it — could not be asked without mapping
message types to topics outside the telemetry.

All three ingest instruments now carry `messaging.destination.name`. Adding an
attribute changes series identity, so an existing dashboard sees the ingest
series split until it is updated; the alternative was leaving the two halves of
one topic's traffic permanently unjoinable.

### `outcome` uses one word per state, across instruments

`kafkaman.relay.publish.duration` and `kafkaman.kafka.publish.records` describe
the same event from two heights — the relay's loop and the transport underneath
it. They therefore share a vocabulary: a successful send is `published` on both,
never `acknowledged` on one and `published` on the other. Two words for one state
is invisible in either instrument alone and breaks the first query that spans
them, which is the query an operator writes when publish latency and publish
volume disagree.

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
- The queue summary queries are a background load this library has not previously
  had. It is bounded by `QueueMetricsConfig::refresh_interval` and not by the
  SDK's collection interval: the sampler refreshes a snapshot on its own
  schedule and the callbacks read that snapshot, so a host scraping every second
  does not thereby query the database every second. Configurable, and the
  staleness of the snapshot is itself a reported series.
- The double-count class of defect becomes testable rather than reviewable.
