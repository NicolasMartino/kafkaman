//! OpenTelemetry instruments for the scheduler loops.
//!
//! Behind the `metrics` feature so a consumer that does not want an
//! OpenTelemetry dependency can drop it without losing the loops themselves.
//! The no-op twin keeps every call site identical, so the loops read the same
//! either way — which is why both live in this one file rather than being
//! separated: they are a matched pair and must not drift.
//!
//! # Why instruments are owned by the loop
//!
//! An OpenTelemetry instrument binds to whichever `MeterProvider` is installed
//! when the instrument is *created*, and the API offers no way to rebind one.
//! These instruments were previously cached in a process-wide `OnceLock`, which
//! meant whatever provider existed at the first metric recording was the
//! provider every scheduler used until the process exited. A host that started a
//! relay before finishing its telemetry pipeline got a permanently silent metric
//! surface, with no error and no way to work out why.
//!
//! Owning them per loop narrows that window from the life of the process to the
//! life of one loop, and — because a test binary is one process with one global
//! provider — is what makes the surface assertable at all. `tests/observability`
//! pins both properties.
//!
//! # The schedule is a compatibility surface
//!
//! Instrument names, kinds, units, bucket boundaries, and attribute keys are
//! fixed by `wiki/decisions/metric-instrument-and-attribute-schema.decision.md`
//! and recorded in the M6 compatibility note. Changing one breaks a dashboard,
//! an alert, or a recording rule that we cannot see, so it carries the weight of
//! a public API change rather than of an edit to this file.

#[cfg(feature = "metrics")]
mod enabled {
    use std::time::Duration;

    use kafkaman_core::RelayStats;
    use opentelemetry::metrics::{Counter, Histogram};
    use opentelemetry::{global, KeyValue};
    use opentelemetry_semantic_conventions::attribute::MESSAGING_DESTINATION_NAME;
    use time::OffsetDateTime;

    /// Seconds, for one step kafkaman performs itself: a broker round trip, or a
    /// claim-handle-commit cycle.
    ///
    /// Explicit because the OpenTelemetry default boundaries run from 0 to
    /// 10,000 and are meant for milliseconds. Against a `s` unit they put every
    /// realistic observation in the first bucket, which is not a slow histogram
    /// but a useless one. These span 5ms to 10s: below the floor the operator's
    /// question is throughput rather than latency, and above the ceiling the
    /// answer is "something is broken" regardless of the exact number.
    const STEP_LATENCY_BUCKETS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    /// Seconds, for latency that includes time spent waiting in the outbox.
    ///
    /// Reaches to 15 minutes because that is the shape of the question: a
    /// backlog is measured in poll intervals at best and in minutes when a relay
    /// has been down, and a histogram that saturates at 10s cannot tell an
    /// operator whether the queue is draining or growing.
    const QUEUE_LATENCY_BUCKETS: &[f64] = &[
        0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0,
    ];

    /// The instruments and pre-built attributes for one scheduler/message-type
    /// pair, resolved when the loop starts.
    ///
    /// Attributes are built once per loop rather than per record. `KeyValue::new`
    /// with a `String` allocates, and a relay cycle records six times — so the
    /// naive form allocates the same message type six times per poll interval,
    /// forever, on the hot path of an idle worker.
    #[derive(Debug)]
    pub(crate) struct SchedulerMetrics {
        cycles: Counter<u64>,
        rows: Counter<u64>,
        errors: Counter<u64>,
        /// `[scheduler, message_type]`, for cycle and error counters.
        base: [KeyValue; 2],
    }

    impl SchedulerMetrics {
        pub(crate) fn new(scheduler: &'static str, message_type: &str) -> Self {
            let meter = global::meter("kafkaman");
            Self {
                cycles: meter
                    .u64_counter("kafkaman.scheduler.cycles")
                    .with_description("Scheduler cycles completed by kafkaman workers")
                    .with_unit("{cycle}")
                    .build(),
                rows: meter
                    .u64_counter("kafkaman.scheduler.rows")
                    .with_description("Rows observed or transitioned by kafkaman schedulers")
                    .with_unit("{row}")
                    .build(),
                errors: meter
                    .u64_counter("kafkaman.scheduler.errors")
                    .with_description("Scheduler cycles that returned an error before completion")
                    .with_unit("{error}")
                    .build(),
                base: [
                    KeyValue::new("scheduler", scheduler),
                    KeyValue::new("message_type", message_type.to_owned()),
                ],
            }
        }

        fn with_status(&self, status: &'static str) -> [KeyValue; 3] {
            [
                self.base[0].clone(),
                self.base[1].clone(),
                KeyValue::new("status", status),
            ]
        }

        /// The `message_type` attribute alone, for instruments that are already
        /// specific to one loop and so do not need to say which.
        fn message_type(&self) -> [KeyValue; 1] {
            [self.base[1].clone()]
        }

        pub(crate) fn cycle(&self) {
            self.cycles.add(1, &self.base);
        }

        pub(crate) fn rows(&self, status: &'static str, rows: usize) {
            if rows == 0 {
                return;
            }
            self.rows.add(rows as u64, &self.with_status(status));
        }

        pub(crate) fn error(&self) {
            self.errors.add(1, &self.base);
        }
    }

    /// Everything the relay loop records: the shared scheduler counters plus the
    /// two latencies only an outbox relay can measure.
    #[derive(Debug)]
    pub(crate) struct RelayMetrics {
        scheduler: SchedulerMetrics,
        publish_duration: Histogram<f64>,
        time_to_publish: Histogram<f64>,
        /// `[message_type]`, for `time_to_publish`.
        message_type: [KeyValue; 1],
    }

    impl RelayMetrics {
        pub(crate) fn new(message_type: &str) -> Self {
            let meter = global::meter("kafkaman");
            let scheduler = SchedulerMetrics::new("relay", message_type);
            Self {
                message_type: scheduler.message_type(),
                scheduler,
                publish_duration: meter
                    .f64_histogram("kafkaman.relay.publish.duration")
                    .with_description("Time the relay spent publishing one row to the broker")
                    .with_unit("s")
                    .with_boundaries(STEP_LATENCY_BUCKETS.to_vec())
                    .build(),
                time_to_publish: meter
                    .f64_histogram("kafkaman.outbox.time_to_publish")
                    .with_description(
                        "Time from the business event occurring to the broker acknowledging it",
                    )
                    .with_unit("s")
                    .with_boundaries(QUEUE_LATENCY_BUCKETS.to_vec())
                    .build(),
            }
        }

        /// Records the row-transition counters shared by every relay cycle.
        ///
        /// Kept out of `relay_once` so that calling the public single-cycle
        /// helper directly — as tests do — does not inflate the scheduler
        /// counters of a running deployment.
        pub(crate) fn relay_stats(&self, stats: &RelayStats) {
            self.scheduler.cycle();
            self.scheduler.rows("claimed", stats.claimed);
            self.scheduler.rows("published", stats.published);
            self.scheduler.rows("failed", stats.failed);
            self.scheduler.rows("stale", stats.stale);
            self.scheduler.rows("missing", stats.missing);
        }

        pub(crate) fn error(&self) {
            self.scheduler.error();
        }

        /// Records how long one `Publisher::publish` call took.
        ///
        /// This is broker-side latency only — the claim already committed and the
        /// mark has not run yet — which is what makes it separable from the queue
        /// delay that `published` measures.
        pub(crate) fn publish(&self, topic: &str, outcome: &'static str, elapsed: Duration) {
            self.publish_duration.record(
                elapsed.as_secs_f64(),
                &[
                    // `topic` and `messaging.destination.name` carry the same
                    // value under both keys, deliberately: ours keeps kafkaman's
                    // series identifiable, the standard one lets a dashboard
                    // group kafkaman alongside every other messaging library. The
                    // cost is one extra attribute per data point, accepted in the
                    // schema decision.
                    KeyValue::new("topic", topic.to_owned()),
                    KeyValue::new(MESSAGING_DESTINATION_NAME, topic.to_owned()),
                    KeyValue::new("outcome", outcome),
                    super::messaging_system(),
                ],
            );
        }

        /// Records the age of a row the broker has just acknowledged.
        ///
        /// Measured from `occurred_at` — when the business event happened, inside
        /// the caller's transaction — rather than from the claim, because the
        /// wait this exists to expose is the one the outbox pattern introduces.
        /// Nothing else can see it: the database knows when the row was written,
        /// the broker knows when it arrived, and only kafkaman knows both.
        ///
        /// Recorded on broker acknowledgement regardless of what the subsequent
        /// mark returns. A stale mark means another worker also owns the row, but
        /// this publish still reached the broker, and pretending otherwise would
        /// under-report exactly the latency this measures.
        pub(crate) fn published(&self, occurred_at: OffsetDateTime) {
            let seconds = (OffsetDateTime::now_utc() - occurred_at).as_seconds_f64();
            if seconds < 0.0 {
                // A row that occurs in the future is clock skew between the
                // database and this process, not a negative latency. Dropping it
                // loses one observation; recording it corrupts every percentile
                // drawn from the series.
                return;
            }
            self.time_to_publish.record(seconds, &self.message_type);
        }
    }

    /// Everything the receive dispatcher records.
    #[derive(Debug)]
    pub(crate) struct DispatchMetrics {
        scheduler: SchedulerMetrics,
        duration: Histogram<f64>,
        /// `[message_type]`, extended with `outcome` per observation.
        message_type: [KeyValue; 1],
    }

    impl DispatchMetrics {
        pub(crate) fn new(message_type: &str) -> Self {
            let meter = global::meter("kafkaman");
            let scheduler = SchedulerMetrics::new("dispatcher", message_type);
            Self {
                message_type: scheduler.message_type(),
                scheduler,
                duration: meter
                    .f64_histogram("kafkaman.dispatch.duration")
                    .with_description("Time to claim, handle, and record one received message")
                    .with_unit("s")
                    .with_boundaries(STEP_LATENCY_BUCKETS.to_vec())
                    .build(),
            }
        }

        pub(crate) fn cycle(&self) {
            self.scheduler.cycle();
        }

        pub(crate) fn rows(&self, status: &'static str, rows: usize) {
            self.scheduler.rows(status, rows);
        }

        pub(crate) fn error(&self) {
            self.scheduler.error();
        }

        /// Records one dispatch attempt that claimed a row.
        ///
        /// The span is the whole `dispatch_once` call — claim, handler, and the
        /// commit that makes both durable — not the handler alone. That is what
        /// one dispatch costs, and so what sizes a dispatcher pool; isolating the
        /// handler would mean instrumenting inside `kafkaman-sqlx`, which would
        /// put an OpenTelemetry dependency in the crate that owns the SQL.
        ///
        /// Cycles that claimed nothing are not recorded: an empty poll is not a
        /// fast dispatch, and counting it would drag every percentile toward the
        /// cost of a `SELECT` that returned no rows.
        pub(crate) fn dispatched(&self, outcome: &'static str, elapsed: Duration) {
            self.duration.record(
                elapsed.as_secs_f64(),
                &[
                    self.message_type[0].clone(),
                    KeyValue::new("outcome", outcome),
                ],
            );
        }
    }
}

/// The constant that says these series describe Kafka, for a backend grouping
/// several messaging libraries together.
#[cfg(feature = "metrics")]
fn messaging_system() -> opentelemetry::KeyValue {
    opentelemetry::KeyValue::new(
        opentelemetry_semantic_conventions::attribute::MESSAGING_SYSTEM,
        "kafka",
    )
}

/// No-op stand-ins used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
mod disabled {
    use std::time::Duration;

    use kafkaman_core::RelayStats;
    use time::OffsetDateTime;

    #[derive(Debug)]
    pub(crate) struct SchedulerMetrics;

    impl SchedulerMetrics {
        pub(crate) fn new(_scheduler: &'static str, _message_type: &str) -> Self {
            Self
        }

        pub(crate) fn cycle(&self) {}
        pub(crate) fn rows(&self, _status: &'static str, _rows: usize) {}
        pub(crate) fn error(&self) {}
    }

    #[derive(Debug)]
    pub(crate) struct RelayMetrics;

    impl RelayMetrics {
        pub(crate) fn new(_message_type: &str) -> Self {
            Self
        }

        pub(crate) fn relay_stats(&self, _stats: &RelayStats) {}
        pub(crate) fn error(&self) {}
        pub(crate) fn publish(&self, _topic: &str, _outcome: &'static str, _elapsed: Duration) {}
        pub(crate) fn published(&self, _occurred_at: OffsetDateTime) {}
    }

    #[derive(Debug)]
    pub(crate) struct DispatchMetrics;

    impl DispatchMetrics {
        pub(crate) fn new(_message_type: &str) -> Self {
            Self
        }

        pub(crate) fn cycle(&self) {}
        pub(crate) fn rows(&self, _status: &'static str, _rows: usize) {}
        pub(crate) fn error(&self) {}
        pub(crate) fn dispatched(&self, _outcome: &'static str, _elapsed: Duration) {}
    }
}

#[cfg(feature = "metrics")]
pub(crate) use enabled::{DispatchMetrics, RelayMetrics, SchedulerMetrics};

#[cfg(not(feature = "metrics"))]
pub(crate) use disabled::{DispatchMetrics, RelayMetrics, SchedulerMetrics};
