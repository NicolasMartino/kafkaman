//! OpenTelemetry counters for publish and ingest.
//!
//! Behind the `metrics` feature so a consumer that does not want an
//! OpenTelemetry dependency can drop it. The no-op twin keeps every call site
//! identical, and lives in this same file so the pair cannot drift.
//!
//! # Why these are owned rather than cached
//!
//! An instrument binds to whichever `MeterProvider` is installed when it is
//! created, permanently. Caching them process-wide meant the first record in the
//! process chose the provider for every later one, so a host that published
//! before installing its SDK never reported a metric again. See the module docs
//! in `kafkaman-worker`'s `metrics.rs` for the full account.
//!
//! The two here bind at different moments, and the difference is worth knowing:
//! [`PublishMetrics`] binds when the publisher is *constructed*, while
//! [`IngestMetrics`] binds when the ingest loop *starts*. So the host contract is
//! "install the provider before constructing kafkaman components or starting
//! loops", not merely before starting loops.

#[cfg(feature = "metrics")]
mod enabled {
    use opentelemetry::metrics::Counter;
    use opentelemetry::{global, KeyValue};
    use opentelemetry_semantic_conventions::attribute::{
        MESSAGING_DESTINATION_NAME, MESSAGING_SYSTEM,
    };

    use crate::IngestStats;

    /// Emitted alongside kafkaman's own keys on every Kafka-side series, so a
    /// backend grouping several messaging libraries includes kafkaman's without
    /// special-casing it. Constant for this crate: the transport is rdkafka.
    fn messaging_system() -> KeyValue {
        KeyValue::new(MESSAGING_SYSTEM, "kafka")
    }

    /// Publish-side instruments, resolved when the publisher is constructed.
    ///
    /// `Clone` because `RdkafkaPublisher` is: cloning shares the instrument
    /// handles rather than rebuilding them, so a cloned publisher reports to the
    /// same provider as the original.
    #[derive(Clone, Debug)]
    pub(crate) struct PublishMetrics {
        records: Counter<u64>,
    }

    impl PublishMetrics {
        pub(crate) fn new() -> Self {
            Self {
                records: global::meter("kafkaman")
                    .u64_counter("kafkaman.kafka.publish.records")
                    .with_description("Kafka records published by kafkaman")
                    .with_unit("{record}")
                    .build(),
            }
        }

        pub(crate) fn record(&self, topic: &str, outcome: &'static str) {
            self.records.add(
                1,
                &[
                    // The topic travels under both keys on purpose: ours keeps
                    // kafkaman's series identifiable in a deployment running
                    // several messaging libraries, the standard one lets a
                    // dashboard group them together. One extra attribute per
                    // record, bounded by the topic count.
                    KeyValue::new("topic", topic.to_owned()),
                    KeyValue::new(MESSAGING_DESTINATION_NAME, topic.to_owned()),
                    KeyValue::new("outcome", outcome),
                    messaging_system(),
                ],
            );
        }
    }

    /// Ingest-side instruments, resolved when the ingest loop starts.
    #[derive(Debug)]
    pub(crate) struct IngestMetrics {
        records: Counter<u64>,
        commits: Counter<u64>,
        errors: Counter<u64>,
        message_type: &'static str,
    }

    impl IngestMetrics {
        pub(crate) fn new(message_type: &'static str) -> Self {
            let meter = global::meter("kafkaman");
            Self {
                records: meter
                    .u64_counter("kafkaman.kafka.ingest.records")
                    .with_description("Kafka records consumed and durably classified by kafkaman")
                    .with_unit("{record}")
                    .build(),
                commits: meter
                    .u64_counter("kafkaman.kafka.ingest.commits")
                    .with_description("Kafka offset commits made after a durable classification")
                    .with_unit("{commit}")
                    .build(),
                errors: meter
                    .u64_counter("kafkaman.kafka.ingest.errors")
                    .with_description(
                        "Kafka ingest errors that did not produce a committed classification",
                    )
                    .with_unit("{error}")
                    .build(),
                message_type,
            }
        }

        /// Counts one consumed record under exactly one classification.
        ///
        /// The `outcome` values must stay disjoint and must cover every consumed
        /// record, so that `sum(kafkaman.kafka.ingest.records)` is the number of
        /// records ingested and `sum by (outcome)` splits it without overlap. Offset
        /// commits are deliberately *not* an outcome here: a commit accompanies every
        /// classification rather than replacing one, so counting it alongside them
        /// would double every record. It has its own counter.
        fn record_outcome(&self, outcome: &'static str, count: usize) {
            if count == 0 {
                return;
            }
            self.records.add(
                count as u64,
                &[
                    KeyValue::new("message_type", self.message_type),
                    KeyValue::new("outcome", outcome),
                    messaging_system(),
                ],
            );
        }

        /// Records every counter for one ingest cycle.
        ///
        /// Single entry point so the disjointness invariant is checked in one place:
        /// each consumed record must land under exactly one classification outcome.
        /// Counting commits as a fourth outcome — they accompany a classification
        /// rather than replacing one — silently doubled `ingest.records` until this
        /// assertion existed.
        pub(crate) fn stats(&self, stats: &IngestStats) {
            debug_assert_eq!(
                stats.inserted + stats.duplicates + stats.skipped,
                stats.consumed,
                "ingest outcomes must partition the consumed records exactly once"
            );
            self.record_outcome("inserted", stats.inserted);
            self.record_outcome("duplicate", stats.duplicates);
            self.record_outcome("skipped", stats.skipped);
            self.commit(stats.committed);
        }

        /// Counts offset commits, which track consumer progress rather than record
        /// classification.
        fn commit(&self, count: usize) {
            if count == 0 {
                return;
            }
            self.commits.add(
                count as u64,
                &[
                    KeyValue::new("message_type", self.message_type),
                    messaging_system(),
                ],
            );
        }

        /// Counts an ingest cycle that ended without a committed classification.
        ///
        /// `reason` separates the fatal breaker trip from an ordinary transient
        /// failure. Without it a poison-pill shutdown looks identical to a broker
        /// hiccup on the dashboard, which is exactly the moment the distinction
        /// matters.
        pub(crate) fn error(&self, reason: &'static str) {
            self.errors.add(
                1,
                &[
                    KeyValue::new("message_type", self.message_type),
                    KeyValue::new("reason", reason),
                    messaging_system(),
                ],
            );
        }
    }
}

/// No-op stand-ins used when the `metrics` feature is off.
#[cfg(not(feature = "metrics"))]
mod disabled {
    use crate::IngestStats;

    #[derive(Clone, Debug)]
    pub(crate) struct PublishMetrics;

    impl PublishMetrics {
        pub(crate) fn new() -> Self {
            Self
        }

        pub(crate) fn record(&self, _topic: &str, _outcome: &'static str) {}
    }

    #[derive(Debug)]
    pub(crate) struct IngestMetrics;

    impl IngestMetrics {
        pub(crate) fn new(_message_type: &'static str) -> Self {
            Self
        }

        pub(crate) fn stats(&self, _stats: &IngestStats) {}
        pub(crate) fn error(&self, _reason: &'static str) {}
    }
}

#[cfg(feature = "metrics")]
pub(crate) use enabled::{IngestMetrics, PublishMetrics};

#[cfg(not(feature = "metrics"))]
pub(crate) use disabled::{IngestMetrics, PublishMetrics};
