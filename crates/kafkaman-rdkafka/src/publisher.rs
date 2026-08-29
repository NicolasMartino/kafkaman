use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, PublishAck};
use kafkaman_worker::{BoxError, Publisher};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::metrics::PublishMetrics;
use crate::{Error, Result};

#[derive(Clone)]
pub struct RdkafkaPublisher {
    producer: FutureProducer,
    metrics: PublishMetrics,
}

impl std::fmt::Debug for RdkafkaPublisher {
    /// `FutureProducer` is an opaque librdkafka handle with no `Debug`, so the
    /// struct is reported by name only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdkafkaPublisher").finish_non_exhaustive()
    }
}

impl RdkafkaPublisher {
    pub fn new(producer: FutureProducer) -> Self {
        Self {
            producer,
            // Resolved here rather than at first publish: an instrument binds to
            // whichever provider is installed when it is built, so the host must
            // install its pipeline before constructing the publisher.
            metrics: PublishMetrics::new(),
        }
    }

    pub fn from_brokers(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // An outbox relay republishes on every uncertain outcome, so the
            // producer must deduplicate on the broker side; without idempotence
            // a retried send after a lost ack writes the snapshot twice at two
            // offsets. `acks=all` is its prerequisite and also stops a snapshot
            // from being acknowledged before it is replicated.
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self::new(producer))
    }

    /// Publish `row`, capturing the current span as the context on the wire.
    ///
    /// Prefer [`Self::publish_row_traced`] from inside a call chain: what
    /// "current" means here depends on what has been opened above it.
    pub async fn publish_row(&self, row: &ClaimedOutboxRow) -> Result<PublishAck> {
        self.publish_row_traced(row, kafkaman_core::capture_trace_context())
            .await
    }

    /// Publish `row` with `trace` as the context written to the wire.
    ///
    /// Naming the context rather than reading the ambient one is what keeps the
    /// `traceparent` other services parse pointing at `kafkaman.relay.publish`,
    /// whatever gets opened between that span and this call.
    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
    pub async fn publish_row_traced(
        &self,
        row: &ClaimedOutboxRow,
        trace: Option<kafkaman_core::TraceContext>,
    ) -> Result<PublishAck> {
        let payload = serde_json::to_vec(&row.row.payload)?;
        let managed = managed_headers(row, trace)?;

        let mut headers = OwnedHeaders::new();
        // User headers first, kafkaman's second: a user header cannot occupy a
        // reserved key (enqueue rejects that outright), so the order is only
        // about keeping the managed block contiguous and easy to read on the
        // wire.
        for (key, value) in &row.row.headers {
            // The W3C trace keys are the one exception, and they are dropped
            // rather than rejected. Enqueue does not refuse them the way it
            // refuses `kafkaman-*`, because an application forwarding an
            // incoming request's headers wholesale is doing something ordinary,
            // not something wrong. But they are protocol context rather than
            // application data: publishing the caller's copy would put a stale
            // `traceparent` on the wire ahead of the one this publish belongs
            // to, and ingest resolves duplicates first-wins.
            if is_trace_header(key) {
                tracing::debug!(
                    header = key.as_str(),
                    "dropping a W3C trace header from user headers; kafkaman sets its own"
                );
                continue;
            }
            headers = headers.insert(Header {
                key: key.as_str(),
                value: Some(value.as_str()),
            });
        }
        for (key, value) in &managed {
            headers = headers.insert(Header {
                key,
                value: Some(value.as_str()),
            });
        }

        let mut record = FutureRecord::to(&row.row.topic)
            .payload(payload.as_slice())
            .headers(headers);
        // Fall back to the entity key when a type declares no partition key.
        // Kafka routes keyless records round-robin, but the cache convergence
        // guard compares offsets only within one topic and partition — so a
        // keyless entity type would scatter its snapshots across partitions and
        // could never converge.
        if let Some(key) = row.row.record_key() {
            record = record.key(key);
        }

        match self.producer.send(record, Timeout::Never).await {
            Ok((partition, offset)) => {
                // `published`, not `acknowledged`: the relay's own
                // `kafkaman.relay.publish.duration` labels the same event that
                // way, and one word for one state is what lets an operator
                // group both instruments by `outcome` in a single query.
                self.metrics.record(&row.row.topic, "published");
                Ok(PublishAck {
                    topic: row.row.topic.clone(),
                    partition,
                    offset,
                })
            }
            Err((error, _message)) => {
                self.metrics.record(&row.row.topic, "failed");
                Err(Error::Delivery(error.to_string()))
            }
        }
    }
}

/// Whether a header key is one of the two W3C trace keys, case-insensitively.
///
/// Kafka header keys are case-sensitive, but the comparison is not: a producer
/// writing `TraceParent` means the standard header, and treating it as an
/// unrelated user header would be a way around every rule that governs the real
/// one.
pub(crate) fn is_trace_header(key: &str) -> bool {
    key.eq_ignore_ascii_case("traceparent") || key.eq_ignore_ascii_case("tracestate")
}

/// The headers kafkaman itself sets on a published record, owned so every value
/// outlives the borrow `OwnedHeaders` takes.
///
/// Two namespaces: the reserved `kafkaman-` keys, and the W3C trace keys, which
/// deliberately carry no prefix. See
/// `wiki/decisions/trace-context-propagation-and-w3c-headers.decision.md`.
fn managed_headers(
    row: &ClaimedOutboxRow,
    trace: Option<kafkaman_core::TraceContext>,
) -> Result<Vec<(&'static str, String)>> {
    let mut managed = vec![
        ("kafkaman-message-id", row.row.message_id.to_string()),
        (
            "kafkaman-correlation-id",
            row.row.correlation_id.to_string(),
        ),
    ];

    // The occurrence time is the producer's, not the consumer's. Without it on
    // the wire the consumer can only stamp its own arrival time, so event time
    // is lost for every message that crosses a broker.
    let occurred_at = kafkaman_core::rfc9557::render(row.row.occurred_at).map_err(|err| {
        Error::InvalidHeader {
            name: "kafkaman-occurred-at",
            message: err.to_string(),
        }
    })?;
    managed.push(("kafkaman-occurred-at", occurred_at));

    if let Some(key) = row.row.idempotency_key {
        managed.push(("kafkaman-idempotency-key", key.to_string()));
    }
    // The digest alone is opaque. Carrying the source keeps the received row's
    // `idempotency_source` column populated across a broker hop, which is what
    // makes a stored digest explicable during triage.
    if let Some(source) = &row.row.idempotency_source {
        managed.push(("kafkaman-idempotency-source", source.to_string()));
    }
    if let Some(id) = row.row.causation_id {
        managed.push(("kafkaman-causation-id", id.to_string()));
    }

    // W3C trace context. It carries no `kafkaman-` prefix on purpose: the entire
    // value of the standard is that a consumer which has never heard of kafkaman
    // still recognizes it.
    //
    // The current span comes first, because the consumer-side handoff should
    // point at *this publish*, not at the enqueue that preceded it — and the
    // relay has already parented this span from the context stored at enqueue,
    // so the two are in the same trace either way.
    //
    // The stored context is the fallback for the case that produces no current
    // span at all: a build with `traces` off, or a host that installs no tracer.
    // Without it such a relay strips the `traceparent` from every message it
    // forwards, breaking the trace for every downstream service that *is*
    // instrumented — a process opting out of producing spans must not thereby
    // opt its neighbours out too. What downstream sees then is a handoff to the
    // enqueue rather than to the publish: one hop coarser, and still the same
    // trace.
    if let Some(trace) = trace.or_else(|| row.row.trace.clone()) {
        managed.push(("traceparent", trace.traceparent().to_owned()));
        if let Some(state) = trace.tracestate() {
            managed.push(("tracestate", state.to_owned()));
        }
    }

    Ok(managed)
}

#[async_trait]
impl Publisher for RdkafkaPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        // Captured here, at the trait boundary, and not one call deeper. The
        // relay instruments *this* call with `kafkaman.relay.publish`, so the
        // current span is the phase span exactly at this point — and pinning it
        // here means nothing opened further in can change what goes on the wire.
        let trace = kafkaman_core::capture_trace_context();
        self.publish_row_traced(row, trace)
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }
}
