use kafkaman_core::KafkaMessage;
use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};
use rdkafka::Timestamp;
use serde::{Deserialize, Serialize};

use crate::ingest_record::record_envelope;
use crate::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ProductSnapshot {
    product_id: String,
    name: String,
}

impl KafkaMessage for ProductSnapshot {
    const MESSAGE_TYPE: &'static str = "product_snapshot";
    const TOPIC: &'static str = "products";

    fn entity_key(&self) -> String {
        self.product_id.clone()
    }
}

/// A 64-hex digest, the only shape `kafkaman-idempotency-key` accepts.
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn record(headers: OwnedHeaders) -> OwnedMessage {
    OwnedMessage::new(
        Some(
            serde_json::to_vec(&serde_json::json!({
                "product_id": "p-1",
                "name": "first",
            }))
            .unwrap(),
        ),
        Some(b"p-1".to_vec()),
        ProductSnapshot::TOPIC.to_owned(),
        Timestamp::NotAvailable,
        0,
        7,
        Some(headers),
    )
}

fn header(key: &str, value: &str) -> OwnedHeaders {
    OwnedHeaders::new().insert(Header {
        key,
        value: Some(value),
    })
}

#[test]
fn a_malformed_idempotency_source_degrades_instead_of_failing_ingest() {
    // Deliberate asymmetry, pinned here because it is easy to mistake for an
    // oversight: every other reserved header is a hard `InvalidHeader` on
    // parse failure, but the source is explanatory metadata only. Dedupe
    // compares the digest, which is present and valid, so quarantining the
    // record would cost real delivery to preserve a triage aid.
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "kafkaman-idempotency-source",
        value: Some("{not json"),
    });

    let envelope = record_envelope::<ProductSnapshot, _>(&record(headers))
        .expect("ingest must proceed")
        .envelope;

    let identity = envelope
        .idempotency_key
        .expect("the digest still identifies the record");
    assert_eq!(identity.key.to_hex(), DIGEST);
    assert!(
        identity.source.is_none(),
        "an unparseable source is dropped, not guessed at"
    );
}

#[test]
fn a_well_formed_idempotency_source_survives_the_wire() {
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "kafkaman-idempotency-source",
        value: Some(r#"{"order_id":"o-1"}"#),
    });

    let envelope = record_envelope::<ProductSnapshot, _>(&record(headers))
        .unwrap()
        .envelope;
    let source = envelope
        .idempotency_key
        .and_then(|identity| identity.source)
        .expect("a valid source must be carried through");
    assert_eq!(source.value(), &serde_json::json!({ "order_id": "o-1" }));
}

#[test]
fn a_malformed_occurred_at_is_rejected_rather_than_degraded() {
    // The contrast that makes the tolerance above a decision rather than an
    // accident: event time is load-bearing, so a bad value must not be
    // silently replaced by the consumer's arrival time.
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "kafkaman-occurred-at",
        value: Some("not-a-timestamp"),
    });

    assert!(matches!(
        record_envelope::<ProductSnapshot, _>(&record(headers)),
        Err(Error::InvalidHeader {
            name: "kafkaman-occurred-at",
            ..
        })
    ));
}

#[test]
fn duplicate_headers_resolve_in_opposite_directions_by_namespace() {
    // Kafka permits repeated header keys, and the two namespaces deliberately
    // disagree about which copy survives. Pinned because the difference is
    // invisible at a call site and easy to "tidy" into one rule: an earlier
    // refactor of this decode did exactly that, and silently changed which
    // duplicate of a user header a received row stores.
    //
    // Reserved: first wins, so a producer appending a second
    // `kafkaman-message-id` cannot override the one the relay wrote.
    // User: last wins, matching the `BTreeMap::insert` this has always used.
    let first = uuid::Uuid::new_v4();
    let second = uuid::Uuid::new_v4();
    let headers = header("kafkaman-idempotency-key", DIGEST)
        .insert(Header {
            key: "kafkaman-message-id",
            value: Some(first.to_string().as_str()),
        })
        .insert(Header {
            key: "kafkaman-message-id",
            value: Some(second.to_string().as_str()),
        })
        .insert(Header {
            key: "x-trace",
            value: Some("first"),
        })
        .insert(Header {
            key: "x-trace",
            value: Some("second"),
        });

    let envelope = record_envelope::<ProductSnapshot, _>(&record(headers))
        .unwrap()
        .envelope;

    assert_eq!(
        envelope.message_id, first,
        "a repeated reserved header must not let a producer override the first copy"
    );
    assert_eq!(
        envelope.headers.get("x-trace").map(String::as_str),
        Some("second"),
        "a repeated user header keeps the last copy, as `insert` always has"
    );
}

/// A `traceparent` the producer sent must reach trace extraction and nothing
/// else.
///
/// The failure this prevents is a handler receiving `traceparent` among its user
/// headers and concluding the producer sent it as application data. A tracing
/// SDK sent it, and it belongs to the third namespace — neither reserved nor
/// user.
#[test]
fn w3c_trace_headers_are_extracted_and_kept_out_of_user_headers() {
    const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    let headers = header("kafkaman-idempotency-key", DIGEST)
        .insert(Header {
            key: "traceparent",
            value: Some(TRACEPARENT),
        })
        .insert(Header {
            key: "tracestate",
            value: Some("vendor=1"),
        })
        .insert(Header {
            key: "x-app-header",
            value: Some("kept"),
        });

    let decoded = record_envelope::<ProductSnapshot, _>(&record(headers))
        .expect("trace headers are never a reason to reject a record");

    let trace = decoded
        .producer_trace
        .expect("a well-formed traceparent is extracted");
    assert_eq!(trace.traceparent(), TRACEPARENT);
    assert_eq!(trace.tracestate(), Some("vendor=1"));

    assert_eq!(
        decoded
            .envelope
            .headers
            .get("x-app-header")
            .map(String::as_str),
        Some("kept"),
        "an ordinary user header is untouched"
    );
    for key in ["traceparent", "tracestate", "TraceParent"] {
        assert!(
            !decoded.envelope.headers.contains_key(key),
            "{key} must not be handed to application code as a user header"
        );
    }
}

/// Case is not a way around the namespace.
///
/// Kafka header keys are case-sensitive, so `TraceParent` is a distinct key to
/// the broker. Treating it as an unrelated user header would let a producer
/// smuggle a trace header past every rule that governs the real one.
#[test]
fn trace_header_matching_ignores_case() {
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "TraceParent",
        value: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    });

    let decoded = record_envelope::<ProductSnapshot, _>(&record(headers)).unwrap();
    assert!(decoded.producer_trace.is_some());
    assert!(decoded.envelope.headers.is_empty());
}

/// A malformed `traceparent` is dropped, not fatal.
///
/// Trace context is never required for a message to be correct, so a broken one
/// must cost a trace and nothing else — least of all a quarantined record.
#[test]
fn a_malformed_traceparent_costs_the_trace_and_nothing_else() {
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "traceparent",
        value: Some("00-not-a-trace-id"),
    });

    let decoded = record_envelope::<ProductSnapshot, _>(&record(headers))
        .expect("a malformed traceparent must not reject the record");
    assert!(decoded.producer_trace.is_none());
    assert!(
        !decoded.envelope.headers.contains_key("traceparent"),
        "unusable trace context is still trace context, and still not user data"
    );
}
