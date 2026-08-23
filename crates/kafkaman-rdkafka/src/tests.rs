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

    let (envelope, _key) =
        record_envelope::<ProductSnapshot, _>(&record(headers)).expect("ingest must proceed");

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

    let (envelope, _key) = record_envelope::<ProductSnapshot, _>(&record(headers)).unwrap();
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

    let (envelope, _key) = record_envelope::<ProductSnapshot, _>(&record(headers)).unwrap();

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
