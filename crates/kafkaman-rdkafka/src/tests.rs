use kafkaman_core::{Envelope, KafkaMessage, TraceContext};
use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};
use rdkafka::Timestamp;
use serde::{Deserialize, Serialize};

use crate::ingest_record::{record_envelope, RecordHeaders};
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

#[derive(Clone, Debug, Serialize)]
struct PanickingPayload;

impl<'de> Deserialize<'de> for PanickingPayload {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        panic!("deserialize exploded");
    }
}

impl KafkaMessage for PanickingPayload {
    const MESSAGE_TYPE: &'static str = "panicking_payload";
    const TOPIC: &'static str = "products";

    fn entity_key(&self) -> String {
        "panic".to_owned()
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

/// One record, decoded the way `ingest_once` decodes it.
///
/// The two halves travel together because they come out of one pass over the
/// headers and because the tests below care about the pair: the producer context
/// the ingest span uses for Kafka trace handoff, and the user headers left once
/// the reserved and trace namespaces have been stripped.
struct Ingested {
    envelope: Envelope<ProductSnapshot>,
    producer_trace: Option<TraceContext>,
}

fn decode(headers: OwnedHeaders) -> crate::Result<Ingested> {
    let message = record(headers);
    // Header scan first, exactly as the consumer does it: the ingest span needs
    // the producer's context before the payload has been looked at.
    let scanned = RecordHeaders::of(&message);
    let producer_trace = scanned.trace_context();
    let decoded = record_envelope::<ProductSnapshot, _>(&message, &scanned)?;
    Ok(Ingested {
        envelope: decoded.envelope,
        producer_trace,
    })
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

    let envelope = decode(headers).expect("ingest must proceed").envelope;

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

    let envelope = decode(headers).unwrap().envelope;
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
        decode(headers),
        Err(Error::InvalidHeader {
            name: "kafkaman-occurred-at",
            ..
        })
    ));
}

#[test]
fn a_payload_deserializer_panic_is_quarantinable() {
    let message = record(header("kafkaman-idempotency-key", DIGEST));
    let scanned = RecordHeaders::of(&message);
    let Err(err) = record_envelope::<PanickingPayload, _>(&message, &scanned) else {
        panic!("a panicking deserializer must not unwind out of ingest");
    };

    assert!(matches!(err, Error::PayloadPanicked(_)));
    assert_eq!(
        err.ingest_failure_kind(),
        Some(kafkaman_core::ReceivedIngestFailureKind::InvalidPayload)
    );
    assert!(
        err.to_string().contains("deserialize exploded"),
        "the quarantine error should retain the panic message: {err}"
    );
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

    let envelope = decode(headers).unwrap().envelope;

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

    let decoded = decode(headers).expect("trace headers are never a reason to reject a record");

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

    let decoded = decode(headers).unwrap();
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

    let decoded = decode(headers).expect("a malformed traceparent must not reject the record");
    assert!(decoded.producer_trace.is_none());
    assert!(
        !decoded.envelope.headers.contains_key("traceparent"),
        "unusable trace context is still trace context, and still not user data"
    );
}

/// A duplicated trace header resolves to the first copy, not the last.
///
/// Kafka allows repeated keys, and the three namespaces do not agree on which
/// copy wins: user headers keep the last, reserved and trace keys keep the
/// first. The direction is what makes it a rule rather than an accident — the
/// later copy of a `traceparent` is either a mistake or an attempt to move a
/// message into a trace it does not belong to, and a consumer that took it would
/// attach its ingest span to whichever trace an attacker preferred.
#[test]
fn a_duplicated_traceparent_keeps_the_first_copy() {
    const FIRST: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const SECOND: &str = "00-11111111111111111111111111111111-2222222222222222-01";

    let headers = header("kafkaman-idempotency-key", DIGEST)
        .insert(Header {
            key: "traceparent",
            value: Some(FIRST),
        })
        .insert(Header {
            key: "traceparent",
            value: Some(SECOND),
        })
        // Case is not a way around it either: the second copy is the same
        // header as far as the namespace is concerned.
        .insert(Header {
            key: "TRACEPARENT",
            value: Some(SECOND),
        });

    let decoded = decode(headers).unwrap();
    let trace = decoded.producer_trace.expect("the first copy is usable");
    assert_eq!(trace.traceparent(), FIRST);
}

/// The grammar is the W3C one, checked at the edge rather than downstream.
///
/// Every value here looks close enough to pass a shape check and is invalid
/// under the Recommendation. Accepting one means kafkaman stores it in a column,
/// puts it back on the wire, and hands another service a trace id that does not
/// match the one its own SDK would have produced for the same trace — a
/// corruption that is essentially undebuggable from the far end.
#[test]
fn a_traceparent_that_breaks_the_w3c_grammar_is_dropped() {
    for value in [
        // Uppercase hex. The grammar is `HEXDIGLC`, and a backend comparing
        // trace ids as bytes sees a different trace.
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        // Version 00 is a closed format: nothing follows the flags.
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        // The specification's invalid all-zero ids.
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        // `ff` is a reserved, forbidden version.
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    ] {
        let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
            key: "traceparent",
            value: Some(value),
        });

        let decoded = decode(headers).expect("an invalid traceparent must never reject a record");
        assert!(
            decoded.producer_trace.is_none(),
            "{value:?} is not a W3C traceparent and must not be treated as one"
        );
        assert!(
            decoded.envelope.headers.is_empty(),
            "{value:?} is unusable trace context, which is still not user data"
        );
    }
}

/// A newer producer's `traceparent` is still usable.
///
/// The other half of the grammar rule, and the one that costs something if it is
/// wrong: the Recommendation requires forward compatibility, so a later version
/// appends fields rather than rearranging the first four. Rejecting them would
/// silently break tracing against every service that upgraded first.
#[test]
fn a_future_version_traceparent_is_still_extracted() {
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "traceparent",
        value: Some("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-something"),
    });

    let decoded = decode(headers).unwrap();
    assert!(decoded.producer_trace.is_some());
}

/// `tracestate` without a `traceparent` describes nothing.
///
/// It is a vendor list keyed to a trace, so on its own it is not a partial
/// context to be salvaged — it is a header with no referent.
#[test]
fn a_tracestate_alone_is_not_a_context() {
    let headers = header("kafkaman-idempotency-key", DIGEST).insert(Header {
        key: "tracestate",
        value: Some("vendor=1"),
    });

    let decoded = decode(headers).unwrap();
    assert!(decoded.producer_trace.is_none());
    assert!(
        !decoded.envelope.headers.contains_key("tracestate"),
        "it is still protocol context rather than application data"
    );
}

/// That this crate's failures reach APM under a declared name.
///
/// Compiler exhaustiveness already refuses a variant nobody classified. What it
/// cannot see is a classification written as a bare string rather than a
/// `problem::*` constant — `"urn:kafkaman:problem:infrastrcture"` compiles and
/// then appears in Kibana as an error group of one that nothing joins.
mod problem_types {
    use kafkaman_core::problem::ALL_PROBLEM_TYPES;
    use kafkaman_core::ProblemType as _;

    #[test]
    fn no_classification_in_this_crate_invents_a_uri() {
        assert!(
            !include_str!("error.rs").contains("\"urn:kafkaman:problem:"),
            "error.rs contains a literal problem URI; classifications must go \
             through a `kafkaman_core::problem::*` constant"
        );
    }

    #[test]
    fn every_uri_this_crate_returns_is_declared() {
        // `ALL_PROBLEM_TYPES` is hand-listed, so a constant can exist without
        // being in it. These are the ones this crate can produce.
        let samples: Vec<crate::Error> = vec![
            crate::Error::MissingPayload,
            crate::Error::MissingIdempotencyKey,
            crate::Error::PayloadPanicked("unwound".to_owned()),
            crate::Error::UnexpectedTopic {
                expected: "orders",
                actual: "products".to_owned(),
            },
            crate::Error::InvalidHeader {
                name: "kafkaman-message-id",
                message: "not a uuid".to_owned(),
            },
            crate::Error::TopicAdmin {
                topic: "orders".to_owned(),
                message: "no such topic".to_owned(),
            },
            crate::Error::ConsecutiveSkipLimitExceeded {
                limit: 3,
                partition: 0,
                offset: 42,
            },
            // The delegating arms. A transparently wrapped error keeps its own
            // classification rather than being relabelled at the boundary.
            crate::Error::Core(kafkaman_core::Error::InvalidOutboxStatus("nope".to_owned())),
            crate::Error::Sqlx(kafkaman_sqlx::Error::Handler("boom".to_owned())),
        ];

        for error in &samples {
            let uri = error.problem_type();
            assert!(
                ALL_PROBLEM_TYPES.contains(&uri),
                "{error} classified as {uri}, which is not in ALL_PROBLEM_TYPES"
            );
        }

        assert_eq!(
            crate::Error::Sqlx(kafkaman_sqlx::Error::Handler("boom".to_owned())).problem_type(),
            kafkaman_core::problem::HANDLER,
            "a wrapped handler failure must still group as a handler failure; \
             relabelling it at the transport boundary would hide the one class \
             an application owner can act on"
        );
    }
}
