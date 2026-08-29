use std::collections::BTreeMap;

use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    IdempotencyKey, OutboxRow, OutboxStatus, ReceiveStatus, ReceivedError, ReceivedFailureKind,
    ReceivedRow, TraceContext,
};

#[test]
fn received_error_serializes_as_an_rfc9457_problem_detail() {
    let occurred_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let error = ReceivedError::new(ReceivedFailureKind::Handler, "boom", occurred_at, None);
    let json = serde_json::to_value(&error).unwrap();

    assert_eq!(json["type"], "urn:kafkaman:problem:handler");
    assert_eq!(json["title"], "Handler returned an error");
    assert_eq!(json["detail"], "boom");
    assert_eq!(json["occurred_at"], "2023-11-14T22:13:20Z[UTC]");
    // RFC 9457's `status` is an HTTP status code and is deliberately absent.
    assert!(json.get("status").is_none());
}

#[test]
fn received_error_reads_pre_problem_detail_rows() {
    // Rows written before the problem-detail format used `kind`/`message`
    // with a bare RFC 3339 timestamp. They must stay readable.
    let legacy = serde_json::json!({
        "kind": "InvalidPayload",
        "message": "could not decode",
        "occurred_at": "2023-11-14T22:13:20Z",
    });
    let error: ReceivedError = serde_json::from_value(legacy).unwrap();

    assert_eq!(error.kind, ReceivedFailureKind::InvalidPayload);
    assert_eq!(error.detail, "could not decode");
    assert_eq!(
        error.occurred_at,
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    );
}

#[test]
fn unknown_problem_type_degrades_to_the_default_kind() {
    // A newer build may write a failure class this binary does not know.
    // Reading the audit trail must not fail because of it.
    let forward = serde_json::json!({
        "type": "urn:kafkaman:problem:not-yet-invented",
        "title": "Something new",
        "detail": "boom",
        "occurred_at": "2023-11-14T22:13:20Z[UTC]",
    });
    let error: ReceivedError = serde_json::from_value(forward).unwrap();
    assert_eq!(error.kind, ReceivedFailureKind::default());
}

#[test]
fn record_key_falls_back_from_partition_key_to_entity_key() {
    let mut row = outbox_row_fixture();

    // A declared partition key wins: the type chose how to co-locate.
    assert_eq!(row.record_key(), Some("eu-west"));

    // With no declared partition key the entity key must be used, or Kafka
    // would scatter one entity across partitions and the offset convergence
    // guard could never compare its snapshots.
    row.partition_key = None;
    assert_eq!(row.record_key(), Some("product-1"));

    row.entity_key = None;
    assert_eq!(row.record_key(), None);
}

fn outbox_row_fixture() -> OutboxRow {
    OutboxRow {
        message_id: Uuid::nil(),
        idempotency_key: None,
        idempotency_source: None,
        status: OutboxStatus::Pending,
        attempts: 0,
        next_attempt_at: OffsetDateTime::UNIX_EPOCH,
        last_error: None,
        claim_id: None,
        claimed_by: None,
        claim_expires_at: None,
        topic: "products".to_owned(),
        partition_key: Some("eu-west".to_owned()),
        entity_key: Some("product-1".to_owned()),
        correlation_id: Uuid::nil(),
        causation_id: None,
        trace: None,
        headers: BTreeMap::new(),
        payload: serde_json::Value::Null,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        created_at: OffsetDateTime::UNIX_EPOCH,
        published_at: None,
    }
}

/// An outbox row as JSON, with `traceparent` set to whatever a column actually
/// holds — including values the constructor would refuse.
///
/// Serializing a real row cannot produce one, which is the point: the stored
/// shape is the only place a context that fails today's grammar can come from,
/// and it is exactly the shape a read has to survive.
pub(super) fn outbox_row_json(traceparent: &str) -> serde_json::Value {
    let mut value =
        serde_json::to_value(outbox_row_fixture()).expect("an outbox row should serialize");
    value["trace"] = serde_json::json!({ "traceparent": traceparent });
    value
}

#[test]
fn a_row_carries_trace_context_as_two_columns_and_back() {
    // The round trip the durable gap depends on. Enqueue captures a context and
    // writes two columns; the relay reads them back minutes later and parents
    // the publish span from the result. Anything lost in between is a trace that
    // stops at the outbox — which is precisely the break the whole design exists
    // to close, and it would be invisible in any test that only checked the
    // columns were written.
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let mut row = outbox_row_fixture();
    row.trace = TraceContext::from_parts(Some(traceparent.to_owned()), Some("vendor=1".to_owned()));

    let trace = row.trace.as_ref().expect("the fixture set a valid context");
    let restored = TraceContext::from_parts(
        Some(trace.traceparent().to_owned()),
        trace.tracestate().map(ToOwned::to_owned),
    )
    .expect("what a column round trip yields must parse again");

    assert_eq!(&restored, trace, "the columns are the whole context");
    assert_eq!(restored.traceparent(), traceparent);
    assert_eq!(restored.tracestate(), Some("vendor=1"));
}

#[test]
fn a_row_without_trace_context_is_an_ordinary_row() {
    // Absent context is normal, not an error: a row enqueued outside any span
    // has none, and a build with `traces` off produces none at all. Nothing
    // about the row's identity or routing may depend on it.
    let row = outbox_row_fixture();
    assert!(row.trace.is_none());
    assert_eq!(row.record_key(), Some("eu-west"));
}

#[test]
fn a_corrupt_stored_traceparent_reads_as_no_context() {
    // Columns are written by a previous version, by a migration, or by hand
    // during an incident. A value that is not a well-formed `traceparent` is
    // dropped rather than propagated: putting it on the wire would corrupt the
    // trace for every downstream service, and refusing to read the row would
    // stop a message for a field that carries no business meaning.
    for stored in [
        "not a traceparent",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "",
    ] {
        let mut row = outbox_row_fixture();
        row.trace = TraceContext::from_parts(Some(stored.to_owned()), None);
        assert!(
            row.trace.is_none(),
            "{stored:?} should read as no context rather than as context"
        );
    }
}

#[test]
fn a_received_row_carries_its_own_ingest_context() {
    // The receive side stores the *ingest* span's context, not the producer's:
    // the producer is linked to, because a poll batch holds records from many
    // traces, while the dispatch that runs later descends from the ingest that
    // stored the row. Two different relationships, and this is the one that
    // survives in a column.
    let mut row = received_row_fixture();
    assert!(row.trace.is_none(), "an untraced ingest stores nothing");

    row.trace = TraceContext::from_parts(
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned()),
        None,
    );
    let trace = row.trace.as_ref().expect("a valid context");
    assert_eq!(trace.tracestate(), None, "no upstream set a tracestate");
    assert!(TraceContext::from_parts(Some(trace.traceparent().to_owned()), None).is_some());
}

fn received_row_fixture() -> ReceivedRow {
    ReceivedRow {
        message_id: Uuid::nil(),
        idempotency_key: IdempotencyKey::from_bytes([0; 32]),
        idempotency_source: None,
        entity_key: Some("product-1".to_owned()),
        status: ReceiveStatus::Pending,
        attempts: 0,
        next_attempt_at: None,
        errors: Vec::new(),
        source_topic: "products".to_owned(),
        source_partition: 0,
        source_offset: 0,
        key: None,
        message_type: "product_snapshot".to_owned(),
        message_version: 1,
        headers: BTreeMap::new(),
        payload: serde_json::Value::Null,
        correlation_id: None,
        causation_id: None,
        trace: None,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        created_at: OffsetDateTime::UNIX_EPOCH,
        processed_at: None,
    }
}

/// A stored problem detail carries its stage, and a row without one still reads.
#[test]
fn a_problem_detail_round_trips_with_and_without_a_stage() {
    let occurred_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

    let staged = ReceivedError::new(
        ReceivedFailureKind::Infrastructure,
        "pool closed",
        occurred_at,
        Some(crate::FailureStage::Handler),
    );
    let json = serde_json::to_value(&staged).unwrap();
    assert_eq!(json["stage"], "handler");
    assert_eq!(
        serde_json::from_value::<ReceivedError>(json).unwrap(),
        staged,
        "the blame axis must survive a round trip; it is what the class stopped \
         carrying"
    );

    // Rows written before the field existed omit it, and a row written now with
    // no stage omits it again — so the absence is not turned into a value that
    // claims a frame nobody recorded.
    let unstaged = ReceivedError::new(ReceivedFailureKind::Handler, "boom", occurred_at, None);
    let json = serde_json::to_value(&unstaged).unwrap();
    assert!(
        json.get("stage").is_none(),
        "an absent stage must not be serialized as null: {json}"
    );

    let legacy = serde_json::json!({
        "type": "urn:kafkaman:problem:handler",
        "title": "Handler returned an error",
        "detail": "boom",
        "occurred_at": "2023-11-14T22:13:20Z[UTC]",
    });
    let parsed: ReceivedError = serde_json::from_value(legacy).unwrap();
    assert_eq!(
        parsed.stage, None,
        "a row from before this reads as unstaged"
    );
}
