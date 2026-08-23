use std::collections::BTreeMap;

use time::OffsetDateTime;
use uuid::Uuid;

use crate::{OutboxRow, OutboxStatus, ReceivedError, ReceivedFailureKind};

#[test]
fn received_error_serializes_as_an_rfc9457_problem_detail() {
    let occurred_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let error = ReceivedError::new(ReceivedFailureKind::Handler, "boom", occurred_at);
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
        headers: BTreeMap::new(),
        payload: serde_json::Value::Null,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        created_at: OffsetDateTime::UNIX_EPOCH,
        published_at: None,
    }
}
