//! What the operator responses look like on the wire.
//!
//! The workspace does not enable `time/serde-human-readable`, so a bare
//! `OffsetDateTime` serializes as a nine-integer array. Every timestamp in
//! these responses therefore carries an explicit RFC 9557 adapter, and these
//! tests fail loudly if one is ever dropped.

use super::*;

// The workspace does not enable `time/serde-human-readable`, so a bare
// `OffsetDateTime` serializes as a nine-integer array. Every timestamp in
// these responses therefore carries an explicit RFC 9557 adapter, and these
// tests fail loudly if one is ever dropped.

fn assert_rfc3339_string(value: &serde_json::Value, field: &str) {
    let raw = value
        .get(field)
        .unwrap_or_else(|| panic!("{field} should be present"));
    let text = raw
        .as_str()
        .unwrap_or_else(|| panic!("{field} must serialize as a timestamp string, got {raw}"));
    let base = text.split_once('[').map_or(text, |(base, _)| base);
    OffsetDateTime::parse(base, &Rfc3339)
        .unwrap_or_else(|err| panic!("{field} must parse as RFC 3339: {text:?} ({err})"));
}

#[test]
fn dlq_row_summary_serializes_timestamps_as_strings() {
    let now = OffsetDateTime::now_utc();
    let row = DlqRowSummary {
        message_id: Uuid::nil(),
        entity_key: Some("entity-1".to_owned()),
        attempts: 3,
        source_topic: "orders".to_owned(),
        source_partition: 0,
        source_offset: 42,
        correlation_id: None,
        causation_id: None,
        created_at: now,
        processed_at: Some(now),
        latest_error: Some(ReceivedError::new(
            ReceivedFailureKind::Handler,
            "boom",
            now,
            Some(kafkaman_core::FailureStage::Handler),
        )),
        error_count: 1,
    };

    let value = serde_json::to_value(&row).expect("summary should serialize");
    assert_rfc3339_string(&value, "created_at");
    assert_rfc3339_string(&value, "processed_at");
    assert!(
        value.get("payload").is_none() && value.get("headers").is_none(),
        "the DLQ projection must never carry payload bodies or user headers"
    );
}

#[test]
fn dlq_row_summary_omits_a_missing_processed_at_as_null() {
    let now = OffsetDateTime::now_utc();
    let row = DlqRowSummary {
        message_id: Uuid::nil(),
        entity_key: None,
        attempts: 0,
        source_topic: "orders".to_owned(),
        source_partition: 0,
        source_offset: 0,
        correlation_id: None,
        causation_id: None,
        created_at: now,
        processed_at: None,
        latest_error: None,
        error_count: 0,
    };

    let value = serde_json::to_value(&row).expect("summary should serialize");
    assert!(value["processed_at"].is_null());
    assert_rfc3339_string(&value, "created_at");
}

#[test]
fn stuck_response_serializes_every_timestamp_as_a_string() {
    let now = OffsetDateTime::now_utc();
    let response = StuckResponse {
        outbox: vec![kafkaman_sqlx::OutboxStuckRow {
            message_type: "order_created".to_owned(),
            message_id: Uuid::nil(),
            status: kafkaman_core::OutboxStatus::Publishing,
            claimed_by: Some("worker-1".to_owned()),
            claim_expires_at: Some(now),
            created_at: now,
            age_ms: 1_000,
            stuck_for_ms: 250,
        }],
        received: vec![ReceivedStuckRow {
            message_type: "order_created".to_owned(),
            message_id: Uuid::nil(),
            status: kafkaman_core::ReceiveStatus::Retryable,
            attempts: 2,
            next_attempt_at: Some(now),
            created_at: now,
            due_at: now,
            age_ms: 2_000,
            stuck_for_ms: 500,
        }],
        truncated: false,
    };

    let value = serde_json::to_value(&response).expect("response should serialize");
    assert_rfc3339_string(&value["outbox"][0], "created_at");
    assert_rfc3339_string(&value["outbox"][0], "claim_expires_at");
    assert_rfc3339_string(&value["received"][0], "created_at");
    assert_rfc3339_string(&value["received"][0], "due_at");
    assert_rfc3339_string(&value["received"][0], "next_attempt_at");

    // Both durations reach the wire, and separately. `age_ms` alone told an
    // operator how long a message had existed while looking like it told
    // them how long the fault had lasted.
    assert_eq!(value["outbox"][0]["age_ms"], serde_json::json!(1_000));
    assert_eq!(value["outbox"][0]["stuck_for_ms"], serde_json::json!(250));
    assert_eq!(value["received"][0]["age_ms"], serde_json::json!(2_000));
    assert_eq!(value["received"][0]["stuck_for_ms"], serde_json::json!(500));
}

#[test]
fn status_summaries_serialize_timestamps_and_the_queue_age_flag() {
    let now = OffsetDateTime::now_utc();
    let outbox = kafkaman_sqlx::OutboxStatusSummary {
        message_type: "order_created".to_owned(),
        status: kafkaman_core::OutboxStatus::Pending,
        count: 7,
        oldest_created_at: Some(now),
        oldest_age_ms: Some(5),
        over_max_queue_age: true,
    };
    let value = serde_json::to_value(&outbox).expect("summary should serialize");
    assert_rfc3339_string(&value, "oldest_created_at");
    assert_eq!(value["over_max_queue_age"], serde_json::json!(true));

    let empty = kafkaman_sqlx::ReceivedStatusSummary {
        message_type: "order_created".to_owned(),
        status: kafkaman_core::ReceiveStatus::Processed,
        count: 0,
        oldest_created_at: None,
        oldest_age_ms: None,
        over_max_queue_age: false,
    };
    let value = serde_json::to_value(&empty).expect("summary should serialize");
    assert!(value["oldest_created_at"].is_null());
}
