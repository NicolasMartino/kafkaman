//! The redrive request contract.

use super::*;

#[test]
fn redrive_request_rejects_unknown_fields() {
    // A misspelled field on a destructive route must fail loudly rather than
    // quietly doing something other than what the caller asked for.
    let typo = serde_json::json!({ "max_rows": 10, "clearHistory": true });
    assert!(serde_json::from_value::<RedriveRequest>(typo).is_err());

    let ok = serde_json::json!({ "max_rows": 10, "clear_history": true });
    let parsed: RedriveRequest = serde_json::from_value(ok).expect("valid body should parse");
    assert_eq!(parsed.max_rows, 10);
    assert!(parsed.clear_history);
    assert!(parsed.failure_kind.is_none());
}

#[test]
fn redrive_accepts_the_failure_kind_dlq_inspection_prints() {
    // The two halves of one operator workflow: read the DLQ, redrive part of
    // it. Inspection renders the failure as an RFC 9457 `type` URI, so a
    // request body that only accepted the bare discriminant would reject the
    // exact string the operator just copied — and the way out of that is to
    // drop the filter, which redrives the whole queue.
    let printed = ReceivedError::new(
        ReceivedFailureKind::InvalidPayload,
        "boom",
        OffsetDateTime::UNIX_EPOCH,
        Some(kafkaman_core::FailureStage::Handler),
    );
    let printed = serde_json::to_value(&printed).expect("a problem detail should serialize");
    let printed = printed["type"].as_str().expect("RFC 9457 names it `type`");

    for spelling in [printed, "InvalidPayload"] {
        let body = serde_json::json!({ "max_rows": 1, "failure_kind": spelling });
        let parsed: RedriveRequest =
            serde_json::from_value(body).expect("both spellings name one failure class");
        assert_eq!(
            parsed.failure_kind,
            Some(ReceivedFailureKind::InvalidPayload)
        );
    }
}

#[test]
fn redrive_refuses_a_failure_kind_it_does_not_recognize() {
    // Strict where the audit reader is lenient, and deliberately so: this
    // value decides which rows a destructive request touches. Degrading an
    // unknown kind to the default would redrive a different failure class
    // than the operator named, and report success.
    let body = serde_json::json!({ "max_rows": 1, "failure_kind": "Handlr" });
    let err = serde_json::from_value::<RedriveRequest>(body)
        .expect_err("a misspelled failure class must not silently become another one");
    assert!(
        err.to_string().contains("Handler"),
        "the error should name the vocabulary, got {err}"
    );
}

#[test]
fn redrive_request_defaults_clear_history_to_false() {
    let parsed: RedriveRequest =
        serde_json::from_value(serde_json::json!({ "max_rows": 1 })).unwrap();
    assert!(
        !parsed.clear_history,
        "history is what triage reads; dropping it must be explicit"
    );
}
