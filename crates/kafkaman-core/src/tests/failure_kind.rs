use crate::{ReceivedFailureKind, ReceivedIngestFailureKind};

#[test]
fn failure_kind_discriminants_are_stable() {
    // These strings are persisted in `last_failure_kind` and feed replay
    // checksums; changing one silently invalidates migration history.
    assert_eq!(
        ReceivedFailureKind::MissingHandler.discriminant(),
        "MissingHandler"
    );
    assert_eq!(
        ReceivedFailureKind::InvalidPayload.discriminant(),
        "InvalidPayload"
    );
    assert_eq!(
        ReceivedFailureKind::Infrastructure.discriminant(),
        "Infrastructure"
    );
    assert_eq!(ReceivedFailureKind::Handler.discriminant(), "Handler");
}

#[test]
fn every_failure_kind_recovers_from_both_stored_forms() {
    // Rows written before the problem-detail format stored the bare variant
    // name; rows written after store the RFC 9457 `type` URI. Both must still
    // read, and `ALL` drives the check so a new variant cannot be added without
    // a `from_problem_type` arm.
    for kind in ReceivedFailureKind::ALL {
        assert_eq!(
            ReceivedFailureKind::from_problem_type(kind.problem_type()),
            Some(kind)
        );
        assert_eq!(
            ReceivedFailureKind::from_problem_type(kind.discriminant()),
            Some(kind)
        );
    }
    assert_eq!(ReceivedFailureKind::from_problem_type("nope"), None);
}

#[test]
fn every_ingest_failure_kind_round_trips_its_discriminant() {
    // This pair used to live in `kafkaman-sqlx`, one crate away from the
    // variants it enumerated, so a new variant compiled cleanly and failed at
    // runtime on the quarantine path. Driving from `ALL` makes that a test
    // failure instead.
    for kind in ReceivedIngestFailureKind::ALL {
        assert_eq!(
            ReceivedIngestFailureKind::from_discriminant(kind.discriminant()),
            Some(kind)
        );
    }
    assert_eq!(ReceivedIngestFailureKind::from_discriminant("nope"), None);
}
