use crate::{OutboxStatus, ReceiveStatus};

#[test]
fn parses_outbox_status() {
    assert_eq!(
        "Pending".parse::<OutboxStatus>().unwrap(),
        OutboxStatus::Pending
    );
    assert!("Nope".parse::<OutboxStatus>().is_err());
}

#[test]
fn status_sql_helpers_stay_aligned_with_enum() {
    for status in OutboxStatus::ALL {
        // The SQL literal, the display string, and the parser must all agree
        // on the same canonical name for every status.
        assert_eq!(status.sql_literal(), format!("'{status}'"));
        assert_eq!(status.as_str().parse::<OutboxStatus>().unwrap(), status);
    }
    assert_eq!(
        OutboxStatus::sql_literal_list(),
        "'Pending', 'Publishing', 'Published', 'Superseded', 'Failed'"
    );
}

#[test]
fn receive_status_sql_helpers_stay_aligned_with_enum() {
    // The received table's CHECK constraint is generated from this list, so a
    // variant missing from it makes a legitimate status unwritable at runtime.
    for status in ReceiveStatus::ALL {
        assert_eq!(status.sql_literal(), format!("'{status}'"));
        assert_eq!(status.as_str().parse::<ReceiveStatus>().unwrap(), status);
    }
    assert_eq!(
        ReceiveStatus::sql_literal_list(),
        "'Pending', 'Processing', 'Processed', 'Retryable', 'Failed'"
    );
}

#[test]
fn status_names_are_the_serde_representation() {
    // Row decoding parses the column with `FromStr` while the audit JSON is
    // written by serde. The two must agree, or a status stored through one path
    // is unreadable through the other.
    for status in ReceiveStatus::ALL {
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, format!("\"{}\"", status.as_str()));
    }
}

#[test]
fn status_terminality_matches_the_retention_predicate() {
    // The purge predicate and the queue-age warning must agree on which
    // statuses are finished work, or the warning fires on rows retention is
    // already free to delete.
    assert!(!OutboxStatus::Pending.is_terminal());
    assert!(!OutboxStatus::Publishing.is_terminal());
    assert!(OutboxStatus::Published.is_terminal());
    assert!(OutboxStatus::Superseded.is_terminal());
    assert!(OutboxStatus::Failed.is_terminal());

    assert!(!ReceiveStatus::Pending.is_terminal());
    assert!(!ReceiveStatus::Retryable.is_terminal());
    assert!(ReceiveStatus::Processed.is_terminal());
    // A DLQ row waits for an operator, not for the dispatcher.
    assert!(ReceiveStatus::Failed.is_terminal());
}
