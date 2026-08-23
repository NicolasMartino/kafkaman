use std::time::Duration;

use crate::tests::VALID;
use crate::Config;

#[test]
fn retention_section_is_optional_and_validated() {
    // Absent means no purger runs and the outbox keeps growing — the status
    // quo. Making retention required would turn an upgrade into a silent
    // deletion of operational history.
    assert!(Config::parse(VALID).unwrap().retention().unwrap().is_none());

    let cfg = Config::parse(
        r#"
            [retention]
            outbox_after = "30s"
            batch_size = 500
            poll_interval = "60s"
            "#,
    )
    .unwrap();
    let purge = cfg
        .retention()
        .unwrap()
        .expect("present section parses")
        .into_purge_config()
        .unwrap();
    assert_eq!(purge.older_than, Duration::from_secs(30));
    assert_eq!(purge.batch_size, 500);
    assert!(
        !purge.include_failed,
        "the audit trail is spared unless asked for"
    );

    // Zero retention deletes rows the instant they go terminal.
    let zero = Config::parse(
        r#"
            [retention]
            outbox_after = "0s"
            batch_size = 500
            poll_interval = "60s"
            "#,
    )
    .unwrap();
    let err = zero
        .retention()
        .unwrap()
        .unwrap()
        .into_purge_config()
        .expect_err("a zero window must be rejected");
    assert!(err.contains("older_than"), "{err}");
}

#[test]
fn retention_denies_unknown_fields() {
    let cfg = Config::parse(
        r#"
            [retention]
            outbox_after = "30s"
            batch_size = 500
            poll_interval = "60s"
            reclaim_everything = true
            "#,
    )
    .unwrap();
    assert!(cfg
        .retention()
        .unwrap_err()
        .to_string()
        .contains("reclaim_everything"));
}
