use std::time::Duration;

use crate::{Error, PurgeConfig};

#[test]
fn purge_config_rejects_settings_that_delete_live_rows_or_spin() {
    assert!(PurgeConfig::default().validate().is_ok());

    // Zero retention deletes a row the instant it goes terminal, destroying the
    // operational record while an incident is still being diagnosed. Stated per
    // field rather than inherited from a blanket rule, because relaxing exactly
    // such a rule across all durations is what silently un-guarded
    // `initial_backoff` (recorded as R3).
    let zero_window = PurgeConfig {
        older_than: Duration::ZERO,
        ..PurgeConfig::default()
    };
    assert!(matches!(
        zero_window.validate(),
        Err(Error::InvalidPurgeConfig {
            field: "older_than",
            ..
        })
    ));

    let no_batch = PurgeConfig {
        batch_size: 0,
        ..PurgeConfig::default()
    };
    assert!(matches!(
        no_batch.validate(),
        Err(Error::InvalidPurgeConfig {
            field: "batch_size",
            ..
        })
    ));

    // A zero interval turns the idle sweep into a busy loop against the
    // database, the same failure `RelayConfig` guards against.
    let no_interval = PurgeConfig {
        poll_interval: Duration::ZERO,
        ..PurgeConfig::default()
    };
    assert!(matches!(
        no_interval.validate(),
        Err(Error::InvalidPurgeConfig {
            field: "poll_interval",
            ..
        })
    ));
}

#[test]
fn purge_defaults_retain_a_week_and_spare_the_audit_trail() {
    let cfg = PurgeConfig::default();
    assert_eq!(cfg.older_than, Duration::from_secs(7 * 24 * 60 * 60));
    assert!(
        !cfg.include_failed,
        "Failed rows are the invalid-send audit trail and have no successor \
             carrying the same information, so they are never reclaimed by default"
    );
}
