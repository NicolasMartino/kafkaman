use std::time::Duration;

use crate::RelayConfig;

#[test]
fn relay_config_rejects_unsafe_durations() {
    let cfg = RelayConfig::default();
    assert!(cfg.validate().is_ok());

    let zero_lease = RelayConfig {
        lease_for: Duration::from_secs(0),
        ..RelayConfig::default()
    };
    assert!(zero_lease.validate().is_err());

    let zero_poll = RelayConfig {
        poll_interval: Duration::from_secs(0),
        ..RelayConfig::default()
    };
    assert!(zero_poll.validate().is_err());

    let no_batch = RelayConfig {
        batch_limit: 0,
        ..RelayConfig::default()
    };
    assert!(no_batch.validate().is_err());
}

#[test]
fn a_zero_retry_after_stays_legal() {
    // Retry-immediately is a legitimate setting, and unlike the other durations
    // it cannot spin: a failed publish has already cost a broker round trip.
    let immediate = RelayConfig {
        retry_after: Duration::ZERO,
        ..RelayConfig::default()
    };
    assert!(immediate.validate().is_ok());
}
