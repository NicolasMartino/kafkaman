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

#[test]
fn a_lease_shorter_than_a_poll_is_legal_and_worth_saying_why() {
    // Not validated, and deliberately: a lease shorter than the poll interval
    // means a claim can expire before the loop comes back for it, which reads
    // like a misconfiguration but is a legitimate one. The lease bounds how long
    // a *crashed* worker holds a row, and an operator who wants fast recovery
    // over few reclaims is entitled to that trade. Rejecting it would forbid a
    // deployment for having priorities the library disagrees with.
    let short_lease = RelayConfig {
        lease_for: Duration::from_millis(1),
        poll_interval: Duration::from_secs(60),
        ..RelayConfig::default()
    };
    assert!(short_lease.validate().is_ok());
}

#[test]
fn the_smallest_legal_values_are_legal() {
    // The boundary itself, not just a value past it. `is_zero` is the test in
    // `validate`, so one nanosecond and one row must pass — an off-by-one that
    // rejected them would only be found by whoever configured them.
    let minimal = RelayConfig {
        lease_for: Duration::from_nanos(1),
        poll_interval: Duration::from_nanos(1),
        batch_limit: 1,
        retry_after: Duration::ZERO,
        ..RelayConfig::default()
    };
    assert!(minimal.validate().is_ok());
}

#[test]
fn a_negative_batch_limit_is_rejected_like_a_zero_one() {
    // `batch_limit` is `i64` because it goes into a SQL `LIMIT`. Negative is not
    // a smaller batch; Postgres rejects it, so a relay configured this way would
    // fail every cycle at the database instead of once at startup.
    let negative = RelayConfig {
        batch_limit: -1,
        ..RelayConfig::default()
    };
    let err = negative
        .validate()
        .expect_err("a negative batch limit can never claim a row");
    assert!(
        err.to_string().contains("batch_limit"),
        "the error should name the field to fix, got: {err}"
    );
}

#[test]
fn validation_names_the_field_that_is_wrong() {
    // Each of these is a separate line in a config file, and an error that does
    // not say which line is an error that sends the operator to the source.
    for (cfg, field) in [
        (
            RelayConfig {
                lease_for: Duration::ZERO,
                ..RelayConfig::default()
            },
            "lease_for",
        ),
        (
            RelayConfig {
                poll_interval: Duration::ZERO,
                ..RelayConfig::default()
            },
            "poll_interval",
        ),
        (
            RelayConfig {
                batch_limit: 0,
                ..RelayConfig::default()
            },
            "batch_limit",
        ),
    ] {
        let err = cfg.validate().expect_err("{field} is invalid");
        assert!(
            err.to_string().contains(field),
            "the error should name {field}, got: {err}"
        );
    }
}
