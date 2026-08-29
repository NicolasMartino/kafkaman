use std::time::Duration;

use crate::{DispatcherConfig, Error};

#[test]
fn the_default_config_is_valid() {
    DispatcherConfig::default()
        .validate()
        .expect("the defaults must be usable without any config file");
}

/// Ten distinct rows, matching `RdkafkaConsumer::DEFAULT_MAX_CONSECUTIVE_SKIPS`
/// so the two poison breakers in the system behave alike.
///
/// The number used to matter in a way it no longer does. When the breaker
/// counted panics rather than rows it collided with
/// `RetryPolicy::default().max_attempts`, which is also 10: one poison row spent
/// its budget, produced ten panics with no successful claim between them, and
/// tripped the breaker on the very attempt that dead-lettered it. Counting
/// distinct rows removes the collision rather than tuning around it, which is
/// why this can safely stay at 10.
#[test]
fn the_default_breaker_limit_matches_the_ingest_side() {
    assert_eq!(
        DispatcherConfig::default().max_consecutive_panicking_rows,
        10
    );
}

#[test]
fn a_zero_poll_interval_is_rejected() {
    let cfg = DispatcherConfig {
        poll_interval: Duration::ZERO,
        ..Default::default()
    };
    assert!(matches!(
        cfg.validate(),
        Err(Error::InvalidDispatcherConfig {
            field: "poll_interval",
            ..
        })
    ));
}

/// Rejected rather than clamped to one. An operator writing zero means "never
/// trip"; clamping would give them a dispatcher that stops on the first
/// panicking row, which is the opposite of what they asked for.
#[test]
fn a_zero_breaker_limit_is_rejected_rather_than_clamped() {
    let cfg = DispatcherConfig {
        max_consecutive_panicking_rows: 0,
        ..Default::default()
    };
    assert!(matches!(
        cfg.validate(),
        Err(Error::InvalidDispatcherConfig {
            field: "max_consecutive_panicking_rows",
            ..
        })
    ));
}
