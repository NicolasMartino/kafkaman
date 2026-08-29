use std::time::Duration;

use kafkaman_core::LifecycleEmission;

use crate::tests::VALID;
use crate::Config;

/// The relay's poll interval, used as the fallback below.
const RELAY_POLL: Duration = Duration::from_millis(250);

#[test]
fn an_absent_section_resolves_to_the_behaviour_that_predates_it() {
    let section = Config::parse(VALID).unwrap().dispatcher().unwrap();
    let cfg = section
        .into_dispatcher_config(LifecycleEmission::default(), RELAY_POLL)
        .unwrap();

    assert_eq!(
        cfg.poll_interval, RELAY_POLL,
        "the dispatcher read `relay.poll_interval` before this section existed, \
         so an absent section must keep reading it"
    );
    assert_eq!(cfg.max_consecutive_panicking_rows, 10);
}

#[test]
fn each_field_is_optional_on_its_own() {
    let cfg = Config::parse(
        r#"
            [dispatcher]
            max_consecutive_panicking_rows = 3
            "#,
    )
    .unwrap()
    .dispatcher()
    .unwrap()
    .into_dispatcher_config(LifecycleEmission::default(), RELAY_POLL)
    .unwrap();

    assert_eq!(cfg.max_consecutive_panicking_rows, 3);
    assert_eq!(
        cfg.poll_interval, RELAY_POLL,
        "setting one field must not silently reset the other to a library default"
    );
}

#[test]
fn a_declared_poll_interval_overrides_the_relay_fallback() {
    let cfg = Config::parse(
        r#"
            [dispatcher]
            poll_interval = "10ms"
            "#,
    )
    .unwrap()
    .dispatcher()
    .unwrap()
    .into_dispatcher_config(LifecycleEmission::default(), RELAY_POLL)
    .unwrap();

    assert_eq!(cfg.poll_interval, Duration::from_millis(10));
}

/// Zero reads as "never trip", and there is no way to spell that. Clamping to
/// one would give an operator who wrote it the exact opposite: a dispatcher that
/// stops on the first panicking row.
#[test]
fn a_zero_breaker_limit_is_reported_rather_than_clamped() {
    let err = Config::parse(
        r#"
            [dispatcher]
            max_consecutive_panicking_rows = 0
            "#,
    )
    .unwrap()
    .dispatcher()
    .unwrap()
    .into_dispatcher_config(LifecycleEmission::default(), RELAY_POLL)
    .expect_err("zero must be rejected");
    assert!(err.contains("max_consecutive_panicking_rows"), "{err}");
}

#[test]
fn a_zero_poll_interval_is_rejected() {
    let err = Config::parse(
        r#"
            [dispatcher]
            poll_interval = "0ms"
            "#,
    )
    .unwrap()
    .dispatcher()
    .unwrap()
    .into_dispatcher_config(LifecycleEmission::default(), RELAY_POLL)
    .expect_err("a zero interval turns the idle loop into a busy spin");
    assert!(err.contains("poll_interval"), "{err}");
}

#[test]
fn dispatcher_denies_unknown_fields() {
    let cfg = Config::parse(
        r#"
            [dispatcher]
            max_consecutive_handler_panics = 10
            "#,
    )
    .unwrap();
    // The old name, in particular: it described a breaker that counted panics
    // rather than rows, and silently ignoring it would leave an operator who
    // carried it forward with a limit they did not set.
    assert!(cfg
        .dispatcher()
        .unwrap_err()
        .to_string()
        .contains("max_consecutive_handler_panics"));
}
