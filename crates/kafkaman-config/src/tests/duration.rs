use std::time::Duration;

use crate::duration::parse_duration;

#[test]
fn every_unit_scales_correctly() {
    assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
    assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7_200));
    assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86_400));
}

#[test]
fn zero_is_expressible() {
    // `retry_after = 0` means retry immediately, which is a supported setting.
    // The fields where zero is unsafe reject it in their own validators.
    assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
    assert_eq!(parse_duration("0ms").unwrap(), Duration::ZERO);
}

#[test]
fn surrounding_whitespace_is_tolerated() {
    assert_eq!(parse_duration("  30s  ").unwrap(), Duration::from_secs(30));
}

#[test]
fn malformed_durations_are_rejected_with_a_reason() {
    // Each message names what is wrong, because this is the error an operator
    // sees when a deploy fails on a config typo.
    let cases = [
        ("", "must not be empty"),
        ("   ", "must not be empty"),
        ("30", "must include a unit"),
        ("s", "unsigned integer"),
        ("-5s", "unsigned integer"),
        ("30 s", "unit must be one of"),
        ("30y", "unit must be one of"),
    ];

    for (input, expected) in cases {
        let err = parse_duration(input).expect_err(input);
        assert!(err.contains(expected), "{input:?} reported {err:?}");
    }
}

#[test]
fn overflow_is_reported_rather_than_wrapping() {
    // A duration that silently wrapped would schedule a retry in the past and
    // turn a backoff into a hot loop.
    let err = parse_duration("999999999999999999999999999999999999999999h")
        .expect_err("an absurd duration must be rejected");
    assert!(
        err.contains("too large") || err.contains("overflow"),
        "{err}"
    );

    let err = parse_duration("9223372036854775807d").expect_err("overflow must be rejected");
    assert!(err.contains("overflow"), "{err}");
}
