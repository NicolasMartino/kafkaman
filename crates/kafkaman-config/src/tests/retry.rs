use std::time::Duration;

use crate::duration::parse_duration;
use crate::tests::VALID;
use crate::{Config, RetryPolicy, RetryPolicyOverride};

#[test]
fn retry_config_merges_and_validates_registered_messages() {
    let cfg = Config::parse(VALID).unwrap();
    let retry = cfg.retry_config(["order_created"]).unwrap();

    let policy = retry.policy_for("order_created");
    assert_eq!(policy.max_attempts, 7);
    assert_eq!(policy.initial_backoff, Duration::from_millis(250));
    // Not overridden, so inherited from the defaults.
    assert_eq!(policy.max_backoff, Duration::from_secs(30));
}

#[test]
fn an_override_replaces_only_the_fields_it_sets() {
    // `apply_to` is a struct literal precisely so a new field cannot be missed;
    // this pins the inheriting half of that behaviour.
    let base = RetryPolicy::default();
    let merged = RetryPolicyOverride {
        max_attempts: Some(3),
        ..RetryPolicyOverride::default()
    }
    .apply_to(base.clone());

    assert_eq!(merged.max_attempts, 3);
    assert_eq!(merged.initial_backoff, base.initial_backoff);
    assert_eq!(merged.max_backoff, base.max_backoff);
    assert_eq!(merged.multiplier, base.multiplier);
    assert_eq!(merged.errors_limit, base.errors_limit);
    assert_eq!(merged.dlq, base.dlq);

    // An empty override changes nothing at all.
    assert_eq!(RetryPolicyOverride::default().apply_to(base.clone()), base);
}

#[test]
fn retry_config_rejects_invalid_policy_and_unknown_message() {
    let cfg = Config::parse(
        r#"
            [retry.defaults]
            max_attempts = 0
            initial_backoff = "60s"
            max_backoff = "1s"
            multiplier = 0.9
            errors_limit = 0
            dlq = "table"

            [retry.messages.unregistered]
            max_attempts = 3
            "#,
    )
    .unwrap();

    let err = cfg.retry_config(["order_created"]).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("retry.defaults.max_attempts"));
    assert!(rendered.contains("retry.defaults.initial_backoff"));
    assert!(rendered.contains("retry.defaults.multiplier"));
    assert!(rendered.contains("retry.defaults.errors_limit"));
    assert!(rendered.contains("retry.messages.unregistered"));
}

#[test]
fn retry_config_rejects_a_zero_initial_backoff() {
    // Regression. `parse_duration` used to reject every zero duration, which
    // incidentally covered this field; relaxing it so `retry_after = 0` could
    // be expressed removed the only guard. A zero first backoff schedules the
    // retry at the failure instant, so the row is immediately claimable again
    // and `run_dispatcher`'s "claimed something, skip the sleep" path spins
    // against the database for the whole attempt budget.
    let cfg = Config::parse(
        r#"
            [retry.defaults]
            max_attempts = 5
            initial_backoff = "0s"
            max_backoff = "30s"
            multiplier = 2.0
            errors_limit = 16
            dlq = "table"
            "#,
    )
    .unwrap();

    let rendered = cfg
        .retry_config(["order_created"])
        .expect_err("a zero initial backoff must be rejected")
        .to_string();
    assert!(
        rendered.contains("retry.defaults.initial_backoff"),
        "{rendered}"
    );
    assert!(rendered.contains("must be greater than zero"), "{rendered}");

    // Zero remains expressible where it is meaningful, so the parser itself
    // must not have been re-tightened.
    assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
}

#[test]
fn retry_config_rejects_bad_variant_and_duration_overflow() {
    assert!(Config::parse(
        r#"
            [retry.defaults]
            max_attempts = 3
            initial_backoff = "999999999999999999999999999999999999999999h"
            max_backoff = "1s"
            multiplier = 2.0
            errors_limit = 1
            dlq = "table"
            "#
    )
    .unwrap()
    .retry()
    .is_err());

    assert!(Config::parse(
        r#"
            [retry.defaults]
            max_attempts = 3
            initial_backoff = "1s"
            max_backoff = "2s"
            multiplier = 2.0
            errors_limit = 1
            dlq = "topic"
            "#
    )
    .unwrap()
    .retry()
    .is_err());
}

#[test]
fn an_override_is_validated_against_what_it_inherits() {
    // The override alone looks fine — a 60s initial backoff is a legal
    // duration. It is only invalid once merged with a default `max_backoff`
    // it now exceeds, which is why the merged policy is what gets checked.
    let cfg = Config::parse(
        r#"
            [retry.defaults]
            max_attempts = 5
            initial_backoff = "1s"
            max_backoff = "30s"
            multiplier = 2.0
            errors_limit = 16
            dlq = "table"

            [retry.messages.order_created]
            initial_backoff = "60s"
            "#,
    )
    .unwrap();

    let rendered = cfg
        .retry_config(["order_created"])
        .expect_err("initial_backoff above the inherited max must be rejected")
        .to_string();
    assert!(
        rendered.contains("retry.messages.order_created.initial_backoff"),
        "{rendered}"
    );
}
