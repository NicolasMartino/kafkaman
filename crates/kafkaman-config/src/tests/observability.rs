use std::time::Duration;

use crate::{Config, HeaderLogging, LifecycleLogging, ObservabilityLevel, PayloadLogging};

#[test]
fn observability_config_merges_and_validates_registered_messages() {
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        level = "info"
        lifecycle = "summary"
        payload = "off"
        headers = "kafkaman-only"
        sample_success = 0.0
        stuck_after = "30s"
        max_queue_age = "5m"

        [observability.messages.order_created]
        level = "debug"
        lifecycle = "per-message"
        sample_success = 0.25
        "#,
    )
    .unwrap();

    let observability = cfg.observability_config(["order_created"]).unwrap();
    assert_eq!(observability.defaults.level, ObservabilityLevel::Info);
    assert_eq!(observability.defaults.lifecycle, LifecycleLogging::Summary);
    assert_eq!(observability.defaults.payload, PayloadLogging::Off);
    assert_eq!(observability.defaults.headers, HeaderLogging::KafkamanOnly);

    let policy = observability.policy_for("order_created");
    assert_eq!(policy.level, ObservabilityLevel::Debug);
    assert_eq!(policy.lifecycle, LifecycleLogging::PerMessage);
    assert_eq!(policy.payload, PayloadLogging::Off);
    assert_eq!(policy.sample_success, 0.25);
    assert_eq!(policy.stuck_after, Duration::from_secs(30));
    assert_eq!(policy.max_queue_age, Duration::from_secs(5 * 60));
}

#[test]
fn observability_config_rejects_bad_values_and_unknown_message() {
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        sample_success = 1.5
        stuck_after = "0s"
        max_queue_age = "0s"

        [observability.messages.unregistered]
        level = "debug"
        "#,
    )
    .unwrap();

    let rendered = cfg
        .observability_config(["order_created"])
        .expect_err("invalid observability config must fail")
        .to_string();
    assert!(rendered.contains("observability.defaults.sample_success"));
    assert!(rendered.contains("observability.defaults.stuck_after"));
    assert!(rendered.contains("observability.defaults.max_queue_age"));
    assert!(rendered.contains("observability.messages.unregistered"));
}
