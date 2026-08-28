use std::time::Duration;

use crate::{
    Config, HeaderLogging, KafkaTraceHandoff, LifecycleLogging, ObservabilityLevel,
    ObservabilityPolicy, PayloadLogging,
};

#[test]
fn observability_config_merges_and_validates_registered_messages() {
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        level = "info"
        lifecycle = "summary"
        payload = "off"
        headers = "kafkaman-only"
        kafka_trace_handoff = "linked"
        sample_success = 0.0
        stuck_after = "30s"
        max_queue_age = "5m"

        [observability.messages.order_created]
        level = "debug"
        lifecycle = "per-message"
        kafka_trace_handoff = "parented"
        sample_success = 0.25
        "#,
    )
    .unwrap();

    let observability = cfg.observability_config(["order_created"]).unwrap();
    assert_eq!(observability.defaults.level, ObservabilityLevel::Info);
    assert_eq!(observability.defaults.lifecycle, LifecycleLogging::Summary);
    assert_eq!(observability.defaults.payload, PayloadLogging::Off);
    assert_eq!(observability.defaults.headers, HeaderLogging::KafkamanOnly);
    assert_eq!(
        observability.defaults.kafka_trace_handoff,
        KafkaTraceHandoff::Linked
    );

    let policy = observability.policy_for("order_created");
    assert_eq!(policy.level, ObservabilityLevel::Debug);
    assert_eq!(policy.lifecycle, LifecycleLogging::PerMessage);
    assert_eq!(policy.payload, PayloadLogging::Off);
    assert_eq!(policy.kafka_trace_handoff, KafkaTraceHandoff::Parented);
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

#[test]
fn an_absent_section_resolves_to_the_documented_defaults() {
    // `[observability]` is marked OPTIONAL in the example config, and every
    // field in it has a quiet production default — so an absent section is a
    // complete policy, not a missing key. Reading it directly used to return
    // `MissingKey` while `ResolvedConfig` quietly defaulted it, which meant the
    // section was optional in one code path and required in another.
    let cfg = Config::parse(
        r#"
        [relay]
        worker_id = "relay-1"
        batch_limit = 100
        lease_for = "30s"
        poll_interval = "1s"
        retry_after = "5s"
        "#,
    )
    .unwrap();

    assert!(
        cfg.observability().unwrap().is_none(),
        "an absent section reads as absent rather than as an error"
    );

    let observability = cfg
        .observability_config(["order_created"])
        .expect("an absent section is a valid configuration");
    assert_eq!(observability.defaults, ObservabilityPolicy::default());
    assert_eq!(
        observability.policy_for("order_created"),
        ObservabilityPolicy::default(),
        "a type with no override inherits the defaults"
    );
    assert_eq!(
        observability
            .policy_for("order_created")
            .kafka_trace_handoff,
        KafkaTraceHandoff::Linked,
        "omitting the field keeps the OpenTelemetry messaging default"
    );
    assert!(
        !observability
            .policy_for("order_created")
            .lifecycle_emission()
            .emits_success_event(),
        "the default is silent"
    );
}

#[test]
fn kafka_trace_handoff_defaults_apply_to_every_message() {
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        kafka_trace_handoff = "parented"
        "#,
    )
    .unwrap();

    let observability = cfg
        .observability_config(["product_snapshot", "order_snapshot"])
        .unwrap();
    assert_eq!(
        observability
            .policy_for("product_snapshot")
            .kafka_trace_handoff,
        KafkaTraceHandoff::Parented
    );
    assert_eq!(
        observability
            .policy_for("order_snapshot")
            .kafka_trace_handoff,
        KafkaTraceHandoff::Parented
    );
}

#[test]
fn kafka_trace_handoff_override_wins_for_one_message() {
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        kafka_trace_handoff = "parented"

        [observability.messages.order_snapshot]
        kafka_trace_handoff = "linked"
        "#,
    )
    .unwrap();

    let observability = cfg
        .observability_config(["product_snapshot", "order_snapshot"])
        .unwrap();
    assert_eq!(
        observability
            .policy_for("product_snapshot")
            .kafka_trace_handoff,
        KafkaTraceHandoff::Parented
    );
    assert_eq!(
        observability
            .policy_for("order_snapshot")
            .kafka_trace_handoff,
        KafkaTraceHandoff::Linked
    );
}

#[test]
fn an_empty_section_matches_an_absent_one() {
    // The property that lets the section be optional without a second code
    // path: writing it and leaving it empty must mean exactly what omitting it
    // means, or `Default` would be a third behaviour nobody documented.
    let empty = Config::parse("[observability]\n").unwrap();
    let absent = Config::parse("").unwrap();
    assert_eq!(
        empty.observability_config(["order_created"]).unwrap(),
        absent.observability_config(["order_created"]).unwrap()
    );
}

#[test]
fn an_unknown_enum_value_names_the_legal_ones() {
    // The error has to name the alternatives: these are closed sets written by
    // hand in a TOML file, and "invalid value" alone sends the operator to the
    // source. The variants are lowercase and hyphenated — `Warn` and
    // `per_message` are both rejected, which is worth pinning because neither
    // is an unreasonable guess.
    for (source, expected) in [
        (
            "[observability.defaults]\nlevel = \"Warn\"\n",
            "error, warn, info, debug, or trace",
        ),
        (
            "[observability.defaults]\nlifecycle = \"per_message\"\n",
            "summary or per-message",
        ),
        (
            "[observability.defaults]\npayload = \"verbose\"\n",
            "off, redacted, sampled, or full",
        ),
        (
            "[observability.defaults]\nheaders = \"kafkaman\"\n",
            "off, kafkaman-only, or all",
        ),
        (
            "[observability.defaults]\nkafka_trace_handoff = \"continued\"\n",
            "linked or parented",
        ),
    ] {
        let cfg = Config::parse(source).unwrap();
        let rendered = cfg
            .observability_config(["order_created"])
            .expect_err("an unknown enum value must fail")
            .to_string();
        assert!(
            rendered.contains(expected),
            "the error for {source:?} should name the legal values, got: {rendered}"
        );
    }
}

#[test]
fn an_unknown_key_is_rejected_rather_than_ignored() {
    // `deny_unknown_fields`, checked here because a silently ignored key is the
    // worst config failure there is: the operator sets a knob, reads it back in
    // their own file, and the behaviour never changes.
    let cfg = Config::parse("[observability.defaults]\nsample_sucess = 0.5\n").unwrap();
    assert!(cfg.observability_config(["order_created"]).is_err());
}

#[test]
fn an_override_replaces_only_the_fields_it_sets() {
    // A per-type override is a patch, not a replacement. Setting
    // `sample_success` for one message type must not quietly reset that type's
    // `stuck_after` to the built-in default and drop it below the threshold the
    // operator wrote in `[observability.defaults]`.
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        level = "debug"
        lifecycle = "per-message"
        sample_success = 0.1
        stuck_after = "45s"
        max_queue_age = "10m"

        [observability.messages.order_created]
        sample_success = 0.5
        "#,
    )
    .unwrap();

    let policy = cfg
        .observability_config(["order_created"])
        .unwrap()
        .policy_for("order_created");
    assert_eq!(policy.sample_success, 0.5, "the field the override sets");
    assert_eq!(policy.level, ObservabilityLevel::Debug);
    assert_eq!(policy.lifecycle, LifecycleLogging::PerMessage);
    assert_eq!(policy.stuck_after, Duration::from_secs(45));
    assert_eq!(policy.max_queue_age, Duration::from_secs(10 * 60));
}

#[test]
fn an_override_is_validated_where_it_is_written() {
    // The override's own fields, not the merged result: a bad default is one
    // mistake in one place, and repeating it under every type that inherits it
    // buries the actual fix. Here the default is fine and the override is not,
    // so exactly one path should be named.
    let cfg = Config::parse(
        r#"
        [observability.defaults]
        sample_success = 0.1

        [observability.messages.order_created]
        sample_success = 2.0
        "#,
    )
    .unwrap();

    let rendered = cfg
        .observability_config(["order_created"])
        .expect_err("an out-of-range override must fail")
        .to_string();
    assert!(rendered.contains("observability.messages.order_created.sample_success"));
    assert!(
        !rendered.contains("observability.defaults.sample_success"),
        "the default is valid and must not be reported, got: {rendered}"
    );
}
