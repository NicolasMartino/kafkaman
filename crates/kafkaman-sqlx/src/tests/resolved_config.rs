use kafkaman_core::MessageDescriptor;

use crate::tests::{descriptor, minimal_config};
use crate::{Error, ResolvedConfig};

#[test]
fn re_registering_a_message_type_under_a_new_topic_is_rejected() {
    // Silently keeping the first topic would publish to a topic the caller
    // never asked for. Nothing fails at startup and nothing fails at publish
    // time; the only symptom is a consumer somewhere that stops receiving.
    let cfg = ResolvedConfig::default().with_message(descriptor("order_created"));

    let conflicting = MessageDescriptor::new("order_created", "orders_v2").unwrap();
    let err = cfg
        .clone()
        .try_with_message(conflicting)
        .expect_err("a topic change must be reported");
    assert!(matches!(
        err,
        Error::ConflictingMessageType {
            ref message_type,
            ref registered,
            ref conflicting,
        } if message_type == "order_created"
            && registered == "topic"
            && conflicting == "orders_v2"
    ));

    // An identical re-registration is still idempotent, not an error: a
    // changelog assembled from several modules may legitimately name the
    // same type twice.
    let same = cfg
        .try_with_message(descriptor("order_created"))
        .expect("an identical descriptor is a duplicate, not a conflict");
    assert_eq!(same.messages().len(), 1);
}

#[test]
fn registering_the_same_message_type_twice_is_idempotent() {
    let cfg = ResolvedConfig::default()
        .with_message(descriptor("order_created"))
        .with_message(descriptor("order_created"));
    assert_eq!(cfg.messages().len(), 1);
}

#[test]
fn resolved_config_from_config_validates_before_runtime_use() {
    let cfg = minimal_config();

    let resolved = ResolvedConfig::from_config(Some(&cfg), [descriptor("order_created")])
        .expect("valid config resolves");
    assert_eq!(resolved.schema().as_str(), "kafkaman");
    assert_eq!(resolved.relay.worker_id, "worker-a");
    assert_eq!(resolved.messages().len(), 1);

    let trivial = ResolvedConfig::from_config(None, std::iter::empty()).unwrap();
    assert!(trivial.messages().is_empty());

    let err = ResolvedConfig::from_config(None, [descriptor("order_created")]).unwrap_err();
    assert!(err.to_string().contains("missing config file"));
}

#[test]
fn an_unregistered_message_type_is_refused_a_descriptor() {
    // Refusing here is what stops a query being built against a table no
    // changeset ever created.
    let cfg = ResolvedConfig::default();
    let err = cfg
        .descriptor_for::<crate::tests::OrderCreated>()
        .expect_err("an unregistered type has no tables");
    assert!(matches!(err, Error::UnknownMessageType(ref name) if name == "order_created"));
}

#[test]
fn the_shipped_example_config_resolves_and_covers_every_section() {
    // `kafkaman.example.toml` is the only place a crate user learns what knobs
    // exist, and nothing exercised it — which is how the `[retention]` section
    // came to be missing from it without anyone noticing. Loading it here makes
    // it executable documentation: a knob renamed in code, or a section added
    // without being documented, fails this test.
    //
    // `deny_unknown_fields` on every section means the reverse also holds —
    // documenting a knob that does not exist fails to parse.
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../kafkaman.example.toml");
    let cfg = kafkaman_config::Config::from_path(path).expect("the shipped example must parse");

    // Resolve against the message type the example's per-type retry override
    // names, since an unregistered type is itself a configuration error.
    let resolved = ResolvedConfig::from_config(
        Some(&cfg),
        [MessageDescriptor::new("order_created", "orders").unwrap()],
    )
    .expect("the shipped example must resolve");

    assert_eq!(resolved.schema().as_str(), "kafkaman");
    resolved
        .relay
        .validate()
        .expect("the example's relay section must be valid");

    // The per-type override must actually differ from the defaults, or the
    // example is not demonstrating what it claims to.
    let defaults = resolved.retry.policy_for("unregistered_type");
    let overridden = resolved.retry.policy_for("order_created");
    assert_ne!(
        defaults.max_attempts, overridden.max_attempts,
        "the example's per-type override should demonstrate an actual override"
    );

    let default_observability = resolved.observability.policy_for("unregistered_type");
    let overridden_observability = resolved.observability.policy_for("order_created");
    assert_ne!(
        default_observability.sample_success, overridden_observability.sample_success,
        "the example's per-type observability override should demonstrate an actual override"
    );

    // Every optional section the crate reads must be present and valid, or the
    // example is not the complete knob list it claims to be.
    cfg.retention()
        .expect("retention must parse")
        .expect("the example must document the [retention] section")
        .into_purge_config()
        .expect("the example's retention section must be valid");
}

#[test]
fn from_config_rejects_a_message_type_registered_under_two_topics() {
    // `from_config` is the path every application takes. Registering the
    // conflict-checking helper but calling the silent one here would leave
    // the misconfiguration exactly as invisible as before: the second topic
    // is dropped, the type publishes to the first, and nothing reports it
    // until a consumer of the second topic notices it receives nothing.
    let cfg = minimal_config();

    let err = ResolvedConfig::from_config(
        Some(&cfg),
        [
            MessageDescriptor::new("order_created", "orders").unwrap(),
            MessageDescriptor::new("order_created", "orders_v2").unwrap(),
        ],
    )
    .expect_err("two topics for one message type must be rejected");

    assert!(
        matches!(
            err,
            Error::ConflictingMessageType {
                ref message_type,
                ref registered,
                ref conflicting,
            } if message_type == "order_created"
                && registered == "orders"
                && conflicting == "orders_v2"
        ),
        "expected ConflictingMessageType, got {err:?}"
    );

    // The identical-descriptor case stays idempotent: a changelog assembled
    // from several modules may legitimately name the same type twice.
    let resolved = ResolvedConfig::from_config(
        Some(&cfg),
        [descriptor("order_created"), descriptor("order_created")],
    )
    .expect("an identical descriptor is a duplicate, not a conflict");
    assert_eq!(resolved.messages().len(), 1);
}

#[test]
fn the_topic_mode_resolves_with_everything_else() {
    // Reading `[topics]` at the call site instead would mean a mistyped mode
    // surfaced later and separately from the rest of the config report — after
    // a pool was already open, which is exactly what this crate promises not to
    // do.
    use kafkaman_core::TopicMode;

    // Absent means `verify`, not "skip": a service publishing entity snapshots
    // onto an uncompacted topic cannot rebuild them, and a silent default would
    // preserve that failure.
    let cfg = ResolvedConfig::from_config(Some(&minimal_config()), [descriptor("order_created")])
        .expect("a config without a [topics] section still resolves");
    assert_eq!(cfg.topics, TopicMode::Verify);

    let explicit = kafkaman_config::Config::parse(
        r#"
            [database]
            schema = "kafkaman"

            [relay]
            worker_id = "worker-a"
            batch_limit = 10
            lease_for = "30s"
            retry_after = "1s"
            poll_interval = "250ms"

            [topics]
            mode = "create"
            "#,
    )
    .unwrap();
    let cfg = ResolvedConfig::from_config(Some(&explicit), [descriptor("order_created")])
        .expect("an explicit mode resolves");
    assert_eq!(cfg.topics, TopicMode::Create);
}

#[test]
fn an_unknown_topic_mode_fails_resolution_rather_than_defaulting() {
    // The failure that matters: `mode = "verfy"` silently taking the default
    // would look identical to a working config while checking nothing the
    // operator asked for. It must be rejected, and it must name the section.
    let cfg = kafkaman_config::Config::parse(
        r#"
            [database]
            schema = "kafkaman"

            [relay]
            worker_id = "worker-a"
            batch_limit = 10
            lease_for = "30s"
            retry_after = "1s"
            poll_interval = "250ms"

            [topics]
            mode = "verfy"
            "#,
    )
    .unwrap();

    let err = ResolvedConfig::from_config(Some(&cfg), [descriptor("order_created")])
        .expect_err("an unknown mode must be rejected");
    let message = err.to_string();
    assert!(message.contains("topics"), "{message}");
    assert!(message.contains("verfy"), "{message}");
}
