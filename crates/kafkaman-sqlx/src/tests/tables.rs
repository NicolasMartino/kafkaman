use kafkaman_core::SqlIdentifier;

use crate::tests::descriptor;
use crate::{OutboxTable, ReceivedTable};

#[test]
fn long_outbox_table_names_get_distinct_state_indexes() {
    // Two message types whose outbox table names share the first 53 chars
    // would collide under plain truncation; the hash suffix keeps them apart.
    let schema = SqlIdentifier::new("kafkaman").unwrap();
    let prefix = "a".repeat(46);
    let table_a = OutboxTable::new(schema.clone(), descriptor(&format!("{prefix}_one"))).unwrap();
    let table_b = OutboxTable::new(schema, descriptor(&format!("{prefix}_two"))).unwrap();

    let idx_a = table_a.state_index_name();
    let idx_b = table_b.state_index_name();

    assert_ne!(idx_a.as_str(), idx_b.as_str());
    assert!(idx_a.as_str().len() <= SqlIdentifier::MAX_LEN);
    assert!(idx_b.as_str().len() <= SqlIdentifier::MAX_LEN);
}

#[test]
fn long_received_table_names_get_distinct_bounded_indexes() {
    // Same collision hazard as the outbox side: PostgreSQL truncates
    // identifiers at 63 bytes, so two long names must not collapse onto one
    // index name.
    let schema = SqlIdentifier::new("kafkaman").unwrap();
    let prefix = "b".repeat(46);
    let a = ReceivedTable::new(schema.clone(), descriptor(&format!("{prefix}_one"))).unwrap();
    let b = ReceivedTable::new(schema, descriptor(&format!("{prefix}_two"))).unwrap();

    for name in [
        a.idempotency_index_name(),
        a.state_index_name(),
        b.idempotency_index_name(),
        b.state_index_name(),
    ] {
        assert!(name.as_str().len() <= SqlIdentifier::MAX_LEN, "{name}");
    }
    assert_ne!(
        a.idempotency_index_name().as_str(),
        b.idempotency_index_name().as_str()
    );
    assert_ne!(a.state_index_name().as_str(), b.state_index_name().as_str());
}

#[test]
fn table_names_are_derived_per_message_type() {
    let schema = SqlIdentifier::new("app").unwrap();
    let outbox = OutboxTable::new(schema.clone(), descriptor("order_created")).unwrap();
    let received = ReceivedTable::new(schema, descriptor("order_created")).unwrap();

    assert_eq!(outbox.qualified_name(), "\"app\".\"outbox_order_created\"");
    assert_eq!(
        received.qualified_name(),
        "\"app\".\"received_order_created\""
    );
}

/// `ReceivedTable::new` substitutes library defaults for the configured retry
/// policy, and `for_descriptor` is the constructor that does not.
///
/// This is the difference that made a whole configuration section stop working.
/// `new` takes a schema and a descriptor, which is exactly the shape a generic
/// "build me a table" helper wants, so the runtime builder used it — and every
/// service booted the blessed way silently ignored its own `[retry]` section,
/// retrying on library defaults instead. Nothing failed: a default policy
/// retries perfectly well, just not the way the operator asked, and only a
/// deliberately failing handler makes the difference visible.
#[test]
fn the_retry_policy_comes_from_the_config_only_through_for_descriptor() {
    use kafkaman_config::Config;

    use crate::ResolvedConfig;

    let config = Config::parse(
        r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "w"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "50ms"

        [retry.defaults]
        max_attempts = 5
        initial_backoff = "250ms"
        max_backoff = "10s"
        multiplier = 2.0
        errors_limit = 16
        dlq = "table"

        [retry.messages.order_created]
        max_attempts = 8
        "#,
    )
    .unwrap();
    let cfg =
        ResolvedConfig::from_config(Some(&config), std::iter::once(descriptor("order_created")))
            .unwrap();

    let configured = ReceivedTable::for_descriptor(&cfg, descriptor("order_created")).unwrap();
    assert_eq!(
        configured.retry.max_attempts, 8,
        "the per-message override is the whole point of declaring one"
    );
    assert_eq!(
        configured.retry.initial_backoff,
        std::time::Duration::from_millis(250),
        "fields the override leaves out inherit from [retry.defaults], not from \
         the library"
    );
    assert_eq!(
        configured.retry.max_backoff,
        std::time::Duration::from_secs(10)
    );
    assert_eq!(configured.retry.errors_limit, 16);

    // The trap, stated so that reaching for the shorter constructor on a path
    // that dispatches is a visible choice rather than an accident.
    let unconfigured = ReceivedTable::new(cfg.schema, descriptor("order_created")).unwrap();
    assert_eq!(
        unconfigured.retry,
        kafkaman_config::RetryPolicy::default(),
        "`new` cannot see the config, so it fills in library defaults; only use \
         it where the table's name is all that is wanted"
    );
    assert_ne!(
        unconfigured.retry.max_attempts, configured.retry.max_attempts,
        "if these ever agree this test is proving nothing"
    );
}
