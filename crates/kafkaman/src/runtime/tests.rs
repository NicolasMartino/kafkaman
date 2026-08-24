//! Builder validation, all of it reachable without a database or a broker.
//!
//! That is the property being tested as much as the individual messages: a
//! declaration mistake must be caught before `build()` opens a connection,
//! because the alternative is a service that dials Postgres and Kafka in order
//! to discover it was misconfigured locally.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_config::Config;
use kafkaman_core::KafkaMessage;
use serde::{Deserialize, Serialize};

use super::{BuildError, RuntimeBuilder};

#[derive(Serialize, Deserialize)]
struct OrderSnapshot;

impl KafkaMessage for OrderSnapshot {
    const MESSAGE_TYPE: &'static str = "order_snapshot";
    const TOPIC: &'static str = "orders";

    fn entity_key(&self) -> String {
        "order".to_owned()
    }
}

#[derive(Serialize, Deserialize)]
struct ProductSnapshot;

impl KafkaMessage for ProductSnapshot {
    const MESSAGE_TYPE: &'static str = "product_snapshot";
    const TOPIC: &'static str = "products";

    fn entity_key(&self) -> String {
        "product".to_owned()
    }
}

fn config() -> Config {
    Config::parse(
        r#"
            [database]
            schema = "kafkaman"

            [relay]
            worker_id = "worker-a"
            batch_limit = 10
            lease_for = "30s"
            retry_after = "1s"
            poll_interval = "250ms"
            "#,
    )
    .unwrap()
}

/// `build()` is async, but every assertion here resolves before the first
/// `.await` that would touch the network. Driving the future to completion on a
/// current-thread runtime with no I/O driver would panic if anything dialled
/// out, which is the point.
fn build_error(builder: RuntimeBuilder) -> BuildError {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(builder.build())
        .expect_err("this builder must not produce a runtime")
}

// ---------------------------------------------------------------------------
// Missing inputs
// ---------------------------------------------------------------------------

#[test]
fn a_builder_with_no_roles_is_refused() {
    let error = build_error(RuntimeBuilder::new().config(config()));
    assert!(matches!(error, BuildError::NoRoles), "{error:?}");

    let rendered = error.to_string();
    assert!(
        rendered.contains("publish") && rendered.contains("handle_before"),
        "the message must list the roles available: {rendered}"
    );
}

/// No discovery fallback, and the message has to say why rather than leaving the
/// caller looking for the setter that would enable it.
#[test]
fn config_is_required_and_the_message_explains_the_absent_discovery() {
    let error = build_error(RuntimeBuilder::new().publish::<OrderSnapshot>());
    assert!(matches!(error, BuildError::MissingConfig), "{error:?}");

    let rendered = error.to_string();
    assert!(rendered.contains("RuntimeBuilder::config"), "{rendered}");
    assert!(
        rendered.contains("discover"),
        "the message must say discovery is deliberately absent: {rendered}"
    );
}

#[test]
fn a_pool_is_required_and_the_message_explains_why_it_is_not_a_url() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .publish::<OrderSnapshot>(),
    );
    assert!(matches!(error, BuildError::MissingPool), "{error:?}");

    let rendered = error.to_string();
    assert!(rendered.contains("RuntimeBuilder::pool"), "{rendered}");
    assert!(
        rendered.contains("pool sizing"),
        "the message must name what the host keeps: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// Conflicting roles
// ---------------------------------------------------------------------------

/// Role validation runs ahead of every other check, so a conflict is reported
/// even though this builder is also missing its config, pool, and brokers.
#[test]
fn a_role_conflict_is_reported_before_any_missing_input() {
    let error = build_error(
        RuntimeBuilder::new()
            .cache::<ProductSnapshot>()
            .handle::<ProductSnapshot, _>(|_payload, _cx| Box::pin(async move { Ok(()) })),
    );
    assert!(matches!(error, BuildError::Roles(_)), "{error:?}");

    let rendered = error.to_string();
    assert!(rendered.contains("product_snapshot"), "{rendered}");
    assert!(rendered.contains("cache"), "{rendered}");
    assert!(
        rendered.contains("handle_before"),
        "the message must point at the one legal pair: {rendered}"
    );
}

#[test]
fn two_handlers_at_one_position_are_refused() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .handle::<OrderSnapshot, _>(|_payload, _cx| Box::pin(async move { Ok(()) }))
            .handle::<OrderSnapshot, _>(|_payload, _cx| Box::pin(async move { Ok(()) })),
    );
    assert!(matches!(error, BuildError::Roles(_)), "{error:?}");
}

/// The one legal pair, which must reach the missing-pool check rather than being
/// rejected as a conflict.
#[test]
fn handle_before_plus_handle_on_one_type_is_accepted() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .consumer_group("orders")
            .handle_before::<OrderSnapshot, _>(|_payload, _cx| {
                Box::pin(async move { Ok(kafkaman_sqlx::HandlerFlow::Continue) })
            })
            .handle::<OrderSnapshot, _>(|_payload, _cx| Box::pin(async move { Ok(()) })),
    );
    assert!(
        matches!(error, BuildError::MissingPool),
        "the pair must be accepted and fail only on the missing pool: {error:?}"
    );
}

/// Publishing and consuming one type is legal when it is written down, and is a
/// normal shape for a service that owns a topic and also keeps a cache of it.
#[test]
fn publishing_and_consuming_one_type_is_accepted() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .consumer_group("orders")
            .publish::<OrderSnapshot>()
            .cache::<OrderSnapshot>(),
    );
    assert!(matches!(error, BuildError::MissingPool), "{error:?}");
}

/// Repeats are deduplicated rather than rejected.
#[test]
fn declaring_the_same_role_twice_is_not_a_conflict() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .publish::<OrderSnapshot>()
            .publish::<OrderSnapshot>(),
    );
    assert!(matches!(error, BuildError::MissingPool), "{error:?}");
}

// ---------------------------------------------------------------------------
// Consumer group
// ---------------------------------------------------------------------------

/// A publish-only service needs no group, and demanding one would make the
/// simplest possible runtime require a value with nothing to name.
#[test]
fn a_publish_only_runtime_needs_no_consumer_group() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .publish::<OrderSnapshot>(),
    );
    assert!(matches!(error, BuildError::MissingPool), "{error:?}");
}

#[test]
fn a_consuming_runtime_without_a_group_names_the_type_that_needs_one() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .cache::<ProductSnapshot>(),
    );
    let BuildError::MissingConsumerGroup { message_type } = &error else {
        panic!("expected a missing consumer group, got {error:?}");
    };
    assert_eq!(message_type, "product_snapshot");

    let rendered = error.to_string();
    assert!(
        rendered.contains("RuntimeBuilder::consumer_group"),
        "{rendered}"
    );
    assert!(
        rendered.contains("replicas"),
        "the message must explain what a group means: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// Shape
// ---------------------------------------------------------------------------

/// The builder is `Debug` without leaking closures, because a caller debugging a
/// boot failure wants to see what they declared.
#[test]
fn the_builder_debug_shows_the_declared_roles() {
    let rendered = format!(
        "{:?}",
        RuntimeBuilder::new()
            .brokers("localhost:9092")
            .publish::<OrderSnapshot>()
            .cache::<ProductSnapshot>()
    );
    assert!(rendered.contains("order_snapshot"), "{rendered}");
    assert!(rendered.contains("product_snapshot"), "{rendered}");
    assert!(rendered.contains("localhost:9092"), "{rendered}");
}
