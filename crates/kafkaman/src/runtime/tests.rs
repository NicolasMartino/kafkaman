//! Builder validation, all of it reachable without a database or a broker.
//!
//! That is the property being tested as much as the individual messages: a
//! declaration mistake must be caught before `build()` opens a connection,
//! because the alternative is a service that dials Postgres and Kafka in order
//! to discover it was misconfigured locally.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafkaman_config::Config;
use kafkaman_core::{problem, KafkaMessage, ProblemType};
use serde::{Deserialize, Serialize};

use super::tasks::LoopFuture;
use super::{
    BoxLoopError, BuildError, CancellationToken, RuntimeBuilder, RuntimeError, RuntimeTasks,
    Subsystems,
};

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

#[test]
fn build_and_runtime_errors_have_problem_types() {
    assert_eq!(
        BuildError::MissingConfig.problem_type(),
        problem::CONFIGURATION
    );
    assert_eq!(
        BuildError::MissingPool.problem_type(),
        problem::CONFIGURATION
    );
    assert_eq!(BuildError::NoRoles.problem_type(), problem::CONFIGURATION);

    assert_eq!(
        RuntimeError::LoopExited {
            loop_name: "relay:order_snapshot".to_owned(),
            message: "completed".to_owned(),
        }
        .problem_type(),
        problem::INFRASTRUCTURE
    );
    assert_eq!(
        RuntimeError::Build(BuildError::MissingConfig).problem_type(),
        problem::CONFIGURATION
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

#[test]
fn a_dispatch_only_runtime_without_a_group_reaches_the_next_missing_input() {
    let error = build_error(
        RuntimeBuilder::new()
            .config(config())
            .subsystems(Subsystems::DISPATCH)
            .cache::<ProductSnapshot>(),
    );
    assert!(
        matches!(error, BuildError::MissingPool),
        "a worker that never ingests should not need a Kafka consumer group: {error:?}"
    );
}

// ---------------------------------------------------------------------------
// Shape
// ---------------------------------------------------------------------------

#[test]
fn subsystem_flags_compose_for_worker_roles() {
    let selected = Subsystems::RELAY | Subsystems::DISPATCH;
    assert!(selected.contains(Subsystems::RELAY), "{selected:?}");
    assert!(selected.contains(Subsystems::DISPATCH), "{selected:?}");
    assert!(!selected.contains(Subsystems::INGEST), "{selected:?}");
    assert!(!selected.contains(Subsystems::PURGE), "{selected:?}");
    assert_eq!(
        selected.without(Subsystems::DISPATCH),
        Subsystems::RELAY,
        "{selected:?}"
    );

    assert_eq!(Subsystems::pipeline(), Subsystems::PIPELINE);
    assert!(Subsystems::PIPELINE.contains(Subsystems::QUEUE_METRICS));
    assert!(!Subsystems::PIPELINE.contains(Subsystems::PURGE));
    assert!(format!("{selected:?}").contains("relay"));
}

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

// ---------------------------------------------------------------------------
// Runtime task supervision
// ---------------------------------------------------------------------------

fn successful_loop(future: impl Future<Output = ()> + Send + 'static) -> LoopFuture {
    Box::pin(async move {
        future.await;
        Ok(())
    })
}

fn failing_loop(
    future: impl Future<Output = ()> + Send + 'static,
    message: &'static str,
) -> LoopFuture {
    Box::pin(async move {
        future.await;
        Err(Box::new(std::io::Error::other(message)) as BoxLoopError)
    })
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn wait_for_flag(flag: &AtomicBool) {
    let observed = tokio::time::timeout(Duration::from_secs(1), async {
        while !flag.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(observed.is_ok(), "timed out waiting for task future drop");
}

#[tokio::test]
async fn runtime_tasks_report_clean_exit_before_shutdown() {
    // A relay or dispatcher that returns `Ok(())` while the service is still up
    // is not healthy; it means the runtime silently stopped doing part of its
    // job. Clean completion only becomes normal after cancellation is requested.
    let mut tasks = RuntimeTasks::spawn(
        CancellationToken::new(),
        vec![("relay:order_snapshot".to_owned(), successful_loop(async {}))],
    );

    let err = tasks
        .wait()
        .await
        .expect_err("early clean exit must be a supervision error");
    match err {
        RuntimeError::LoopExited { loop_name, message } => {
            assert_eq!(loop_name, "relay:order_snapshot");
            assert!(message.contains("completed"), "{message}");
        }
        other => panic!("expected LoopExited, got {other:?}"),
    }

    tasks
        .shutdown()
        .await
        .expect("no remaining tasks should drain");
}

#[tokio::test]
async fn runtime_tasks_shutdown_reports_already_completed_clean_loops() {
    let tasks = RuntimeTasks::spawn(
        CancellationToken::new(),
        vec![("relay:order_snapshot".to_owned(), successful_loop(async {}))],
    );
    tokio::task::yield_now().await;

    let err = tasks
        .shutdown_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("shutdown should not hide a loop that completed before cancellation");
    match err {
        RuntimeError::LoopExited { loop_name, .. } => {
            assert_eq!(loop_name, "relay:order_snapshot");
        }
        other => panic!("expected LoopExited, got {other:?}"),
    }
}

#[tokio::test]
async fn runtime_tasks_allow_clean_exit_after_shutdown() {
    let shutdown = CancellationToken::new();
    let observed = shutdown.clone();
    let mut tasks = RuntimeTasks::spawn(
        shutdown.clone(),
        vec![(
            "dispatcher:product_snapshot".to_owned(),
            successful_loop(async move {
                observed.cancelled().await;
            }),
        )],
    );

    shutdown.cancel();
    tasks
        .wait()
        .await
        .expect("clean completion after shutdown is the normal path");
    tasks.shutdown().await.expect("drain after wait is empty");
}

#[tokio::test]
async fn runtime_tasks_shutdown_waits_for_in_flight_drain() {
    let shutdown = CancellationToken::new();
    let observed = shutdown.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let mark_finished = Arc::clone(&finished);
    let tasks = RuntimeTasks::spawn(
        shutdown,
        vec![(
            "dispatcher:product_snapshot".to_owned(),
            successful_loop(async move {
                observed.cancelled().await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                mark_finished.store(true, Ordering::SeqCst);
            }),
        )],
    );

    tasks
        .shutdown_with_timeout(Duration::from_secs(1))
        .await
        .expect("a cooperative task should drain before the timeout");
    assert!(
        finished.load(Ordering::SeqCst),
        "shutdown returned before the task finished its current cycle"
    );
}

#[tokio::test]
async fn runtime_tasks_shutdown_times_out_and_aborts_wedged_loops() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mark_dropped = Arc::clone(&dropped);
    let tasks = RuntimeTasks::spawn(
        CancellationToken::new(),
        vec![(
            "ingester:product_snapshot".to_owned(),
            successful_loop(async move {
                let _drop_flag = DropFlag(mark_dropped);
                std::future::pending::<()>().await;
            }),
        )],
    );

    let err = tasks
        .shutdown_with_timeout(Duration::from_millis(50))
        .await
        .expect_err("a task ignoring shutdown must not hang the runtime");
    match err {
        RuntimeError::DrainTimeout { timeout, remaining } => {
            assert_eq!(timeout, Duration::from_millis(50));
            assert_eq!(remaining, 1);
        }
        other => panic!("expected DrainTimeout, got {other:?}"),
    }

    wait_for_flag(&dropped).await;
}

#[tokio::test]
async fn runtime_tasks_shutdown_reports_loop_errors_during_drain() {
    let shutdown = CancellationToken::new();
    let observed = shutdown.clone();
    let tasks = RuntimeTasks::spawn(
        shutdown,
        vec![(
            "purger:order_snapshot".to_owned(),
            failing_loop(
                async move {
                    observed.cancelled().await;
                },
                "database is gone",
            ),
        )],
    );

    let err = tasks
        .shutdown_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("a loop error during drain should be reported");
    match err {
        RuntimeError::Loop { loop_name, source } => {
            assert_eq!(loop_name, "purger:order_snapshot");
            assert!(source.to_string().contains("database is gone"));
        }
        other => panic!("expected Loop, got {other:?}"),
    }
}

#[tokio::test]
async fn runtime_tasks_shutdown_reports_loop_error_before_a_later_timeout() {
    let shutdown = CancellationToken::new();
    let observed = shutdown.clone();
    let tasks = RuntimeTasks::spawn(
        shutdown,
        vec![
            (
                "dispatcher:product_snapshot".to_owned(),
                failing_loop(
                    async move {
                        observed.cancelled().await;
                    },
                    "database is gone",
                ),
            ),
            (
                "ingester:product_snapshot".to_owned(),
                successful_loop(async {
                    std::future::pending::<()>().await;
                }),
            ),
        ],
    );

    let err = tasks
        .shutdown_with_timeout(Duration::from_millis(100))
        .await
        .expect_err("the first loop error should outrank a later drain timeout");
    match err {
        RuntimeError::Loop { loop_name, source } => {
            assert_eq!(loop_name, "dispatcher:product_snapshot");
            assert!(source.to_string().contains("database is gone"));
        }
        other => panic!("expected Loop, got {other:?}"),
    }
}

#[tokio::test]
async fn runtime_tasks_report_named_panics() {
    let mut tasks = RuntimeTasks::spawn(
        CancellationToken::new(),
        vec![(
            "dispatcher:product_snapshot".to_owned(),
            successful_loop(async {
                panic!("handler panic crossed the loop boundary");
            }),
        )],
    );

    let err = tasks
        .wait()
        .await
        .expect_err("a panicking loop must fail supervision");
    match err {
        RuntimeError::Panicked { loop_name, source } => {
            assert_eq!(loop_name, "dispatcher:product_snapshot");
            assert!(source.is_panic());
        }
        other => panic!("expected Panicked, got {other:?}"),
    }

    tasks
        .shutdown()
        .await
        .expect("no remaining tasks should drain");
}
