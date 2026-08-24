//! Boot-time topic convergence against a real broker.
//!
//! Enabled with `--features redpanda`. Every branch of the *decision* is
//! unit-tested in `kafkaman_core::topics` without a broker; what can only be
//! proved here is that the *observation* is read correctly — that a
//! `delete`-retention topic really does fail, that a created topic really is
//! compacted, and above all that asking the question does not itself create the
//! topic.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use durable_send_tests::{
    TestResult, CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE,
    CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE, CONTAINER_LABEL_SERVICE,
    CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE,
};
use kafkaman_core::{CleanupPolicy, MessageDescriptor, TopicMode, TopicSpec};
use kafkaman_rdkafka::{converge_topics, TopicAdmin};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Reserve a port from the OS rather than hardcoding one.
///
/// `redpanda_full_loop.rs` pins 19092 behind a process-global mutex, which
/// cannot protect against *this* binary running at the same time under
/// `cargo test --workspace`. A race remains in principle — something else could
/// take the port between the bind and the container's — but it is a far smaller
/// window than a constant that is guaranteed to clash.
async fn reserve_port() -> TestResult<u16> {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    Ok(socket.local_addr()?.port())
}

async fn start_redpanda() -> TestResult<(ContainerAsync<GenericImage>, String)> {
    let port = reserve_port().await?;
    let advertised = format!("PLAINTEXT://127.0.0.1:{port}");
    let listener = format!("PLAINTEXT://0.0.0.0:{port}");
    let container = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.7")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
        .with_label(CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE)
        .with_label(CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE)
        .with_label(CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE)
        .with_label(CONTAINER_LABEL_SERVICE, "redpanda")
        .with_mapped_port(port, port.tcp())
        .with_startup_timeout(Duration::from_secs(180))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--kafka-addr",
            &listener,
            "--advertise-kafka-addr",
            &advertised,
        ])
        .start()
        .await?;
    Ok((container, format!("127.0.0.1:{port}")))
}

fn descriptor(topic: &str, spec: TopicSpec) -> MessageDescriptor {
    MessageDescriptor::new("product_snapshot", topic)
        .expect("descriptor is valid")
        .with_topic_spec(spec)
}

fn spec_with(policy: CleanupPolicy, partitions: Option<i32>) -> TopicSpec {
    TopicSpec {
        cleanup_policy: policy,
        partitions,
        replication_factor: None,
    }
}

/// Provision a topic with an arbitrary policy, standing in for one an operator
/// or a broker default already created.
async fn provision(admin: &TopicAdmin, topic: &str, policy: CleanupPolicy) -> TestResult {
    admin
        .create(topic, 1, None, &spec_with(policy, Some(1)))
        .await?;
    Ok(())
}

#[tokio::test]
async fn verify_reads_real_topic_configuration_and_creates_nothing() -> TestResult {
    let (_redpanda, brokers) = start_redpanda().await?;
    let admin = TopicAdmin::from_brokers(&brokers)?;
    let compacted = spec_with(CleanupPolicy::Compact, None);

    // 1. A missing topic fails — and, the part that matters, checking must not
    //    have brought it into existence. `--mode dev-container` has
    //    `auto_create_topics_enabled`, so a metadata request that named the
    //    topic would create it with the broker's `delete` default: the exact
    //    misconfiguration this code exists to detect, manufactured by the check
    //    itself.
    let err = converge_topics(
        &admin,
        TopicMode::Verify,
        &[descriptor("absent-topic", compacted.clone())],
    )
    .await
    .expect_err("a missing topic must fail verification");
    assert!(err.to_string().contains("absent-topic"), "{err}");
    assert!(
        admin.observe("absent-topic").await?.is_none(),
        "verifying a topic must not create it"
    );

    // 2. A `delete` topic is rejected, naming both policies.
    provision(&admin, "delete-topic", CleanupPolicy::Delete).await?;
    let err = converge_topics(
        &admin,
        TopicMode::Verify,
        &[descriptor("delete-topic", compacted.clone())],
    )
    .await
    .expect_err("delete retention must fail boot");
    let message = err.to_string();
    assert!(message.contains("delete-topic"), "{message}");
    assert!(message.contains("compact"), "{message}");

    // 3. `compact,delete` is rejected just as firmly. This is the case most
    //    likely to be mistaken for correct: compaction is on, but records still
    //    age out, so the log cannot rebuild an entity.
    provision(&admin, "both-topic", CleanupPolicy::CompactAndDelete).await?;
    let observed = admin
        .observe("both-topic")
        .await?
        .expect("the topic was just created");
    assert_eq!(
        observed.cleanup_policy,
        CleanupPolicy::CompactAndDelete,
        "the broker's own spelling must parse back to the policy we asked for"
    );
    converge_topics(
        &admin,
        TopicMode::Verify,
        &[descriptor("both-topic", compacted.clone())],
    )
    .await
    .expect_err("compact,delete must fail boot");

    // 4. A properly compacted topic passes.
    provision(&admin, "good-topic", CleanupPolicy::Compact).await?;
    let drifts = converge_topics(
        &admin,
        TopicMode::Verify,
        &[descriptor("good-topic", compacted.clone())],
    )
    .await?;
    assert!(drifts.is_empty(), "a correct topic reports no drift");

    // 5. `off` tolerates everything, including a topic that does not exist.
    converge_topics(
        &admin,
        TopicMode::Off,
        &[descriptor("still-absent", compacted)],
    )
    .await
    .expect("off must never fail");

    Ok(())
}

#[tokio::test]
async fn create_provisions_a_compacted_topic_and_refuses_to_guess() -> TestResult {
    let (_redpanda, brokers) = start_redpanda().await?;
    let admin = TopicAdmin::from_brokers(&brokers)?;

    // 1. Creating without a declared partition count fails rather than picking
    //    one, because changing it later means republishing every entity.
    let err = converge_topics(
        &admin,
        TopicMode::Create,
        &[descriptor(
            "guess-topic",
            spec_with(CleanupPolicy::Compact, None),
        )],
    )
    .await
    .expect_err("create must not invent a partition count");
    assert!(err.to_string().contains("guess-topic"), "{err}");
    assert!(
        admin.observe("guess-topic").await?.is_none(),
        "a refused create must not leave a topic behind"
    );

    // 2. With one declared, the topic is created compacted.
    let spec = spec_with(CleanupPolicy::Compact, Some(3));
    let drifts = converge_topics(
        &admin,
        TopicMode::Create,
        &[descriptor("made-topic", spec.clone())],
    )
    .await?;
    assert!(drifts.is_empty());

    let observed = admin
        .observe("made-topic")
        .await?
        .expect("the topic must exist after create");
    assert_eq!(
        observed.cleanup_policy,
        CleanupPolicy::Compact,
        "a created entity topic must be compacted, not left on the broker default"
    );
    assert_eq!(observed.partitions, 3);

    // 3. Running it again is a no-op rather than an error: two services booting
    //    against one cluster race here by construction.
    converge_topics(&admin, TopicMode::Create, &[descriptor("made-topic", spec)])
        .await
        .expect("create must be idempotent");

    // 4. `create` fills in what is missing; it does not repair what is wrong.
    provision(&admin, "wrong-topic", CleanupPolicy::Delete).await?;
    converge_topics(
        &admin,
        TopicMode::Create,
        &[descriptor(
            "wrong-topic",
            spec_with(CleanupPolicy::Compact, Some(1)),
        )],
    )
    .await
    .expect_err("an existing wrong topic must fail even under create");
    assert_eq!(
        admin
            .observe("wrong-topic")
            .await?
            .expect("still there")
            .cleanup_policy,
        CleanupPolicy::Delete,
        "a failed convergence must not have rewritten the topic's policy"
    );

    // 5. A declared partition count that disagrees with the broker warns rather
    //    than failing, so an intentional, completed repartition cannot take the
    //    fleet down on its next deploy.
    let drifts = converge_topics(
        &admin,
        TopicMode::Verify,
        &[descriptor(
            "made-topic",
            spec_with(CleanupPolicy::Compact, Some(6)),
        )],
    )
    .await?;
    assert_eq!(drifts.len(), 1);
    assert_eq!((drifts[0].declared, drifts[0].found), (6, 3));

    Ok(())
}
