//! What `examples/provision` builds, and what happens to a service when it has
//! not run.
//!
//! Every branch of the reconciliation *decision* is unit-tested without a broker
//! in `kafkaman_core::topics`, and the library-tier observation is covered
//! against a real broker in `tests/durable-send/tests/topic_convergence.rs`.
//! What is left, and what only this tier can show, is the wiring: that the
//! binary compose runs actually creates the topics the two services declare, and
//! that a service really does refuse to start when it has not.
//!
//! That second half is why this workstream exists. The demo used to run on
//! `cleanup.policy=delete` topics the broker auto-created on first publish,
//! contradicting what `README.md` claims about compaction, and nothing anywhere
//! reported it.
//!
//! # Why one test function
//!
//! One PostgreSQL and one Redpanda per test function, and these assertions are
//! ordered: the unprovisioned case is only observable *before* provisioning has
//! happened, and idempotency only after. Splitting them across functions would
//! mean starting a second cluster to assert one sequence, so the phases are
//! separate functions sharing a cluster instead.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use distributed_cache_tests::{service_config, Cluster, TestResult};
use example_provision::{
    ensure_database, ensure_databases, entity_topics, provision, DatabaseOutcome, ProvisionOptions,
    DEFAULT_PARTITIONS, EXAMPLE_DATABASES,
};
use kafkaman::rdkafka::TopicAdmin;
use kafkaman::CleanupPolicy;
use sqlx::postgres::PgPoolOptions;

/// The topics the two example services exchange.
const ENTITY_TOPICS: [&str; 2] = ["products", "orders"];

#[tokio::test]
async fn provisioning_precedes_boot_and_is_the_only_thing_that_creates_a_topic() -> TestResult {
    let cluster = Cluster::start().await?;
    let admin = TopicAdmin::from_brokers(cluster.brokers())?;

    let order_db = boot_fails_without_topics(&cluster, &admin).await?;
    let options = provisioning_creates_compacted_topics(&cluster, &admin).await?;
    a_re_run_changes_nothing(&options, &admin).await?;
    the_same_service_boots_once_provisioned(&cluster, order_db).await?;
    database_creation_is_idempotent_and_validated(&cluster).await?;

    Ok(())
}

/// A service must not start against a broker with no entity topics — and
/// asking must not have created them.
///
/// Returns the database URL it used, so the positive case can reuse it and
/// differ from this one in exactly one variable.
async fn boot_fails_without_topics(cluster: &Cluster, admin: &TopicAdmin) -> TestResult<String> {
    // The database exists first, so this cannot pass or fail for a storage
    // reason: the only thing missing is the topics.
    let order_db = cluster.create_database("order_unprovisioned").await?;

    let err = example_order::start(example_order::ServiceOptions {
        database_url: order_db.clone(),
        brokers: cluster.brokers().to_owned(),
        bind: ([127, 0, 0, 1], 0).into(),
        consumer_group: "order-unprovisioned".to_owned(),
        config: Some(service_config("order")?),
    })
    .await
    .expect_err("a service must not start against a broker with no entity topics");

    let message = err.to_string();
    assert!(
        message.contains("orders") && message.contains("does not exist"),
        "the failure must name the missing topic, got: {message}"
    );

    // The load-bearing half. Redpanda's `--mode dev-container` has
    // `auto_create_topics_enabled`, so a boot check that named the topic in its
    // metadata request would have created it — with the broker's default
    // `cleanup.policy=delete`, which is exactly the misconfiguration the check
    // exists to find. A check that manufactures what it is looking for is worse
    // than no check at all.
    for topic in ENTITY_TOPICS {
        assert!(
            admin.observe(topic).await?.is_none(),
            "a failed boot must not have created `{topic}`"
        );
    }

    Ok(order_db)
}

/// Provisioning creates both databases and both topics, compacted.
async fn provisioning_creates_compacted_topics(
    cluster: &Cluster,
    admin: &TopicAdmin,
) -> TestResult<ProvisionOptions> {
    let options = ProvisionOptions {
        admin_database_url: cluster.admin_url(),
        databases: EXAMPLE_DATABASES.iter().copied().map(Into::into).collect(),
        brokers: cluster.brokers().to_owned(),
        topics: entity_topics(DEFAULT_PARTITIONS)?,
    };
    let report = provision(&options).await?;

    assert_eq!(
        report
            .databases
            .iter()
            .map(|(name, outcome)| (name.as_str(), *outcome))
            .collect::<Vec<_>>(),
        vec![
            ("product_service", DatabaseOutcome::Created),
            ("order_service", DatabaseOutcome::Created),
        ]
    );
    assert!(
        report.drifts.is_empty(),
        "a freshly created topic cannot have drifted: {:?}",
        report.drifts
    );

    for topic in ENTITY_TOPICS {
        let observed = admin
            .observe(topic)
            .await?
            .unwrap_or_else(|| panic!("`{topic}` must exist after provisioning"));
        // The plan's acceptance gate, in a form that runs on every build rather
        // than being read off a dashboard once.
        assert_eq!(
            observed.cleanup_policy,
            CleanupPolicy::Compact,
            "`{topic}` must be compacted, not left on the broker's `delete` default"
        );
        assert_eq!(observed.partitions, DEFAULT_PARTITIONS);
    }

    Ok(options)
}

/// Re-running provisioning changes nothing and fails nothing.
///
/// Not a nicety: compose re-runs this container on every `up`, so the second
/// `just examples demo` of the day has to behave like the first.
async fn a_re_run_changes_nothing(options: &ProvisionOptions, admin: &TopicAdmin) -> TestResult {
    let report = provision(options).await?;
    assert!(
        report
            .databases
            .iter()
            .all(|(_, outcome)| *outcome == DatabaseOutcome::AlreadyExisted),
        "a re-run must report the databases as already present: {:?}",
        report.databases
    );
    for topic in ENTITY_TOPICS {
        assert_eq!(
            admin.observe(topic).await?.map(|t| t.cleanup_policy),
            Some(CleanupPolicy::Compact),
            "a re-run must not have disturbed `{topic}`"
        );
    }
    Ok(())
}

/// The same service that refused to start now does.
///
/// Without this, the negative assertion would be satisfied by a service that
/// cannot start for any reason at all.
async fn the_same_service_boots_once_provisioned(
    cluster: &Cluster,
    database_url: String,
) -> TestResult {
    let service = example_order::start(example_order::ServiceOptions {
        database_url,
        brokers: cluster.brokers().to_owned(),
        bind: ([127, 0, 0, 1], 0).into(),
        consumer_group: "order-provisioned".to_owned(),
        config: Some(service_config("order")?),
    })
    .await
    .expect("the same service must start once its topics exist");
    service.shutdown().await?;
    Ok(())
}

/// Creating a database twice is absorbed, and an unusable name is refused
/// before any statement is issued.
async fn database_creation_is_idempotent_and_validated(cluster: &Cluster) -> TestResult {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&cluster.admin_url())
        .await?;

    assert_eq!(
        ensure_database(&admin, "twice_over").await?,
        DatabaseOutcome::Created
    );
    assert_eq!(
        ensure_database(&admin, "twice_over").await?,
        DatabaseOutcome::AlreadyExisted,
        "the second create must be absorbed, not raised"
    );

    // The valid name comes *first* in this call. If validation happened per
    // statement rather than up front, `safe_name` would exist by the time the
    // second entry was rejected — a half-built environment, which is the state
    // a provisioner most wants to avoid leaving behind.
    let err = ensure_databases(
        &cluster.admin_url(),
        &[
            "safe_name".to_owned(),
            "evil\"; DROP DATABASE \"twice_over".to_owned(),
        ],
    )
    .await
    .expect_err("an unusable name must be refused");
    assert!(err.to_string().contains("database name"), "{err}");

    let existing: Vec<String> =
        sqlx::query_scalar("SELECT datname FROM pg_database WHERE datname = ANY($1) ORDER BY 1")
            .bind(vec!["safe_name".to_owned(), "twice_over".to_owned()])
            .fetch_all(&admin)
            .await?;
    assert_eq!(
        existing,
        vec!["twice_over".to_owned()],
        "validation must precede the first CREATE, so `safe_name` is never reached"
    );

    admin.close().await;
    Ok(())
}
