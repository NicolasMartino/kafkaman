//! Binary entry point for the example provisioner.
//!
//! Runs once, exits, and is expected to be re-run on every `up`:
//!
//! ```text
//! ADMIN_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
//! KAFKA_BROKERS=127.0.0.1:19092 \
//! cargo run -p example-provision
//! ```
//!
//! In `examples/compose.yaml` it is a one-shot service that both other services
//! gate on with `condition: service_completed_successfully`, which is what
//! guarantees the ordering the model requires: topics exist, and are compacted,
//! before anything publishes to them.
//!
//! # Environment
//!
//! | Variable | Required | Meaning |
//! |---|---|---|
//! | `ADMIN_DATABASE_URL` | yes | A connection string for a database that already exists — conventionally `postgres`. Not a service's own URL: `CREATE DATABASE` cannot run from the database being created. |
//! | `KAFKA_BROKERS` | yes | Bootstrap servers. |
//! | `PROVISION_DATABASES` | no | Comma-separated; defaults to the two the examples use. |
//! | `TOPIC_PARTITIONS` | no | Partitions per entity topic; defaults to 1. |

use example_provision::{
    entity_topics, provision, ProvisionOptions, DEFAULT_PARTITIONS, EXAMPLE_DATABASES,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Read a required environment variable, reporting the variable name rather
/// than panicking with a backtrace.
fn required_env(name: &str, purpose: &str) -> Result<String, BoxError> {
    std::env::var(name).map_err(|_| format!("{name} must be set: {purpose}").into())
}

/// The databases to create, defaulting to the two the example stack uses.
///
/// Empty entries are dropped so a trailing comma is a typo rather than a
/// request to create a database with no name.
fn databases() -> Vec<String> {
    match std::env::var("PROVISION_DATABASES") {
        Ok(list) => list
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        Err(_) => EXAMPLE_DATABASES.iter().copied().map(Into::into).collect(),
    }
}

fn partitions() -> Result<i32, BoxError> {
    match std::env::var("TOPIC_PARTITIONS") {
        Ok(raw) => raw
            .trim()
            .parse()
            .map_err(|err| format!("TOPIC_PARTITIONS must be a positive integer: {err}").into()),
        Err(_) => Ok(DEFAULT_PARTITIONS),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let options = ProvisionOptions {
        admin_database_url: required_env(
            "ADMIN_DATABASE_URL",
            "points at an existing database on the target server, such as `postgres`",
        )?,
        databases: databases(),
        brokers: required_env("KAFKA_BROKERS", "points to Kafka/Redpanda")?,
        topics: entity_topics(partitions()?)?,
    };

    let report = provision(&options).await?;

    // Databases and topic *creations* are logged as they happen, from the
    // library and from `converge_topics` respectively. What is left to say here
    // is the state each topic ended in — which on a re-run, where nothing was
    // created, is the only line about topics at all.
    for topic in &options.topics {
        tracing::info!(
            topic = %topic.topic,
            cleanup_policy = %topic.topic_spec.cleanup_policy,
            "entity topic ready"
        );
    }
    // Drift is not a provisioning failure — the broker's count wins, because a
    // completed repartition must not take the next deploy down — but it is the
    // one thing in this report an operator should read twice.
    for drift in &report.drifts {
        tracing::warn!(%drift, "declared partition count differs from the broker's");
    }

    tracing::info!(
        databases = report.databases.len(),
        topics = options.topics.len(),
        "environment provisioned"
    );
    Ok(())
}
