//! Shared scaffolding for the durable-send integration tests.
//!
//! # Why one owned container per test
//!
//! Testcontainers cleanup is driven by `ContainerAsync`'s `Drop`. A static
//! `OnceCell<ContainerAsync<_>>` never drops at process exit, so sharing one
//! PostgreSQL container per test binary leaked containers across runs. Every
//! test therefore owns its container, and [`start_harness`] returns the guard
//! alongside the harness so it stays alive for the test's scope rather than
//! being dropped inside a helper — which would close the database out from
//! under the pool.
//!
//! Assertion helpers legitimately panic, so the workspace's no-panic lints are
//! relaxed here.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::error::Error as StdError;

mod containers;
mod fixtures;

pub use containers::{
    postgres, postgres_for_suite, redpanda, TestPostgres, CONTAINER_LABEL_MANAGED_BY,
    CONTAINER_LABEL_MANAGED_BY_VALUE, CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE,
    CONTAINER_LABEL_SERVICE, CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE,
};
pub use fixtures::{KeylessProduct, ProductSnapshot, RegionalProduct};

use kafkaman_config::Config;
use kafkaman_core::{IdempotencyIdentity, IdempotencyKey};
use kafkaman_test::Harness;

pub type BoxError = Box<dyn StdError + Send + Sync>;
pub type TestResult<T = ()> = Result<T, BoxError>;

/// Start a PostgreSQL container and connect a harness to it.
///
/// Returned as a pair, and the container must be bound rather than discarded:
/// dropping it closes the database the harness is connected to. Every test in
/// this suite opened with the same three lines before this existed.
pub async fn start_harness() -> TestResult<(TestPostgres, Harness)> {
    let postgres = postgres().await?;
    let harness = Harness::connect(postgres.url()).await?;
    Ok((postgres, harness))
}

/// Start PostgreSQL and Redpanda, and connect a harness that publishes to the
/// broker.
///
/// All three guards are returned: dropping either container closes something
/// the harness is still using.
#[cfg(feature = "redpanda")]
pub async fn start_redpanda_harness() -> TestResult<(
    TestPostgres,
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
    String,
    Harness,
)> {
    let postgres = postgres().await?;
    let (container, brokers) = redpanda().await?;
    let harness = Harness::connect_redpanda(postgres.url(), &brokers).await?;
    Ok((postgres, container, brokers, harness))
}

/// [`start_harness`], with a caller-supplied config — for tests about configuration
/// itself, or about a retry policy other than the default.
pub async fn start_harness_with_config(config: Config) -> TestResult<(TestPostgres, Harness)> {
    let postgres = postgres().await?;
    let harness = Harness::connect_with_config(postgres.url(), config).await?;
    Ok((postgres, harness))
}

/// The digest a string idempotency source derives to.
///
/// Stored rows key on the digest, never the source, so an assertion about which
/// row was written has to derive it the same way production does.
pub fn idem_key(value: &str) -> IdempotencyKey {
    IdempotencyIdentity::derive_from_string(value)
        .expect("test idempotency source is valid")
        .key
}

/// The same digest as hex, for the wire header.
pub fn idem_hex(value: &str) -> String {
    idem_key(value).to_hex()
}

/// A config with a chosen retry policy, for tests that need a specific attempt
/// budget or error-history bound rather than the defaults.
pub fn retry_test_config(schema: &str, max_attempts: u32, errors_limit: u32) -> Config {
    Config::parse(&format!(
        r#"
        [database]
        schema = "{schema}"

        [relay]
        worker_id = "worker-retry-test"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "50ms"

        [retry.defaults]
        max_attempts = {max_attempts}
        initial_backoff = "2s"
        max_backoff = "5s"
        multiplier = 2.0
        errors_limit = {errors_limit}
        dlq = "table"
        "#
    ))
    .expect("retry test config is valid")
}

/// (Re)create a table used to observe handler side effects.
///
/// Test binaries share one PostgreSQL server, so a table left behind by an
/// earlier test in the same file would make `CREATE TABLE` fail and would leak
/// its rows into the next test's assertions. This drops first.
///
/// Callers must hold their file's serializing lock, because the table name is
/// shared across the tests in that file.
pub async fn recreate_effect_table(
    pool: &sqlx::PgPool,
    name: &str,
    columns: &str,
) -> TestResult<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {name}"))
        .execute(pool)
        .await?;
    sqlx::query(&format!("CREATE TABLE {name} ({columns})"))
        .execute(pool)
        .await?;
    Ok(())
}

/// A schema name unique to one test, for the cases that configure the schema
/// explicitly instead of letting the harness generate one.
pub fn unique_schema(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}
