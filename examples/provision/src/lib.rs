//! Building the environment the example services boot into.
//!
//! # Why this exists
//!
//! The services are deliberately unable to build their own environment. A
//! kafkaman service verifies its topics at boot and refuses to start if one is
//! missing, because a broker with `auto.create.topics.enable` would otherwise
//! manufacture the topic with its default `cleanup.policy=delete` — and an
//! entity topic that ages records out cannot rebuild the entities on it. The
//! check exists precisely so that nothing quietly creates a wrong topic, which
//! means *something else* has to create the right one first.
//!
//! That something is this crate. It runs once, before either service, and does
//! the two things that are the environment rather than the application:
//!
//! 1. creates each service's database, and
//! 2. converges the entity topics with [`TopicMode::Create`].
//!
//! # What it deliberately does not do
//!
//! **Tables.** Not the kafkaman ones, not the business ones. Each service
//! migrates its own schema at boot under an advisory lock, and a second writer
//! of the same tables would be a second source of truth for their shape. The
//! boundary is: things that exist *before* a connection can be opened are the
//! environment's; everything reachable *through* one belongs to the service that
//! owns it.
//!
//! **Repair.** Convergence creates what is absent and fails on what is present
//! and wrong. A topic already carrying records under the wrong retention policy
//! is an operational decision — the supported recovery is republishing every
//! entity onto a new topic — and not something a provisioner should make on
//! someone's behalf at 3am.

use example_contracts::{OrderSnapshot, ProductSnapshot};
use kafkaman::rdkafka::{converge_topics, TopicAdmin};
use kafkaman::{KafkaMessage, MessageDescriptor, PartitionDrift, TopicMode, TopicSpec};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// The database `product` connects to.
pub const PRODUCT_DATABASE: &str = "product_service";

/// The database `order` connects to.
pub const ORDER_DATABASE: &str = "order_service";

/// Both, in the order the report lists them.
pub const EXAMPLE_DATABASES: [&str; 2] = [PRODUCT_DATABASE, ORDER_DATABASE];

/// One partition per entity topic.
///
/// Not a recommendation, a demo choice. Every entity's snapshots must land on
/// one partition for the cache guard to order them, which any count satisfies;
/// one keeps the `applied_offset` that `GET /products/{id}` reports directly
/// comparable to what Redpanda Console shows, which is the point of the demo.
///
/// Raising it later is not free — see [`TopicSpec::partitions`].
pub const DEFAULT_PARTITIONS: i32 = 1;

/// Postgres caps an identifier at 63 bytes and **truncates** anything longer
/// rather than refusing it. A truncated database name is the worst outcome
/// available: creation succeeds, and the service whose connection string still
/// carries the full name fails to connect to a database that does not exist.
const MAX_IDENTIFIER_BYTES: usize = 63;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("`{name}` is not a usable database name: {reason}")]
    InvalidDatabaseName { name: String, reason: &'static str },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Contract(#[from] kafkaman::Error),
    #[error(transparent)]
    Broker(#[from] kafkaman::rdkafka::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Whether a database had to be created.
///
/// Reported rather than swallowed so a re-run reads as a re-run. "Created two
/// databases" on the fifth `just examples demo` would mean the previous four
/// had somehow lost them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseOutcome {
    Created,
    AlreadyExisted,
}

impl std::fmt::Display for DatabaseOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Created => "created",
            Self::AlreadyExisted => "already existed",
        })
    }
}

/// What to build.
#[derive(Clone, Debug)]
pub struct ProvisionOptions {
    /// A connection string for a database that already exists on the target
    /// server — conventionally `postgres`. `CREATE DATABASE` needs a session,
    /// and the session cannot be on the database being created.
    pub admin_database_url: String,
    pub databases: Vec<String>,
    pub brokers: String,
    /// The topics to converge, as message-type declarations.
    ///
    /// Descriptors rather than bare topic names because creation needs the
    /// cleanup policy and the partition count, and taking them from the
    /// declaration is what keeps this binary from drifting away from the
    /// services it provisions for. See [`entity_topics`].
    pub topics: Vec<MessageDescriptor>,
}

/// What was built, in enough detail to read a re-run.
#[derive(Clone, Debug)]
pub struct ProvisionReport {
    pub databases: Vec<(String, DatabaseOutcome)>,
    /// Topics whose broker partition count differs from the declared one.
    ///
    /// A warning rather than a failure; see [`TopicSpec::check`].
    pub drifts: Vec<PartitionDrift>,
}

/// The entity topics the two example services exchange, at `partitions` each.
///
/// The partition count is supplied by the caller rather than declared in
/// `example-contracts`, and that split is the point: how a topic is partitioned
/// is a property of the deployment, while its cleanup policy is a property of
/// the model. A contract crate that pinned a partition count would be telling
/// every environment how to size itself.
pub fn entity_topics(partitions: i32) -> Result<Vec<MessageDescriptor>> {
    let spec = TopicSpec::compacted().with_partitions(partitions)?;
    Ok(vec![
        ProductSnapshot::descriptor()?.with_topic_spec(spec.clone()),
        OrderSnapshot::descriptor()?.with_topic_spec(spec),
    ])
}

/// Create the databases, then the topics.
///
/// Idempotent in both halves, because it runs on every `up` rather than once at
/// install time.
pub async fn provision(options: &ProvisionOptions) -> Result<ProvisionReport> {
    let databases = ensure_databases(&options.admin_database_url, &options.databases).await?;
    let drifts = provision_topics(&options.brokers, &options.topics).await?;
    Ok(ProvisionReport { databases, drifts })
}

/// Create each named database on the server `admin_database_url` points at,
/// over a single connection.
pub async fn ensure_databases(
    admin_database_url: &str,
    names: &[String],
) -> Result<Vec<(String, DatabaseOutcome)>> {
    // Every name is validated before the pool is opened, so an unusable one
    // fails without a round trip and without leaving half the set created.
    let validated = names
        .iter()
        .map(|name| validate_database_name(name).map(|()| name.clone()))
        .collect::<Result<Vec<_>>>()?;

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_database_url)
        .await?;

    let mut outcomes = Vec::with_capacity(validated.len());
    for name in validated {
        // Not `?`: the pool must close even on the failing path, or the
        // connection lingers until the process exits — which, for a one-shot
        // container, is immediately, but for a test harness is not.
        let outcome = ensure_database(&admin, &name).await;
        match outcome {
            Ok(outcome) => outcomes.push((name, outcome)),
            Err(err) => {
                admin.close().await;
                return Err(err);
            }
        }
    }
    admin.close().await;
    Ok(outcomes)
}

/// Create one database if it is not already there.
///
/// Takes an open pool so a caller creating several pays for one connection, and
/// so a test can supply its own.
pub async fn ensure_database(admin: &PgPool, name: &str) -> Result<DatabaseOutcome> {
    validate_database_name(name)?;

    // Postgres has no `CREATE DATABASE IF NOT EXISTS`, and the obvious
    // substitute — check `pg_database`, then create — has a window between the
    // two in which another provisioner can win. So the create *is* the check:
    // its own duplicate error is the authoritative answer, and there is no
    // window to lose.
    //
    // `CREATE DATABASE` cannot run inside a transaction block. sqlx issues this
    // in autocommit, which is why it is `execute` on a pool and not part of one.
    let result = sqlx::query(&format!("CREATE DATABASE \"{name}\""))
        .execute(admin)
        .await;

    let outcome = match result {
        Ok(_) => DatabaseOutcome::Created,
        Err(err) if is_duplicate_database(&err) => DatabaseOutcome::AlreadyExisted,
        Err(err) => return Err(err.into()),
    };

    // Logged here rather than from the report the caller gets back, so the log
    // is a timeline instead of a summary. Reporting it at the end put the
    // database lines *after* `converge_topics`' own, which reads as though the
    // topics were created first.
    tracing::info!(database = %name, %outcome, "database ready");
    Ok(outcome)
}

/// Whether a failed `CREATE DATABASE` failed because the database was there.
///
/// Two spellings, because the losing side of a race does not always get the
/// friendly one: Postgres reports `42P04 duplicate_database` when it can see the
/// existing row, but two concurrent creates can instead collide on
/// `pg_database`'s unique index and surface as `23505 unique_violation`.
fn is_duplicate_database(err: &sqlx::Error) -> bool {
    let sqlx::Error::Database(err) = err else {
        return false;
    };
    matches!(err.code().as_deref(), Some("42P04" | "23505"))
}

/// Bring the entity topics into line with what the message types declare,
/// creating any that are absent.
pub async fn provision_topics(
    brokers: &str,
    topics: &[MessageDescriptor],
) -> Result<Vec<PartitionDrift>> {
    let admin = TopicAdmin::from_brokers(brokers)?;
    // `Create`, which is this binary's whole reason to exist. The services run
    // the same reconciliation in `Verify` and so can only ever report.
    Ok(converge_topics(&admin, TopicMode::Create, topics).await?)
}

/// Reject a database name that cannot be safely interpolated into
/// `CREATE DATABASE`, or that Postgres would silently mangle.
///
/// The name reaches SQL as a quoted identifier, so this is a real boundary and
/// not a style check: it comes from the environment, and a `"` in it would end
/// the quoted identifier early. Restricting to an unquoted-identifier alphabet
/// leaves nothing to escape.
fn validate_database_name(name: &str) -> Result<()> {
    let invalid = |reason| Error::InvalidDatabaseName {
        name: name.to_owned(),
        reason,
    };

    let Some(first) = name.chars().next() else {
        return Err(invalid("it is empty"));
    };
    if name.len() > MAX_IDENTIFIER_BYTES {
        return Err(invalid(
            "it is longer than 63 bytes, which Postgres truncates rather than rejects",
        ));
    }
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(invalid("it must start with an ASCII letter or underscore"));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(invalid(
            "it may contain only ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use kafkaman::CleanupPolicy;

    #[test]
    fn the_names_this_stack_actually_uses_are_accepted() {
        for name in EXAMPLE_DATABASES {
            validate_database_name(name).expect("the shipped names must be usable");
        }
        // The distributed-cache fixture appends a UUID's simple form, so the
        // generated shape has to pass too.
        validate_database_name("product_9f1c8e2b4d7a4f0d9e3b1a6c5d8e7f00").unwrap();
        validate_database_name("_leading_underscore").unwrap();
    }

    #[test]
    fn a_name_that_would_escape_the_quoted_identifier_is_refused() {
        // The failure being prevented: `CREATE DATABASE "a"; DROP DATABASE
        // "order_service"; --"` would be three statements, not one. The name
        // comes from the environment, so this is a boundary rather than a
        // formality.
        let err = validate_database_name("a\"; DROP DATABASE \"order_service").unwrap_err();
        assert!(
            matches!(err, Error::InvalidDatabaseName { .. }),
            "expected a name rejection, got {err:?}"
        );

        for rejected in ["", "9lives", "-dashes", "has space", "sémantique"] {
            assert!(
                validate_database_name(rejected).is_err(),
                "`{rejected}` must be rejected"
            );
        }
    }

    #[test]
    fn an_over_long_name_is_refused_rather_than_silently_truncated() {
        // 63 is the limit, so 63 passes and 64 does not. Truncation is the
        // dangerous outcome: the database would be created under a name the
        // service's connection string does not use, and the failure would
        // surface as "database does not exist" pointing at a database that
        // visibly does.
        validate_database_name(&"a".repeat(MAX_IDENTIFIER_BYTES)).unwrap();
        let err = validate_database_name(&"a".repeat(MAX_IDENTIFIER_BYTES + 1)).unwrap_err();
        assert!(err.to_string().contains("63"), "{err}");
    }

    #[test]
    fn the_declared_topics_are_compacted_and_carry_the_partition_count() {
        let topics = entity_topics(3).unwrap();
        let names = topics
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["products", "orders"]);

        for topic in &topics {
            assert_eq!(
                topic.topic_spec.cleanup_policy,
                CleanupPolicy::Compact,
                "an entity topic is compacted by the model, not by choice"
            );
            assert_eq!(topic.topic_spec.partitions, Some(3));
        }
    }

    #[test]
    fn a_partition_count_below_one_is_refused_before_any_broker_is_dialled() {
        // `converge_topics` would reject this too, but only after connecting.
        // Failing here means a mistyped TOPIC_PARTITIONS reports as a bad
        // argument rather than as a broker problem.
        assert!(entity_topics(0).is_err());
        assert!(entity_topics(-1).is_err());
    }
}
