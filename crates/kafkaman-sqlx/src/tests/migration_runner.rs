use sqlx::postgres::PgPoolOptions;

use crate::changeset::MigrationContext;
use crate::{migrate, Error, ResolvedConfig};

/// `migrate` holds the advisory-lock connection and the changeset connection at
/// the same time. A pool that can only ever hand out one would block on the
/// second acquire until it timed out, and report a pool timeout that says
/// nothing about the configuration that caused it.
///
/// Uses a lazy pool: the check must happen before anything connects, so this
/// needs no database to prove it fires.
#[tokio::test]
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn migrate_rejects_a_pool_too_small_to_hold_its_two_connections() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://kafkaman:kafkaman@127.0.0.1:1/kafkaman")
        .expect("a lazy pool never contacts the server");

    let err = migrate(
        &pool,
        &ResolvedConfig::default(),
        &MigrationContext::new(),
        &[],
    )
    .await
    .expect_err("a single-connection pool cannot run a migration");

    assert!(
        matches!(err, Error::MigrationPoolTooSmall { max_connections: 1 }),
        "expected the pool-size error, got: {err}"
    );
}
