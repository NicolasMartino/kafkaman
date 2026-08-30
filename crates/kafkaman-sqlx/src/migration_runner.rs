use kafkaman_core::SqlIdentifier;
use sqlx::{Connection, PgConnection, PgPool, Postgres, Row, Transaction};

use crate::changelog::assert_changelog_order;
use crate::changeset::{
    ChangeBuilder, Changeset, MigrationAction, MigrationContext, MigrationReport,
    MigrationStepReport,
};
use crate::lock_keys::advisory_lock_key;
use crate::schema_sql::create_received_ingest_failures_table_sql;
use crate::tables::qualified_name;
use crate::{Error, ResolvedConfig, Result};

/// Serializes migrations of one schema against every other replica.
///
/// The lock is `pg_advisory_xact_lock` held inside an otherwise-empty
/// transaction, not a session-scoped `pg_advisory_lock`. That distinction is the
/// whole point of the type. A session lock outlives a dropped future: if the
/// migration is cancelled between taking the lock and releasing it — a
/// `select!` losing the race, a deploy timeout, a killed task — the explicit
/// unlock never runs, and the pooled connection is handed back to the next
/// caller still holding the lock. Every later migration of that schema then
/// blocks forever, with nothing to report why.
///
/// Dropping a [`Transaction`] queues a `ROLLBACK` on its connection, and a
/// rollback releases a transaction-scoped lock. So cancellation at any point
/// releases the lock, with no `Drop` impl of our own and no async drop.
///
/// The cost is that [`migrate`] holds two pooled connections at once — this one
/// and the one the changesets run on — because per-changeset commits cannot
/// share a transaction with a lock that must outlive them. Size the pool for at
/// least two; [`migrate`] rejects a smaller pool up front rather than blocking
/// on it. [`migrate_dry_run`] needs only one, because a preview
/// *is* a single rolled-back transaction and takes its lock inside it.
struct SchemaLock {
    transaction: Transaction<'static, Postgres>,
}

impl SchemaLock {
    /// Wait for the lock, however long another replica holds it.
    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
    async fn acquire(pool: &PgPool, cfg: &ResolvedConfig) -> Result<Self> {
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(advisory_lock_key(cfg.schema.as_str()))
            .execute(&mut *transaction)
            .await?;
        Ok(Self { transaction })
    }

    /// Release the lock, reporting `result`'s error in preference to a release
    /// failure: the caller cares far more about why the migration failed than
    /// about the rollback that followed.
    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
    async fn release<T>(self, result: Result<T>) -> Result<T> {
        // Rollback, not commit: this transaction exists only to scope the lock
        // and has written nothing.
        match (result, self.transaction.rollback().await) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(Error::Sqlx(err)),
        }
    }
}

/// Apply every changeset the context allows that has not already been applied.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn migrate(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    assert_changelog_order(changesets)?;

    // Checked here rather than discovered by deadlock. The lock connection and
    // the changeset connection are held simultaneously, so a single-connection
    // pool blocks on the second acquire until it times out — and reports a pool
    // timeout, which says nothing about the configuration that caused it.
    let max_connections = pool.options().get_max_connections();
    if max_connections < 2 {
        return Err(Error::MigrationPoolTooSmall { max_connections });
    }

    let lock = SchemaLock::acquire(pool, cfg).await?;
    let mut conn = pool.acquire().await?;
    let result = run_migrations(&mut conn, cfg, ctx, changesets).await;
    drop(conn);

    lock.release(result).await
}

/// Report what [`migrate`] would do, without doing any of it.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn migrate_dry_run(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    assert_changelog_order(changesets)?;

    // The whole preview runs inside one transaction that is always rolled back,
    // so the lock belongs in it too. Bootstrap DDL (`CREATE SCHEMA/TABLE`) and
    // the per-changeset reads therefore leave no trace: a dry-run never mutates
    // `changelog_history`.
    let mut tx = pool.begin().await?;

    // A dry-run is a non-mutating preview, so it takes the lock without
    // blocking: a long preview can never stall a real deploy migration, and a
    // deploy in progress short-circuits the preview instead of queueing behind
    // it.
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
        .bind(advisory_lock_key(cfg.schema.as_str()))
        .fetch_one(&mut *tx)
        .await?;
    if !acquired {
        return Err(Error::MigrationLockBusy);
    }

    let result = dry_run_in_tx(&mut tx, cfg, ctx, changesets).await;
    match (result, tx.rollback().await) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(Error::Sqlx(err)),
    }
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn dry_run_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    bootstrap_history(tx, cfg).await?;
    let mut report = MigrationReport::default();

    for changeset in changesets {
        let changeset = changeset.as_ref();
        if let Some(skipped) = MigrationStepReport::context_skip(changeset, ctx) {
            report.push(skipped);
            continue;
        }

        let checksum = changeset.checksum();
        if let Some(history) = history_row(cfg, tx, changeset.version()).await? {
            // A preview reports a checksum mismatch rather than failing on it:
            // seeing every drifted changeset at once is the point of a dry run,
            // where `run_migrations` must stop at the first.
            report.push(MigrationStepReport::new(
                changeset,
                if history.checksum == checksum {
                    MigrationAction::SkippedAlreadyApplied
                } else {
                    MigrationAction::ChecksumMismatch {
                        stored: history.checksum,
                        current: checksum,
                    }
                },
            ));
            continue;
        }

        let preview = match changeset.estimate_replay_count(cfg, tx).await? {
            Some(count) => changeset
                .dry_run_preview(cfg)?
                .replace("~N", &format!("~{count}")),
            None => changeset.dry_run_preview(cfg)?,
        };

        report.push(
            MigrationStepReport::new(changeset, MigrationAction::WouldApply).with_preview(preview),
        );
    }

    Ok(report)
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn run_migrations(
    conn: &mut PgConnection,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    bootstrap_history(conn, cfg).await?;
    let mut report = MigrationReport::default();

    for changeset in changesets {
        let changeset = changeset.as_ref();
        if let Some(skipped) = MigrationStepReport::context_skip(changeset, ctx) {
            report.push(skipped);
            continue;
        }

        let checksum = changeset.checksum();
        // One transaction per changeset, not one for the changelog: a migration
        // that fails halfway keeps everything already applied, and the history
        // row lands with the DDL it records.
        let mut tx = conn.begin().await?;
        if let Some(history) = history_row(cfg, &mut tx, changeset.version()).await? {
            if history.checksum != checksum {
                // Stop rather than report: a changeset edited after it was
                // applied means the source and the database disagree about
                // what version N *is*, so every later version is suspect.
                return Err(Error::ChecksumMismatch {
                    version: changeset.version(),
                    stored: history.checksum,
                    current: checksum,
                });
            }
            tx.commit().await?;
            report.push(MigrationStepReport::new(
                changeset,
                MigrationAction::SkippedAlreadyApplied,
            ));
            continue;
        }

        let mut builder = ChangeBuilder::new();
        changeset.build(cfg, &mut builder)?;
        for statement in builder.into_statements() {
            sqlx::query(&statement).execute(&mut *tx).await?;
        }
        insert_history(
            cfg,
            &mut tx,
            changeset.version(),
            changeset.name(),
            &checksum,
            ctx.applied_by(),
        )
        .await?;
        tx.commit().await?;

        report.push(MigrationStepReport::new(
            changeset,
            MigrationAction::Applied,
        ));
    }

    Ok(report)
}

/// Create the schema, the changelog history table, and the ingest-failure table,
/// and bring a history table written by an older build up to shape.
///
/// Idempotent and unversioned by necessity: these must exist before any
/// changeset can be version-checked, so they cannot themselves be changesets.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn bootstrap_history(conn: &mut PgConnection, cfg: &ResolvedConfig) -> Result<()> {
    let schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", cfg.schema.quoted());
    sqlx::query(&schema_sql).execute(&mut *conn).await?;

    let history = history_table_name(cfg)?;
    let statements = [
        // `checksum` and `applied_by` are `NOT NULL`: every row this crate writes
        // supplies both, and V1 is the first release, so there is no history
        // table that predates either column.
        format!(
            "CREATE TABLE IF NOT EXISTS {history} (
                version BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_by TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ),
        create_received_ingest_failures_table_sql(cfg)?,
    ];

    for statement in statements {
        sqlx::query(&statement).execute(&mut *conn).await?;
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct HistoryRow {
    checksum: String,
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn history_row(
    cfg: &ResolvedConfig,
    tx: &mut Transaction<'_, Postgres>,
    version: i64,
) -> Result<Option<HistoryRow>> {
    let history = history_table_name(cfg)?;
    let sql = format!("SELECT checksum FROM {history} WHERE version = $1");
    let row = sqlx::query(&sql)
        .bind(version)
        .fetch_optional(&mut **tx)
        .await?;

    match row {
        // Decoded with `try_get` rather than `.ok()` so a genuine decode error
        // surfaces instead of being read as "no checksum recorded".
        Some(row) => Ok(Some(HistoryRow {
            checksum: row.try_get::<String, _>("checksum")?,
        })),
        None => Ok(None),
    }
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn insert_history(
    cfg: &ResolvedConfig,
    tx: &mut Transaction<'_, Postgres>,
    version: i64,
    name: &str,
    checksum: &str,
    applied_by: &str,
) -> Result<()> {
    let history = history_table_name(cfg)?;
    let sql = format!(
        "INSERT INTO {history} (version, name, checksum, applied_by) VALUES ($1, $2, $3, $4)"
    );
    sqlx::query(&sql)
        .bind(version)
        .bind(name)
        .bind(checksum)
        .bind(applied_by)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn history_table_name(cfg: &ResolvedConfig) -> Result<String> {
    Ok(qualified_name(
        &cfg.schema,
        &SqlIdentifier::new("changelog_history")?,
    ))
}
