//! Returning dead-lettered received rows to dispatch.
use super::*;

use durable_send_tests::start_harness;
use kafkaman::recovery::Failure;
use kafkaman_core::ReceiveStatus;
use kafkaman_sqlx::{dispatch_once, MessageRouter, TableAccess};
use kafkaman_test::Harness;
use time::OffsetDateTime;

fn config(harness: &Harness) -> Config {
    Config::parse(&format!(
        "[database]\nschema = \"{}\"\n{RELAY}",
        harness.config().schema().as_str()
    ))
    .unwrap()
}

async fn redrive(
    harness: &Harness,
    pool: &PgPool,
    source: &str,
    max_rows: &str,
) -> Result<Report, RecoveryError> {
    let mut recovery = Recovery::new(vec![
        Source::received::<ProductSnapshot>("products"),
        Source::received::<RegionalProduct>("regional"),
    ])
    .unwrap();
    let request = recovery
        .request_from(
            &config(harness),
            RecoveryInputs {
                operation: Some("redrive".to_owned()),
                source: Some(source.to_owned()),
                max_rows: Some(max_rows.to_owned()),
                ..RecoveryInputs::default()
            },
        )
        .unwrap();
    // No brokers: a redrive never needs one.
    recovery
        .run(
            pool,
            &config(harness),
            None,
            &MigrationContext::default(),
            request,
        )
        .await
}

fn moved(report: &Report) -> u64 {
    match report.completed[0].progress {
        SourceProgress::Redrive { moved } => moved,
        ref other => panic!("{other:?}"),
    }
}

/// Insert one received row per `(id, status)` at offset = index, with a
/// failure history on every row.
async fn seed(harness: &Harness, rows: &[(&str, ReceiveStatus)]) -> TestResult<String> {
    let table = harness
        .received_table::<ProductSnapshot>()
        .await?
        .qualified_name();
    for (offset, (id, status)) in rows.iter().enumerate() {
        let event = ProductSnapshot::envelope(id, "n").with_idempotency_key(format!("rx-{id}"));
        harness
            .insert_received(&event, 0, offset as i64, Some(id.as_bytes()))
            .await?;
        sqlx::query(&format!(
            "UPDATE {table} SET status = {status}, attempts = 3, errors = '[{{\"detail\": \"boom\", \"occurred_at\": \"2026-01-01T00:00:00Z\"}}]'::jsonb,
             last_failed_at = '2026-01-01T00:00:00Z', last_failure_kind = '{kind}'
             WHERE source_offset = $1",
            status = status.sql_literal(),
            kind = kafkaman_core::ReceivedFailureKind::Handler.discriminant()
        ))
        .bind(offset as i64)
        .execute(harness.pool())
        .await?;
    }
    Ok(table)
}

/// One received row: its offset and status, then everything a redrive keeps.
type Row = (i64, String, History);

/// `attempts`, `errors`, `last_failed_at`, `last_failure_kind`, `message_id`
/// and `idempotency_key`, as text.
type History = (i32, String, Option<String>, Option<String>, String, String);

async fn statuses(harness: &Harness, table: &str) -> TestResult<Vec<Row>> {
    use sqlx::Row as _;
    let rows = sqlx::query(&format!(
        "SELECT source_offset, status, attempts, errors::text, last_failed_at::text,
                last_failure_kind, message_id::text, idempotency_key
         FROM {table} ORDER BY source_offset"
    ))
    .fetch_all(harness.pool())
    .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get(0)?,
                row.try_get(1)?,
                (
                    row.try_get(2)?,
                    row.try_get(3)?,
                    row.try_get(4)?,
                    row.try_get(5)?,
                    row.try_get(6)?,
                    row.try_get(7)?,
                ),
            ))
        })
        .collect()
}

/// T2 and T10: a bounded redrive moves only unlocked `Failed` rows, keeps
/// their offsets and history, and ordinary dispatch then handles them.
#[tokio::test]
async fn a_redrive_moves_at_most_max_rows_failed_rows() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    use ReceiveStatus::{Failed, Pending, Processed, Retryable};
    let table = seed(
        &harness,
        &[
            ("f-0", Failed),
            ("f-1", Failed),
            ("f-2", Failed),
            ("f-3", Failed),
            ("f-4", Failed),
            ("pending", Pending),
            ("retryable", Retryable),
            ("processed", Processed),
        ],
    )
    .await?;
    let failed = Failed.sql_literal();
    let pending = Pending.sql_literal();
    let before = statuses(&harness, &table).await?;

    // Hold one Failed row the way a concurrent operator would.
    let mut holder = harness.pool().begin().await?;
    sqlx::query(&format!(
        "SELECT 1 FROM {table} WHERE source_offset = 0 FOR UPDATE"
    ))
    .execute(&mut *holder)
    .await?;

    let events = Events::default();
    let report = {
        let _capture = events.capture();
        redrive(&harness, harness.pool(), "products", "3").await?
    };
    assert_eq!(moved(&report), 3);
    assert!(report.failed.is_none() && report.not_run.is_empty());
    let completion = &events.named("recovery source completed; rows requeued for dispatch")[0];
    assert_eq!(completion["fields"]["moved"], 3);
    assert_eq!(completion["fields"]["run_id"], report.run_id.to_string());
    holder.rollback().await?;

    let after = statuses(&harness, &table).await?;
    let still_failed = |rows: &[Row]| {
        rows.iter()
            .filter(|(_, status, _)| format!("'{status}'") == failed)
            .map(|(offset, _, _)| *offset)
            .collect::<Vec<_>>()
    };
    let now_failed = still_failed(&after);
    assert_eq!(now_failed.len(), 2);
    assert!(now_failed.contains(&0), "the locked row was skipped");
    for ((offset, was, history_before), (same_offset, is, history)) in before.iter().zip(&after) {
        assert_eq!(offset, same_offset);
        assert_eq!(history_before, history, "history and identity are kept");
        if format!("'{was}'") != failed {
            assert_eq!(was, is, "only Failed rows move");
        }
    }
    let pending_now: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table} WHERE status = {pending}"
    ))
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(pending_now, 4);

    // Ordinary dispatch picks the moved rows up.
    let router =
        MessageRouter::new().handler::<ProductSnapshot>(|_, _, _| Box::pin(async { Ok(()) }));
    let received = harness.received_table::<ProductSnapshot>().await?;
    let mut processed = 0;
    loop {
        let stats = dispatch_once(
            harness.pool(),
            &received,
            &router,
            OffsetDateTime::now_utc(),
        )
        .await?;
        if stats.processed == 0 {
            break;
        }
        processed += stats.processed;
    }
    assert!(processed >= 4, "the moved rows dispatch like any other");

    // T10: the rest move on the next run, then nothing is left to move.
    assert_eq!(
        moved(&redrive(&harness, harness.pool(), "products", "10").await?),
        2
    );
    assert_eq!(
        moved(&redrive(&harness, harness.pool(), "products", "10").await?),
        0
    );

    // A row that fails again later is eligible again.
    sqlx::query(&format!(
        "UPDATE {table} SET status = {failed} WHERE source_offset = 1"
    ))
    .execute(harness.pool())
    .await?;
    assert_eq!(
        moved(&redrive(&harness, harness.pool(), "products", "10").await?),
        1
    );
    Ok(())
}

/// A login role for `table`, with `grants` on it and no schema `USAGE`
/// unless `usage` says so.
async fn role(harness: &Harness, table: &str, grants: &str, usage: bool) -> TestResult<String> {
    let name = format!("redrive_{}", Uuid::new_v4().simple());
    let schema = harness.config().schema().quoted();
    sqlx::query(&format!("CREATE ROLE {name} LOGIN PASSWORD 'secret'"))
        .execute(harness.pool())
        .await?;
    sqlx::query(&format!("GRANT {grants} ON {table} TO {name}"))
        .execute(harness.pool())
        .await?;
    if usage {
        sqlx::query(&format!("GRANT USAGE ON SCHEMA {schema} TO {name}"))
            .execute(harness.pool())
            .await?;
    }
    // Prove the fixture: the table grants hold, and USAGE is only what was
    // granted, with nothing inherited from PUBLIC.
    let (table_ok, update_ok, usage_ok): (bool, bool, bool) = sqlx::query_as(
        "SELECT has_table_privilege($1, $2, 'SELECT'), has_table_privilege($1, $2, 'UPDATE'),
                has_schema_privilege($1, $3, 'USAGE')",
    )
    .bind(&name)
    .bind(table)
    .bind(harness.config().schema().as_str())
    .fetch_one(harness.pool())
    .await?;
    assert!(table_ok);
    assert_eq!(update_ok, grants.contains("UPDATE"));
    assert_eq!(usage_ok, usage);
    Ok(name)
}

async fn connect_as(postgres: &TestPostgres, role: &str) -> TestResult<PgPool> {
    let url = postgres
        .url()
        .replacen("postgres:postgres@", &format!("{role}:secret@"), 1);
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?)
}

fn refused(error: RecoveryError) -> (String, TableAccess, Report) {
    match error {
        RecoveryError::Failed { report, cause, .. } => match *cause {
            Failure::NotRedrivable { table, access } => (table, access, *report),
            other => panic!("expected a named refusal, got {other}"),
        },
        other => panic!("expected a named refusal, got {other}"),
    }
}

/// T3: absent or unreadable storage is a named refusal that changes nothing
/// and creates nothing.
#[tokio::test]
async fn missing_or_inaccessible_storage_is_refused_by_name() -> TestResult {
    let (postgres, harness) = start_harness().await?;
    let table = seed(&harness, &[("f-0", ReceiveStatus::Failed)]).await?;
    let before = statuses(&harness, &table).await?;

    // Never migrated: refused, and still absent afterwards.
    let (name, access, report) = refused(
        redrive(&harness, harness.pool(), "regional", "5")
            .await
            .unwrap_err(),
    );
    assert!(name.ends_with("received_regional_product\""), "{name}");
    assert_eq!(access, TableAccess::Missing);
    assert_eq!(report.failed.unwrap().source, "regional");
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(&name)
        .fetch_one(harness.pool())
        .await?;
    assert!(!exists, "a refused redrive creates nothing");

    // Read-only credentials cannot move rows.
    let reader = role(&harness, &table, "SELECT", true).await?;
    let pool = connect_as(&postgres, &reader).await?;
    let (_, access, _) = refused(redrive(&harness, &pool, "products", "5").await.unwrap_err());
    assert_eq!(access, TableAccess::NoPrivilege);

    // Table grants without schema USAGE: has_table_privilege alone would pass
    // this role, and the UPDATE would then fail with a raw permission error.
    let no_usage = role(&harness, &table, "SELECT, UPDATE", false).await?;
    let pool = connect_as(&postgres, &no_usage).await?;
    let error = redrive(&harness, &pool, "products", "5").await.unwrap_err();
    assert!(error.to_string().contains("USAGE"), "{error}");
    let (_, access, _) = refused(error);
    assert_eq!(access, TableAccess::NoPrivilege);

    assert_eq!(statuses(&harness, &table).await?, before, "no row changed");

    // With USAGE too, the same role moves the row.
    let writer = role(&harness, &table, "SELECT, UPDATE", true).await?;
    let pool = connect_as(&postgres, &writer).await?;
    assert_eq!(moved(&redrive(&harness, &pool, "products", "5").await?), 1);
    Ok(())
}
