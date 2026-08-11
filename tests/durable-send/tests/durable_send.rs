use std::time::Duration;

use async_trait::async_trait;
use kafkaman_core::{
    ClaimedOutboxRow, Envelope, IdempotencyIdentity, IdempotencyKey, KafkaMessage, MarkOutcome,
    OutboxStatus, PublishAck, SqlIdentifier,
};
use kafkaman_sqlx::{
    changelog, claim_batch, enqueue, mark_publish_failed, mark_published, migrate, migrate_dry_run,
    AddIdempotencyKey, Changeset, CreateOutboxTable, InitSchema, MigrationAction, MigrationContext,
    OutboxTable, Replay,
};
use kafkaman_test::Harness;
use kafkaman_worker::{BoxError, Publisher};
use serde::Serialize;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::OffsetDateTime;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn idem_key(value: &str) -> IdempotencyKey {
    IdempotencyIdentity::derive_legacy_string(value)
        .expect("test idempotency source is valid")
        .key
}

#[derive(Clone, Debug, Serialize)]
struct OrderCreated {
    order_id: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }
}

#[derive(Clone, Debug, Serialize)]
struct InvoiceCreated {
    invoice_id: String,
}

impl KafkaMessage for InvoiceCreated {
    const MESSAGE_TYPE: &'static str = "invoice_created";
    const TOPIC: &'static str = "invoices";

    fn partition_key(&self) -> Option<String> {
        Some(self.invoice_id.clone())
    }
}

#[tokio::test]
async fn migrate_is_idempotent_and_template_generalizes() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;

    harness.outbox_table::<OrderCreated>().await?;
    harness.outbox_table::<InvoiceCreated>().await?;
    let cfg = harness.config();
    let changesets = changelog_for_two_messages()?;

    migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default(),
        &changesets,
    )
    .await?;
    migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default(),
        &changesets,
    )
    .await?;

    let table_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.tables
         WHERE table_schema = $1
           AND table_name IN ('outbox_order_created', 'outbox_invoice_created')",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(table_count, 2);

    let history_count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {}.{}",
        cfg.schema.quoted(),
        "changelog_history"
    ))
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(history_count, 3);

    Ok(())
}

#[tokio::test]
async fn worker_run_loop_relays_until_shutdown() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    for index in 0..3 {
        harness
            .enqueue(
                &Envelope::new(OrderCreated {
                    order_id: format!("order-run-{index}"),
                })
                .with_idempotency_key(format!("idem-order-run-{index}")),
            )
            .await?;
    }

    let publisher = harness.publisher();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(kafkaman_worker::run(
        harness.pool().clone(),
        publisher,
        table,
        cfg.relay.clone(),
        shutdown.clone(),
    ));

    // Wait for the background loop to drain the backlog, then stop it.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while harness.published_on(OrderCreated::TOPIC).len() < 3 {
        if std::time::Instant::now() > deadline {
            panic!("worker did not publish all rows before the deadline");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.cancel();
    worker.await??;

    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 3);
    Ok(())
}

#[tokio::test]
async fn durable_send_publishes_record_and_marks_row() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-1".to_owned(),
    })
    .with_idempotency_key("idem-order-1");
    let message_id = event.message_id;

    harness.enqueue(&event).await?;

    // The idempotency key the caller set must be persisted durably, not dropped.
    let stored = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(stored.idempotency_key, Some(idem_key("idem-order-1")));
    assert_eq!(
        stored.idempotency_source,
        Some(serde_json::json!("idem-order-1"))
    );

    let stats = harness.relay_once::<OrderCreated>().await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    let published = harness.published_on(OrderCreated::TOPIC);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].message_id, message_id);
    assert_eq!(published[0].key.as_deref(), Some("order-1"));

    Ok(())
}

#[tokio::test]
async fn missing_idempotency_is_recorded_as_failed_outbox_row_when_committed() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    sqlx::query("CREATE TABLE committed_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-missing-idem-commit".to_owned(),
    });
    let message_id = event.message_id;

    let mut tx = harness.pool().begin().await?;
    // The caller's own write shares the transaction with the audit row. That is
    // what makes the commit/rollback choice meaningful: kafkaman never decides
    // the fate of business data it does not own.
    sqlx::query("INSERT INTO committed_orders (order_id) VALUES ($1)")
        .bind("order-missing-idem-commit")
        .execute(&mut *tx)
        .await?;
    let error = enqueue(&mut tx, &cfg, &event)
        .await
        .expect_err("missing idempotency must return an error");
    assert!(error.to_string().contains("idempotency"));
    // Committing is the caller electing to keep the business row plus a durable
    // record of why no event accompanies it.
    tx.commit().await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM committed_orders")
            .fetch_one(harness.pool())
            .await?,
        1,
        "committing must keep the caller's business row"
    );

    let row = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(row.status, OutboxStatus::Failed);
    assert_eq!(row.idempotency_key, None);
    assert_eq!(row.idempotency_source, None);
    assert!(row
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("missing idempotency")));

    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 0);
    let claimed = {
        let mut tx = harness.pool().begin().await?;
        let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 10).await?;
        tx.commit().await?;
        claimed
    };
    assert!(claimed.is_empty());

    Ok(())
}

#[tokio::test]
async fn missing_idempotency_audit_row_rolls_back_with_caller_transaction() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    sqlx::query("CREATE TABLE rolled_back_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
        .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-missing-idem-rollback".to_owned(),
    });
    let message_id = event.message_id;

    let mut tx = harness.pool().begin().await?;
    sqlx::query("INSERT INTO rolled_back_orders (order_id) VALUES ($1)")
        .bind("order-missing-idem-rollback")
        .execute(&mut *tx)
        .await?;
    let error = enqueue(&mut tx, &cfg, &event)
        .await
        .expect_err("missing idempotency must return an error");
    assert!(error.to_string().contains("idempotency"));
    // Rolling back is the caller electing atomicity over forensics: no order may
    // exist without its event. The audit row goes with it, and the work is
    // recovered by retry rather than by the audit trail.
    tx.rollback().await?;

    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(message_id)
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(count, 0);

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM rolled_back_orders")
            .fetch_one(harness.pool())
            .await?,
        0,
        "rolling back must discard the caller's business row with the audit row"
    );

    Ok(())
}

/// Pins the current asymmetry in the error-row rule: a reserved-header rejection
/// returns before the insert, so unlike a missing idempotency identity it leaves
/// the caller nothing to commit. This records the behaviour rather than
/// endorsing it — the two invalid-send paths arguably should agree.
#[tokio::test]
async fn reserved_header_rejection_leaves_no_audit_row_to_commit() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let table = harness.outbox_table::<OrderCreated>().await?;
    let cfg = harness.config();

    let mut event = Envelope::new(OrderCreated {
        order_id: "order-reserved-audit".to_owned(),
    })
    .with_idempotency_key("idem-order-reserved-audit");
    event
        .headers
        .insert("kafkaman-message-id".to_owned(), "spoofed".to_owned());
    let message_id = event.message_id;

    let mut tx = harness.pool().begin().await?;
    let error = enqueue(&mut tx, &cfg, &event)
        .await
        .expect_err("reserved header must return an error");
    assert!(
        error.to_string().contains("reserved"),
        "unexpected error: {error}"
    );
    // Even electing to commit yields no record of the rejected send.
    tx.commit().await?;

    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(message_id)
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test]
async fn publish_error_requeues_row_with_last_error() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-error".to_owned(),
    })
    .with_idempotency_key("idem-order-error");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let cfg = harness.config();
    let table = harness.outbox_table::<OrderCreated>().await?;
    let stats =
        kafkaman_worker::relay_once(harness.pool(), &FailingPublisher, &table, &cfg.relay).await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.failed, 1);

    let row = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(row.status, OutboxStatus::Pending);
    assert_eq!(row.attempts, 1);
    assert!(row.claim_id.is_none());
    assert!(row.claimed_by.is_none());
    assert!(row.claim_expires_at.is_none());
    assert!(row
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("synthetic publish failure")));

    Ok(())
}

#[tokio::test]
async fn stale_claim_cannot_mark_row_published() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-stale".to_owned(),
    })
    .with_idempotency_key("idem-order-stale");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let outcome = mark_published(harness.pool(), &table, message_id, Uuid::new_v4()).await?;
    assert_eq!(outcome, MarkOutcome::StaleClaim);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Publishing)
        .await?;

    let outcome = mark_published(harness.pool(), &table, message_id, claimed[0].claim_id).await?;
    assert_eq!(outcome, MarkOutcome::Updated);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    Ok(())
}

#[tokio::test]
async fn ack_before_mark_republishes_after_claim_lease_expiry() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-dup".to_owned(),
    })
    .with_idempotency_key("idem-order-dup");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let publisher = harness.publisher();

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "manual-crash", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    publisher.publish(&claimed[0]).await?;
    assert_eq!(publisher.records_on(OrderCreated::TOPIC).len(), 1);

    let expire_sql = format!(
        "UPDATE {} SET claim_expires_at = now() - interval '1 second' WHERE message_id = $1",
        table.qualified_name()
    );
    sqlx::query(&expire_sql)
        .bind(message_id)
        .execute(harness.pool())
        .await?;

    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;
    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 2);

    Ok(())
}

#[tokio::test]
async fn mark_publish_failed_rejects_stale_claim() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let event = Envelope::new(OrderCreated {
        order_id: "order-failed-stale".to_owned(),
    })
    .with_idempotency_key("idem-order-failed-stale");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let table = harness.outbox_table::<OrderCreated>().await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "worker-a", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let outcome = mark_publish_failed(
        harness.pool(),
        &table,
        message_id,
        Uuid::new_v4(),
        "wrong claim",
        Duration::from_secs(1),
    )
    .await?;
    assert_eq!(outcome, MarkOutcome::StaleClaim);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Publishing)
        .await?;

    Ok(())
}

#[tokio::test]
async fn add_idempotency_key_upgrades_a_pre_idempotency_outbox_table() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = OrderCreated::descriptor()?;
    let table = OutboxTable::new(cfg.schema.clone(), descriptor.clone())?;

    // Simulate a table created before idempotency support: no idempotency_key
    // column, and changeset v2 already recorded as applied so migrate() will not
    // recreate it.
    let legacy_ddl = format!(
        "CREATE TABLE {} (
            message_id UUID PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'Pending',
            topic TEXT NOT NULL,
            correlation_id UUID NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL
        )",
        table.qualified_name()
    );
    sqlx::query(&legacy_ddl).execute(pool).await?;
    sqlx::query(&format!(
        "INSERT INTO {}.\"changelog_history\" (version, name) VALUES (2, 'create_outbox_table')",
        cfg.schema.quoted()
    ))
    .execute(pool)
    .await?;

    assert_eq!(idempotency_key_columns(pool, &cfg).await?, 0);

    let changesets: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(2, descriptor.clone())),
        Box::new(AddIdempotencyKey::new(3, descriptor.clone())),
    ];
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;

    // The upgrade changeset must have added the column, and re-running is a no-op.
    assert_eq!(idempotency_key_columns(pool, &cfg).await?, 1);
    migrate(pool, &cfg, &MigrationContext::default(), &changesets).await?;
    assert_eq!(idempotency_key_columns(pool, &cfg).await?, 1);

    Ok(())
}

#[tokio::test]
async fn config_validation_fails_before_database_work() -> TestResult {
    let config = kafkaman_test::kafkaman_config::Config::from_str(
        r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "worker-a"
        batch_limit = 10
        lease_for = "not-a-duration"
        retry_after = "1s"
        "#,
    )?;

    let err = match Harness::connect_with_config(
        "postgres://postgres:postgres@127.0.0.1:1/should_not_connect",
        config,
    )
    .await
    {
        Ok(_) => panic!("config validation must fail before connecting to Postgres"),
        Err(err) => err,
    };
    let rendered = err.to_string();
    assert!(rendered.contains("relay.lease_for"), "{rendered}");
    assert!(!rendered.contains("connection refused"), "{rendered}");

    Ok(())
}

#[tokio::test]
async fn invalid_retry_config_fails_before_database_work() -> TestResult {
    // A bad retry/DLQ policy must be rejected by the boot resolver, before any
    // pool connect or migration. The Postgres URL points at a dead port to prove
    // resolution never reaches the database.
    let config = kafkaman_test::kafkaman_config::Config::from_str(
        r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "worker-a"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "250ms"

        [retry.defaults]
        max_attempts = 0
        initial_backoff = "100ms"
        max_backoff = "30s"
        multiplier = 2.0
        errors_limit = 16
        dlq = "table"
        "#,
    )?;

    let err = match Harness::connect_with_config(
        "postgres://postgres:postgres@127.0.0.1:1/should_not_connect",
        config,
    )
    .await
    {
        Ok(_) => panic!("retry validation must fail before connecting to Postgres"),
        Err(err) => err,
    };
    let rendered = err.to_string();
    assert!(
        rendered.contains("retry.defaults.max_attempts"),
        "{rendered}"
    );
    assert!(!rendered.contains("connection refused"), "{rendered}");

    Ok(())
}

#[tokio::test]
async fn dry_run_does_not_mutate_legacy_history() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
    let cfg = kafkaman_sqlx::ResolvedConfig::new(schema).with_message(OrderCreated::descriptor()?);
    let history = format!("{}.{}", cfg.schema().quoted(), "\"changelog_history\"");

    // Seed a pre-idempotency, pre-audit M1-shaped history table with one row.
    sqlx::query(&format!("CREATE SCHEMA {}", cfg.schema().quoted()))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "CREATE TABLE {history} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {history} (version, name) VALUES (1, 'init_schema')"
    ))
    .execute(&pool)
    .await?;

    let changesets = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ];
    let ctx = MigrationContext::default().with_applied_by("dry-runner");
    let report = migrate_dry_run(&pool, &cfg, &ctx, &changesets).await?;
    assert_eq!(
        report.steps()[0].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(report.steps()[1].action, MigrationAction::WouldApply);

    // The bootstrap that adds + backfills `applied_by` ran only inside the
    // rolled-back dry-run transaction, so the column must not exist afterwards.
    let applied_by_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 'changelog_history' \
         AND column_name = 'applied_by')",
    )
    .bind(cfg.schema().as_str())
    .fetch_one(&pool)
    .await?;
    assert!(
        !applied_by_exists,
        "dry-run must not persist the applied_by column"
    );

    let row_count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {history}"))
        .fetch_one(&pool)
        .await?;
    assert_eq!(row_count, 1, "dry-run must not insert changelog rows");

    Ok(())
}

#[tokio::test]
async fn migrate_records_report_checksum_and_applied_by() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
    let cfg = kafkaman_sqlx::ResolvedConfig::new(schema)
        .with_message(OrderCreated::descriptor()?)
        .with_message(InvoiceCreated::descriptor()?);
    let changesets = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ];
    let ctx = MigrationContext::default().with_applied_by("integration-test");

    let report = migrate(&pool, &cfg, &ctx, &changesets).await?;
    assert_eq!(report.applied_count(), 2);

    let history = format!("{}.{}", cfg.schema().quoted(), "\"changelog_history\"");
    let rows = sqlx::query(&format!(
        "SELECT version, checksum, applied_by FROM {history} ORDER BY version"
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 2);
    for row in rows {
        let checksum: Option<String> = sqlx::Row::try_get(&row, "checksum")?;
        let applied_by: Option<String> = sqlx::Row::try_get(&row, "applied_by")?;
        assert!(checksum.as_deref().is_some_and(
            |value| value.starts_with("sha256:") && value.len() == "sha256:".len() + 64
        ));
        assert_eq!(applied_by.as_deref(), Some("integration-test"));
    }

    let second = migrate(&pool, &cfg, &ctx, &changesets).await?;
    assert!(second
        .steps()
        .iter()
        .all(|step| step.action == MigrationAction::SkippedAlreadyApplied));

    let mutated = changelog![
        InitSchema,
        CreateOutboxTable::new(2, InvoiceCreated::descriptor()?),
    ];
    let err = migrate(&pool, &cfg, &ctx, &mutated)
        .await
        .expect_err("mutated applied changeset must be rejected");
    assert!(matches!(err, kafkaman_sqlx::Error::ChecksumMismatch { .. }));

    Ok(())
}

#[tokio::test]
async fn legacy_null_checksum_history_upgrades_without_mismatch() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
    let cfg = kafkaman_sqlx::ResolvedConfig::new(schema).with_message(OrderCreated::descriptor()?);
    let history = format!("{}.{}", cfg.schema().quoted(), "\"changelog_history\"");

    sqlx::query(&format!("CREATE SCHEMA {}", cfg.schema().quoted()))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "CREATE TABLE {history} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {history} (version, name) VALUES (1, 'init_schema')"
    ))
    .execute(&pool)
    .await?;

    let changesets = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ];
    let report = migrate(
        &pool,
        &cfg,
        &MigrationContext::default().with_applied_by("upgrade-test"),
        &changesets,
    )
    .await?;

    assert_eq!(
        report.steps()[0].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(report.steps()[1].action, MigrationAction::Applied);

    let legacy = sqlx::query(&format!(
        "SELECT checksum, applied_by FROM {history} WHERE version = 1"
    ))
    .fetch_one(&pool)
    .await?;
    let checksum: Option<String> = sqlx::Row::try_get(&legacy, "checksum")?;
    let applied_by: Option<String> = sqlx::Row::try_get(&legacy, "applied_by")?;
    assert!(checksum.is_none());
    assert_eq!(applied_by.as_deref(), Some("unknown"));

    Ok(())
}

#[test]
fn changelog_macro_rejects_disordered_or_duplicate_versions() {
    let disordered = std::panic::catch_unwind(|| {
        let _ = changelog![
            CreateOutboxTable::new(2, OrderCreated::descriptor().unwrap()),
            InitSchema,
        ];
    });
    assert!(disordered.is_err());

    let duplicate = std::panic::catch_unwind(|| {
        let _ = changelog![
            InitSchema,
            CreateOutboxTable::new(2, OrderCreated::descriptor().unwrap()),
            CreateOutboxTable::new(2, InvoiceCreated::descriptor().unwrap()),
        ];
    });
    assert!(duplicate.is_err());
}

#[tokio::test]
async fn replay_is_bounded_context_targeted_dry_runnable_and_republished() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;

    let events = ["replay-a", "replay-b", "replay-c"]
        .into_iter()
        .map(|order_id| {
            Envelope::new(OrderCreated {
                order_id: order_id.to_owned(),
            })
            .with_idempotency_key(format!("idem-{order_id}"))
        })
        .collect::<Vec<_>>();
    for event in &events {
        harness.enqueue(event).await?;
    }
    let first_relay = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(first_relay.published, 3);

    let cfg = harness.config();
    let table = harness.outbox_table::<OrderCreated>().await?;
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Published).await?,
        3
    );

    let skipped = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
        Replay::outbox::<OrderCreated>(3)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(2)
            .contexts(&["prod"]),
    ];
    let skipped_report = migrate(
        harness.pool(),
        &cfg,
        &MigrationContext::default().with_context("staging"),
        &skipped,
    )
    .await?;
    assert_eq!(
        skipped_report.steps()[2].action,
        MigrationAction::SkippedContext
    );
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Published).await?,
        3
    );

    let replay = changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
        Replay::outbox::<OrderCreated>(3)?
            .since(OffsetDateTime::UNIX_EPOCH)
            .max_rows(2)
            .contexts(&["staging"]),
    ];
    let ctx = MigrationContext::default().with_context("staging");
    let dry_run = migrate_dry_run(harness.pool(), &cfg, &ctx, &replay).await?;
    let preview = dry_run.steps()[2]
        .preview
        .as_deref()
        .expect("replay dry-run should include preview");
    assert!(preview.contains("~2"), "{preview}");
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Published).await?,
        3
    );

    let applied = migrate(harness.pool(), &cfg, &ctx, &replay).await?;
    assert_eq!(applied.steps()[2].action, MigrationAction::Applied);
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Pending).await?,
        2
    );
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Published).await?,
        1
    );

    // Replay resets delivery bookkeeping: requeued rows must start a fresh
    // attempt budget with no carried-over failure state.
    let stale_requeued: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {name} WHERE status = {pending} \
         AND (attempts <> 0 OR last_error IS NOT NULL)",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
    ))
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(
        stale_requeued, 0,
        "replay must clear attempts and last_error"
    );

    let rerun = migrate(harness.pool(), &cfg, &ctx, &replay).await?;
    assert_eq!(
        rerun.steps()[2].action,
        MigrationAction::SkippedAlreadyApplied
    );
    assert_eq!(
        status_count(harness.pool(), &table, OutboxStatus::Pending).await?,
        2
    );

    let second_relay = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(second_relay.published, 2);
    assert_eq!(harness.published_on(OrderCreated::TOPIC).len(), 5);

    Ok(())
}

async fn idempotency_key_columns(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'outbox_order_created'
           AND column_name = 'idempotency_key'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
}

async fn status_count(
    pool: &sqlx::PgPool,
    table: &OutboxTable,
    status: OutboxStatus,
) -> Result<i64, sqlx::Error> {
    let sql = format!(
        "SELECT COUNT(*)::BIGINT FROM {} WHERE status = $1",
        table.qualified_name()
    );
    sqlx::query_scalar::<_, i64>(&sql)
        .bind(status.as_str())
        .fetch_one(pool)
        .await
}

#[tokio::test]
async fn concurrent_registration_on_one_harness_is_safe() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = std::sync::Arc::new(Harness::connect(&database_url).await?);

    // Many tasks register and use the same not-yet-migrated message type at once.
    // None may observe the type registered before its table exists.
    let mut handles = Vec::new();
    for index in 0..8 {
        let harness = harness.clone();
        handles.push(tokio::spawn(async move {
            let event = Envelope::new(InvoiceCreated {
                invoice_id: format!("invoice-{index}"),
            })
            .with_idempotency_key(format!("idem-invoice-{index}"));
            harness.enqueue(&event).await?;
            harness.relay_once::<InvoiceCreated>().await?;
            Ok::<(), kafkaman_test::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    assert_eq!(harness.published_on(InvoiceCreated::TOPIC).len(), 8);
    Ok(())
}

#[tokio::test]
async fn enqueue_rejects_reserved_kafkaman_headers() -> TestResult {
    let (_postgres, database_url) = start_postgres().await?;
    let harness = Harness::connect(&database_url).await?;

    let mut event = Envelope::new(OrderCreated {
        order_id: "order-reserved".to_owned(),
    });
    event
        .headers
        .insert("kafkaman-message-id".to_owned(), "spoofed".to_owned());

    let error = harness.enqueue(&event).await.expect_err("must be rejected");
    assert!(
        error.to_string().contains("reserved"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[derive(Debug)]
struct FailingPublisher;

#[async_trait]
impl Publisher for FailingPublisher {
    async fn publish(&self, _row: &ClaimedOutboxRow) -> Result<PublishAck, BoxError> {
        Err("synthetic publish failure".into())
    }
}

fn changelog_for_two_messages() -> Result<Vec<Box<dyn Changeset>>, kafkaman_sqlx::Error> {
    Ok(changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
        CreateOutboxTable::new(3, InvoiceCreated::descriptor()?),
    ])
}

async fn start_postgres() -> Result<(ContainerAsync<GenericImage>, String), BoxError> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "kafkaman_test")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(port).await?;
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/kafkaman_test");

    Ok((container, database_url))
}
