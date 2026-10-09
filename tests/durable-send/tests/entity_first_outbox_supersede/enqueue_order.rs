//! INSERT order, including upgrades from the shipped slot-zero schema.
use super::*;
use kafkaman_sqlx::{migrate, MigrationContext, OutboxTable, Role, RoleRegistry};
use uuid::Uuid;

#[tokio::test]
async fn enqueue_between_collapse_and_claim_cannot_leave_a_stale_retry() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let cfg = harness.config();
    let mut last = harness.pool().begin().await?;
    let mut middle = harness.pool().begin().await?;
    for tx in [&mut last, &mut middle] {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut **tx)
            .await?;
        sqlx::query("SELECT now()").execute(&mut **tx).await?;
    }
    let v4 = product("boundary", "v4").with_idempotency_key("v4");
    let v5 = product("boundary", "v5").with_idempotency_key("v5");
    let v6 = product("boundary", "v6").with_idempotency_key("v6");
    harness.enqueue(&v4).await?;
    enqueue(&mut middle, &cfg, &v5).await?;
    middle.commit().await?;
    // Pause the relay inside the first collapse UPDATE, after its snapshot was
    // taken. A database lock provides the boundary without scheduler sleeps.
    sqlx::query(&format!(
        "CREATE FUNCTION {}.pause_collapse() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock(706109); RETURN NEW; END $$",
        cfg.schema.quoted()
    ))
    .execute(harness.pool())
    .await?;
    sqlx::query(&format!(
        "CREATE TRIGGER pause_collapse BEFORE UPDATE ON {} FOR EACH ROW
         WHEN (OLD.message_id = '{}'::uuid AND NEW.status = 'Superseded')
         EXECUTE FUNCTION {}.pause_collapse()",
        table.qualified_name(),
        v4.message_id,
        cfg.schema.quoted()
    ))
    .execute(harness.pool())
    .await?;
    let mut blocker = harness.pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(706109)")
        .execute(&mut *blocker)
        .await?;
    let pool = harness.pool().clone();
    let relay_table = table.clone();
    let relay = tokio::spawn(async move {
        let mut tx = pool.begin().await?;
        let rows = claim_batch(
            &mut tx,
            &relay_table,
            "boundary",
            Duration::from_secs(30),
            10,
        )
        .await?;
        tx.commit().await?;
        Ok::<_, kafkaman_sqlx::Error>(rows)
    });
    timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE locktype = 'advisory' AND objid = 706109 AND NOT granted)"
            ).fetch_one(harness.pool()).await?;
            if waiting { return Ok::<_, sqlx::Error>(()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    enqueue(&mut last, &cfg, &v6).await?;
    last.commit().await?;
    blocker.commit().await?;
    let rows = timeout(Duration::from_secs(5), relay).await???;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row.message_id, v6.message_id);
    assert_eq!(
        mark_published(harness.pool(), &table, v6.message_id, rows[0].claim_id).await?,
        MarkOutcome::Updated
    );
    assert_eq!(
        harness.relay_once::<ProductSnapshot>().await?.published,
        0,
        "a predecessor missed by the first collapse must never publish after v6"
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(v5.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    Ok(())
}

#[tokio::test]
async fn pending_sibling_outside_collapse_window_cannot_be_claimed() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    // An in-flight entity's next row would occupy an unfiltered collapse window
    // but cannot be claimed. Both windows must reach the other entity's pair.
    harness
        .enqueue(&product("blocked", "inflight").with_idempotency_key("inflight"))
        .await?;
    let mut tx = harness.pool().begin().await?;
    assert_eq!(
        claim_batch(&mut tx, &table, "blocker", Duration::from_secs(30), 1)
            .await?
            .len(),
        1
    );
    tx.commit().await?;
    harness
        .enqueue(&product("blocked", "pending").with_idempotency_key("blocked"))
        .await?;
    let mut fixed = harness.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *fixed)
        .await?;
    sqlx::query("SELECT now()").execute(&mut *fixed).await?;
    let old = product("target", "old").with_idempotency_key("old");
    let new = product("target", "new").with_idempotency_key("new");
    harness.enqueue(&old).await?;
    enqueue(&mut fixed, &harness.config(), &new).await?;
    fixed.commit().await?;
    let mut tx = harness.pool().begin().await?;
    let rows = claim_batch(&mut tx, &table, "target", Duration::from_secs(30), 1).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row.message_id, new.message_id);
    tx.commit().await?;
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(old.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    assert_eq!(
        mark_published(harness.pool(), &table, new.message_id, rows[0].claim_id).await?,
        MarkOutcome::Updated
    );
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 0);
    Ok(())
}

#[tokio::test]
async fn newer_pending_backoff_blocks_older_due_sibling() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let mut fixed = harness.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *fixed)
        .await?;
    sqlx::query("SELECT now()").execute(&mut *fixed).await?;
    let old = product("backoff", "old").with_idempotency_key("old");
    let new = product("backoff", "new").with_idempotency_key("new");
    harness.enqueue(&old).await?;
    enqueue(&mut fixed, &harness.config(), &new).await?;
    fixed.commit().await?;
    sqlx::query(&format!(
        "UPDATE {} SET next_attempt_at = now() + interval '60 seconds' WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(new.message_id)
    .execute(harness.pool())
    .await?;
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 0);
    Ok(())
}

#[tokio::test]
async fn enqueue_order_survives_live_writer_retry_without_resync() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let mut early = harness.pool().begin().await?;
    sqlx::query("SELECT now()").execute(&mut *early).await?;
    let old = product("live", "v5").with_idempotency_key("v5");
    let new = product("live", "v6").with_idempotency_key("v6");
    harness.enqueue(&old).await?;
    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &table, "old", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    enqueue(&mut early, &harness.config(), &new).await?;
    early.commit().await?;
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 0);
    assert_eq!(
        mark_publish_failed(
            harness.pool(),
            &table,
            old.message_id,
            claimed[0].claim_id,
            "broker unavailable",
            Duration::from_secs(60)
        )
        .await?,
        MarkOutcome::Updated
    );
    assert_eq!(harness.relay_once::<ProductSnapshot>().await?.published, 1);
    assert_eq!(
        harness.published_on(ProductSnapshot::TOPIC)[0].payload["name"],
        "v6"
    );
    assert_eq!(
        harness
            .outbox_row::<ProductSnapshot>(old.message_id)
            .await?
            .status,
        OutboxStatus::Superseded
    );
    Ok(())
}

#[tokio::test]
async fn claim_limit_and_return_order_follow_enqueue_order() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    let cfg = harness.config();
    let mut early = harness.pool().begin().await?;
    sqlx::query("SELECT now()").execute(&mut *early).await?;
    let first = product("p-first", "first").with_idempotency_key("first");
    let second = product("p-second", "second").with_idempotency_key("second");
    harness.enqueue(&first).await?;
    enqueue(&mut early, &cfg, &second).await?;
    early.commit().await?;
    assert!(
        harness
            .outbox_row::<ProductSnapshot>(second.message_id)
            .await?
            .created_at
            < harness
                .outbox_row::<ProductSnapshot>(first.message_id)
                .await?
                .created_at
    );
    let mut tx = harness.pool().begin().await?;
    let one = claim_batch(&mut tx, &table, "one", Duration::from_secs(30), 1).await?;
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].row.message_id, first.message_id);
    tx.rollback().await?;
    let mut tx = harness.pool().begin().await?;
    let all = claim_batch(&mut tx, &table, "all", Duration::from_secs(30), 10).await?;
    assert_eq!(
        all.iter().map(|row| row.row.message_id).collect::<Vec<_>>(),
        [first.message_id, second.message_id]
    );
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn migration_backfills_legacy_order_and_advances_new_inserts() -> TestResult {
    let (_postgres, harness) = start_harness().await?;
    let descriptor = ProductSnapshot::descriptor()?;
    let cfg = harness.config().with_message(descriptor.clone());
    let table = OutboxTable::for_message::<ProductSnapshot>(&cfg)?;
    let mut roles = RoleRegistry::new();
    roles.declare(descriptor, Role::Publish)?;
    let legacy = roles
        .changelog()?
        .into_iter()
        .filter(|step| step.name() != "add_outbox_enqueue_order")
        .collect::<Vec<_>>();
    let ctx = MigrationContext::default();
    migrate(harness.pool(), &cfg, &ctx, &legacy).await?;
    let old = Uuid::from_u128(1);
    let newer = Uuid::from_u128(2);
    // Physical insertion order disagrees with the legacy timestamp/UUID order.
    for id in [newer, old] {
        sqlx::query(&format!(
            "INSERT INTO {} (message_id, topic, entity_key, correlation_id, payload, occurred_at, created_at)
             VALUES ($1, $2, 'legacy', $1, '{{}}', now(), '2030-01-01')", table.qualified_name()
        )).bind(id).bind(ProductSnapshot::TOPIC).execute(harness.pool()).await?;
    }
    let changelog = roles.changelog()?;
    let report = migrate(harness.pool(), &cfg, &ctx, &changelog).await?;
    assert_eq!(
        report.applied_count(),
        1,
        "existing outboxes need a versioned upgrade"
    );
    let ordinals: Vec<(Uuid, i64)> = sqlx::query_as(&format!(
        "SELECT message_id, enqueue_order FROM {} ORDER BY enqueue_order",
        table.qualified_name()
    ))
    .fetch_all(harness.pool())
    .await?;
    assert_eq!(ordinals, [(old, 1), (newer, 2)]);
    assert_eq!(
        migrate(harness.pool(), &cfg, &ctx, &changelog)
            .await?
            .applied_count(),
        0
    );
    // A manual history can later acquire the generated upgrade identity too.
    let duplicate_upgrade: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(
        kafkaman_sqlx::AddOutboxEnqueueOrder::new(2, ProductSnapshot::descriptor()?),
    )];
    migrate(harness.pool(), &cfg, &ctx, &duplicate_upgrade).await?;
    let fresh = product("fresh", "fresh").with_idempotency_key("fresh");
    let mut tx = harness.pool().begin().await?;
    enqueue(&mut tx, &cfg, &fresh).await?;
    tx.commit().await?;
    let ordinal: i64 = sqlx::query_scalar(&format!(
        "SELECT enqueue_order FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(fresh.message_id)
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(ordinal, 3);
    // Limit one must collapse the oldest legacy driver against its sibling
    // outside the claim window, then claim the surviving legacy row first.
    let mut tx = harness.pool().begin().await?;
    let claim = claim_batch(&mut tx, &table, "migration", Duration::from_secs(30), 1).await?;
    assert_eq!(claim.len(), 1);
    assert_eq!(claim[0].row.message_id, newer);
    tx.commit().await?;
    let status: String = sqlx::query_scalar(&format!(
        "SELECT status FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(old)
    .fetch_one(harness.pool())
    .await?;
    assert_eq!(status, "Superseded");
    assert_eq!(
        mark_published(harness.pool(), &table, newer, claim[0].claim_id).await?,
        MarkOutcome::Updated
    );
    let mut tx = harness.pool().begin().await?;
    let claim = claim_batch(&mut tx, &table, "migration", Duration::from_secs(30), 1).await?;
    assert_eq!(claim.len(), 1);
    assert_eq!(claim[0].row.message_id, fresh.message_id);
    tx.commit().await?;
    Ok(())
}
