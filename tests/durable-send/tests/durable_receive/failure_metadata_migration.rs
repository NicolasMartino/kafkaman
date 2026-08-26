use super::*;

/// Every changeset that brings a received table up to the current shape, in the
/// order a changelog must list them: the index keys on both failure columns, so
/// the changeset that adds and fills them has to run first.
fn upgraded(descriptor: kafkaman_core::MessageDescriptor) -> Vec<Box<dyn Changeset>> {
    vec![
        Box::new(InitSchema),
        Box::new(CreateReceivedTable::new(2, descriptor.clone())),
        Box::new(AddReceivedFailureMetadata::new(3, descriptor.clone())),
        Box::new(AddReceivedFailedIndex::new(4, descriptor)),
    ]
}

/// Count the failure-metadata columns present on the received table.
async fn failure_metadata_columns(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        "SELECT count(*)
         FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name = 'received_order_created'
           AND column_name IN ('last_failed_at', 'last_failure_kind')",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
}

/// Whether the DLQ's partial index exists, identified by what it is *for*
/// rather than by its name — a rename should not silently pass this.
async fn dlq_indexes(pool: &sqlx::PgPool, cfg: &kafkaman_sqlx::ResolvedConfig) -> TestResult<i64> {
    let count = sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_indexes
         WHERE schemaname = $1
           AND tablename = 'received_order_created'
           AND indexdef ILIKE '%last_failed_at%'
           AND indexdef ILIKE '%last_failure_kind%'
           AND indexdef ILIKE '%WHERE (status = ''Failed''%'",
    )
    .bind(cfg.schema.as_str())
    .fetch_one(pool)
    .await?;
    Ok(count)
}

/// The stored failure columns, read raw so an un-backfilled `NULL` is visible.
///
/// `ReceivedRow` cannot answer this: reading a row degrades an unrecognized
/// audit `type` to the default kind, which is exactly the value the column must
/// *not* be allowed to invent.
async fn stored_failure(
    pool: &sqlx::PgPool,
    table: &ReceivedTable,
    message_id: uuid::Uuid,
) -> TestResult<(Option<OffsetDateTime>, Option<String>)> {
    let row: (Option<OffsetDateTime>, Option<String>) = sqlx::query_as(&format!(
        "SELECT last_failed_at, last_failure_kind FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(message_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// The `errors` array an older binary wrote.
///
/// `field` selects the spelling: `type` for the RFC 9457 problem detail in use
/// now, `kind` for the bare discriminant that predates it. Both are on disk in
/// the wild, and the backfill has to read both.
fn legacy_errors(field: &str, value: &str, at: OffsetDateTime) -> serde_json::Value {
    serde_json::json!([{
        field: value,
        "title": "recorded by an older binary",
        "detail": "boom",
        "occurred_at": kafkaman_core::rfc9557::render(at)
            .expect("an RFC 9557 timestamp should render"),
    }])
}

/// The same audit entry with its timestamp written verbatim rather than
/// rendered, which is the only way to reproduce a stored value no version of
/// this library would produce: a corrupted, truncated or hand-edited one.
fn legacy_errors_verbatim(field: &str, value: &str, occurred_at: &str) -> serde_json::Value {
    serde_json::json!([{
        field: value,
        "title": "recorded by an older binary",
        "detail": "boom",
        "occurred_at": occurred_at,
    }])
}

/// Insert a dead letter the way a binary without the failure columns would
/// have: terminal status and an audit trail, and nothing else.
async fn insert_legacy_failure(
    pool: &sqlx::PgPool,
    table: &ReceivedTable,
    offset: i64,
    errors: serde_json::Value,
) -> TestResult<uuid::Uuid> {
    let message_id = uuid::Uuid::new_v4();
    sqlx::query(&format!(
        "INSERT INTO {} (
            message_id, idempotency_key, status, attempts, errors, source_topic,
            source_partition, source_offset, message_type, entity_key, payload, occurred_at
         ) VALUES ($1, $2, 'Failed', 3, $3, $4, 0, $5, $6, $7, $8, now())",
        table.qualified_name()
    ))
    .bind(message_id)
    .bind(format!("{:064x}", offset))
    .bind(errors)
    .bind(OrderCreated::TOPIC)
    .bind(offset)
    .bind(OrderCreated::MESSAGE_TYPE)
    .bind(format!("order-legacy-{offset}"))
    .bind(serde_json::json!({ "order_id": format!("order-legacy-{offset}") }))
    .execute(pool)
    .await?;
    Ok(message_id)
}

/// Reproduce the pre-column shape by building the current table and dropping
/// what came later. Hand-writing the old DDL would drift from the real one and
/// stop testing the migration the day a column is added elsewhere.
async fn strip_failure_metadata(
    pool: &sqlx::PgPool,
    cfg: &kafkaman_sqlx::ResolvedConfig,
    table: &ReceivedTable,
    descriptor: &kafkaman_core::MessageDescriptor,
) -> TestResult {
    let base: Vec<Box<dyn Changeset>> = vec![
        Box::new(InitSchema),
        Box::new(CreateReceivedTable::new(2, descriptor.clone())),
    ];
    migrate(pool, cfg, &MigrationContext::default(), &base).await?;
    sqlx::query(&format!(
        "ALTER TABLE {} DROP COLUMN last_failed_at, DROP COLUMN last_failure_kind",
        table.qualified_name()
    ))
    .execute(pool)
    .await?;
    assert_eq!(failure_metadata_columns(pool, cfg).await?, 0);
    Ok(())
}

#[tokio::test]
async fn add_received_failure_metadata_upgrades_a_legacy_received_table() -> TestResult {
    // The upgrade path for a received table created before dispatch recorded why
    // a row last failed. It is the one additive changeset here that is not
    // optional: every DLQ query names `last_failed_at` and every failed dispatch
    // writes `last_failure_kind`, so a table missing them does not degrade — it
    // raises, on the path an operator reaches for during an incident.
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = OrderCreated::descriptor()?;
    let table = ReceivedTable::new(cfg.schema.clone(), descriptor.clone())?;

    strip_failure_metadata(pool, &cfg, &table, &descriptor).await?;

    // Three dead letters written by binaries of three different ages: the
    // problem-detail format in use now, the bare discriminant that predates it,
    // and a failure class this binary has never heard of.
    let now = OffsetDateTime::now_utc();
    let recent_at = now - Duration::from_secs(3 * 60 * 60);
    let ancient_at = now - Duration::from_secs(5 * 60 * 60);
    let unknown_at = now - Duration::from_secs(60 * 60);

    let recent = insert_legacy_failure(
        pool,
        &table,
        11,
        legacy_errors(
            "type",
            ReceivedFailureKind::Handler.problem_type(),
            recent_at,
        ),
    )
    .await?;
    let ancient = insert_legacy_failure(
        pool,
        &table,
        12,
        legacy_errors(
            "kind",
            ReceivedFailureKind::InvalidPayload.discriminant(),
            ancient_at,
        ),
    )
    .await?;
    let unknown = insert_legacy_failure(
        pool,
        &table,
        13,
        legacy_errors(
            "type",
            "urn:kafkaman:problem:from-a-later-release",
            unknown_at,
        ),
    )
    .await?;

    // The failure this changeset exists to prevent, asserted rather than
    // asserted about: the DLQ page names columns the table does not have, and so
    // does the filter an operator redrives by.
    assert!(
        received_failed_rows(pool, &table, &ReceivedFailureFilter::default(), 10)
            .await
            .is_err(),
        "a table without the failure columns must fail the DLQ page, which is \
         why the changeset is mandatory rather than optional"
    );
    let handler_filter = ReceivedFailureFilter::default().kind(ReceivedFailureKind::Handler);
    assert!(
        received_failed_count(pool, &table, &handler_filter)
            .await
            .is_err(),
        "and a kind-filtered count fails the same way"
    );

    // The unfiltered count is the one read that survives, and that is what makes
    // the mismatch quiet rather than loud: it names `status` alone, so the
    // number beside the page goes on counting rows the page cannot render.
    assert_eq!(
        received_failed_count(pool, &table, &ReceivedFailureFilter::default()).await?,
        3
    );

    migrate(
        pool,
        &cfg,
        &MigrationContext::default(),
        &upgraded(descriptor.clone()),
    )
    .await?;
    assert_eq!(failure_metadata_columns(pool, &cfg).await?, 2);
    assert_eq!(dlq_indexes(pool, &cfg).await?, 1);

    // The columns arrive populated, from the audit trail they were always a
    // projection of. Adding them empty would leave every one of these rows
    // visible in `/dlq` — which renders the newest audit entry — and unreachable
    // by every filter, which reads the columns.
    let (failed_at, kind) = stored_failure(pool, &table, recent).await?;
    assert_eq!(kind.as_deref(), Some("Handler"));
    assert_eq!(
        failed_at.map(|at| at.unix_timestamp()),
        Some(recent_at.unix_timestamp()),
        "recovered from the RFC 9557 `occurred_at`, annotation and all"
    );

    let (ancient_failed_at, ancient_kind) = stored_failure(pool, &table, ancient).await?;
    assert_eq!(
        ancient_kind.as_deref(),
        Some("InvalidPayload"),
        "the bare-discriminant spelling is on disk in the wild and must be read"
    );
    assert_eq!(
        ancient_failed_at.map(|at| at.unix_timestamp()),
        Some(ancient_at.unix_timestamp())
    );

    // A kind this binary cannot name stays NULL. Guessing is the one thing the
    // backfill must not do — and could not, since the CHECK arriving alongside
    // it would reject anything invented here.
    let (unknown_failed_at, unknown_kind) = stored_failure(pool, &table, unknown).await?;
    assert_eq!(unknown_kind, None);
    assert_eq!(
        unknown_failed_at.map(|at| at.unix_timestamp()),
        Some(unknown_at.unix_timestamp()),
        "an unreadable kind does not cost the timestamp beside it"
    );

    // Migrations must converge, not merely apply: running the same changelog
    // twice is the property the whole engine rests on.
    migrate(
        pool,
        &cfg,
        &MigrationContext::default(),
        &upgraded(descriptor.clone()),
    )
    .await?;
    assert_eq!(failure_metadata_columns(pool, &cfg).await?, 2);
    assert_eq!(dlq_indexes(pool, &cfg).await?, 1);
    assert_eq!(
        stored_failure(pool, &table, recent).await?.1.as_deref(),
        Some("Handler")
    );

    // Ordering now runs off recovered values, which is what makes the DLQ page
    // read oldest-failure-first across the upgrade rather than sorting every
    // pre-existing row to the end where a `LIMIT` drops it.
    let rows = received_failed_rows(pool, &table, &ReceivedFailureFilter::default(), 10).await?;
    assert_eq!(
        rows.iter().map(|row| row.message_id).collect::<Vec<_>>(),
        vec![ancient, recent, unknown],
        "oldest recovered failure first"
    );
    assert!(
        rows.iter().all(|row| row.errors.len() == 1),
        "the audit history survives verbatim"
    );

    // The review scenario, end to end: an operator reads a kind off the DLQ and
    // redrives by it. Before the backfill this matched zero rows and reported
    // success.
    assert_eq!(
        received_failed_count(pool, &table, &handler_filter).await?,
        1
    );
    let replay = Replay::received_descriptor(Replay::RUNTIME_VERSION, descriptor.clone())
        .failure_kind(ReceivedFailureKind::Handler)
        .max_rows(10);
    assert_eq!(
        redrive_received(pool, &cfg, &replay).await?,
        1,
        "a legacy dead letter must be reachable by the kind an operator can see"
    );
    let status: String = sqlx::query_scalar(&format!(
        "SELECT status FROM {} WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(recent)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        status, "Pending",
        "the row is back in the dispatcher's queue"
    );
    assert_eq!(
        received_failed_count(pool, &table, &handler_filter).await?,
        0,
        "and out of the DLQ it was filtered from"
    );

    // And the row whose kind could not be recovered is *not* swept up by it.
    // Read as a `ReceivedRow` its audit entry degrades to the default kind,
    // `Handler` — the same kind just redriven. The column is what keeps the two
    // apart, and NULL there means "unknown", not "assume the default".
    let unknown_row = received_failed_rows(pool, &table, &ReceivedFailureFilter::default(), 10)
        .await?
        .into_iter()
        .find(|row| row.message_id == unknown)
        .expect("the unknown-kind row is still a dead letter");
    assert_eq!(
        unknown_row.errors[0].kind,
        ReceivedFailureKind::Handler,
        "the audit read degrades an unrecognized type to the default"
    );
    assert_eq!(
        stored_failure(pool, &table, unknown).await?.1,
        None,
        "and the column refuses to make that degradation queryable"
    );

    // The time filter recovers with the timestamp. It fails the same way a kind
    // filter does — `last_failed_at >= x` is NULL, so the row is silently
    // outside every window.
    let since = ReceivedFailureFilter::default().since(now - Duration::from_secs(4 * 60 * 60));
    assert_eq!(
        received_failed_count(pool, &table, &since).await?,
        1,
        "only the unknown-kind row is left inside the window after the redrive"
    );

    // And the CHECK arrived with the column: the vocabulary is enforced by the
    // database, not only by the Rust enum that generates it.
    let bogus = sqlx::query(&format!(
        "UPDATE {} SET last_failure_kind = 'NotAFailureKind' WHERE message_id = $1",
        table.qualified_name()
    ))
    .bind(unknown)
    .execute(pool)
    .await;
    assert!(
        bogus.is_err(),
        "the failure-kind CHECK must land with the column it constrains"
    );

    Ok(())
}

#[tokio::test]
async fn a_date_shaped_non_date_costs_only_its_own_row() -> TestResult {
    // A shape test is not a parser. `2026-99-99T…` matches every regex that
    // describes an RFC 3339 date and still raises on the cast, and so does
    // `2026-02-30` — no pattern can rule out a day the calendar does not have.
    // Inside a single set-based `UPDATE` that raise aborts the statement, the
    // migration transaction, and the boot behind it: one corrupted audit entry
    // written years ago becomes a process that will not start.
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let cfg = harness.config();
    let pool = harness.pool();
    let descriptor = OrderCreated::descriptor()?;
    let table = ReceivedTable::new(cfg.schema.clone(), descriptor.clone())?;

    strip_failure_metadata(pool, &cfg, &table, &descriptor).await?;

    let failed_at = OffsetDateTime::now_utc() - Duration::from_secs(2 * 60 * 60);
    let sound = insert_legacy_failure(
        pool,
        &table,
        21,
        legacy_errors(
            "type",
            ReceivedFailureKind::Handler.problem_type(),
            failed_at,
        ),
    )
    .await?;
    // Both halves of what a cast can refuse: a value that is not date-shaped at
    // all, and one that is and still has no place on a calendar.
    let shapeless = insert_legacy_failure(
        pool,
        &table,
        22,
        legacy_errors_verbatim(
            "type",
            ReceivedFailureKind::Infrastructure.problem_type(),
            "whenever",
        ),
    )
    .await?;
    let impossible = insert_legacy_failure(
        pool,
        &table,
        23,
        legacy_errors_verbatim(
            "kind",
            ReceivedFailureKind::MissingHandler.discriminant(),
            "2026-99-99T10:00:00Z[UTC]",
        ),
    )
    .await?;

    migrate(
        pool,
        &cfg,
        &MigrationContext::default(),
        &upgraded(descriptor.clone()),
    )
    .await?;

    // The row beside them recovers in full. That is the assertion that matters:
    // before the cast ran in its own subtransaction, this row was NULL too — not
    // because its own timestamp was bad, but because another row's was.
    let (sound_at, sound_kind) = stored_failure(pool, &table, sound).await?;
    assert_eq!(sound_kind.as_deref(), Some("Handler"));
    assert_eq!(
        sound_at.map(|at| at.unix_timestamp()),
        Some(failed_at.unix_timestamp())
    );

    // The unreadable timestamps stay NULL — and cost nothing but themselves. The
    // kind beside each is recovered from the same entry, because that pass is
    // string equality and cannot raise.
    for (message_id, kind) in [
        (shapeless, "Infrastructure"),
        (impossible, "MissingHandler"),
    ] {
        let (at, recovered) = stored_failure(pool, &table, message_id).await?;
        assert_eq!(at, None, "an unparseable timestamp is not invented");
        assert_eq!(
            recovered.as_deref(),
            Some(kind),
            "and does not cost the kind stored beside it"
        );
    }

    // Re-running converges rather than retrying the rows it could not read.
    migrate(
        pool,
        &cfg,
        &MigrationContext::default(),
        &upgraded(descriptor),
    )
    .await?;
    assert_eq!(stored_failure(pool, &table, impossible).await?.0, None);
    assert_eq!(
        stored_failure(pool, &table, sound)
            .await?
            .0
            .map(|at| at.unix_timestamp()),
        Some(failed_at.unix_timestamp())
    );

    Ok(())
}
