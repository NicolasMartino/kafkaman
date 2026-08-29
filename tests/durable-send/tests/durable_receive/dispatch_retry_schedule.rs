//! Failures that are retried rather than parked, and when they come back.
use kafkaman_core::ProblemType as _;

use super::*;

#[tokio::test]
async fn handler_sql_constraint_error_records_its_class_and_its_frame() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;
    sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
        .bind("order-business-constraint")
        .execute(harness.pool())
        .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-business-constraint".to_owned(),
    })
    .with_idempotency_key("idem-business-constraint");
    assert!(
        harness
            .insert_received(&envelope, 5, 68, Some(b"order-business-constraint"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let stats = dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-business-constraint")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.errors.len(), 1);

    // Two fields, and they have to be read together. This row used to record
    // `Handler` alone, which said "something inside the handler" and nothing
    // about what; it now records the class of the failure and the frame it came
    // out of separately, which is strictly more than the single value carried.
    //
    // `Infrastructure` for a constraint violation is coarse, and deliberately
    // so for now: kafkaman cannot tell a unique violation from a pool exhaustion
    // without inspecting SQLSTATE, and both arrive as `Error::Sqlx`. The
    // coarseness was always there — it was previously masked by the catch-all
    // that relabelled everything a handler returned. Refining it is named in the
    // taxonomy/blame decision's "Revisit If"; it would add a telemetry URI, not
    // a stored class, since none of the four fits a constraint violation better.
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Infrastructure);
    assert_eq!(
        row.errors[0].stage,
        Some(kafkaman_core::FailureStage::Handler),
        "the frame is what says this was the handler's own statement rather \
         than kafkaman's bookkeeping"
    );

    // The telemetry class, against a real `PgDatabaseError` rather than a stub.
    // This is the path the unit tests cannot reach: a genuine unique violation
    // answers `kind()` directly, so the SQLSTATE table is never consulted.
    let refused = sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
        .bind("order-business-constraint")
        .execute(harness.pool())
        .await
        .expect_err("the row is already there");
    assert_eq!(
        kafkaman_sqlx::Error::Sqlx(refused).problem_type(),
        "urn:kafkaman:problem:constraint",
        "a refused write groups apart from a broken environment in APM, even \
         though both still store as Infrastructure"
    );

    Ok(())
}

#[tokio::test]
async fn retryable_failure_schedules_backoff_and_due_dispatch() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_retry_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 3, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-retry-backoff".to_owned(),
    })
    .with_idempotency_key("idem-retry-backoff");
    assert!(
        harness
            .insert_received(&envelope, 8, 88, Some(b"order-retry-backoff"))
            .await?
    );

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_handler = Arc::clone(&attempts);
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let attempts = Arc::clone(&attempts_for_handler);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                return Err(kafkaman_sqlx::Error::Handler("retry-once".to_owned()));
            }
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.claimed, 1);
    assert_eq!(first.processed, 0);
    assert_eq!(first.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-retry-backoff")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    // Backoff carries equal jitter, so the retry lands in [base/2, base] rather
    // than on an exact instant. Pinning the instant would be pinning the absence
    // of jitter, which is the thundering-herd bug the jitter exists to prevent.
    assert_retry_within(row.next_attempt_at, now, 2);

    let early = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(1),
    )
    .await?;
    assert_eq!(early.claimed, 0);
    assert_eq!(early.processed, 0);
    assert_eq!(early.failed, 0);

    let due = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(2),
    )
    .await?;
    assert_eq!(due.claimed, 1);
    assert_eq!(due.processed, 1);
    assert_eq!(due.failed, 0);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-retry-backoff")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Processed);
    assert_eq!(row.attempts, 1);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}
