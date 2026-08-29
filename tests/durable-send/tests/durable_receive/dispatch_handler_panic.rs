//! Dispatch when the handler panics rather than returning.
//!
//! A panic used to unwind out of the handler, out of the dispatch loop's task,
//! and out of the supervised runtime — taking the whole process down, HTTP
//! server included, for one bad message. These tests pin the replacement: the
//! panic is caught at the handler call boundary, recorded as an ordinary handler
//! failure, and retried on the row's normal budget.
//!
//! The loop and breaker tests are the ones that matter most. The single-row
//! tests would still pass if the panic were caught somewhere that killed the
//! loop afterwards.
use super::*;

#[tokio::test]
async fn handler_panic_is_recorded_as_a_handler_failure_and_parks_retryable() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-panic".to_owned(),
    })
    .with_idempotency_key("idem-panic");
    assert!(
        harness
            .insert_received(&envelope, 9, 91, Some(b"order-panic"))
            .await?
    );

    // Writes before the panic, so the savepoint rollback is under test too: a
    // caught panic must unwind the handler's effects exactly as a returned error
    // does, or the retry would run against half-applied state.
    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            panic!("handler exploded");
        })
    });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-panic")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert!(row.next_attempt_at.is_some_and(|retry_at| retry_at > now));
    assert_eq!(row.errors.len(), 1);
    // One kind for both, deliberately: `ReceivedFailureKind` variants carry
    // permanent RFC 9457 URIs, and a panic is a handler failure. The stored
    // detail is what distinguishes it.
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Handler);
    assert!(
        row.errors[0].detail.contains("handler panicked"),
        "a panic must be distinguishable from a returned error in the stored \
         detail, since it shares a failure kind with one: {}",
        row.errors[0].detail
    );
    assert!(row.errors[0].detail.contains("handler exploded"));

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 0);

    Ok(())
}

#[tokio::test]
async fn a_panicking_handler_spends_its_budget_and_dead_letters() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_panic_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 2, 4)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-panic-budget".to_owned(),
    })
    .with_idempotency_key("idem-panic-budget");
    assert!(
        harness
            .insert_received(&envelope, 9, 92, Some(b"order-panic-budget"))
            .await?
    );

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_handler = Arc::clone(&attempts);
    let router = MessageRouter::new().handler::<OrderCreated>(move |_conn, _meta, _msg| {
        let attempts = Arc::clone(&attempts_for_handler);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            // A formatted panic, so the `String` payload arm is exercised as
            // well as the `&'static str` one above.
            panic!("panic-budget-{attempt}");
        })
    });

    let now = OffsetDateTime::from_unix_timestamp(1_700_020_000)?;
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.failed, 1);
    assert_eq!(first.panicked, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-panic-budget")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert!(row.errors[0].detail.contains("panic-budget-1"));

    let second = dispatch_once(
        harness.pool(),
        &table,
        &router,
        now + time::Duration::seconds(2),
    )
    .await?;
    assert_eq!(second.claimed, 1);
    assert_eq!(second.failed, 1);
    assert_eq!(second.panicked, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-panic-budget")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Failed);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.next_attempt_at, None);
    assert_eq!(row.errors.len(), 2);
    assert!(row.errors[1].detail.contains("panic-budget-2"));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    Ok(())
}

#[tokio::test]
async fn a_panicking_handler_does_not_stop_the_dispatch_loop() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_panic_loop_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 5, 5)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    // The panicking row is inserted first and claimed first: rows are taken in
    // `created_at` order, so the good row is genuinely *behind* the bad one and
    // only converges if the loop survived it.
    let poison = Envelope::new(OrderCreated {
        order_id: "order-panic-loop-poison".to_owned(),
    })
    .with_idempotency_key("idem-panic-loop-poison");
    let good = Envelope::new(OrderCreated {
        order_id: "order-panic-loop-good".to_owned(),
    })
    .with_idempotency_key("idem-panic-loop-good");
    assert!(
        harness
            .insert_received(&poison, 9, 93, Some(b"order-panic-loop-poison"))
            .await?
    );
    assert!(
        harness
            .insert_received(&good, 9, 94, Some(b"order-panic-loop-good"))
            .await?
    );

    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let processed_tx = processed_tx.clone();
        Box::pin(async move {
            if msg.order_id.ends_with("poison") {
                panic!("the poison row explodes on every attempt");
            }
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            processed_tx
                .send(())
                .expect("processed receiver should still be alive");
            Ok(())
        })
    });

    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(run_dispatcher(
        harness.pool().clone(),
        table.clone(),
        router,
        DispatcherConfig {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
        shutdown.clone(),
    ));

    // The assertion the other two cannot make: work behind the panic still gets
    // done, which is only possible if the loop is still running.
    tokio::time::timeout(Duration::from_secs(10), processed_rx.recv()).await?;
    assert!(
        !worker.is_finished(),
        "the dispatch loop must survive a panicking handler; a finished task \
         here is the old behaviour, where one bad message took the process down"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), worker).await???;

    let good_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-panic-loop-good")
        .await?;
    assert_eq!(good_row.status, ReceiveStatus::Processed);

    let poison_row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-panic-loop-poison")
        .await?;
    assert!(
        matches!(
            poison_row.status,
            ReceiveStatus::Retryable | ReceiveStatus::Failed
        ),
        "the poison row must be parked for retry or dead-lettered, not lost: {:?}",
        poison_row.status
    );
    assert!(poison_row.attempts >= 1);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn a_pre_upsert_handler_panic_is_recorded_and_rolls_back_its_writes() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY, phase TEXT NOT NULL",
    )
    .await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-before-panic".to_owned(),
    })
    .with_idempotency_key("idem-before-panic");
    assert!(
        harness
            .insert_received(&envelope, 9, 95, Some(b"order-before-panic"))
            .await?
    );

    let router = MessageRouter::new()
        .handler_before::<OrderCreated>(|conn, _meta, msg| {
            Box::pin(async move {
                sqlx::query("INSERT INTO handled_orders (order_id, phase) VALUES ($1, 'before')")
                    .bind(msg.order_id)
                    .execute(conn)
                    .await?;
                panic!("before handler exploded");
            })
        })
        .handler::<OrderCreated>(|conn, _meta, msg| {
            Box::pin(async move {
                sqlx::query("INSERT INTO handled_orders (order_id, phase) VALUES ($1, 'after')")
                    .bind(msg.order_id)
                    .execute(conn)
                    .await?;
                Ok(())
            })
        });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-before-panic")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert!(row.errors[0].detail.contains("before handler exploded"));

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(
        handled, 0,
        "the before-handler write and the after handler must both be absent"
    );

    Ok(())
}

#[tokio::test]
async fn a_handler_panic_while_a_query_is_live_still_records_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-mid-query-panic".to_owned(),
    })
    .with_idempotency_key("idem-mid-query-panic");
    assert!(
        harness
            .insert_received(&envelope, 9, 96, Some(b"order-mid-query-panic"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, _msg| {
        Box::pin(async move {
            let mut query = Box::pin(sqlx::query("SELECT pg_sleep(5)").execute(conn));
            tokio::select! {
                result = &mut query => {
                    result?;
                    Ok(())
                }
                _ = tokio::time::sleep(Duration::from_millis(25)) => {
                    panic!("panic while query future is live");
                }
            }
        })
    });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-mid-query-panic")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert!(row.errors[0]
        .detail
        .contains("panic while query future is live"));

    Ok(())
}

/// Seed `count` rows that all panic, then run the dispatcher with `limit` as its
/// breaker and report how it ended.
///
/// Shared by the three tests below because the thing under test is *which* of
/// them trips: same handler, same loop, same limit, and only the number of
/// distinct rows differs.
async fn run_until_breaker_or_idle(
    schema_tag: &str,
    max_attempts: u32,
    rows: usize,
    limit: usize,
    run_for: Duration,
) -> TestResult<std::result::Result<(), kafkaman_worker::Error>> {
    let schema = format!("kafkaman_{schema_tag}_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) =
        start_harness_with_config(retry_test_config(&schema, max_attempts, 40)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    for idx in 0..rows {
        let order_id = format!("order-{schema_tag}-{idx}");
        let envelope = Envelope::new(OrderCreated {
            order_id: order_id.clone(),
        })
        .with_idempotency_key(format!("idem-{schema_tag}-{idx}"));
        assert!(
            harness
                .insert_received(
                    &envelope,
                    9,
                    100 + i64::try_from(idx).expect("test row count fits an i64"),
                    Some(order_id.as_bytes()),
                )
                .await?
        );
    }

    let router = MessageRouter::new().handler::<OrderCreated>(|_conn, _meta, _msg| {
        Box::pin(async move {
            panic!("every row panics");
        })
    });

    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run_dispatcher(
        harness.pool().clone(),
        table,
        router,
        DispatcherConfig {
            poll_interval: Duration::from_millis(10),
            max_consecutive_panicking_rows: limit,
            ..Default::default()
        },
        shutdown.clone(),
    ));

    // A breaker trip ends the loop on its own; a loop that is behaving has to be
    // stopped, and the time until then is the window it had to misbehave in.
    let mut dispatcher = dispatcher;
    match tokio::time::timeout(run_for, &mut dispatcher).await {
        Ok(joined) => Ok(joined?),
        Err(_elapsed) => {
            shutdown.cancel();
            Ok(tokio::time::timeout(Duration::from_secs(10), dispatcher).await??)
        }
    }
}

/// **The regression this breaker was rewritten for.**
///
/// One poison row, retried past the breaker's limit. Absorbing that is precisely
/// what the retry budget and the dead-letter queue exist for, and stopping the
/// service over it is the failure the panic boundary was built to remove.
///
/// The first implementation counted panics rather than rows, and could not tell
/// this from a bad deploy: `dispatch_once` claims one row per cycle and only a
/// *successful* claim ended a streak, so one row retrying on the default
/// `max_attempts` of 10 reached a limit of 10 — tripping the breaker on the very
/// attempt that dead-lettered it. Six attempts against a limit of three is the
/// same shape, small enough to run fast.
#[tokio::test]
async fn one_poison_row_never_trips_the_breaker_however_often_it_is_retried() -> TestResult {
    let result =
        run_until_breaker_or_idle("panic_one_row", 6, 1, 3, Duration::from_secs(8)).await?;
    assert!(
        result.is_ok(),
        "one row must not trip a breaker meant for a deploy that panics on \
         everything, no matter how many times its budget retries it: {result:?}"
    );
    Ok(())
}

/// Two poison rows alternating, each retried well past the limit.
///
/// The streak is a *set*, so re-seeing a row already in it adds nothing. A
/// counter — or a "did the id change since last time" check — would climb here
/// and trip on two bad messages.
#[tokio::test]
async fn a_few_poison_rows_retrying_never_trip_the_breaker() -> TestResult {
    let result =
        run_until_breaker_or_idle("panic_two_rows", 8, 2, 3, Duration::from_secs(8)).await?;
    assert!(
        result.is_ok(),
        "two rows cycling through their budgets are still two rows, and the \
         breaker counts distinct rows: {result:?}"
    );
    Ok(())
}

/// What the breaker is actually for: a deploy whose handler panics on every row.
#[tokio::test]
async fn distinct_panicking_rows_stop_the_dispatch_loop() -> TestResult {
    let result =
        run_until_breaker_or_idle("panic_many_rows", 20, 3, 3, Duration::from_secs(20)).await?;
    assert!(
        matches!(
            result,
            Err(kafkaman_worker::Error::ConsecutivePanickingRowLimitExceeded { limit: 3, .. })
        ),
        "three distinct rows panicking is a deploy that fails on everything, \
         which no retry budget can absorb: {result:?}"
    );
    Ok(())
}

/// A row that gets through ends the streak, so panics separated by successful
/// work never accumulate into a trip.
#[tokio::test]
async fn a_successful_row_clears_the_panic_streak() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_panic_streak_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) = start_harness_with_config(retry_test_config(&schema, 1, 40)).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    // Alternating poison and good rows, claimed in `created_at` order. With a
    // limit of 2 the streak reaches one and is cleared by each good row, so a
    // breaker that reset only on *panic-free cycles that claimed nothing* — or
    // never reset at all — trips here and a correct one does not.
    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    for idx in 0..4 {
        let poison = format!("order-streak-poison-{idx}");
        let good = format!("order-streak-good-{idx}");
        for (order_id, offset) in [(poison, 200 + idx * 2), (good, 201 + idx * 2)] {
            let envelope = Envelope::new(OrderCreated {
                order_id: order_id.clone(),
            })
            .with_idempotency_key(format!("idem-streak-{order_id}"));
            assert!(
                harness
                    .insert_received(&envelope, 9, offset, Some(order_id.as_bytes()))
                    .await?
            );
        }
    }

    let router = MessageRouter::new().handler::<OrderCreated>(move |_conn, _meta, msg| {
        let processed_tx = processed_tx.clone();
        Box::pin(async move {
            assert!(
                !msg.order_id.contains("poison"),
                "poison row panics on every attempt"
            );
            processed_tx
                .send(msg.order_id)
                .expect("processed receiver should still be alive");
            Ok(())
        })
    });

    let shutdown = CancellationToken::new();
    let dispatcher = tokio::spawn(run_dispatcher(
        harness.pool().clone(),
        table,
        router,
        DispatcherConfig {
            poll_interval: Duration::from_millis(10),
            max_consecutive_panicking_rows: 2,
            ..Default::default()
        },
        shutdown.clone(),
    ));

    for _ in 0..4 {
        tokio::time::timeout(Duration::from_secs(10), processed_rx.recv())
            .await?
            .expect("every good row should be processed between the poison ones");
    }
    assert!(
        !dispatcher.is_finished(),
        "the streak must be cleared by each row that got through"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), dispatcher).await???;
    Ok(())
}
