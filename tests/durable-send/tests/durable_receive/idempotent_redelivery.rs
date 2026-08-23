//! Redelivery converging on one effect per idempotency key.
use super::*;

#[tokio::test]
async fn received_failure_errors_keep_most_recent_twenty_entries() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let schema = format!("kafkaman_retry_{}", uuid::Uuid::new_v4().simple());
    let (_postgres, harness) =
        start_harness_with_config(retry_test_config(&schema, 30, 20)).await?;
    let table = harness.received_table::<OrderCreated>().await?;
    let table_name = format!("{}.{}", table.schema.quoted(), table.table.quoted());

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-error-ring".to_owned(),
    })
    .with_idempotency_key("idem-error-ring");
    assert!(
        harness
            .insert_received(&envelope, 3, 45, Some(b"order-error-ring"))
            .await?
    );

    for attempt in 0..25 {
        if attempt > 0 {
            let unpark_sql = format!(
                "UPDATE {table_name}
                 SET next_attempt_at = $2
                 WHERE idempotency_key = $1"
            );
            sqlx::query(&unpark_sql)
                .bind(idem_key("idem-error-ring").to_string())
                .bind(OffsetDateTime::now_utc())
                .execute(harness.pool())
                .await?;
        }

        let message = format!("boom-{attempt}");
        let router = MessageRouter::new().handler::<OrderCreated>(move |_conn, _meta, _msg| {
            let message = message.clone();
            Box::pin(async move { Err(kafkaman_sqlx::Error::Handler(message)) })
        });
        let stats =
            dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
        assert_eq!(stats.claimed, 1);
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.failed, 1);
    }

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-error-ring")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 25);
    assert_eq!(row.errors.len(), 20);
    assert!(row.errors[0].detail.contains("boom-5"));
    assert!(row.errors[19].detail.contains("boom-24"));

    Ok(())
}

#[tokio::test]
async fn duplicate_received_rows_converge_to_one_dispatch_effect() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    for offset in 100..105 {
        let envelope = Envelope::new(OrderCreated {
            order_id: "order-effective-once".to_owned(),
        })
        .with_idempotency_key("idem-effective-once");
        let inserted = harness
            .insert_received(&envelope, 0, offset, Some(b"order-effective-once"))
            .await?;
        assert_eq!(inserted, offset == 100);
    }

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let now = OffsetDateTime::now_utc();
    let first = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(first.processed, 1);
    let second = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(second.claimed, 0);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn random_redeliveries_converge_to_one_effect_per_idempotency_key() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut first_order_for_key = vec![None::<String>; 12];
    for offset in 0..96 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let logical = if offset < first_order_for_key.len() as i64 {
            offset as usize
        } else {
            ((seed >> 32) % first_order_for_key.len() as u64) as usize
        };
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let duplicate_variant = seed % 31;
        let idempotency_key = format!("idem-property-{logical}");
        let order_id = format!("order-property-{logical}-{duplicate_variant}");
        let envelope = Envelope::new(OrderCreated {
            order_id: order_id.clone(),
        })
        .with_idempotency_key(idempotency_key);

        let inserted = harness
            .insert_received(&envelope, 4, offset, Some(order_id.as_bytes()))
            .await?;
        if inserted {
            first_order_for_key[logical] = Some(order_id);
        }
    }

    let expected_orders = first_order_for_key
        .iter()
        .filter_map(Clone::clone)
        .collect::<Vec<_>>();
    assert_eq!(expected_orders.len(), first_order_for_key.len());

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });

    let mut processed = 0;
    loop {
        let stats =
            dispatch_once(harness.pool(), &table, &router, OffsetDateTime::now_utc()).await?;
        processed += stats.processed;
        if stats.claimed == 0 {
            break;
        }
    }
    assert_eq!(processed, expected_orders.len());

    for (logical, expected_order) in first_order_for_key.iter().enumerate() {
        let row = harness
            .received_row_by_idempotency_key::<OrderCreated>(&format!("idem-property-{logical}"))
            .await?;
        assert_eq!(row.status, ReceiveStatus::Processed);
        assert_eq!(row.payload["order_id"].as_str(), expected_order.as_deref());
    }

    let handled = sqlx::query_scalar::<_, String>(
        "SELECT string_agg(order_id, ',' ORDER BY order_id) FROM handled_orders",
    )
    .fetch_one(harness.pool())
    .await?;
    let mut expected_sorted = expected_orders;
    expected_sorted.sort();
    assert_eq!(handled, expected_sorted.join(","));

    Ok(())
}
