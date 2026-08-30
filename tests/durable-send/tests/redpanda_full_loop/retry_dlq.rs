use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::*;

#[tokio::test]
async fn redpanda_input_exhausts_dlq_and_redrives_to_success() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let postgres = postgres().await?;
    let (_redpanda, brokers) = redpanda().await?;
    let schema = unique_schema("kafkaman_rp_dlq");
    let harness =
        Harness::connect_with_config(postgres.url(), retry_test_config(&schema, 2, 10)).await?;
    let received = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;
    publish_order_record(
        &producer,
        OrderCreated::TOPIC,
        "redpanda-dlq-key",
        "order-redpanda-dlq",
        idempotency_headers("idem-rp-dlq-redrive"),
    )
    .await?;

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-dlq-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let ingest = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(ingest.inserted, 1);
    assert_eq!(ingest.duplicates, 0);
    assert_eq!(ingest.committed, 1);

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_handler = Arc::clone(&attempts);
    let failing_router = MessageRouter::new().handler::<OrderCreated>(move |_conn, _meta, _msg| {
        let attempts = Arc::clone(&attempts_for_handler);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            Err(kafkaman_sqlx::Error::Handler(format!(
                "redpanda-dlq-{attempt}"
            )))
        })
    });

    let first_due = time::OffsetDateTime::from_unix_timestamp(1_700_020_000)?;
    let first = dispatch_once(harness.pool(), &received, &failing_router, first_due).await?;
    assert_eq!(first.claimed, 1);
    assert_eq!(first.processed, 0);
    assert_eq!(first.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-dlq-redrive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.source_offset, ingest.offset);

    let exhausted = dispatch_once(
        harness.pool(),
        &received,
        &failing_router,
        first_due + time::Duration::seconds(2),
    )
    .await?;
    assert_eq!(exhausted.claimed, 1);
    assert_eq!(exhausted.processed, 0);
    assert_eq!(exhausted.failed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-dlq-redrive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Failed);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.errors.len(), 2);
    assert!(row.errors[1].detail.contains("redpanda-dlq-2"));

    let replay = Replay::received_descriptor(Replay::RUNTIME_VERSION, OrderCreated::descriptor()?)
        .max_rows(10);
    let moved = redrive_received(harness.pool(), &harness.config(), &replay).await?;
    assert_eq!(moved, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-dlq-redrive")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Pending);
    assert_eq!(row.attempts, 2);
    assert_eq!(
        row.source_offset, ingest.offset,
        "received redrive must preserve the broker offset ordinal"
    );
    assert_eq!(row.errors.len(), 2, "default redrive preserves forensics");

    let success_router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    let success = dispatch_once(
        harness.pool(),
        &received,
        &success_router,
        first_due + time::Duration::seconds(30),
    )
    .await?;
    assert_eq!(success.claimed, 1);
    assert_eq!(success.processed, 1);
    assert_eq!(success.failed, 0);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}
