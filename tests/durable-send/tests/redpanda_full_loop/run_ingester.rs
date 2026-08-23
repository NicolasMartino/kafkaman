use super::*;

#[tokio::test]
async fn run_ingester_processes_records_until_cancelled() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let _table = harness.received_table::<OrderCreated>().await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;
    publish_order_record(
        &producer,
        OrderCreated::TOPIC,
        "runner-key",
        "order-runner",
        idempotency_headers("idem-rp-runner"),
    )
    .await?;

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-runner-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let pool = harness.pool().clone();
    let cfg = harness.config();
    let shutdown = CancellationToken::new();
    let runner_shutdown = shutdown.clone();
    let runner = tokio::spawn(async move {
        consumer
            .run_ingester::<OrderCreated>(&pool, &cfg, Duration::from_millis(50), runner_shutdown)
            .await
    });

    let row = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match harness
                .received_row_by_idempotency_key::<OrderCreated>("idem-rp-runner")
                .await
            {
                Ok(row) => break Ok(row),
                Err(HarnessError::MissingReceivedRow { .. }) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => break Err(err),
            }
        }
    })
    .await??;
    assert_eq!(row.idempotency_key, idem_key("idem-rp-runner"));
    assert_eq!(row.key.as_deref(), Some(b"runner-key".as_slice()));

    shutdown.cancel();
    let stats = tokio::time::timeout(Duration::from_secs(10), runner).await???;
    assert_eq!(stats.cycles, 1);
    assert_eq!(stats.consumed, 1);
    assert_eq!(stats.inserted, 1);
    assert_eq!(stats.committed, 1);
    assert_eq!(stats.transient_errors, 0);

    Ok(())
}
