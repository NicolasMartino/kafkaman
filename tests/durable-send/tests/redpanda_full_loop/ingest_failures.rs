use super::*;

#[tokio::test]
async fn ingest_skips_poison_record_then_deduplicates_redelivery() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

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
        "ingest-sequence",
        "poison-case-header",
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some(""),
        }),
    )
    .await?;

    let message_id = Uuid::new_v4().to_string();
    for _ in 0..2 {
        publish_order_record(
            &producer,
            OrderCreated::TOPIC,
            "ingest-sequence",
            "order-redpanda-redelivery",
            with_idempotency_header(OwnedHeaders::new(), "idem-rp-redelivery").insert(Header {
                key: "kafkaman-message-id",
                value: Some(message_id.as_str()),
            }),
        )
        .await?;
    }

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-poison-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let skipped = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(skipped.consumed, 1);
    assert_eq!(skipped.skipped, 1);
    assert_eq!(skipped.inserted, 0);
    assert_eq!(skipped.committed, 1);
    let failure = received_ingest_failure_by_source(
        harness.pool(),
        &harness.config(),
        OrderCreated::TOPIC,
        skipped.partition,
        skipped.offset,
    )
    .await?
    .expect("skipped poison record should be durably quarantined");
    assert_eq!(failure.kind, ReceivedIngestFailureKind::InvalidHeader);
    assert_eq!(failure.expected_topic, OrderCreated::TOPIC);
    assert_eq!(failure.message_type, OrderCreated::MESSAGE_TYPE);
    assert!(failure.error.contains("kafkaman-idempotency-key"));

    let inserted = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(inserted.skipped, 0);
    assert_eq!(inserted.inserted, 1);
    assert_eq!(inserted.duplicates, 0);

    let duplicate = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(duplicate.skipped, 0);
    assert_eq!(duplicate.inserted, 0);
    assert_eq!(duplicate.duplicates, 1);
    assert_eq!(duplicate.committed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-redelivery")
        .await?;
    assert_eq!(row.source_offset, inserted.offset);

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, msg| {
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id)
                .execute(conn)
                .await?;
            Ok(())
        })
    });
    let dispatch_stats = dispatch_once(
        harness.pool(),
        &table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(dispatch_stats.processed, 1);

    let handled: i64 = sqlx::query_scalar("SELECT count(*) FROM handled_orders")
        .fetch_one(harness.pool())
        .await?;
    assert_eq!(handled, 1);

    Ok(())
}

#[tokio::test]
async fn ingest_circuit_breaker_stops_committing_repeated_schema_failures() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let _table = harness.received_table::<OrderCreated>().await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    for idx in 1..=2 {
        let idempotency_key = format!("idem-schema-break-{idx}");
        publish_raw_record(
            &producer,
            OrderCreated::TOPIC,
            &format!("schema-break-{idx}"),
            br#"{"order_id":42}"#,
            idempotency_headers(&idempotency_key),
        )
        .await?;
    }

    let group_id = format!("kafkaman-breaker-{}", Uuid::new_v4());
    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &group_id)?.with_max_consecutive_skips(2);
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let first = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(first.skipped, 1);
    assert_eq!(first.committed, 1);
    let failure = received_ingest_failure_by_source(
        harness.pool(),
        &harness.config(),
        OrderCreated::TOPIC,
        first.partition,
        first.offset,
    )
    .await?
    .expect("first schema failure should be quarantined");
    assert_eq!(failure.kind, ReceivedIngestFailureKind::InvalidPayload);

    let err = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await?
    .expect_err("second consecutive schema failure should trip the breaker");
    let (partition, offset) = match err {
        RdkafkaError::ConsecutiveSkipLimitExceeded {
            partition, offset, ..
        } => (partition, offset),
        other => panic!("unexpected ingest error: {other}"),
    };
    let failure = received_ingest_failure_by_source(
        harness.pool(),
        &harness.config(),
        OrderCreated::TOPIC,
        partition,
        offset,
    )
    .await?
    .expect("breaker record should be quarantined before refusing commit");
    assert_eq!(failure.kind, ReceivedIngestFailureKind::InvalidPayload);

    drop(consumer);
    let retry_consumer =
        RdkafkaConsumer::from_brokers(&brokers, &group_id)?.with_max_consecutive_skips(2);
    retry_consumer.subscribe(&[OrderCreated::TOPIC])?;
    let retry = tokio::time::timeout(
        Duration::from_secs(30),
        retry_consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(retry.skipped, 1);
    assert_eq!(retry.committed, 1);
    assert_eq!(retry.partition, partition);
    assert_eq!(retry.offset, offset);

    Ok(())
}

#[tokio::test]
async fn run_ingester_stops_loudly_on_consecutive_schema_failures() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let _table = harness.received_table::<OrderCreated>().await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    for idx in 1..=2 {
        let idempotency_key = format!("idem-runner-schema-break-{idx}");
        publish_raw_record(
            &producer,
            OrderCreated::TOPIC,
            &format!("runner-schema-break-{idx}"),
            br#"{"order_id":42}"#,
            idempotency_headers(&idempotency_key),
        )
        .await?;
    }

    let group_id = format!("kafkaman-runner-breaker-{}", Uuid::new_v4());
    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &group_id)?.with_max_consecutive_skips(2);
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let pool = harness.pool().clone();
    let cfg = harness.config();
    let shutdown = CancellationToken::new();
    let runner = tokio::spawn(async move {
        consumer
            .run_ingester::<OrderCreated>(&pool, &cfg, Duration::from_millis(50), shutdown)
            .await
    });

    let err = tokio::time::timeout(Duration::from_secs(30), runner)
        .await??
        .expect_err("second consecutive schema failure should stop the runner");
    let (partition, offset) = match err {
        RdkafkaError::ConsecutiveSkipLimitExceeded {
            partition, offset, ..
        } => (partition, offset),
        other => panic!("unexpected runner error: {other}"),
    };
    let failure = received_ingest_failure_by_source(
        harness.pool(),
        &harness.config(),
        OrderCreated::TOPIC,
        partition,
        offset,
    )
    .await?
    .expect("breaker record should be quarantined before the runner stops");
    assert_eq!(failure.kind, ReceivedIngestFailureKind::InvalidPayload);

    let retry_consumer =
        RdkafkaConsumer::from_brokers(&brokers, &group_id)?.with_max_consecutive_skips(2);
    retry_consumer.subscribe(&[OrderCreated::TOPIC])?;
    let retry = tokio::time::timeout(
        Duration::from_secs(30),
        retry_consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(retry.skipped, 1);
    assert_eq!(retry.committed, 1);
    assert_eq!(retry.partition, partition);
    assert_eq!(retry.offset, offset);

    Ok(())
}

#[tokio::test]
async fn ingest_skips_records_from_unexpected_source_topic() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let _table = harness.received_table::<OrderCreated>().await?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;
    publish_order_record(
        &producer,
        "wrong-orders",
        "wrong-topic-key",
        "order-wrong-topic",
        idempotency_headers("idem-wrong-topic"),
    )
    .await?;

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-topic-{}", Uuid::new_v4()))?;
    consumer.subscribe(&["wrong-orders"])?;
    let skipped = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(skipped.skipped, 1);
    assert_eq!(skipped.inserted, 0);
    assert_eq!(skipped.committed, 1);

    let err = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-wrong-topic")
        .await
        .expect_err("unexpected-topic record must not be stored");
    assert!(err.to_string().contains("idem-wrong-topic"));

    Ok(())
}
