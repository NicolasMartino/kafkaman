use super::*;

#[tokio::test]
async fn full_loop_consume_then_produce_deduplicates_duplicate_input() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let received_table = harness.received_table::<OrderCreated>().await?;
    let outbox_table = harness.outbox_table::<OrderAccepted>().await?;
    let outbox_name = outbox_table.qualified_name();

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
        "phase4-key",
        "order-phase4",
        idempotency_headers("idem-rp-phase4"),
    )
    .await?;

    let group_id = format!("kafkaman-phase4-{}", Uuid::new_v4());
    let consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;
    let first_ingest = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(first_ingest.inserted, 1);
    assert_eq!(first_ingest.committed, 1);

    let cfg = harness.config();
    let router = MessageRouter::new().handler::<OrderCreated>(move |conn, _meta, msg| {
        let cfg = cfg.clone();
        Box::pin(async move {
            sqlx::query("INSERT INTO handled_orders (order_id) VALUES ($1)")
                .bind(msg.order_id.as_str())
                .execute(&mut *conn)
                .await?;
            let accepted = Envelope::new(OrderAccepted {
                order_id: msg.order_id,
            })
            .with_idempotency_key("accepted-rp-phase4");
            enqueue_on_connection(conn, &cfg, &accepted).await?;
            Ok(())
        })
    });

    let dispatch_stats = dispatch_once(
        harness.pool(),
        &received_table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(dispatch_stats.processed, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE idempotency_key = $1"
        ))
        .bind(idem_hex("accepted-rp-phase4"))
        .fetch_one(harness.pool())
        .await?,
        1
    );

    let relay_stats = harness.relay_once::<OrderAccepted>().await?;
    assert_eq!(relay_stats.claimed, 1);
    assert_eq!(relay_stats.published, 1);

    let accepted_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("kafkaman-accepted-{}", Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    accepted_consumer.subscribe(&[OrderAccepted::TOPIC])?;
    let accepted =
        tokio::time::timeout(Duration::from_secs(30), accepted_consumer.recv()).await??;
    let accepted_payload: OrderAccepted =
        serde_json::from_slice(accepted.payload().expect("accepted payload"))?;
    assert_eq!(
        accepted_payload,
        OrderAccepted {
            order_id: "order-phase4".to_owned(),
        }
    );

    publish_order_record(
        &producer,
        OrderCreated::TOPIC,
        "phase4-key-duplicate",
        "order-phase4-duplicate",
        idempotency_headers("idem-rp-phase4"),
    )
    .await?;
    let duplicate_ingest = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(duplicate_ingest.inserted, 0);
    assert_eq!(duplicate_ingest.duplicates, 1);
    assert_eq!(duplicate_ingest.committed, 1);

    let duplicate_dispatch = dispatch_once(
        harness.pool(),
        &received_table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(duplicate_dispatch.claimed, 0);
    assert_eq!(duplicate_dispatch.processed, 0);
    assert_eq!(duplicate_dispatch.failed, 0);

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM handled_orders")
            .fetch_one(harness.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {outbox_name} WHERE idempotency_key = $1"
        ))
        .bind(idem_hex("accepted-rp-phase4"))
        .fetch_one(harness.pool())
        .await?,
        1
    );

    Ok(())
}

#[tokio::test]
async fn ack_before_mark_republish_is_deduplicated_after_real_broker_hop() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let outbox = harness.outbox_table::<OrderCreated>().await?;
    let received = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-rp-ack-before-mark".to_owned(),
    })
    .with_idempotency_key("idem-rp-ack-before-mark");
    let message_id = event.message_id;
    harness.enqueue(&event).await?;

    let mut tx = harness.pool().begin().await?;
    let claimed = claim_batch(&mut tx, &outbox, "manual-crash", Duration::from_secs(30), 1).await?;
    tx.commit().await?;
    assert_eq!(claimed.len(), 1);

    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let first_ack = publisher.publish_row(&claimed[0]).await?;
    assert_eq!(first_ack.topic, OrderCreated::TOPIC);

    let row = harness.outbox_row::<OrderCreated>(message_id).await?;
    assert_eq!(
        row.status,
        OutboxStatus::Publishing,
        "the simulated crash happens after the broker ack but before mark_published"
    );

    let expire_sql = format!(
        "UPDATE {} SET claim_expires_at = now() - interval '1 second' WHERE message_id = $1",
        outbox.qualified_name()
    );
    sqlx::query(&expire_sql)
        .bind(message_id)
        .execute(harness.pool())
        .await?;

    let relay_stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(relay_stats.claimed, 1);
    assert_eq!(relay_stats.published, 1);
    harness
        .assert_status::<OrderCreated>(message_id, OutboxStatus::Published)
        .await?;

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-ackmark-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let inserted = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(inserted.inserted, 1);
    assert_eq!(inserted.duplicates, 0);
    assert_eq!(inserted.committed, 1);

    let duplicate = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(duplicate.inserted, 0);
    assert_eq!(duplicate.duplicates, 1);
    assert_eq!(duplicate.committed, 1);
    assert_eq!(duplicate.partition, inserted.partition);
    assert!(
        duplicate.offset > inserted.offset,
        "the republish must be a second broker record, not a hidden database retry"
    );

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-ack-before-mark")
        .await?;
    assert_eq!(
        row.source_offset, inserted.offset,
        "the duplicate broker record must not rewrite the durable received row"
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
    let dispatch_stats = dispatch_once(
        harness.pool(),
        &received,
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
async fn ingest_deduplicates_redelivery_after_crash_before_offset_commit() -> TestResult {
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
        "crash-window-key",
        "order-crash-window",
        idempotency_headers("idem-rp-crash-window"),
    )
    .await?;

    let group_id = format!("kafkaman-crash-window-{}", Uuid::new_v4());
    let consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?
        .with_post_durable_write_hook(|context| {
            assert_eq!(context.outcome, ReceivedInsertOutcome::Inserted);
            Err(RdkafkaError::Observer(
                "forced crash before offset commit".to_owned(),
            ))
        });
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let err = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await?
    .expect_err("post-durable-write hook should fail before offset commit");
    assert!(matches!(err, RdkafkaError::Observer(_)));

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-crash-window")
        .await?;
    assert_eq!(row.key.as_deref(), Some(b"crash-window-key".as_slice()));

    drop(consumer);
    let retry_consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?;
    retry_consumer.subscribe(&[OrderCreated::TOPIC])?;
    let retry = tokio::time::timeout(
        Duration::from_secs(30),
        retry_consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(retry.inserted, 0);
    assert_eq!(retry.duplicates, 1);
    assert_eq!(retry.committed, 1);
    assert_eq!(retry.partition, row.source_partition);
    assert_eq!(retry.offset, row.source_offset);

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
async fn run_ingester_deduplicates_redelivery_after_offset_commit_uncertainty() -> TestResult {
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
        "uncertain-commit-key",
        "order-uncertain-commit",
        idempotency_headers("idem-rp-uncertain-commit"),
    )
    .await?;

    let group_id = format!("kafkaman-uncertain-commit-{}", Uuid::new_v4());
    let shutdown = CancellationToken::new();
    let hook_shutdown = shutdown.clone();
    let consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?
        .with_post_durable_write_hook(move |context| {
            assert_eq!(context.outcome, ReceivedInsertOutcome::Inserted);
            hook_shutdown.cancel();
            Err(RdkafkaError::Observer(
                "simulated offset commit uncertainty".to_owned(),
            ))
        });
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let pool = harness.pool().clone();
    let cfg = harness.config();
    let runner_shutdown = shutdown.clone();
    let runner = tokio::spawn(async move {
        consumer
            .run_ingester::<OrderCreated>(&pool, &cfg, Duration::from_millis(50), runner_shutdown)
            .await
    });

    let stats = tokio::time::timeout(Duration::from_secs(30), runner).await???;
    assert_eq!(stats.cycles, 0);
    assert_eq!(stats.consumed, 0);
    assert_eq!(stats.committed, 0);
    assert_eq!(stats.transient_errors, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-uncertain-commit")
        .await?;
    assert_eq!(row.key.as_deref(), Some(b"uncertain-commit-key".as_slice()));

    let retry_consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?;
    retry_consumer.subscribe(&[OrderCreated::TOPIC])?;
    let retry = tokio::time::timeout(
        Duration::from_secs(30),
        retry_consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(retry.inserted, 0);
    assert_eq!(retry.duplicates, 1);
    assert_eq!(retry.committed, 1);
    assert_eq!(retry.partition, row.source_partition);
    assert_eq!(retry.offset, row.source_offset);

    Ok(())
}
