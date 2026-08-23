use super::*;

#[tokio::test]
async fn full_loop_publishes_to_redpanda_and_consumer_reads_it_back() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-redpanda".to_owned(),
    })
    .with_idempotency_key("idem-rp");
    let message_id = event.message_id;
    let mut event = event;
    event
        .headers
        .insert("x-user-header".to_owned(), "user-value".to_owned());

    harness.enqueue(&event).await?;
    let stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("kafkaman-test-{}", Uuid::new_v4()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .map_err(|_| "timed out waiting for the published record on Redpanda")??;

    let payload = message.payload().ok_or("record had no payload")?;
    let consumed: OrderCreated = serde_json::from_slice(payload)?;
    assert_eq!(
        consumed,
        OrderCreated {
            order_id: "order-redpanda".to_owned()
        }
    );

    let key = message
        .key()
        .map(|k| String::from_utf8_lossy(k).into_owned());
    assert_eq!(key.as_deref(), Some("order-redpanda"));

    let mut headers = HashMap::new();
    if let Some(record_headers) = message.headers() {
        for header in record_headers.iter() {
            let value = header
                .value
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default();
            headers.insert(header.key.to_owned(), value);
        }
    }

    assert_eq!(
        headers.get("kafkaman-message-id").map(String::as_str),
        Some(message_id.to_string().as_str())
    );
    let idem_rp = idem_hex("idem-rp");
    assert_eq!(
        headers.get("kafkaman-idempotency-key").map(String::as_str),
        Some(idem_rp.as_str())
    );
    assert_eq!(
        headers.get("x-user-header").map(String::as_str),
        Some("user-value")
    );

    Ok(())
}

#[tokio::test]
async fn full_loop_carries_the_entity_key_across_a_real_broker_hop() -> TestResult {
    // The F2 regression test asserts the right things but inserts the received
    // row directly, so it never runs the code that caused F2: `user_headers`
    // stripping the whole `kafkaman-` namespace on ingest. Only a real broker hop
    // proves the entity key survives, and only a type whose partition key differs
    // from its entity key can tell the two apart at all.
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let table = harness.received_table::<RegionalOrder>().await?;

    let event = Envelope::new(RegionalOrder {
        order_id: "order-77".to_owned(),
        region: "eu-west".to_owned(),
    })
    .with_idempotency_key("idem-regional-loop");
    harness.enqueue(&event).await?;

    // Precondition for the strip assertion below: the producer really does put
    // the header on the wire, so its absence after ingest is the strip and not a
    // header that was never sent.
    assert_eq!(
        harness
            .outbox_row::<RegionalOrder>(event.message_id)
            .await?
            .headers
            .get("kafkaman-entity-key")
            .map(String::as_str),
        Some("order-77")
    );

    let relay_stats = harness.relay_once::<RegionalOrder>().await?;
    assert_eq!(relay_stats.published, 1);

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-regional-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[RegionalOrder::TOPIC])?;

    let ingest_stats = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<RegionalOrder>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(ingest_stats.inserted, 1);

    let row = harness
        .received_row_by_idempotency_key::<RegionalOrder>("idem-regional-loop")
        .await?;

    // Routing followed the declared partition key...
    assert_eq!(
        row.key.as_deref().map(String::from_utf8_lossy),
        Some(std::borrow::Cow::Borrowed("eu-west")),
        "the record key must still be the declared partition key"
    );
    // ...while convergence identity followed the entity key. Before the fix this
    // column did not exist and resolution fell through to the record key, so the
    // cache converged every eu-west order onto one row.
    assert_eq!(
        row.entity_key.as_deref(),
        Some("order-77"),
        "the entity key must survive the broker hop, not collapse to the partition key"
    );

    // And it survived *despite* the header, not because of it: ingest strips the
    // reserved namespace, which is precisely why the column exists.
    assert!(
        !row.headers.contains_key("kafkaman-entity-key"),
        "ingest must still strip reserved headers; the column is what carries the key"
    );

    let router = MessageRouter::new()
        .handler::<RegionalOrder>(|_conn, _meta, _msg| Box::pin(async move { Ok(()) }));
    let dispatch_stats = dispatch_once(
        harness.pool(),
        &table,
        &router,
        time::OffsetDateTime::now_utc(),
    )
    .await?;
    assert_eq!(dispatch_stats.processed, 1);

    let cache = CacheTable::for_message::<RegionalOrder>(&harness.config())?;
    let keys: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT entity_key FROM {}",
        cache.qualified_name()
    ))
    .fetch_all(harness.pool())
    .await?;
    assert_eq!(
        keys,
        vec!["order-77".to_owned()],
        "the cache must be keyed per order, not per region"
    );

    Ok(())
}

#[tokio::test]
async fn full_loop_ingests_from_redpanda_and_dispatches_received_row() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    recreate_effect_table(
        harness.pool(),
        "handled_orders",
        "order_id TEXT PRIMARY KEY",
    )
    .await?;

    let event = Envelope::new(OrderCreated {
        order_id: "order-redpanda-receive".to_owned(),
    })
    .with_idempotency_key("idem-rp-receive");
    harness.enqueue(&event).await?;

    let relay_stats = harness.relay_once::<OrderCreated>().await?;
    assert_eq!(relay_stats.claimed, 1);
    assert_eq!(relay_stats.published, 1);

    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-receive-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;

    let ingest_stats = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &harness.config()),
    )
    .await??;
    assert_eq!(ingest_stats.consumed, 1);
    assert_eq!(ingest_stats.inserted, 1);
    assert_eq!(ingest_stats.duplicates, 0);
    assert_eq!(ingest_stats.committed, 1);

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-rp-receive")
        .await?;
    assert_eq!(row.source_partition, ingest_stats.partition);
    assert_eq!(row.source_offset, ingest_stats.offset);

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
