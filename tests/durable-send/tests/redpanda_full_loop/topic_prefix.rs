use super::*;

use kafkaman_config::Config;
use kafkaman_sqlx::{OutboxTable, ResolvedConfig};

/// The harness config with `[topics] prefix` set. The schema is the harness's,
/// so the tables it migrated are the ones this config reads and writes; table
/// names do not depend on the topic.
fn prefixed_config(harness: &Harness, prefix: &str) -> TestResult<ResolvedConfig> {
    let schema = harness.config().schema().as_str().to_owned();
    let config = Config::parse(&format!(
        r#"
        [database]
        schema = "{schema}"

        [relay]
        worker_id = "worker-prefix-test"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "50ms"

        [topics]
        prefix = "{prefix}"
        "#
    ))?;
    Ok(ResolvedConfig::from_config(
        Some(&config),
        [OrderCreated::descriptor()?],
    )?)
}

/// A host running one environment per prefix on a shared broker: the message is
/// enqueued for, published to, subscribed on and accepted from `p.orders`, while
/// `OrderCreated::TOPIC` stays `orders`. The same record on the bare topic is
/// someone else's and is quarantined as `UnexpectedTopic`, with no fallback.
#[tokio::test]
async fn a_prefixed_message_round_trips_on_the_prefixed_topic_and_the_bare_one_is_refused(
) -> TestResult {
    // The file's prefix is what this test pins; an override in the environment
    // would move every name below. The override is covered by `kafkaman-sqlx`'s
    // resolution tests, which do not read the process environment.
    if std::env::var("KAFKAMAN_TOPICS_PREFIX").is_ok_and(|value| !value.trim().is_empty()) {
        return Ok(());
    }

    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, _redpanda, brokers, harness) = start_redpanda_harness().await?;
    let _outbox = harness.outbox_table::<OrderCreated>().await?;
    let _received = harness.received_table::<OrderCreated>().await?;
    let cfg = prefixed_config(&harness, "p.")?;
    let prefixed = format!("p.{}", OrderCreated::TOPIC);
    assert_eq!(OrderCreated::TOPIC, "orders");

    // Enqueue: the outbox row is written for the prefixed topic.
    let event = Envelope::new(OrderCreated {
        order_id: "order-prefixed".to_owned(),
    })
    .with_idempotency_key("idem-prefixed");
    let mut tx = harness.pool().begin().await?;
    enqueue_on_connection(&mut tx, &cfg, &event).await?;
    tx.commit().await?;
    let row = harness.outbox_row::<OrderCreated>(event.message_id).await?;
    assert_eq!(row.topic, prefixed);

    // Relay: the row is published to the topic stored on it.
    let publisher = RdkafkaPublisher::from_brokers(&brokers)?;
    let table = OutboxTable::for_message::<OrderCreated>(&cfg)?;
    let stats = kafkaman_worker::relay_once(harness.pool(), &publisher, &table, &cfg.relay).await?;
    assert_eq!(stats.published, 1);

    // Subscribe and ingest on the prefixed topic: accepted and stored.
    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-prefix-{}", Uuid::new_v4()))?;
    let subscribed = cfg.descriptor_for::<OrderCreated>()?.topic;
    assert_eq!(subscribed, prefixed);
    consumer.subscribe(&[subscribed.as_str()])?;
    let accepted = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &cfg),
    )
    .await??;
    assert_eq!(accepted.inserted, 1);
    assert_eq!(accepted.skipped, 0);
    let received = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-prefixed")
        .await?;
    assert_eq!(received.source_topic, prefixed);

    // The same type on the bare topic: refused and quarantined, not stored.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;
    publish_order_record(
        &producer,
        OrderCreated::TOPIC,
        "bare-key",
        "order-bare",
        idempotency_headers("idem-bare"),
    )
    .await?;
    let consumer =
        RdkafkaConsumer::from_brokers(&brokers, &format!("kafkaman-bare-{}", Uuid::new_v4()))?;
    consumer.subscribe(&[OrderCreated::TOPIC])?;
    let refused = tokio::time::timeout(
        Duration::from_secs(30),
        consumer.ingest_once::<OrderCreated>(harness.pool(), &cfg),
    )
    .await??;
    assert_eq!(refused.skipped, 1);
    assert_eq!(refused.inserted, 0);
    assert_eq!(refused.committed, 1);

    let failure = received_ingest_failure_by_source(
        harness.pool(),
        &cfg,
        OrderCreated::TOPIC,
        refused.partition,
        refused.offset,
    )
    .await?
    .expect("a record on the bare topic must be quarantined");
    assert_eq!(failure.kind, ReceivedIngestFailureKind::UnexpectedTopic);
    assert_eq!(failure.expected_topic, prefixed);
    assert_eq!(failure.source_topic, OrderCreated::TOPIC);
    harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-bare")
        .await
        .expect_err("a record on the bare topic must not be stored");

    Ok(())
}
