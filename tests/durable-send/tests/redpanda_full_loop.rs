//! Opt-in, prod-representative full-loop test.
//!
//! Enabled with `--features redpanda`. It starts a real Redpanda broker as a
//! testcontainer, publishes through [`kafkaman_rdkafka::RdkafkaPublisher`] via
//! the worker, then consumes the record straight off the broker and asserts the
//! payload, partition key, and `kafkaman-*` metadata headers survived the trip.
#![cfg(feature = "redpanda")]

use std::collections::HashMap;
use std::time::Duration;

use kafkaman_core::{Envelope, KafkaMessage, ReceivedIngestFailureKind};
use kafkaman_rdkafka::{Error as RdkafkaError, RdkafkaConsumer};
use kafkaman_sqlx::{
    dispatch_once, enqueue_on_connection, received_ingest_failure_by_source, MessageRouter,
    ReceivedInsertOutcome,
};
use kafkaman_test::{Error as HarnessError, Harness};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

// Fixed host port so Redpanda can advertise a broker address the host-side
// client can actually dial. The full-loop test is single-instance, so a fixed
// port is acceptable and keeps the advertised-address handshake simple.
const KAFKA_PORT: u16 = 19092;

fn redpanda_test_lock() -> &'static Mutex<()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct OrderCreated {
    order_id: String,
}

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct OrderAccepted {
    order_id: String,
}

impl KafkaMessage for OrderAccepted {
    const MESSAGE_TYPE: &'static str = "order_accepted";
    const TOPIC: &'static str = "accepted-orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.clone())
    }
}

#[tokio::test]
async fn full_loop_publishes_to_redpanda_and_consumer_reads_it_back() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;

    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;

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
    assert_eq!(
        headers.get("kafkaman-idempotency-key").map(String::as_str),
        Some("idem-rp")
    );
    assert_eq!(
        headers.get("x-user-header").map(String::as_str),
        Some("user-value")
    );

    Ok(())
}

#[tokio::test]
async fn full_loop_ingests_from_redpanda_and_dispatches_received_row() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
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

#[tokio::test]
async fn full_loop_consume_then_produce_deduplicates_duplicate_input() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
    let received_table = harness.received_table::<OrderCreated>().await?;
    let outbox_table = harness.outbox_table::<OrderAccepted>().await?;
    let outbox_name = outbox_table.qualified_name();

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
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
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some("idem-rp-phase4"),
        }),
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
        .bind("accepted-rp-phase4")
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
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some("idem-rp-phase4"),
        }),
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
        .bind("accepted-rp-phase4")
        .fetch_one(harness.pool())
        .await?,
        1
    );

    Ok(())
}

#[tokio::test]
async fn ingest_deduplicates_redelivery_after_crash_before_offset_commit() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
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
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some("idem-rp-crash-window"),
        }),
    )
    .await?;

    let group_id = format!("kafkaman-crash-window-{}", Uuid::new_v4());
    let consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?
        .with_post_durable_write_hook(|context| {
            assert_eq!(context.outcome, ReceivedInsertOutcome::Inserted);
            Err(RdkafkaError::TestHook(
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
    assert!(matches!(err, RdkafkaError::TestHook(_)));

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
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
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
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some("idem-rp-uncertain-commit"),
        }),
    )
    .await?;

    let group_id = format!("kafkaman-uncertain-commit-{}", Uuid::new_v4());
    let shutdown = CancellationToken::new();
    let hook_shutdown = shutdown.clone();
    let consumer = RdkafkaConsumer::from_brokers(&brokers, &group_id)?
        .with_post_durable_write_hook(move |context| {
            assert_eq!(context.outcome, ReceivedInsertOutcome::Inserted);
            hook_shutdown.cancel();
            Err(RdkafkaError::TestHook(
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

#[tokio::test]
async fn ingest_skips_poison_record_then_deduplicates_redelivery() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
    let table = harness.received_table::<OrderCreated>().await?;

    sqlx::query("CREATE TABLE handled_orders (order_id TEXT PRIMARY KEY)")
        .execute(harness.pool())
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
        OwnedHeaders::new()
            .insert(Header {
                key: "kafkaman-idempotency-key",
                value: Some("idem-poison-case-header"),
            })
            .insert(Header {
                key: "Kafkaman-Message-Id",
                value: Some("not-a-uuid"),
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
            OwnedHeaders::new()
                .insert(Header {
                    key: "kafkaman-idempotency-key",
                    value: Some("idem-rp-redelivery"),
                })
                .insert(Header {
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
    assert!(failure.error.contains("invalid Kafka header"));

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
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
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
            OwnedHeaders::new().insert(Header {
                key: "kafkaman-idempotency-key",
                value: Some(idempotency_key.as_str()),
            }),
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
async fn run_ingester_processes_records_until_cancelled() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
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
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some("idem-rp-runner"),
        }),
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
                Err(HarnessError::MissingReceivedRow(_)) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => break Err(err),
            }
        }
    })
    .await??;
    assert_eq!(row.idempotency_key, "idem-rp-runner");
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

#[tokio::test]
async fn run_ingester_stops_loudly_on_consecutive_schema_failures() -> TestResult {
    let _test_guard = redpanda_test_lock().lock().await;
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
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
            OwnedHeaders::new().insert(Header {
                key: "kafkaman-idempotency-key",
                value: Some(idempotency_key.as_str()),
            }),
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
    let (_postgres, database_url) = start_postgres().await?;
    let (_redpanda, brokers) = start_redpanda().await?;
    let harness = Harness::connect_redpanda(&database_url, &brokers).await?;
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
        OwnedHeaders::new().insert(Header {
            key: "kafkaman-idempotency-key",
            value: Some("idem-wrong-topic"),
        }),
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

async fn publish_order_record(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    order_id: &str,
    headers: OwnedHeaders,
) -> TestResult {
    let payload = serde_json::to_vec(&OrderCreated {
        order_id: order_id.to_owned(),
    })?;
    let record = FutureRecord::to(topic)
        .payload(payload.as_slice())
        .key(key)
        .headers(headers);

    match producer.send(record, Timeout::Never).await {
        Ok(_) => Ok(()),
        Err((error, _message)) => Err(Box::new(error)),
    }
}

async fn publish_raw_record(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    payload: &[u8],
    headers: OwnedHeaders,
) -> TestResult {
    let record = FutureRecord::to(topic)
        .payload(payload)
        .key(key)
        .headers(headers);

    match producer.send(record, Timeout::Never).await {
        Ok(_) => Ok(()),
        Err((error, _message)) => Err(Box::new(error)),
    }
}

async fn start_postgres(
) -> Result<(ContainerAsync<GenericImage>, String), Box<dyn std::error::Error + Send + Sync>> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "kafkaman_test")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(port).await?;
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/kafkaman_test");
    Ok((container, database_url))
}

async fn start_redpanda(
) -> Result<(ContainerAsync<GenericImage>, String), Box<dyn std::error::Error + Send + Sync>> {
    let advertised = format!("PLAINTEXT://127.0.0.1:{KAFKA_PORT}");
    let listener = format!("PLAINTEXT://0.0.0.0:{KAFKA_PORT}");
    let container = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.7")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
        .with_mapped_port(KAFKA_PORT, KAFKA_PORT.tcp())
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--kafka-addr",
            &listener,
            "--advertise-kafka-addr",
            &advertised,
        ])
        .start()
        .await?;

    Ok((container, format!("127.0.0.1:{KAFKA_PORT}")))
}
