//! Unit tests, split to mirror the module they cover, with the fixtures every
//! file needs shared from here.

use kafkaman_config::Config;
use kafkaman_core::{
    IdempotencyKey, KafkaMessage, MessageDescriptor, ReceiveStatus, ReceivedRow, SqlIdentifier,
};
use serde::Serialize;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::ReceivedTable;

mod catch_panic;
mod changelog;
mod dispatch_cache;
mod generated_changelog;
mod lock_keys;
mod migration_runner;
mod problem;
mod replay;
mod resolved_config;
mod retry_backoff;
mod roles;
mod schema_sql;
mod tables;

/// The message type most tests register, chosen so its table names are short
/// enough to read in an assertion failure.
#[derive(Serialize)]
pub(crate) struct OrderCreated;

impl KafkaMessage for OrderCreated {
    const MESSAGE_TYPE: &'static str = "order_created";
    const TOPIC: &'static str = "orders";

    fn entity_key(&self) -> String {
        "order_created".to_owned()
    }
}

pub(crate) fn descriptor(message_type: &str) -> MessageDescriptor {
    MessageDescriptor::new(message_type, "topic").unwrap()
}

pub(crate) fn received_table(message_type: &str) -> ReceivedTable {
    ReceivedTable::new(
        SqlIdentifier::new("kafkaman").unwrap(),
        descriptor(message_type),
    )
    .unwrap()
}

/// A minimal config file that satisfies every required key, so a test can
/// isolate the behaviour it actually cares about.
pub(crate) fn minimal_config() -> Config {
    Config::parse(
        r#"
            [database]
            schema = "kafkaman"

            [relay]
            worker_id = "worker-a"
            batch_limit = 10
            lease_for = "30s"
            retry_after = "1s"
            poll_interval = "250ms"
            "#,
    )
    .unwrap()
}

/// A received row with nothing resolved: no entity key column, no record key.
/// Tests set only the field they are about.
pub(crate) fn received_row_fixture() -> ReceivedRow {
    ReceivedRow {
        message_id: Uuid::from_u128(1),
        idempotency_key: IdempotencyKey::from_bytes([0; 32]),
        idempotency_source: None,
        entity_key: None,
        status: ReceiveStatus::Pending,
        attempts: 0,
        next_attempt_at: None,
        errors: Vec::new(),
        source_topic: "products".to_owned(),
        source_partition: 0,
        source_offset: 7,
        key: None,
        message_type: "product_snapshot".to_owned(),
        message_version: 1,
        headers: BTreeMap::new(),
        payload: serde_json::json!({}),
        correlation_id: None,
        trace: None,
        causation_id: None,
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        created_at: OffsetDateTime::UNIX_EPOCH,
        processed_at: None,
    }
}
