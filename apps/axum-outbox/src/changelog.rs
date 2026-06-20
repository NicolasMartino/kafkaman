use kafkaman::sqlx::{changelog, Changeset, CreateOutboxTable, InitSchema};
use kafkaman::KafkaMessage;

use crate::OrderCreated;

pub fn changelog() -> Vec<Box<dyn Changeset>> {
    changelog![
        InitSchema,
        CreateOutboxTable::new(
            2,
            OrderCreated::descriptor().expect("OrderCreated descriptor is valid"),
        ),
    ]
}
