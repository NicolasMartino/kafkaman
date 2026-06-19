use kafkaman_core::KafkaMessage;
use kafkaman_sqlx::{Changeset, CreateOutboxTable, InitSchema};

use crate::OrderCreated;

pub fn changelog() -> Vec<Box<dyn Changeset>> {
    vec![
        Box::new(InitSchema),
        Box::new(CreateOutboxTable::new(
            2,
            OrderCreated::descriptor().expect("OrderCreated descriptor is valid"),
        )),
    ]
}
