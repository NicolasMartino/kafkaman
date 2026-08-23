use kafkaman::sqlx::{try_changelog, Changeset, CreateOutboxTable, Error, InitSchema};
use kafkaman::KafkaMessage;

use crate::OrderCreated;

/// The application's changelog.
///
/// Uses the fallible `try_changelog!` rather than the panicking `changelog!`:
/// an example is the thing people copy, and a library consumer should see the
/// ordering error, not a panic.
pub fn changelog() -> Result<Vec<Box<dyn Changeset>>, Error> {
    try_changelog![
        InitSchema,
        CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    ]
}
