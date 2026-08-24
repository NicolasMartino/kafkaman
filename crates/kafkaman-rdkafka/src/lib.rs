//! Kafka transport for kafkaman: publishing claimed outbox rows, and ingesting
//! records into receive tables.
//!
//! Each module owns its own imports rather than inheriting the crate root's.
//! The root therefore declares the module graph and the public surface, and
//! nothing else.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub use kafkaman_core;

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod consumer;
mod error;
#[cfg(feature = "internal-hooks")]
mod hooks;
mod ingest_record;
mod metrics;
mod publisher;
mod stats;
mod topics;

pub use consumer::RdkafkaConsumer;
pub use error::Error;
#[cfg(feature = "internal-hooks")]
pub use hooks::IngestCommitEvent;
pub use publisher::RdkafkaPublisher;
pub use stats::{IngestLoopStats, IngestStats};
pub use topics::{converge_topics, TopicAdmin};

#[cfg(test)]
mod tests;
