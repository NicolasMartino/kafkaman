//! Access to the internal test seams the production crates expose only under
//! their `internal-hooks` feature.
//!
//! Everything here is a thin rename. The seams themselves live in the crates
//! they observe, so a production build without the feature contains no hook at
//! all — and this crate is the only thing that turns the feature on.

use kafkaman_sqlx::{MessageRouter, ReceivedTable};
use sqlx::PgPool;
use time::OffsetDateTime;

pub type DispatchFailureHookContext = kafkaman_sqlx::DispatchFailureEvent;
pub type DispatchTestHooks = kafkaman_sqlx::DispatchHooks;

/// Dispatch one row with hooks attached at the points a crash could interleave.
pub async fn dispatch_once_with_hooks(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    hooks: &DispatchTestHooks,
) -> kafkaman_sqlx::Result<kafkaman_sqlx::DispatchStats> {
    kafkaman_sqlx::dispatch_once_with_observer(pool, table, router, due_at, hooks).await
}

#[cfg(feature = "redpanda")]
pub type IngestCommitContext = kafkaman_rdkafka::IngestCommitEvent;

/// Attach an observer to the window between a durable receive write and the
/// Kafka offset commit — the only point where the row exists and the record is
/// not yet acknowledged, and therefore the only point where a simulated crash
/// tests redelivery rather than loss.
#[cfg(feature = "redpanda")]
pub trait RdkafkaConsumerTestExt: Sized {
    fn with_post_durable_write_hook<F>(self, hook: F) -> Self
    where
        F: Fn(IngestCommitContext) -> kafkaman_rdkafka::Result<()> + Send + Sync + 'static;
}

#[cfg(feature = "redpanda")]
impl RdkafkaConsumerTestExt for kafkaman_rdkafka::RdkafkaConsumer {
    fn with_post_durable_write_hook<F>(self, hook: F) -> Self
    where
        F: Fn(IngestCommitContext) -> kafkaman_rdkafka::Result<()> + Send + Sync + 'static,
    {
        self.with_post_durable_write_observer(hook)
    }
}
