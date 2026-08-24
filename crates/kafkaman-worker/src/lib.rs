//! The run loops: an outbox relay, a receive dispatcher, and an outbox purger.
//!
//! All three share a shape — do a bounded unit of work, log it, and pace
//! against a shutdown token — but not a body, because what each logs and how
//! each paces are genuinely different. What they do share is
//! `sleep_or_shutdown`: racing the sleep against
//! the token is the part that is easy to get wrong, and getting it wrong makes
//! a shutdown take a full poll interval to be noticed.
//!
//! Every loop treats a failed cycle as transient: it logs and retries, because a
//! database blip must not take a worker down. Only a configuration that can
//! never succeed returns `Err`, and it is checked once up front rather than
//! rediscovered every cycle.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use std::error::Error as StdError;

use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, PublishAck};

pub use kafkaman_core;
pub use kafkaman_sqlx;

mod dispatcher;
mod metrics;
mod purger;
#[cfg(feature = "metrics")]
mod queue_metrics;
mod relay;
mod run_loop;

pub use dispatcher::run_dispatcher;
pub use purger::run_purger;
#[cfg(feature = "metrics")]
pub use queue_metrics::{run_queue_metrics, QueueMetricsConfig};
pub use relay::{relay_once, run};

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error(transparent)]
    Core(#[from] kafkaman_core::Error),

    #[error("invalid dispatcher config: {field} {reason}")]
    InvalidDispatcherConfig {
        field: &'static str,
        reason: &'static str,
    },

    #[error("invalid queue metrics config: {field} {reason}")]
    InvalidQueueMetricsConfig {
        field: &'static str,
        reason: &'static str,
    },
}

/// Somewhere to publish a claimed outbox row.
///
/// Returns a boxed error rather than a kafkaman one so a transport crate can
/// report its own failures without this crate depending on it.
#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError>;
}
