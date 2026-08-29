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
use uuid::Uuid;

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
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error(transparent)]
    Core(#[from] kafkaman_core::Error),

    #[error(
        "{limit} consecutive received rows of message type `{message_type}` panicked in their \
         handler{}: this is a deploy that fails on every row rather than one poison message, \
         which a retry budget cannot absorb",
        message_id.map(|id| format!(", most recently {id}")).unwrap_or_default()
    )]
    ConsecutivePanickingRowLimitExceeded {
        limit: usize,
        message_type: String,
        /// The last row to panic, when the dispatch that tripped the breaker
        /// named one. Somewhere for an operator to start.
        message_id: Option<Uuid>,
    },

    #[error("invalid queue metrics config: {field} {reason}")]
    InvalidQueueMetricsConfig {
        field: &'static str,
        reason: &'static str,
    },

    #[error(
        "a queue metrics sampler is already running in this process; \
         one sampler covers every table, so start a second only after the first has stopped"
    )]
    QueueMetricsAlreadyRunning,
}

/// Somewhere to publish a claimed outbox row.
///
/// Returns a boxed error rather than a kafkaman one so a transport crate can
/// report its own failures without this crate depending on it.
#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError>;
}

impl kafkaman_core::ProblemType for Error {
    fn problem_type(&self) -> &'static str {
        use kafkaman_core::problem;
        match self {
            Self::Sqlx(error) => error.problem_type(),
            Self::Core(error) => error.problem_type(),
            Self::ConsecutivePanickingRowLimitExceeded { .. } => problem::BREAKER_TRIPPED,
            Self::InvalidQueueMetricsConfig { .. } | Self::QueueMetricsAlreadyRunning => {
                problem::CONFIGURATION
            }
        }
    }
}

/// That this crate's failures reach APM under a declared name.
///
/// Compiler exhaustiveness already refuses a variant nobody classified. What it
/// cannot see is a classification written as a bare string rather than a
/// `problem::*` constant, which compiles and then appears in Kibana as an error
/// group of one that nothing else ever joins.
#[cfg(test)]
mod problem_types {
    use kafkaman_core::problem::ALL_PROBLEM_TYPES;
    use kafkaman_core::ProblemType as _;

    #[test]
    fn no_classification_in_this_crate_invents_a_uri() {
        assert!(
            !include_str!("lib.rs").contains(concat!("\"urn:kafkaman:", "problem:")),
            "lib.rs contains a literal problem URI; classifications must go \
             through a `kafkaman_core::problem::*` constant"
        );
    }

    #[test]
    fn every_uri_this_crate_returns_is_declared() {
        // `ALL_PROBLEM_TYPES` is hand-listed, so a constant can exist without
        // being in it. These are the ones this crate can produce.
        let samples = vec![
            super::Error::ConsecutivePanickingRowLimitExceeded {
                limit: 10,
                message_type: "order_snapshot".to_owned(),
                message_id: None,
            },
            super::Error::QueueMetricsAlreadyRunning,
            super::Error::InvalidQueueMetricsConfig {
                field: "refresh_interval",
                reason: "must be greater than zero",
            },
            // The delegating arms. A transparently wrapped error keeps its own
            // classification rather than being relabelled by the wrapper.
            super::Error::Sqlx(kafkaman_sqlx::Error::Handler("boom".to_owned())),
            super::Error::Core(kafkaman_core::Error::InvalidOutboxStatus("nope".to_owned())),
        ];

        for error in &samples {
            let uri = error.problem_type();
            assert!(
                ALL_PROBLEM_TYPES.contains(&uri),
                "{error} classified as {uri}, which is not in ALL_PROBLEM_TYPES"
            );
        }

        assert_eq!(
            super::Error::Sqlx(kafkaman_sqlx::Error::HandlerPanicked("unwound".to_owned()))
                .problem_type(),
            kafkaman_core::problem::HANDLER_PANICKED,
            "a wrapped panic must still group as a panic; the distinction from a \
             returned error is the reason the exception vocabulary is finer than \
             the persisted one"
        );
    }
}
