//! What can go wrong assembling a runtime.

use std::net::AddrParseError;
use std::time::Duration;

use kafkaman_core::{problem, ProblemType};

/// A failure while building or running a [`Runtime`](super::Runtime).
///
/// Every variant names the thing the caller has to change. A builder is the one
/// place where a vague error is most expensive: the caller has written a dozen
/// declarative lines and has no stack to read, so "invalid configuration" would
/// leave them bisecting their own boot code.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    #[error(
        "no kafkaman config was supplied: call `RuntimeBuilder::config(..)` with a parsed \
         `kafkaman.toml`. The builder deliberately does not call `Config::discover()` — \
         discovery walks up from the process's current directory, which is the host's \
         decision to make and cannot serve two runtimes in one process"
    )]
    MissingConfig,

    #[error(
        "no connection pool was supplied: call `RuntimeBuilder::pool(..)`. The builder takes a \
         `PgPool` rather than a database URL so the host keeps pool sizing, and so HTTP and the \
         kafkaman loops can share one pool or be given separate ones"
    )]
    MissingPool,

    #[error(
        "no broker list was supplied: call `RuntimeBuilder::brokers(..)` with the same \
         `bootstrap.servers` value the rest of your stack uses"
    )]
    MissingBrokers,

    #[error(
        "message type `{message_type}` is consumed but no consumer group was supplied: call \
         `RuntimeBuilder::consumer_group(..)`. Two replicas of one service must share a group, \
         and two different services must not"
    )]
    MissingConsumerGroup { message_type: String },

    #[error(
        "no roles were declared: a runtime with no `publish`, `cache`, `handle`, or \
         `handle_before` would create no tables and start no loops. Declare at least one"
    )]
    NoRoles,

    #[error("invalid role declaration: {0}")]
    Roles(#[source] kafkaman_sqlx::Error),

    #[error("kafkaman config did not resolve: {0}")]
    Config(#[source] kafkaman_sqlx::Error),

    #[error(
        "topic convergence failed before any loop started, which is deliberate — boot is the \
         last moment at which refusing to start is still cheap: {0}"
    )]
    Topics(#[source] kafkaman_rdkafka::Error),

    #[error("the kafkaman schema did not converge: {0}")]
    Migrate(#[source] kafkaman_sqlx::Error),

    #[error("could not resolve a table for message type `{message_type}`: {source}")]
    Table {
        message_type: String,
        #[source]
        source: kafkaman_sqlx::Error,
    },

    #[error("could not connect to the broker at `{brokers}`: {source}")]
    Transport {
        brokers: String,
        #[source]
        source: kafkaman_rdkafka::Error,
    },

    #[error(
        "the `[retention]` section is invalid: {0}. Retention is opt-in, so removing the section \
         entirely is a valid fix — nothing is deleted when it is absent"
    )]
    Retention(String),

    #[error("the supplied listener has no usable local address: {0}")]
    Listener(String),

    #[error(transparent)]
    Address(#[from] AddrParseError),
}

/// A failure reported by a running loop.
///
/// The runtime supervises rather than exits: it reports the first failure and
/// drains, and the host decides what a dead relay means for its process.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    #[error("the `{loop_name}` loop failed: {source}")]
    Loop {
        loop_name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("the `{loop_name}` loop exited before shutdown: {message}")]
    LoopExited { loop_name: String, message: String },

    #[error("the `{loop_name}` loop panicked: {source}")]
    Panicked {
        loop_name: String,
        #[source]
        source: tokio::task::JoinError,
    },

    #[error(
        "runtime loops did not finish within {timeout:?} of shutdown; \
         {remaining} loop(s) were still running"
    )]
    DrainTimeout { timeout: Duration, remaining: usize },

    #[error("the runtime could not start its loops: {0}")]
    Build(#[source] super::BuildError),
}

impl ProblemType for BuildError {
    fn problem_type(&self) -> &'static str {
        match self {
            Self::MissingConfig
            | Self::MissingPool
            | Self::MissingBrokers
            | Self::MissingConsumerGroup { .. }
            | Self::NoRoles
            | Self::Retention(_)
            | Self::Listener(_)
            | Self::Address(_) => problem::CONFIGURATION,
            Self::Roles(source) | Self::Config(source) | Self::Migrate(source) => {
                source.problem_type()
            }
            Self::Topics(source) | Self::Transport { source, .. } => source.problem_type(),
            Self::Table { source, .. } => source.problem_type(),
        }
    }
}

impl ProblemType for RuntimeError {
    fn problem_type(&self) -> &'static str {
        match self {
            Self::Loop { .. }
            | Self::LoopExited { .. }
            | Self::Panicked { .. }
            | Self::DrainTimeout { .. } => problem::INFRASTRUCTURE,
            Self::Build(source) => source.problem_type(),
        }
    }
}
