//! Test support for kafkaman: a migrating harness, a capturing publisher, and
//! the panicking conveniences a library API should not have.
//!
//! Assertion helpers legitimately panic, so the workspace's no-panic lints are
//! relaxed here — and only here. That relaxation is the reason this crate
//! exists: `Envelope` deliberately has no infallible `with_idempotency_key`,
//! because a builder that aborts the caller's process on bad input has no place
//! in a library, but a test passing literals it controls wants exactly that.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use uuid::Uuid;

use kafkaman_core::OutboxStatus;

pub use kafkaman_config;
pub use kafkaman_core;
pub use kafkaman_sqlx;
pub use kafkaman_worker;

mod envelope_ext;
mod harness;
mod hooks;
mod publisher;

pub use envelope_ext::EnvelopeTestExt;
pub use harness::Harness;
pub use hooks::{dispatch_once_with_hooks, DispatchFailureHookContext, DispatchTestHooks};
pub use publisher::{CapturingPublisher, HarnessPublisher};

#[cfg(feature = "redpanda")]
pub use hooks::{IngestCommitContext, RdkafkaConsumerTestExt};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Core(#[from] kafkaman_core::Error),

    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error(transparent)]
    Pool(#[from] sqlx::Error),

    #[error(transparent)]
    Worker(#[from] kafkaman_worker::Error),

    #[cfg(feature = "redpanda")]
    #[error(transparent)]
    Rdkafka(#[from] kafkaman_rdkafka::Error),

    #[error("outbox row `{0}` was not found")]
    MissingRow(Uuid),

    /// Reports the caller-supplied source alongside the derived digest. The
    /// digest alone is unreadable, so a failed lookup would otherwise name a
    /// 64-character hash instead of the key the test actually asked for.
    // Field is not named `source`: thiserror would treat it as the error cause.
    #[error("received row for idempotency source {key_source} (digest `{digest}`) was not found")]
    MissingReceivedRow { key_source: String, digest: String },

    #[error("outbox row `{message_id}` expected status `{expected}` but found `{actual}`")]
    UnexpectedStatus {
        message_id: Uuid,
        expected: OutboxStatus,
        actual: OutboxStatus,
    },

    #[error("the capturing publisher is not available on a Redpanda harness; assert through a broker consumer instead")]
    NotCapturing,
}
