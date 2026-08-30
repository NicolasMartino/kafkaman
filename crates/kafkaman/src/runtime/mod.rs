//! The blessed way to assemble a kafkaman service.
//!
//! Declare roles; get a runtime. See [`RuntimeBuilder`] for what that means and,
//! more importantly, for what stays with the host.
//!
//! The low-level primitives this is built from — [`migrate`](kafkaman_sqlx::migrate),
//! [`converge_topics`](kafkaman_rdkafka::converge_topics),
//! [`OutboxTable`](kafkaman_sqlx::OutboxTable), the worker loops — remain public
//! and supported. This is the default service UX, not a closed framework.

mod builder;
mod context;
mod error;
mod subsystems;
mod tasks;

#[cfg(test)]
mod tests;

pub use builder::{Runtime, RuntimeBuilder};
pub use context::{HandlerCtx, RuntimeContext};
pub use error::{BuildError, RuntimeError};
pub use subsystems::Subsystems;
pub use tasks::{RuntimeTasks, DEFAULT_DRAIN_TIMEOUT};

#[doc(inline)]
pub use kafkaman_core::ReceivedMeta;

pub use tasks::BoxError as BoxLoopError;

/// Re-exported because [`Runtime::run`] and [`Runtime::into_tasks_with`] take
/// one. A caller should not have to add `tokio-util` to their manifest in order
/// to name a type this API requires of them.
pub use tokio_util::sync::CancellationToken;
