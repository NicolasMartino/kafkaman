//! Starting the loops, supervising them, and draining on shutdown.
//!
//! This is the boring assembly every kafkaman host was writing by hand: one
//! cancellation token, one `JoinSet`, a clone of the pool per loop, and a
//! supervise/drain pair. None of it is interesting, all of it is easy to get
//! subtly wrong, and it is identical in every service.

use std::future::Future;
use std::pin::Pin;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::error::RuntimeError;

/// The boxed error a supervised loop reports.
///
/// Loops come from four different crates with four different error types, and a
/// supervisor that had to name them all would be a supervisor that changes every
/// time one is added.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type LoopFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// The runtime's loops, running.
///
/// Returned rather than awaited so a host can bind its HTTP listener, learn the
/// address, and *then* decide how to supervise. `run` is the shorthand for hosts
/// that do not need that.
#[derive(Debug)]
pub struct RuntimeTasks {
    shutdown: CancellationToken,
    tasks: JoinSet<(&'static str, Result<(), BoxError>)>,
}

impl RuntimeTasks {
    /// A supervisor with no kafkaman loops.
    ///
    /// For `kafkaman::axum` serving a router with no runtime attached, which is
    /// a legitimate shape while a service is being built up.
    #[cfg(feature = "axum")]
    pub(crate) fn empty(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            tasks: JoinSet::new(),
        }
    }

    pub(crate) fn spawn(
        shutdown: CancellationToken,
        loops: Vec<(&'static str, LoopFuture)>,
    ) -> Self {
        let mut tasks = JoinSet::new();
        for (name, future) in loops {
            tasks.spawn(async move { (name, future.await) });
        }
        Self { shutdown, tasks }
    }

    /// Add one more supervised task to a runtime that is already running.
    ///
    /// What `kafkaman::axum` uses to make an HTTP server just another loop. It
    /// then shares the cancellation token and the drain with the relay and the
    /// dispatcher, which is what makes "the first kafkaman loop to die stops the
    /// service accepting traffic" fall out of the structure rather than needing
    /// to be arranged.
    #[cfg(feature = "axum")]
    pub(crate) fn push(&mut self, name: &'static str, future: LoopFuture) {
        self.tasks.spawn(async move { (name, future.await) });
    }

    /// The token every loop is watching.
    ///
    /// Handed out so a host can compose its own graceful shutdown with the
    /// runtime's — cancel it and the loops drain, or watch it and stop serving
    /// when something else cancels it.
    #[must_use]
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    /// How many loops are running.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Resolves when the *first* loop exits.
    ///
    /// Supervision, not cleanup. If the relay dies the host must stop accepting
    /// writes it can no longer publish, rather than filling an outbox nothing is
    /// draining — so the first exit is the signal, whether or not it was an
    /// error.
    pub async fn wait(&mut self) -> Result<(), RuntimeError> {
        match self.tasks.join_next().await {
            Some(Ok((name, Err(source)))) => Err(RuntimeError::Loop {
                loop_name: name,
                source,
            }),
            Some(Ok((_, Ok(())))) | None => Ok(()),
            Some(Err(join)) => Err(RuntimeError::Panicked(join)),
        }
    }

    /// Cancel every loop, wait for all of them, and report the first failure.
    ///
    /// Drains rather than aborting: a loop cancelled mid-transaction still needs
    /// to reach its own commit-or-rollback, and killing the task would leave the
    /// decision to the connection being dropped.
    pub async fn shutdown(mut self) -> Result<(), RuntimeError> {
        self.shutdown.cancel();
        let mut first: Option<RuntimeError> = None;

        while let Some(joined) = self.tasks.join_next().await {
            let failure = match joined {
                Ok((name, Err(source))) => Some(RuntimeError::Loop {
                    loop_name: name,
                    source,
                }),
                Ok((_, Ok(()))) => None,
                Err(join) => Some(RuntimeError::Panicked(join)),
            };
            if first.is_none() {
                first = failure;
            }
        }

        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
