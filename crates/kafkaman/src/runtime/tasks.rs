//! Starting the loops, supervising them, and draining on shutdown.
//!
//! This is the boring assembly every kafkaman host was writing by hand: one
//! cancellation token, one `JoinSet`, a clone of the pool per loop, and a
//! supervise/drain pair. None of it is interesting, all of it is easy to get
//! subtly wrong, and it is identical in every service.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::task::{Id as TaskId, JoinError, JoinSet};
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;

use super::error::RuntimeError;

/// The boxed error a supervised loop reports.
///
/// Loops come from four different crates with four different error types, and a
/// supervisor that had to name them all would be a supervisor that changes every
/// time one is added.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub(crate) type LoopFuture = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

/// How long [`RuntimeTasks::shutdown`] waits for loops to finish their current
/// cycle after cancellation.
///
/// Healthy loops observe cancellation between cycles and finish quickly. The
/// bound is for unhealthy shutdowns: a blocked broker call, stuck database query,
/// or task that forgot to watch the token must not make process shutdown wait
/// forever.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// The runtime's loops, running.
///
/// Returned rather than awaited so a host can bind its HTTP listener, learn the
/// address, and *then* decide how to supervise. `run` is the shorthand for hosts
/// that do not need that.
#[derive(Debug)]
pub struct RuntimeTasks {
    shutdown: CancellationToken,
    tasks: JoinSet<Result<(), BoxError>>,
    names: HashMap<TaskId, String>,
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
            names: HashMap::new(),
        }
    }

    pub(crate) fn spawn(shutdown: CancellationToken, loops: Vec<(String, LoopFuture)>) -> Self {
        let mut runtime_tasks = Self {
            shutdown,
            tasks: JoinSet::new(),
            names: HashMap::new(),
        };
        for (name, future) in loops {
            runtime_tasks.spawn_loop(name, future);
        }
        runtime_tasks
    }

    /// Add one more supervised task to a runtime that is already running.
    ///
    /// What `kafkaman::axum` uses to make an HTTP server just another loop. It
    /// then shares the cancellation token and the drain with the relay and the
    /// dispatcher, which is what makes "the first kafkaman loop to die stops the
    /// service accepting traffic" fall out of the structure rather than needing
    /// to be arranged.
    #[cfg(feature = "axum")]
    pub(crate) fn push(&mut self, name: impl Into<String>, future: LoopFuture) {
        self.spawn_loop(name, future);
    }

    fn spawn_loop(&mut self, name: impl Into<String>, future: LoopFuture) {
        let name = name.into();
        let handle = self.tasks.spawn(future);
        let previous = self.names.insert(handle.id(), name);
        debug_assert!(previous.is_none(), "tokio reused a running task id");
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
    /// draining. A clean exit is therefore a failure unless shutdown had already
    /// been requested.
    pub async fn wait(&mut self) -> Result<(), RuntimeError> {
        match self.tasks.join_next_with_id().await {
            Some(joined) => match self.classify_completion(joined, !self.shutdown.is_cancelled()) {
                Some(error) => Err(error),
                None => Ok(()),
            },
            None => Ok(()),
        }
    }

    /// Cancel every loop, wait for all of them, and report the first failure.
    ///
    /// Drains rather than aborting: a loop cancelled mid-transaction still needs
    /// to reach its own commit-or-rollback, and killing the task would leave the
    /// decision to the connection being dropped.
    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        self.shutdown_with_timeout(DEFAULT_DRAIN_TIMEOUT).await
    }

    /// [`shutdown`](Self::shutdown) with an explicit drain bound.
    ///
    /// A zero timeout skips the drain and aborts any still-running loops.
    pub async fn shutdown_with_timeout(
        mut self,
        drain_timeout: Duration,
    ) -> Result<(), RuntimeError> {
        let first = if self.shutdown.is_cancelled() {
            None
        } else {
            self.drain_ready_before_shutdown()
        };
        self.shutdown.cancel();
        let drained = self.drain(drain_timeout, first).await;
        self.tasks.abort_all();
        drained
    }

    fn drain_ready_before_shutdown(&mut self) -> Option<RuntimeError> {
        let mut first = None;
        while let Some(joined) = self.tasks.try_join_next_with_id() {
            let failure = self.classify_completion(joined, true);
            if first.is_none() {
                first = failure;
            }
        }
        first
    }

    async fn drain(
        &mut self,
        drain_timeout: Duration,
        mut first: Option<RuntimeError>,
    ) -> Result<(), RuntimeError> {
        if drain_timeout.is_zero() {
            return match first {
                Some(error) => Err(error),
                None => Ok(()),
            };
        }

        let deadline = Instant::now() + drain_timeout;
        while !self.tasks.is_empty() {
            match timeout_at(deadline, self.tasks.join_next_with_id()).await {
                Ok(Some(joined)) => {
                    let failure = self.classify_completion(joined, false);
                    if first.is_none() {
                        first = failure;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    return match first {
                        Some(error) => Err(error),
                        None => Err(RuntimeError::DrainTimeout {
                            timeout: drain_timeout,
                            remaining: self.tasks.len(),
                        }),
                    };
                }
            }
        }

        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn classify_completion(
        &mut self,
        joined: Result<(TaskId, Result<(), BoxError>), JoinError>,
        clean_exit_before_shutdown_is_error: bool,
    ) -> Option<RuntimeError> {
        match joined {
            Ok((id, Err(source))) => Some(RuntimeError::Loop {
                loop_name: self.take_name(id),
                source,
            }),
            Ok((id, Ok(()))) if clean_exit_before_shutdown_is_error => {
                Some(RuntimeError::LoopExited {
                    loop_name: self.take_name(id),
                    message: "completed before shutdown".to_owned(),
                })
            }
            Ok((id, Ok(()))) => {
                self.take_name(id);
                None
            }
            Err(join) if join.is_cancelled() => {
                self.take_name(join.id());
                None
            }
            Err(join) => {
                let loop_name = self.take_name(join.id());
                Some(RuntimeError::Panicked {
                    loop_name,
                    source: join,
                })
            }
        }
    }

    fn take_name(&mut self, id: TaskId) -> String {
        self.names
            .remove(&id)
            .unwrap_or_else(|| format!("unknown-task-{id:?}"))
    }
}
