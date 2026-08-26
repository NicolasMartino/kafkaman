//! Composing a built [`Runtime`] with an Axum service.
//!
//! Core kafkaman is HTTP-free and stays that way: this module binds no socket of
//! its own, owns no router, defines no state, and installs no signal handler.
//! What it does is make the HTTP server *one more supervised loop*, so it shares
//! the cancellation token and the drain with the relay, the ingester, and the
//! dispatcher.
//!
//! That structure is the point. It means the first kafkaman loop to die stops
//! the service accepting traffic, without anything having to watch for it —
//! which is the behaviour a host wants and the one it is easiest to forget to
//! wire by hand.
//!
//! # Example
//!
//! ```no_run
//! # use kafkaman::Runtime;
//! # async fn serve(
//! #     runtime: Runtime,
//! #     app: axum::Router,
//! # ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
//!
//! let mut service = kafkaman::axum::serve(listener, app)
//!     .with_runtime(runtime)
//!     .spawn()?;
//!
//! println!("listening on {}", service.addr());
//! service.wait().await?;
//! service.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;

// Leading `::` is load-bearing. This module is mounted as `axum_runtime` but
// the crate root also declares `pub mod axum`, and a bare `use axum::` is
// ambiguous between that module and the `axum` crate.
use ::axum::Router;
use tokio::net::TcpListener;

use crate::runtime::{BuildError, CancellationToken, Runtime, RuntimeError, RuntimeTasks};

/// Compose an already-built router with an already-built runtime.
///
/// The host binds the listener, so it keeps the bind address, the router, the
/// routes, and the state. Nothing here reaches back into any of them.
#[must_use]
pub fn serve(listener: TcpListener, app: Router) -> Serve {
    Serve {
        listener,
        app,
        runtime: None,
        shutdown: CancellationToken::new(),
    }
}

/// An HTTP service and, optionally, the kafkaman runtime beside it.
#[derive(Debug)]
pub struct Serve {
    listener: TcpListener,
    app: Router,
    runtime: Option<Runtime>,
    shutdown: CancellationToken,
}

impl Serve {
    /// Supervise this runtime alongside the HTTP server.
    #[must_use]
    pub fn with_runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Use a token the host already owns.
    ///
    /// For composing with something outside kafkaman — a signal handler the host
    /// installed, or a supervisor that shuts down several services together.
    #[must_use]
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// The address the listener actually bound.
    ///
    /// The reason this exists: a test binds port `0` and needs to know where to
    /// send requests, and a host that took the listener back to ask would have
    /// to un-build the service to do it.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Start every loop and the HTTP server, and return a handle.
    ///
    /// Returns rather than awaits, so the caller can learn the bound address and
    /// hand it to a test or a log line before anything blocks.
    pub fn spawn(self) -> Result<RunningService, BuildError> {
        let addr = self
            .local_addr()
            .map_err(|err| BuildError::Listener(err.to_string()))?;

        let mut tasks = match self.runtime {
            Some(runtime) => runtime.into_tasks_with(self.shutdown.clone())?,
            None => RuntimeTasks::empty(self.shutdown.clone()),
        };

        let listener = self.listener;
        let app = self.app;
        let shutdown = self.shutdown.clone();
        tasks.push(
            "http",
            Box::pin(async move {
                ::axum::serve(listener, app)
                    .with_graceful_shutdown(async move { shutdown.cancelled().await })
                    .await
                    .map_err(|err| Box::new(err) as crate::runtime::BoxLoopError)
            }),
        );

        Ok(RunningService {
            addr,
            shutdown: self.shutdown,
            tasks,
        })
    }

    /// Start everything, wait for the first loop to exit, then drain.
    ///
    /// The shorthand for a host that has nothing to do with the bound address.
    pub async fn run(self) -> Result<(), RuntimeError> {
        let service = self.spawn().map_err(RuntimeError::Build)?;
        service.run().await
    }
}

/// A running HTTP service and its kafkaman loops.
#[derive(Debug)]
pub struct RunningService {
    addr: SocketAddr,
    shutdown: CancellationToken,
    tasks: RuntimeTasks,
}

impl RunningService {
    /// The bound address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The base URL a client should use.
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The token every loop, HTTP included, is watching.
    #[must_use]
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown
    }

    /// Resolves when the first loop exits.
    ///
    /// Supervision, not cleanup: if the relay dies the process must stop
    /// accepting writes it can no longer publish, rather than filling an outbox
    /// nothing is draining.
    pub async fn wait(&mut self) -> Result<(), RuntimeError> {
        self.tasks.wait().await
    }

    /// Cancel everything and drain, reporting the first failure.
    pub async fn shutdown(self) -> Result<(), RuntimeError> {
        self.tasks.shutdown().await
    }

    /// [`wait`](Self::wait) then [`shutdown`](Self::shutdown).
    pub async fn run(mut self) -> Result<(), RuntimeError> {
        let first = self.wait().await;
        let drained = self.shutdown().await;
        first.and(drained)
    }
}
