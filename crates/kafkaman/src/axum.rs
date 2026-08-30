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
//! let first = service.wait().await;
//! let drained = service.shutdown().await;
//! first.and(drained)?;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;
use std::time::Duration;

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

    /// Cancel everything and drain with an explicit bound.
    pub async fn shutdown_with_timeout(self, drain_timeout: Duration) -> Result<(), RuntimeError> {
        self.tasks.shutdown_with_timeout(drain_timeout).await
    }

    /// [`wait`](Self::wait) then [`shutdown`](Self::shutdown).
    ///
    /// Instrumented at `info` on purpose. This is the message-path half of the
    /// `kafkaman::internal` tier — the span every supervised loop's work hangs
    /// under — so a default `info` filter has to open it or the waterfall starts
    /// with a gap where the service lifetime should be. The poll half of the
    /// tier (`health`, `ready`) stays at `debug` for the opposite reason.
    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
    pub async fn run(mut self) -> Result<(), RuntimeError> {
        let first = self.wait().await;
        let drained = self.shutdown().await;
        first.and(drained)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use ::axum::routing::get;
    use ::axum::Router;
    use kafkaman_core::{problem, ProblemType};
    use tokio::net::TcpStream;

    use super::*;
    use crate::runtime::BoxLoopError;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn listener() -> std::io::Result<TcpListener> {
        TcpListener::bind("127.0.0.1:0").await
    }

    fn router() -> Router {
        Router::new().route("/health", get(|| async { "ok" }))
    }

    #[test]
    fn facade_axum_exports_the_canonical_runtime_supervision_names() {
        let error = crate::axum::RuntimeError::Build(crate::BuildError::MissingConfig);
        assert_eq!(error.problem_type(), problem::CONFIGURATION);
        assert_eq!(
            crate::axum::DEFAULT_DRAIN_TIMEOUT,
            crate::DEFAULT_DRAIN_TIMEOUT
        );
    }

    #[tokio::test]
    async fn running_service_wait_allows_http_exit_after_external_shutdown() -> TestResult {
        let shutdown = CancellationToken::new();
        let mut service = serve(listener().await?, router())
            .with_shutdown(shutdown.clone())
            .spawn()?;
        let addr = service.addr();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match TcpStream::connect(addr).await {
                    Ok(stream) => {
                        drop(stream);
                        break Ok::<(), std::io::Error>(());
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(err) => break Err(err),
                }
            }
        })
        .await??;

        shutdown.cancel();

        service.wait().await?;
        service.shutdown().await?;
        Ok(())
    }

    /// The message-path half of the `kafkaman::internal` span tier is visible
    /// under a plain `info` filter.
    ///
    /// The other half — that the poll functions an orchestrator probes forever
    /// stay *out* of the default filter — is pinned by
    /// `the_internal_span_tier_is_split_by_level_and_reachable_by_target` in
    /// `kafkaman-axum`. Both halves are needed: without this one the split test
    /// passes just as well against a tier reverted to `debug` wholesale, and the
    /// waterfall would start with a gap where the service lifetime should be.
    #[tokio::test]
    async fn running_service_run_is_visible_under_the_default_filter() -> TestResult {
        use std::sync::{Arc, Mutex, PoisonError};

        use tracing_subscriber::layer::{Context, SubscriberExt as _};
        use tracing_subscriber::Layer as _;

        #[derive(Clone, Default)]
        struct RecordedSpans(Arc<Mutex<Vec<String>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RecordedSpans {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _id: &tracing::Id,
                _ctx: Context<'_, S>,
            ) {
                // Poisoning is recovered rather than propagated: this layer runs
                // on whatever thread opened a span, and a panic elsewhere in the
                // test should surface as that panic, not as a second one here.
                self.0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(attrs.metadata().name().to_owned());
            }
        }

        // Cancelled before the service starts, so the HTTP loop stops before
        // accepting anything and the drain has nothing to wait for. The span is
        // what is under test, not the serving.
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let service = serve(listener().await?, router())
            .with_shutdown(shutdown)
            .spawn()?;

        let recorded = RecordedSpans::default();
        {
            let _guard = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(
                    recorded
                        .clone()
                        .with_filter(tracing_subscriber::EnvFilter::new("info")),
                ),
            );
            let _ = service.run().await;
        }

        let opened = recorded
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert!(
            opened.contains(&"run".to_owned()),
            "the supervision span must open under the default filter; opened: {opened:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn running_service_shutdown_with_timeout_delegates_to_runtime_tasks() {
        let shutdown = CancellationToken::new();
        let mut tasks = RuntimeTasks::empty(shutdown.clone());
        tasks.push(
            "wedged",
            Box::pin(async {
                std::future::pending::<()>().await;
                Ok::<(), BoxLoopError>(())
            }),
        );

        let service = RunningService {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            shutdown,
            tasks,
        };
        let result = service
            .shutdown_with_timeout(Duration::from_millis(50))
            .await;

        assert!(
            matches!(result, Err(RuntimeError::DrainTimeout { remaining: 1, .. })),
            "got {result:?}"
        );
    }
}
