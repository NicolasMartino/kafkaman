//! What both of this service's boot paths have in common.
//!
//! `product` boots two ways on purpose. [`service`](crate::service) declares
//! roles and lets the runtime builder derive the rest;
//! [`service_manual`](crate::service_manual) assembles the same runtime out of
//! the low-level primitives by hand. The `distributed-cache` suite runs against
//! both, which is what turns "the escape hatch still works" from a claim in the
//! documentation into something a failing test would report.
//!
//! They therefore have to be observably interchangeable, which is what this
//! module is: one options struct, one handle, one signature.

use std::net::SocketAddr;

use kafkaman::config::Config;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Which of the two boot paths to take.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BootMode {
    /// Declare roles; the runtime builder derives tables, loops, and shutdown.
    #[default]
    Builder,
    /// Assemble the same runtime by hand out of the low-level primitives.
    Manual,
}

/// What the service needs that `kafkaman.toml` does not carry.
#[derive(Debug)]
pub struct ServiceOptions {
    pub database_url: String,
    pub brokers: String,
    pub bind: SocketAddr,
    pub consumer_group: String,
    /// `None` is a boot failure, deliberately: a service with no config file
    /// must not silently start on defaults.
    pub config: Option<Config>,
}

/// A running service, whichever way it was booted.
#[derive(Debug)]
pub enum RunningService {
    /// Supervised by kafkaman: the loops and the HTTP server share one token.
    Builder(kafkaman::axum::RunningService),
    /// Supervised by this example's own `JoinSet`.
    Manual(ManualService),
}

impl RunningService {
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        match self {
            Self::Builder(service) => service.addr(),
            Self::Manual(service) => service.addr,
        }
    }

    /// The base URL a client should use.
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr())
    }

    /// Resolves when the *first* loop exits.
    ///
    /// Supervision, not cleanup. If the relay dies the process must stop
    /// accepting writes it can no longer publish, rather than filling an outbox
    /// nothing is draining.
    pub async fn wait(&mut self) -> Result<(), BoxError> {
        match self {
            Self::Builder(service) => service.wait().await.map_err(Into::into),
            Self::Manual(service) => service.wait().await,
        }
    }

    /// Cancel every loop and drain, reporting the first failure.
    pub async fn shutdown(self) -> Result<(), BoxError> {
        match self {
            Self::Builder(service) => service.shutdown().await.map_err(Into::into),
            Self::Manual(service) => service.shutdown().await,
        }
    }
}

/// The hand-rolled half: a token, a `JoinSet`, and the supervise/drain pair the
/// runtime builder exists to absorb.
pub struct ManualService {
    pub(crate) addr: SocketAddr,
    pub(crate) shutdown: CancellationToken,
    pub(crate) tasks: JoinSet<Result<(), BoxError>>,
}

impl std::fmt::Debug for ManualService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManualService")
            .field("addr", &self.addr)
            .field("tasks", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

impl ManualService {
    async fn wait(&mut self) -> Result<(), BoxError> {
        match self.tasks.join_next().await {
            Some(joined) => joined.map_err(|err| Box::new(err) as BoxError)?,
            None => Ok(()),
        }
    }

    async fn shutdown(mut self) -> Result<(), BoxError> {
        self.shutdown.cancel();
        let mut first_error = None;
        while let Some(joined) = self.tasks.join_next().await {
            let outcome = match joined {
                Ok(outcome) => outcome,
                Err(err) => Err(Box::new(err) as BoxError),
            };
            if let Err(err) = outcome {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// Boot through the builder.
pub async fn start(options: ServiceOptions) -> Result<RunningService, BoxError> {
    start_with(BootMode::default(), options).await
}

/// Boot through the named path.
pub async fn start_with(
    mode: BootMode,
    options: ServiceOptions,
) -> Result<RunningService, BoxError> {
    match mode {
        BootMode::Builder => crate::service::start(options).await,
        BootMode::Manual => crate::service_manual::start(options).await,
    }
}
