//! The inputs and the handle `service::start` works with.
//!
//! Split out so `service.rs` is boot logic and nothing else, mirroring
//! `example-product`'s equivalent — where the split earns more, because that
//! service has two boot paths to keep interchangeable.

use std::net::SocketAddr;

use kafkaman::config::Config;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The service's address and the handles to stop it.
///
/// kafkaman's own, because the loops and the HTTP server are supervised
/// together: the first to exit stops the others, without this example having to
/// arrange it.
pub use kafkaman::axum::RunningService;

/// What the service needs that `kafkaman.toml` does not carry.
///
/// The connection string and broker list are deliberately *not* kafkaman config
/// keys: they are per-environment host wiring, and kafkaman's contract covers
/// only the tables and loops it owns.
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
