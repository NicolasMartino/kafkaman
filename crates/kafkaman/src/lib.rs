//! Facade over the kafkaman crates.
//!
//! Depend on this crate alone rather than on the individual `kafkaman-*` crates,
//! so a workspace layout change does not become a downstream breaking change.
//!
//! The core types are re-exported at the root because they are the vocabulary
//! every other module speaks ([`Envelope`], [`KafkaMessage`], the status enums).
//! The subsystems keep their own namespaces.

/// Core types: envelopes, identities, statuses, and row shapes.
pub use kafkaman_core::*;

/// The core crate itself, for callers that prefer an explicit path.
pub use kafkaman_core as core;

/// Configuration loading and the typed `kafkaman.toml` contract.
pub use kafkaman_config as config;

/// PostgreSQL persistence: migrations, outbox, inbox, dispatch, and cache.
pub use kafkaman_sqlx as sqlx;

/// The relay and dispatcher run loops.
pub use kafkaman_worker as worker;

/// Axum admin/health routes and correlation middleware, behind the `axum` feature.
#[cfg(feature = "axum")]
pub use kafkaman_axum as axum;

/// Kafka transport, behind the `rdkafka` feature.
///
/// Gated because it links librdkafka, which not every consumer wants to build;
/// exposed here so an application never has to name a second kafkaman crate.
#[cfg(feature = "rdkafka")]
pub use kafkaman_rdkafka as rdkafka;

/// Role-driven service assembly, behind the `rdkafka` feature.
///
/// Gated with the transport rather than beside it: assembling a runtime means
/// constructing a publisher, a consumer, and a topic admin, so a builder without
/// librdkafka would be a builder that cannot build anything.
#[cfg(feature = "rdkafka")]
pub mod runtime;

/// Axum composition, behind the `axum` feature.
///
/// Needs a [`Runtime`] to compose with, so it carries the `rdkafka` requirement
/// too. Enabling `axum` alone gets you nothing, which is why the module is
/// gated on both.
#[cfg(all(feature = "axum", feature = "rdkafka"))]
pub mod axum;

#[cfg(feature = "rdkafka")]
pub use runtime::{
    BuildError, CancellationToken, HandlerCtx, Runtime, RuntimeBuilder, RuntimeContext,
    RuntimeError, RuntimeTasks,
};
