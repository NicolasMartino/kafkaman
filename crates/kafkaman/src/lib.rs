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

/// Axum admin/health routes, correlation middleware, and runtime composition.
///
/// A module rather than `pub use kafkaman_axum as axum`, because the runtime
/// composition helpers below have to live in the same namespace and a crate
/// re-export cannot be extended. It is a glob rather than a list so that a new
/// public item in `kafkaman-axum` reaches adopters without anyone remembering to
/// name it here.
#[cfg(feature = "axum")]
pub mod axum {
    pub use kafkaman_axum::*;

    /// Runtime composition: `serve(listener, router).with_runtime(rt).spawn()`.
    ///
    /// `serve` is deliberately explicit rather than left to the glob above.
    /// `kafkaman-axum` exports a `serve` of its own that takes a router and
    /// nothing else, and an explicit import shadows a glob one, so this is the
    /// `serve` the facade offers — the composing form, which is the one an
    /// application assembling a [`crate::Runtime`] wants. Reach for
    /// [`kafkaman_axum::serve`] directly on the rare occasion the plain form is
    /// what you meant.
    #[cfg(feature = "rdkafka")]
    pub use crate::axum_runtime::{serve, RunningService, Serve};
}

#[cfg(all(feature = "axum", feature = "rdkafka"))]
#[path = "axum.rs"]
mod axum_runtime;

#[cfg(feature = "rdkafka")]
pub use runtime::{
    BuildError, CancellationToken, HandlerCtx, Runtime, RuntimeBuilder, RuntimeContext,
    RuntimeError, RuntimeTasks,
};
