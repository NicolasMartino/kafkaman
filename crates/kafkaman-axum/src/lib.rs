//! Axum integration for kafkaman: request correlation and operator routes.
//!
//! Nothing here is required to use kafkaman. It exists so applications do not
//! each rewrite the same health check and the same queue-depth query.
//!
//! Supervision is not here. Running the HTTP server as one more loop beside the
//! relay, the ingester, and the dispatcher is [`kafkaman::axum::serve`], which
//! needs the runtime this crate deliberately knows nothing about.
//!
//! [`kafkaman::axum::serve`]: https://docs.rs/kafkaman
//!
//! # Security
//!
//! Nothing here is authenticated. [`admin_router`] reads queue metadata and
//! failure details, and [`redrive_router`] re-enqueues dead-lettered messages.
//! They are separate functions — and, since the split, separate modules — so
//! that mounting the destructive one is a decision rather than a side effect.
//! See their documentation.
//!
//! # Layout
//!
//! Each module declares its own imports rather than inheriting the crate root's,
//! matching every sibling crate in this workspace. The root declares the module
//! graph and the public surface, and nothing else.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod admin;
mod correlation;
mod error;
mod redrive;
mod state;

pub use admin::{admin_router, DlqRowSummary, DlqSummary, StuckResponse};
pub use correlation::{CorrelationId, CorrelationLayer, CorrelationService, CORRELATION_ID_HEADER};
pub use error::AdminError;
pub use redrive::{redrive_router, RedriveRequest, RedriveResponse, MAX_REDRIVE_ROWS};
pub use state::AdminState;

#[cfg(test)]
mod tests;
