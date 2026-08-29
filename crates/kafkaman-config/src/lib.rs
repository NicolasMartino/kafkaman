//! Loading and validating `kafkaman.toml`.
//!
//! Two error shapes, deliberately. [`ConfigError`] reports one problem, for the
//! lookups that can only have one. [`ConfigErrors`] accumulates, because a
//! config file with four mistakes should report four mistakes rather than
//! sending the operator round the loop four times.
//!
//! Each module declares its own imports rather than inheriting the crate root's.
//! The root declares the module graph and the public surface, and nothing else.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub type Result<T, E = ConfigError> = std::result::Result<T, E>;

mod config;
mod duration;
mod error;
mod observability;
mod retry;
mod schema;
mod sections;
mod serde_enum;

pub use config::{Config, FromConfigValue};
pub use error::{ConfigError, ConfigErrors, ConfigIssue};
pub use observability::{
    HeaderLogging, KafkaTraceHandoff, LifecycleLogging, ObservabilityConfig, ObservabilityLevel,
    ObservabilityPolicy, ObservabilityPolicyOverride, PayloadLogging,
};
pub use retry::{DlqMode, RetryConfig, RetryPolicy, RetryPolicyOverride};
pub use schema::ConfigSchema;
pub use sections::{
    DispatcherSection, ObservabilitySection, RelaySection, RetentionSection, RetrySection,
    TopicsSection,
};

#[cfg(test)]
mod tests;
