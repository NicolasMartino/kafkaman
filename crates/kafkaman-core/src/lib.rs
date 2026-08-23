//! The vocabulary every other kafkaman crate speaks: envelopes, identities,
//! statuses, row shapes, and the two runtime configs.
//!
//! Nothing here performs I/O. Every error is a validation failure on
//! caller-supplied data, which is why [`Error`] has no transient variant and
//! nothing in this crate is worth retrying.
//!
//! Each module declares its own imports rather than inheriting the crate root's,
//! so a module's dependencies are visible at the top of the file that has them.
//! The root declares the module graph and the public surface, and nothing else.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Declares enums together with the `ALL` arrays derived from them. Used through
/// `sql_enum!` and `discriminant_enum!` rather than called directly, which is why
/// it is private.
mod enum_macros;
mod envelope;
mod error;
mod failure_kind;
mod idempotency;
mod identifier;
mod message;
mod purge_config;
mod relay_config;
mod rows;
mod status;

/// Serde support for [`ReceivedFailureKind`] as an RFC 9457 `type` URI. Named in
/// `#[serde(with = "crate::problem_type")]` attributes rather than called
/// directly, which is why it is private.
mod problem_type;

pub mod rfc9557;

pub use envelope::Envelope;
pub use error::Error;
pub use failure_kind::{ReceivedFailureKind, ReceivedIngestFailureKind};
pub use idempotency::{
    IdempotencyIdentity, IdempotencyKey, IdempotencySource, IntoIdempotencyIdentity,
    LEGACY_STRING_IDEMPOTENCY_NAMESPACE,
};
pub use identifier::SqlIdentifier;
pub use message::{reserved_header, KafkaMessage, MessageDescriptor, RESERVED_HEADER_PREFIX};
pub use purge_config::{PurgeConfig, PurgeStats};
pub use relay_config::{RelayConfig, RelayStats};
pub use rows::{
    ClaimedOutboxRow, MarkOutcome, OutboxRow, PublishAck, PublishedRecord, ReceivedError,
    ReceivedMeta, ReceivedRow,
};
pub use status::{OutboxStatus, ReceiveStatus};

#[cfg(test)]
mod tests;
