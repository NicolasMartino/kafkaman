//! Unit tests, split to mirror the module they cover.
//!
//! One file per module rather than one flat list, so a test's neighbours are the
//! tests of the same behaviour and adding one has an obvious home.

mod failure_kind;
mod idempotency;
mod identifier;
mod message;
mod purge_config;
mod relay_config;
mod rfc9557;
mod rows;
mod status;
