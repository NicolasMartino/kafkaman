//! PostgreSQL persistence for kafkaman: migrations, outbox, received tables,
//! dispatch, and entity caches.
//!
//! Table names are derived per message type and therefore cannot be bound as
//! query parameters, so every statement here is built by `format!`. That is safe
//! only because the interpolated pieces are validated
//! [`SqlIdentifier`](kafkaman_core::SqlIdentifier)s and status literals from the
//! enums themselves — never caller input.
//!
//! Each module declares its own imports rather than inheriting the crate root's.
//! The root declares the module graph and the public surface, and nothing else.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub use kafkaman_core;

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod changelog;
mod changeset;
mod changesets;
mod dispatch;
mod dispatch_cache;
mod dispatch_failure;
mod error;
mod generated_changelog;
#[cfg(feature = "internal-hooks")]
mod hooks;
mod ingest_failure;
mod lock_keys;
mod migration_runner;
mod operability;
mod outbox_claim;
mod outbox_enqueue;
mod outbox_mark;
mod outbox_purge;
mod queries;
mod received_rows;
mod received_storage;
mod replay;
mod resolved_config;
mod retry_backoff;
mod roles;
mod router;
mod schema_sql;
mod tables;

pub use changelog::assert_changelog_order;
pub use changeset::{
    ChangeBuilder, Changeset, MigrationAction, MigrationContext, MigrationReport,
    MigrationStepReport,
};
pub use changesets::{
    AddIdempotencyKey, AddOutboxEntityKey, AddOutboxRetentionIndex, AddOutboxTraceContext,
    AddReceivedEntityKey, AddReceivedFailedIndex, AddReceivedFailureMetadata,
    AddReceivedTraceContext, CreateCacheTable, CreateOutboxTable, CreateReceivedTable, InitSchema,
};
pub use dispatch::{dispatch_once, dispatch_once_sampled};
pub use dispatch_cache::CacheApplyOutcome;
pub use error::Error;
pub use generated_changelog::{
    band, band_of, changeset_version, TableKind, BAND_WIDTH, RESERVED_CEILING,
};
pub use ingest_failure::{ReceivedIngestFailure, ReceivedIngestFailureRow, ReceivedInsertOutcome};
pub use migration_runner::{migrate, migrate_dry_run};
pub use operability::{
    outbox_status_summary, outbox_stuck_rows, received_status_summary, received_stuck_rows,
    OutboxStatusSummary, OutboxStuckRow, ReceivedStatusSummary, ReceivedStuckRow,
};
pub use outbox_claim::claim_batch;
pub use outbox_enqueue::{enqueue, enqueue_on_connection};
pub use outbox_mark::{mark_publish_failed, mark_published, outbox_row};
pub use outbox_purge::purge_outbox_once;
pub use queries::{
    received_failed_count, received_failed_rows, received_row, received_row_by_idempotency_key,
    ReceivedFailureFilter,
};
pub use received_storage::{
    insert_received, insert_received_ingest_failure, insert_received_with_outcome,
    received_ingest_failure_by_source,
};
pub use replay::{redrive_received, Replay};
pub use resolved_config::ResolvedConfig;
pub use roles::{Role, RoleRegistry};
pub use router::{BeforeHandlerFuture, DispatchStats, HandlerFlow, HandlerFuture, MessageRouter};
pub use schema_sql::{
    add_idempotency_key_sql, add_idempotency_source_sql, add_outbox_entity_key_sql,
    add_received_entity_key_sql, add_received_failure_metadata_sql,
    backfill_received_failure_metadata_sql, create_cache_table_sql,
    create_outbox_entity_state_index_sql, create_outbox_retention_index_sql,
    create_outbox_state_index_sql, create_outbox_table_sql, create_received_failed_index_sql,
    create_received_idempotency_index_sql, create_received_state_index_sql,
    create_received_table_sql, CACHE_TEMPLATE_VERSION, OUTBOX_TEMPLATE_VERSION,
    RECEIVED_TEMPLATE_VERSION,
};
pub use tables::{CacheTable, OutboxTable, ReceivedTable};

#[cfg(feature = "internal-hooks")]
pub use dispatch::dispatch_once_with_observer;
/// Test seams, compiled only under the `internal-hooks` feature.
///
/// Kept behind a feature and `#[doc(hidden)]` so the observable API of a default
/// build contains no hook a production caller could reach for.
#[cfg(feature = "internal-hooks")]
pub use hooks::{DispatchFailureEvent, DispatchHooks};
#[cfg(feature = "internal-hooks")]
pub use outbox_claim::collapse_stale_pending_rows_sql;

#[cfg(test)]
mod tests;
