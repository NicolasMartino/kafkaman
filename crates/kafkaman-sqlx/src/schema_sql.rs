//! Every `CREATE`/`ALTER` statement kafkaman generates.
//!
//! Table names come from validated [`SqlIdentifier`]s and status lists from the
//! enums themselves, so the schema cannot drift from the Rust types and nothing
//! here interpolates caller input.

use kafkaman_core::{OutboxStatus, ReceiveStatus, ReceivedFailureKind, SqlIdentifier};

use crate::tables::qualified_name;
use crate::{CacheTable, OutboxTable, ReceivedTable, ResolvedConfig, Result};

pub fn create_outbox_table_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {name} (
            message_id UUID PRIMARY KEY,
            idempotency_key TEXT,
            idempotency_source JSONB,
            status TEXT NOT NULL DEFAULT {pending},
            attempts INT NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_error TEXT,
            claim_id UUID,
            claimed_by TEXT,
            claim_expires_at TIMESTAMPTZ,
            topic TEXT NOT NULL,
            partition_key TEXT,
            entity_key TEXT,
            correlation_id UUID NOT NULL,
            causation_id UUID,
            headers JSONB NOT NULL DEFAULT '{{}}',
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at TIMESTAMPTZ,
            CHECK (status IN ({status_list})),
            CHECK (idempotency_key IS NULL OR idempotency_key ~ '^[0-9a-f]{{64}}$')
        )",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
        status_list = OutboxStatus::sql_literal_list(),
    )
}

pub fn create_outbox_state_index_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (status, next_attempt_at, claim_expires_at, created_at)",
        table.state_index_name().quoted(),
        table.qualified_name()
    )
}

pub fn create_outbox_entity_state_index_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (entity_key, status, created_at) WHERE entity_key IS NOT NULL",
        table.entity_state_index_name().quoted(),
        table.qualified_name()
    )
}

/// Index serving retention's age scan over terminal rows.
///
/// Keyed on `created_at` rather than `published_at` for two reasons. `Superseded`
/// rows never receive a `published_at` — they were collapsed, not published — so a
/// single age column has to be one both statuses carry. And keying on one column
/// keeps the delete's predicate a plain range scan; a `COALESCE` or an `OR` across
/// two age columns is exactly the shape measured in R14 as unservable by any index.
///
/// The partial predicate lists every terminal status, including `Failed`, so the
/// opt-in variant of the purge can use the same index: a query filtering on a
/// subset of these statuses implies this predicate and remains index-served.
pub fn create_outbox_retention_index_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (created_at) WHERE status IN ({})",
        table.index_name("_retention").quoted(),
        table.qualified_name(),
        terminal_outbox_status_literals(),
    )
}

/// The statuses a row can hold once no worker will ever act on it again.
fn terminal_outbox_status_literals() -> String {
    [
        OutboxStatus::Published,
        OutboxStatus::Superseded,
        OutboxStatus::Failed,
    ]
    .into_iter()
    .map(|status| status.sql_literal())
    .collect::<Vec<_>>()
    .join(", ")
}

pub fn create_received_table_sql(table: &ReceivedTable) -> String {
    // `correlation_id`/`causation_id` are intentionally nullable: rows inserted
    // through `insert_received` always carry a correlation id from the kafkaman
    // `Envelope`, but a foreign producer's records may carry no kafkaman
    // correlation metadata at all.
    format!(
        "CREATE TABLE IF NOT EXISTS {name} (
            message_id UUID PRIMARY KEY,
            idempotency_key TEXT NOT NULL CHECK (idempotency_key ~ '^[0-9a-f]{{64}}$'),
            idempotency_source JSONB,
            status TEXT NOT NULL DEFAULT {pending} CHECK (status IN ({statuses})),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            next_attempt_at TIMESTAMPTZ,
            errors JSONB NOT NULL DEFAULT '[]'::jsonb,
            last_failed_at TIMESTAMPTZ,
            last_failure_kind TEXT CHECK (last_failure_kind IN ({failure_kinds})),
            source_topic TEXT NOT NULL,
            source_partition INTEGER NOT NULL,
            source_offset BIGINT NOT NULL,
            key BYTEA,
            entity_key TEXT,
            message_type TEXT NOT NULL,
            message_version INTEGER NOT NULL DEFAULT 1 CHECK (message_version >= 1),
            headers JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            payload JSONB NOT NULL,
            correlation_id UUID,
            causation_id UUID,
            occurred_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            processed_at TIMESTAMPTZ
        )",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        statuses = ReceiveStatus::sql_literal_list(),
        failure_kinds = received_failure_kind_sql_literal_list(),
    )
}

pub fn create_received_idempotency_index_sql(table: &ReceivedTable) -> String {
    format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (idempotency_key)",
        table.idempotency_index_name().quoted(),
        table.qualified_name()
    )
}

pub fn create_received_state_index_sql(table: &ReceivedTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (status, next_attempt_at, created_at)",
        table.state_index_name().quoted(),
        table.qualified_name()
    )
}

pub fn create_cache_table_sql(table: &CacheTable) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {name} (
            entity_key TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            applied_topic TEXT NOT NULL,
            applied_partition INTEGER NOT NULL,
            applied_offset BIGINT NOT NULL,
            deleted BOOLEAN NOT NULL DEFAULT false,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        name = table.qualified_name(),
    )
}

pub fn add_idempotency_key_sql(table: &OutboxTable) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS idempotency_key TEXT",
        table.qualified_name()
    )
}

pub fn add_idempotency_source_sql(table: &OutboxTable) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS idempotency_source JSONB",
        table.qualified_name()
    )
}

pub fn add_outbox_entity_key_sql(table: &OutboxTable) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS entity_key TEXT",
        table.qualified_name()
    )
}

pub fn add_received_entity_key_sql(table: &ReceivedTable) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS entity_key TEXT",
        table.qualified_name()
    )
}

/// The `last_failure_kind` CHECK list, generated from [`ReceivedFailureKind::ALL`]
/// so the database and the Rust enum cannot drift. `NULL` satisfies the CHECK, so
/// rows that have never failed are unconstrained.
pub(crate) fn received_failure_kind_sql_literal_list() -> String {
    ReceivedFailureKind::ALL
        .into_iter()
        .map(|kind| sql_string_literal(kind.discriminant()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The schema-wide quarantine table for records that never became receive rows.
///
/// Schema-wide rather than per message type: a record on an unexpected topic has
/// no message type to file it under, which is the whole reason it is here.
pub(crate) fn received_ingest_failures_table_name(cfg: &ResolvedConfig) -> Result<String> {
    Ok(qualified_name(
        &cfg.schema,
        &SqlIdentifier::new("received_ingest_failures")?,
    ))
}

pub(crate) fn create_received_ingest_failures_table_sql(cfg: &ResolvedConfig) -> Result<String> {
    let name = received_ingest_failures_table_name(cfg)?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {name} (
            source_topic TEXT NOT NULL,
            source_partition INT NOT NULL,
            source_offset BIGINT NOT NULL,
            key BYTEA,
            headers JSONB NOT NULL,
            payload BYTEA,
            message_type TEXT NOT NULL,
            expected_topic TEXT NOT NULL,
            failure_kind TEXT NOT NULL,
            error TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (source_topic, source_partition, source_offset)
        )"
    ))
}

/// A single-quoted SQL string literal, doubling any embedded quote.
///
/// Backslashes are left alone: under `standard_conforming_strings = on`, the
/// PostgreSQL default since 9.1, a backslash in a standard literal is an
/// ordinary character, and doubling it would corrupt the value.
pub(crate) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
