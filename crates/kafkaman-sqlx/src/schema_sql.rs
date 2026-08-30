//! Every `CREATE`/`ALTER` statement kafkaman generates.
//!
//! Table names come from validated [`SqlIdentifier`]s and status lists from the
//! enums themselves, so the schema cannot drift from the Rust types and nothing
//! here interpolates caller input.

use kafkaman_core::{OutboxStatus, ReceiveStatus, ReceivedFailureKind, SqlIdentifier};

use crate::tables::qualified_name;
use crate::{CacheTable, OutboxTable, ReceivedTable, ResolvedConfig, Result};

// # Template versions
//
// **A shipped table template is never edited in place. Add an upgrade changeset
// and bump the version below.**
//
// This is not a style preference, it is the only thing standing between an
// edited template and silent schema drift. A changeset's identity is its
// `checksum_material`, which is `version;name;message_type;topic` — it does not
// include one byte of the SQL. So editing `create_outbox_table_sql` changes what
// a *fresh* database gets and changes nothing about an existing one: the version
// is already in `changelog_history`, the checksum still matches, and the
// migration engine reports success while the two databases diverge forever.
//
// Before V1 this file carried nine `ALTER`-shaped changesets that were repairs
// for exactly that mistake, made while nothing was deployed: the templates had
// been edited in place, and the alters brought already-created tables back up to
// the edited shape. They were deleted at the V1 tag, because the databases they
// repaired never existed outside development. Every table kind is therefore on
// slot 0, and V1's schema history is one create per table.
//
// That deletion is also why the rule above starts applying *now* rather than
// having always applied. From the first tagged release the templates are shipped
// and frozen; the next change to any of them is an upgrade changeset, not an
// edit.
//
// The template version is the intra-band slot of a generated changeset (see
// `generated_changelog`), so a bump always sorts after that table's create and
// never disturbs any other table's identity. Bumping means: write the upgrade
// changeset, register it in `generated_changelog::upgrades`, then bump.

/// Slot of the current outbox table template. See the note above before bumping.
pub const OUTBOX_TEMPLATE_VERSION: i64 = 0;

/// Slot of the current received table template. See the note above before bumping.
pub const RECEIVED_TEMPLATE_VERSION: i64 = 0;

/// Slot of the current cache table template. See the note above before bumping.
pub const CACHE_TEMPLATE_VERSION: i64 = 0;

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
            traceparent TEXT,
            tracestate TEXT,
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
            status TEXT NOT NULL DEFAULT {pending},
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
            traceparent TEXT,
            tracestate TEXT,
            occurred_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            processed_at TIMESTAMPTZ,
            CONSTRAINT {status_constraint} CHECK (status IN ({statuses}))
        )",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        status_constraint = received_status_constraint_name(table).quoted(),
        statuses = ReceiveStatus::sql_literal_list(),
        failure_kinds = received_failure_kind_sql_literal_list(),
    )
}

fn received_status_constraint_name(table: &ReceivedTable) -> SqlIdentifier {
    table.index_name("_status_check")
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

/// The index the DLQ views read.
///
/// Partial on `status = 'Failed'`, which is the only status any of them looks
/// at, and ordered the way the DLQ queries order — `last_failed_at`, then
/// `created_at`, then `message_id`, the tie-break chain in `queries.rs`. An operator's
/// DLQ page is `WHERE status = 'Failed' ORDER BY last_failed_at LIMIT n`, and a
/// redrive is the same query with `FOR UPDATE SKIP LOCKED` bolted on. The
/// general `(status, next_attempt_at, created_at)` index can answer the `WHERE`
/// and nothing else — the sort that follows it has to read every failed row and
/// order it, on a table whose live rows are the ones nobody is looking at.
///
/// `last_failure_kind` rides along as a fourth key column, after the ordering
/// chain rather than before it. A DLQ view narrowed to one failure kind is the
/// same ordered scan with an equality test, and a trailing key column is one
/// PostgreSQL can apply inside the index — so rows of other kinds are rejected
/// without a heap fetch, while the unfiltered page keeps the index ordering it
/// already had. Leading with the kind would invert that: filtered pages would
/// get a range scan and unfiltered ones would go back to sorting.
///
/// Partial rather than complete because the DLQ is the small end of the table by
/// design. Indexing every row to serve queries about the failed ones would size
/// the index to the workload rather than to the backlog.
pub fn create_received_failed_index_sql(table: &ReceivedTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} \
         (last_failed_at, created_at, message_id, last_failure_kind) \
         WHERE status = {}",
        table.index_name("_failed").quoted(),
        table.qualified_name(),
        ReceiveStatus::Failed.sql_literal(),
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
