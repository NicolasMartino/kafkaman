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
// The evidence that this happens is in this very file. `create_outbox_table_sql`
// already declares `idempotency_key`, `idempotency_source`, and `entity_key` —
// the exact columns `AddIdempotencyKey`, `AddIdempotencySource`, and
// `AddOutboxEntityKey` exist to add to databases created before them. Those
// alters are the repair for three in-place template edits.
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
            traceparent TEXT,
            tracestate TEXT,
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

/// The two columns that record *why* a received row last failed.
///
/// Written on every failed dispatch and read by the DLQ views, which order by
/// the timestamp and filter on the kind. Nullable because a row that has never
/// failed has neither, which is most rows.
///
/// The CHECK rides on the `ADD COLUMN`, so it lands exactly when the column
/// does. On a table that already has the column the whole statement is skipped —
/// including the constraint — which is the same trade every `IF NOT EXISTS`
/// changeset here makes: converge on the shape, never rewrite what is already
/// there.
pub fn add_received_failure_metadata_sql(table: &ReceivedTable) -> [String; 2] {
    let name = table.qualified_name();
    [
        format!("ALTER TABLE {name} ADD COLUMN IF NOT EXISTS last_failed_at TIMESTAMPTZ"),
        format!(
            "ALTER TABLE {name} ADD COLUMN IF NOT EXISTS last_failure_kind TEXT \
             CHECK (last_failure_kind IN ({}))",
            received_failure_kind_sql_literal_list(),
        ),
    ]
}

/// Recover both failure columns for rows that were dead-lettered before the
/// columns existed.
///
/// # Why `ADD COLUMN` alone is not the migration
///
/// The DLQ views read the columns; the operator's view reads `errors`. A failed
/// row left with NULLs is therefore visible *and* unreachable: `/dlq` renders
/// the last audit entry's `type`, and a redrive filtered by that exact kind
/// tests `last_failure_kind`, matches nothing, and reports success having moved
/// no rows. The `occurred_after` filter misses them the same way, and because
/// `ORDER BY last_failed_at` sorts NULLs last, a paged DLQ view drops precisely
/// these rows off the end while the count beside it still counts them. Every one
/// of those failures is silent.
///
/// # Why the data is there to recover
///
/// Every recorded failure appends an RFC 9457 problem detail to `errors` with
/// both its `type` URI and its `occurred_at`. The columns were only ever a
/// queryable projection of the newest entry, so the backfill reads the same
/// place the projection came from.
///
/// That timestamp is RFC 9557 — RFC 3339 with a `[UTC]` annotation PostgreSQL
/// cannot cast, which is why the DLQ queries do not read the JSON. A migration
/// is the one place stripping the annotation is the right trade: it happens once
/// rather than per query.
///
/// # Why the cast runs inside a subtransaction
///
/// A shape test is not a parser. `2026-99-99T10:00:00Z` matches any regex that
/// describes an RFC 3339 date and still raises `datetime_field_overflow`, and so
/// does `2026-02-30` — no pattern can rule out a day the calendar does not have.
/// A raise inside a set-based `UPDATE` aborts the whole statement, which aborts
/// the migration transaction, which turns one malformed audit row written years
/// ago into a process that will not boot. That is not a trade worth making for a
/// column that is a convenience projection.
///
/// So the timestamp pass is a `DO` block: one set-based `UPDATE` for the case
/// that costs nothing, and — only if that raises — a second pass one row at a
/// time, each in its own subtransaction, where an unparseable entry costs its
/// own row and nothing else. The shape test survives as a *value* filter rather
/// than a safety one: it keeps `infinity`, `now` and the rest of PostgreSQL's
/// special inputs, all of which cast happily, from being mistaken for a recorded
/// failure time.
///
/// The kind pass cannot raise — it is string equality against a generated `CASE`
/// — so it stays a plain statement, and a row whose timestamp is unreadable
/// still recovers the kind beside it.
///
/// A `type` outside the current vocabulary leaves the kind NULL rather than
/// guessing, and must: the CHECK its sibling statement installs would reject
/// anything invented here.
pub fn backfill_received_failure_metadata_sql(table: &ReceivedTable) -> [String; 2] {
    const LAST: &str = "errors -> (jsonb_array_length(errors) - 1)";
    let name = table.qualified_name();
    let failed = ReceiveStatus::Failed.sql_literal();
    let occurred_at = format!("split_part({LAST} ->> 'occurred_at', '[', 1)");
    // Both spellings of every kind: the RFC 9457 `type` URI written today, and
    // the bare discriminant rows carried before the problem-detail format. The
    // pair mirrors `ReceivedFailureKind::from_problem_type`, and generating it
    // from `ALL` is what stops the migration and the enum from drifting apart.
    let kinds = ReceivedFailureKind::ALL
        .into_iter()
        .map(|kind| {
            let discriminant = sql_string_literal(kind.discriminant());
            format!(
                "WHEN {problem_type} THEN {discriminant} WHEN {discriminant} THEN {discriminant}",
                problem_type = sql_string_literal(kind.problem_type()),
            )
        })
        .collect::<Vec<_>>()
        .join(" ");

    // Written once and shared by both passes of the timestamp statement, so the
    // row-at-a-time fallback can never select a different set than the
    // set-based attempt it is standing in for.
    let unrecovered_timestamps = format!(
        "status = {failed}
           AND last_failed_at IS NULL
           AND jsonb_typeof(errors) = 'array'
           AND jsonb_array_length(errors) > 0
           AND {occurred_at} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}[T ]'"
    );

    [
        format!(
            "UPDATE {name} SET last_failure_kind = CASE
                COALESCE({LAST} ->> 'type', {LAST} ->> 'kind') {kinds}
             END
             WHERE status = {failed}
               AND last_failure_kind IS NULL
               AND jsonb_typeof(errors) = 'array'
               AND jsonb_array_length(errors) > 0"
        ),
        format!(
            "DO $kafkaman_backfill$
             DECLARE
                 dead_letter record;
             BEGIN
                 BEGIN
                     UPDATE {name}
                        SET last_failed_at = {occurred_at}::timestamptz
                      WHERE {unrecovered_timestamps};
                     RETURN;
                 EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
                     -- At least one stored timestamp is date-shaped and not a
                     -- date. Fall through and pay for it a row at a time.
                     NULL;
                 END;

                 FOR dead_letter IN
                     SELECT message_id, {occurred_at} AS recovered_at
                       FROM {name}
                      WHERE {unrecovered_timestamps}
                 LOOP
                     BEGIN
                         UPDATE {name}
                            SET last_failed_at = dead_letter.recovered_at::timestamptz
                          WHERE message_id = dead_letter.message_id;
                     EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
                         -- This row's audit trail cannot say when it failed.
                         -- Leaving it NULL is the honest answer; aborting the
                         -- changelog is not.
                         NULL;
                     END;
                 END LOOP;
             END
             $kafkaman_backfill$"
        ),
    ]
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

/// The two W3C trace-context columns, on an outbox table.
///
/// Nullable and unindexed: absent context is the normal case, and nothing
/// queries by trace id — the value is read alongside the row it belongs to, and
/// a backend does the searching.
pub fn add_outbox_trace_context_sql(table: &OutboxTable) -> [String; 2] {
    add_trace_context_sql(&table.qualified_name())
}

/// The same pair on a received table, so a dispatch can descend from the ingest
/// that stored the row.
pub fn add_received_trace_context_sql(table: &ReceivedTable) -> [String; 2] {
    add_trace_context_sql(&table.qualified_name())
}

fn add_trace_context_sql(qualified_name: &str) -> [String; 2] {
    [
        format!("ALTER TABLE {qualified_name} ADD COLUMN IF NOT EXISTS traceparent TEXT"),
        format!("ALTER TABLE {qualified_name} ADD COLUMN IF NOT EXISTS tracestate TEXT"),
    ]
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
