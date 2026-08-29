//! Read-only inspection of outbox and received tables: depth by status, and the
//! rows that have aged past an operator's threshold.
//!
//! # The two ages
//!
//! Every stuck row reports two durations, and conflating them is the mistake
//! this split exists to prevent.
//!
//! `age_ms` is time since `created_at`: how long this message has been waiting,
//! start to finish. It is the number that says how much delay a customer has
//! experienced.
//!
//! `stuck_for_ms` is time since the row became *late* — `claim_expires_at` for
//! an outbox row whose claimant died, `due_at` for a received row past its
//! backoff. It is the number `stuck_after` filters on, so it is the one that
//! says how long the fault has been going on.
//!
//! They differ by however long the row queued legitimately first, which on a
//! healthy backlog is most of it. Reporting only `age_ms` — as this did — makes
//! a thirty-second outage on an hour-old message look like an hour-long outage.
//!
//! Nothing here mutates. The one operational write that belongs with these — a
//! runtime DLQ redrive — lives in `replay.rs` beside the statement it reuses.
use std::collections::BTreeSet;
use std::time::Duration;

use kafkaman_core::{OutboxStatus, ReceiveStatus, SqlIdentifier};
use serde::Serialize;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::resolved_config::ResolvedConfig;
use crate::retry_backoff::duration_to_time;
use crate::{Error, OutboxTable, ReceivedTable, Result};

/// Which of the durable tables a configuration *could* name this service
/// actually has.
///
/// # Why this is needed at all
///
/// A service declares every message type it exchanges, and `ResolvedConfig`
/// records the descriptors — but not which side of each one this service is on.
/// Roles are declared above this crate, and a hand-wired service has no role
/// registry at all, so the configuration genuinely cannot answer "do I publish
/// this, or consume it".
///
/// The schema can. An outbox table exists exactly when this service publishes
/// the type, and a received table exactly when it consumes it, because that is
/// what the migrations create. So anything summarizing "every message type"
/// must ask the schema first, or it queries a table that was never meant to
/// exist and fails the whole request.
///
/// That is not hypothetical: every operator summary route raised
/// `relation ... does not exist` for any service that both publishes and
/// consumes, which is every realistic service. It survived because nothing
/// mounted those routes.
#[derive(Clone, Debug, Default)]
pub struct ServiceTables {
    present: BTreeSet<String>,
}

impl ServiceTables {
    /// Whether the schema holds this table, by the qualified name
    /// [`OutboxTable::qualified_name`] and friends produce.
    #[must_use]
    pub fn contains(&self, qualified_name: &str) -> bool {
        self.present.contains(qualified_name)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }
}

/// Ask the schema which outbox and received tables this service has.
///
/// One round trip for the whole set rather than one per candidate: the routes
/// that need this already run an unbounded aggregate per message type, and
/// doubling that with an existence probe each would be the more expensive half.
///
/// This asks the catalog rather than trying a `SELECT`: a missing table should
/// filter out one side of a service, not fail the whole request. The probe is
/// still stricter than `to_regclass`; a view, index, sequence, or table the
/// current role cannot read is not usable by the admin routes.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn service_tables(pool: &PgPool, cfg: &ResolvedConfig) -> Result<ServiceTables> {
    let mut qualified_names = Vec::with_capacity(cfg.messages().len() * 2);
    let mut schema_names = Vec::with_capacity(cfg.messages().len() * 2);
    let mut table_names = Vec::with_capacity(cfg.messages().len() * 2);
    for descriptor in cfg.messages() {
        let outbox = OutboxTable::new(cfg.schema.clone(), descriptor.clone())?;
        push_table_candidate(
            &mut qualified_names,
            &mut schema_names,
            &mut table_names,
            &outbox.schema,
            &outbox.table,
        );

        let received = ReceivedTable::for_descriptor(cfg, descriptor.clone())?;
        push_table_candidate(
            &mut qualified_names,
            &mut schema_names,
            &mut table_names,
            &received.schema,
            &received.table,
        );
    }

    let present: Vec<String> = sqlx::query_scalar(
        "SELECT candidate.qualified_name
         FROM unnest($1::text[], $2::text[], $3::text[])
              AS candidate(qualified_name, schema_name, table_name)
         JOIN pg_namespace namespace
           ON namespace.nspname = candidate.schema_name
         JOIN pg_class class
           ON class.relnamespace = namespace.oid
          AND class.relname = candidate.table_name
         WHERE class.relkind IN ('r', 'p')
           AND has_table_privilege(class.oid, 'SELECT')",
    )
    .bind(&qualified_names)
    .bind(&schema_names)
    .bind(&table_names)
    .fetch_all(pool)
    .await?;

    Ok(ServiceTables {
        present: present.into_iter().collect(),
    })
}

/// Why a generated service table cannot be used, when it cannot.
///
/// A `bool` collapsed three genuinely different repairs into one answer, and the
/// route that consumed it had to guess between them in prose: run your
/// migrations, point the request at the service that consumes this type, or
/// grant your database role the privilege. Naming which one it is turns a
/// message an operator has to work through into one they can act on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableAccess {
    /// Present, a real table, and usable for the privileges asked about.
    Ready,
    /// No relation of that name in that schema. Either the migrations have not
    /// run, or this service was never meant to have this side of the type.
    Missing,
    /// A relation of that name exists but is not an ordinary or partitioned
    /// table — a view or a sequence left over from a hand-rolled schema.
    NotATable,
    /// The table is there and the current database role cannot use it as asked.
    NoPrivilege,
}

impl TableAccess {
    #[must_use]
    pub fn is_ready(self) -> bool {
        self == Self::Ready
    }

    /// What an operator should do about it, for an error body.
    #[must_use]
    pub fn repair(self) -> &'static str {
        match self {
            Self::Ready => "nothing; the table is usable",
            Self::Missing => {
                "run this service's migrations, or address the request to the service that has \
                 this side of the message type"
            }
            Self::NotATable => {
                "a relation of that name exists but is not a table; kafkaman's migrations create \
                 one, so something else owns this name"
            }
            Self::NoPrivilege => {
                "grant this service's database role the privileges the route needs on the table"
            }
        }
    }
}

/// Whether one generated service table is present and usable for `privileges`.
///
/// `privileges` is checked with `has_table_privilege`'s **AND** semantics, one
/// call per name — its own comma-separated form means "any of these", which is
/// not what a caller listing what it is about to do wants. A route that reads
/// passes `["SELECT"]`; one that writes has to say so, because a role with
/// `SELECT` and no `UPDATE` would otherwise pass a read-shaped probe and then
/// fail against the statement it was cleared for.
///
/// The predicates are selected as columns rather than filtered on, so an
/// unusable table can say *why* instead of being indistinguishable from an
/// absent one.
///
/// An empty `privileges` is a pure existence probe, and answers `Ready` for a
/// table that exists: "the role holds all of no privileges" is vacuously true.
/// The `LEFT JOIN` is what buys that. A `CROSS JOIN` over an empty array
/// produces no rows at all, so the aggregate never runs, `fetch_optional`
/// returns `None`, and a present, fully-readable table is reported `Missing` —
/// the one answer that is both wrong and actionable, since it sends an operator
/// to re-run migrations that already ran.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn service_table_access(
    pool: &PgPool,
    schema: &SqlIdentifier,
    table: &SqlIdentifier,
    privileges: &[&str],
) -> Result<TableAccess> {
    let found: Option<(bool, bool)> = sqlx::query_as(
        "SELECT class.relkind IN ('r', 'p'),
                coalesce(bool_and(has_table_privilege(class.oid, privilege)), true)
         FROM pg_namespace namespace
         JOIN pg_class class
           ON class.relnamespace = namespace.oid
          AND class.relname = $2
         LEFT JOIN unnest($3::text[]) AS privilege ON true
         WHERE namespace.nspname = $1
         GROUP BY class.relkind",
    )
    .bind(schema.as_str())
    .bind(table.as_str())
    .bind(privileges)
    .fetch_optional(pool)
    .await?;

    Ok(match found {
        None => TableAccess::Missing,
        Some((false, _)) => TableAccess::NotATable,
        Some((true, false)) => TableAccess::NoPrivilege,
        Some((true, true)) => TableAccess::Ready,
    })
}

fn push_table_candidate(
    qualified_names: &mut Vec<String>,
    schema_names: &mut Vec<String>,
    table_names: &mut Vec<String>,
    schema: &SqlIdentifier,
    table: &SqlIdentifier,
) {
    qualified_names.push(crate::tables::qualified_name(schema, table));
    schema_names.push(schema.as_str().to_owned());
    table_names.push(table.as_str().to_owned());
}

/// One `(message type, status)` bucket of an outbox table, with the age of its
/// oldest row.
///
/// Timestamps carry the `rfc9557` adapter rather than `time`'s derived impl.
/// The workspace does not enable `time/serde-human-readable`, so a bare
/// `OffsetDateTime` serializes to a nine-integer array — unreadable to every
/// dashboard that consumes these summaries over HTTP.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OutboxStatusSummary {
    pub message_type: String,
    pub status: OutboxStatus,
    pub count: i64,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub oldest_created_at: Option<OffsetDateTime>,
    pub oldest_age_ms: Option<u64>,
    /// Set when this bucket holds queued work older than the configured
    /// `max_queue_age`. Always false for terminal statuses: a `Published` row
    /// waiting on retention is not a backlog.
    pub over_max_queue_age: bool,
}

/// One `(message type, status)` bucket of a received table. See
/// [`OutboxStatusSummary`] for the timestamp-format rationale.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReceivedStatusSummary {
    pub message_type: String,
    pub status: ReceiveStatus,
    pub count: i64,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub oldest_created_at: Option<OffsetDateTime>,
    pub oldest_age_ms: Option<u64>,
    /// Set when this bucket holds queued work older than the configured
    /// `max_queue_age`. Always false for terminal statuses.
    pub over_max_queue_age: bool,
}

/// An outbox row whose publish claim expired without the claimant marking it
/// either published or failed — the signature of a worker that died mid-publish.
///
/// `age_ms` measures from `created_at` and `stuck_for_ms` from
/// `claim_expires_at` — how long the message has been undelivered, and how long
/// it has been stuck. `stuck_after` filters on the second. They differ by
/// however long the row queued legitimately before its claimant died.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OutboxStuckRow {
    pub message_type: String,
    pub message_id: Uuid,
    pub status: OutboxStatus,
    pub claimed_by: Option<String>,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub claim_expires_at: Option<OffsetDateTime>,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub created_at: OffsetDateTime,
    /// Time since `created_at`: how long this message has been undelivered.
    pub age_ms: u64,
    /// Time since `claim_expires_at`: how long it has been *stuck*.
    pub stuck_for_ms: u64,
}

/// A received row that has been due for dispatch longer than the caller's
/// threshold — a dispatcher that is down, wedged, or falling behind.
///
/// `due_at` is `next_attempt_at` when a retry is scheduled and `created_at`
/// otherwise. A row inside its backoff window is not late, it is waiting, which
/// is why the fault clock starts at `due_at` rather than at `created_at`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReceivedStuckRow {
    pub message_type: String,
    pub message_id: Uuid,
    pub status: ReceiveStatus,
    pub attempts: i32,
    #[serde(with = "kafkaman_core::rfc9557::option")]
    pub next_attempt_at: Option<OffsetDateTime>,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub created_at: OffsetDateTime,
    #[serde(with = "kafkaman_core::rfc9557")]
    pub due_at: OffsetDateTime,
    /// Time since `created_at`: how long this message has been unprocessed.
    pub age_ms: u64,
    /// Time since `due_at`: how long it has been *overdue*.
    pub stuck_for_ms: u64,
}

/// Per-status row counts and oldest-row age for one outbox table.
///
/// This is a full aggregate over the table with no time bound, which is what
/// makes it a correct depth reading and also what makes it expensive: it is
/// index-only at best and a sequential scan on an unvacuumed table. Callers
/// exposing it over HTTP should cache or rate-limit rather than serving it per
/// request.
///
/// `max_queue_age` only decides the `over_max_queue_age` flag; it never filters
/// rows, so the counts stay complete regardless of the threshold.
// Stays in the debug tier: the queue-metrics sampler calls this on `refresh_interval`.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn outbox_status_summary(
    pool: &PgPool,
    table: &OutboxTable,
    now: OffsetDateTime,
    max_queue_age: Duration,
) -> Result<Vec<OutboxStatusSummary>> {
    let sql = format!(
        "SELECT status, count(*) AS row_count, min(created_at) AS oldest_created_at
         FROM {}
         GROUP BY status
         ORDER BY status",
        table.qualified_name()
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status: OutboxStatus = status.parse().map_err(Error::Core)?;
            let oldest_created_at: Option<OffsetDateTime> = row.try_get("oldest_created_at")?;
            let oldest_age_ms = oldest_created_at.map(|created_at| age_ms_since(now, created_at));
            Ok(OutboxStatusSummary {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                status,
                count: row.try_get("row_count")?,
                oldest_created_at,
                oldest_age_ms,
                over_max_queue_age: over_max_queue_age(
                    status.is_terminal(),
                    oldest_age_ms,
                    max_queue_age,
                ),
            })
        })
        .collect()
}

/// Per-status row counts and oldest-row age for one received table. Carries the
/// same cost caveat as [`outbox_status_summary`].
// Stays in the debug tier: the queue-metrics sampler calls this on `refresh_interval`.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn received_status_summary(
    pool: &PgPool,
    table: &ReceivedTable,
    now: OffsetDateTime,
    max_queue_age: Duration,
) -> Result<Vec<ReceivedStatusSummary>> {
    let sql = format!(
        "SELECT status, count(*) AS row_count, min(created_at) AS oldest_created_at
         FROM {}
         GROUP BY status
         ORDER BY status",
        table.qualified_name()
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status: ReceiveStatus = status.parse().map_err(Error::Core)?;
            let oldest_created_at: Option<OffsetDateTime> = row.try_get("oldest_created_at")?;
            let oldest_age_ms = oldest_created_at.map(|created_at| age_ms_since(now, created_at));
            Ok(ReceivedStatusSummary {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                status,
                count: row.try_get("row_count")?,
                oldest_created_at,
                oldest_age_ms,
                over_max_queue_age: over_max_queue_age(
                    status.is_terminal(),
                    oldest_age_ms,
                    max_queue_age,
                ),
            })
        })
        .collect()
}

/// Outbox rows whose publish claim expired more than `stuck_after` ago.
///
/// An expired claim is not itself a fault — the relay reclaims them on the next
/// cycle. Staying expired past the threshold is the fault, so the cutoff is
/// applied to `claim_expires_at` rather than to `created_at`.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn outbox_stuck_rows(
    pool: &PgPool,
    table: &OutboxTable,
    now: OffsetDateTime,
    stuck_after: Duration,
    limit: i64,
) -> Result<Vec<OutboxStuckRow>> {
    let cutoff = now - duration_to_time(stuck_after);
    let sql = format!(
        "SELECT message_id, status, claimed_by, claim_expires_at, created_at
         FROM {name}
         WHERE status = {publishing}
           AND claim_expires_at IS NOT NULL
           AND claim_expires_at <= $1
         ORDER BY claim_expires_at, created_at, message_id
         LIMIT $2",
        name = table.qualified_name(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    let rows = sqlx::query(&sql)
        .bind(cutoff)
        .bind(limit.max(0))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status = status.parse().map_err(Error::Core)?;
            let created_at: OffsetDateTime = row.try_get("created_at")?;
            let claim_expires_at: Option<OffsetDateTime> = row.try_get("claim_expires_at")?;
            Ok(OutboxStuckRow {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                message_id: row.try_get("message_id")?,
                status,
                claimed_by: row.try_get("claimed_by")?,
                claim_expires_at,
                created_at,
                age_ms: age_ms_since(now, created_at),
                // The query selected this row on `claim_expires_at`, so this is
                // the number that answers the question the filter asked.
                stuck_for_ms: claim_expires_at
                    .map(|expired_at| age_ms_since(now, expired_at))
                    .unwrap_or(0),
            })
        })
        .collect()
}

/// Received rows that have been due for dispatch for more than `stuck_after`.
///
/// The predicate is split per status rather than written as
/// `COALESCE(next_attempt_at, created_at) <= $1`. Both forms select the same
/// rows, but only this one is servable by the received table's
/// `(status, next_attempt_at, created_at)` index — a `COALESCE` across two age
/// columns is the shape [`create_outbox_retention_index_sql`] records as
/// unservable by any index. `claim_received_row` splits it for the same reason,
/// and this query must stay cheap: it runs per message type per admin request.
///
/// `due_at` stays a projected `COALESCE` and still drives `ORDER BY`, because
/// "most overdue first" is the only useful order for this list. That costs a
/// top-N sort, but only over rows the indexed `WHERE` already matched — which is
/// the overdue set itself, not the table.
///
/// [`create_outbox_retention_index_sql`]: crate::create_outbox_retention_index_sql
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn received_stuck_rows(
    pool: &PgPool,
    table: &ReceivedTable,
    now: OffsetDateTime,
    stuck_after: Duration,
    limit: i64,
) -> Result<Vec<ReceivedStuckRow>> {
    let cutoff = now - duration_to_time(stuck_after);
    let sql = format!(
        "SELECT message_id, status, attempts, next_attempt_at, created_at,
                COALESCE(next_attempt_at, created_at) AS due_at
         FROM {name}
         WHERE (
             status = {pending}
             AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
             AND created_at <= $1
         ) OR (
             status = {retryable}
             AND next_attempt_at <= $1
         )
         ORDER BY due_at, created_at, message_id
         LIMIT $2",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        retryable = ReceiveStatus::Retryable.sql_literal(),
    );
    let rows = sqlx::query(&sql)
        .bind(cutoff)
        .bind(limit.max(0))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let status = status.parse().map_err(Error::Core)?;
            let created_at: OffsetDateTime = row.try_get("created_at")?;
            let due_at: OffsetDateTime = row.try_get("due_at")?;
            Ok(ReceivedStuckRow {
                message_type: table.descriptor.message_type.as_str().to_owned(),
                message_id: row.try_get("message_id")?,
                status,
                attempts: row.try_get("attempts")?,
                next_attempt_at: row.try_get("next_attempt_at")?,
                created_at,
                due_at,
                age_ms: age_ms_since(now, created_at),
                // Selected on `due_at`, so this is the overdue clock. It used to
                // be reported as `age_ms`, which meant the same field name
                // measured from `created_at` on the outbox side and from
                // `due_at` here — two answers to one question on the same
                // operator screen.
                stuck_for_ms: age_ms_since(now, due_at),
            })
        })
        .collect()
}

/// Milliseconds elapsed from `since` to `now`, clamped at zero.
///
/// `now` is caller-supplied so inspection reads share one clock across every
/// table in a request. A caller passing a `now` behind the row's timestamp gets
/// zero rather than a wrapped `u64`.
fn age_ms_since(now: OffsetDateTime, since: OffsetDateTime) -> u64 {
    let delta = now - since;
    if delta.is_negative() {
        return 0;
    }
    u64::try_from(delta.whole_milliseconds()).unwrap_or(u64::MAX)
}

/// Whether a status bucket holds queued work that has aged past the threshold.
///
/// Terminal buckets never warn. A `Published` outbox row or a `Processed`
/// received row is retained history, and its age reflects the retention window,
/// not a backlog — flagging it would make the warning meaningless on any
/// deployment that keeps history.
fn over_max_queue_age(
    is_terminal: bool,
    oldest_age_ms: Option<u64>,
    max_queue_age: Duration,
) -> bool {
    if is_terminal {
        return false;
    }
    let threshold = u64::try_from(max_queue_age.as_millis()).unwrap_or(u64::MAX);
    oldest_age_ms.is_some_and(|age_ms| age_ms >= threshold)
}
