use kafkaman_core::{KafkaMessage, MessageDescriptor, ReceiveStatus, ReceivedFailureKind};
use sqlx::{PgPool, Postgres, Transaction};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::changeset::{ChangeBuilder, Changeset};
use crate::queries::{
    latest_failure_kind_clause, latest_failure_time_sql, received_failure_order_sql,
};
use crate::schema_sql::sql_string_literal;
use crate::{Error, ReceivedTable, ResolvedConfig, Result};

#[derive(Clone, Debug)]
pub struct Replay {
    version: i64,
    descriptor: MessageDescriptor,
    occurred_after: Option<OffsetDateTime>,
    max_rows: Option<i64>,
    contexts: Vec<String>,
    failure_kind: Option<ReceivedFailureKind>,
    clear_history: bool,
}

impl Replay {
    /// Always fails with [`Error::UnsafeOutboxReplay`].
    ///
    /// Row-sourced outbox replay is incompatible with the offset convergence
    /// ordinal. A republished row is written to Kafka at a *new, higher* offset,
    /// so every consumer's cache guard sees older state carrying a newer ordinal
    /// and applies it — permanently and silently overwriting current state. The
    /// guard cannot detect this, because the record genuinely is newer in the log.
    ///
    /// Repair is state-sourced instead: re-read the entity's current state and
    /// enqueue it normally. A concurrent live update then supersedes the pending
    /// repair row and correctly wins. See the entity-first propagation decision,
    /// point 9.
    ///
    /// This constructor is retained rather than deleted so the failure names the
    /// reason at the call site instead of vanishing into a compile error.
    /// [`Replay::received`] is unaffected: redriven inbound rows keep their
    /// original `source_offset`, so redrive cannot invent a newer ordinal.
    pub fn outbox<P: KafkaMessage>(_version: i64) -> Result<Self> {
        Err(Error::UnsafeOutboxReplay {
            message_type: P::MESSAGE_TYPE.to_owned(),
        })
    }

    /// Redrive terminal (`Failed`) inbound rows back to `Pending` for one more
    /// processing pass. Safe under the offset ordinal because a redriven row
    /// keeps its original `source_offset` and therefore cannot claim to be newer
    /// than state already applied. Contrast [`Replay::outbox`].
    pub fn received<P: KafkaMessage>(version: i64) -> Result<Self> {
        Ok(Self {
            version,
            descriptor: P::descriptor()?,
            occurred_after: None,
            max_rows: None,
            contexts: Vec::new(),
            failure_kind: None,
            clear_history: false,
        })
    }

    /// Version stamp for a replay that is executed directly rather than applied
    /// as a changeset, where the changelog never sees it.
    pub const RUNTIME_VERSION: i64 = 0;

    /// A replay built from a descriptor resolved at runtime rather than from a
    /// `KafkaMessage` type parameter, for callers — admin routes — that only
    /// know the message type as a string.
    ///
    /// `version` is only meaningful when the replay is applied as a changeset.
    /// [`redrive_received`] never touches the changelog, so a runtime redrive
    /// should pass [`Replay::RUNTIME_VERSION`].
    pub fn received_descriptor(version: i64, descriptor: MessageDescriptor) -> Self {
        Self {
            version,
            descriptor,
            occurred_after: None,
            max_rows: None,
            contexts: Vec::new(),
            failure_kind: None,
            clear_history: false,
        }
    }

    pub fn since(mut self, occurred_after: OffsetDateTime) -> Self {
        self.occurred_after = Some(occurred_after);
        self
    }

    pub fn max_rows(mut self, cap: i64) -> Self {
        self.max_rows = Some(cap);
        self
    }

    pub fn contexts(mut self, contexts: &[&str]) -> Self {
        self.contexts = contexts
            .iter()
            .map(|context| (*context).to_owned())
            .collect();
        self
    }

    /// Narrow a received redrive to terminal rows whose most recent failure was
    /// of the given kind, e.g. redrive only `Handler` failures after fixing a
    /// handler bug while leaving `InvalidPayload` rows parked. Received-only; it
    /// has no effect on an outbox replay.
    pub fn failure_kind(mut self, kind: ReceivedFailureKind) -> Self {
        self.failure_kind = Some(kind);
        self
    }

    /// Erase forensic history on redrive: reset `attempts` to zero and clear the
    /// stored error array so the row redrives with a full retry budget and no
    /// past failures. The default redrive preserves both for triage; this opts
    /// into a clean slate. Received-only.
    pub fn clear_history(mut self) -> Self {
        self.clear_history = true;
        self
    }

    fn max_rows_or_error(&self) -> Result<i64> {
        match self.max_rows {
            Some(max_rows) if max_rows > 0 => Ok(max_rows),
            Some(_) => Err(Error::InvalidReplay {
                version: self.version,
                message: "max_rows must be greater than zero".to_owned(),
            }),
            None => Err(Error::InvalidReplay {
                version: self.version,
                message: "max_rows is required".to_owned(),
            }),
        }
    }
}

impl Changeset for Replay {
    fn version(&self) -> i64 {
        self.version
    }

    fn name(&self) -> &str {
        "replay_received"
    }

    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
        let table = ReceivedTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        builder.push(replay_received_update_sql(&table, self)?);
        Ok(())
    }

    fn checksum_material(&self) -> String {
        let occurred_after = self
            .occurred_after
            .map(|timestamp| {
                timestamp
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| timestamp.to_string())
            })
            .unwrap_or_else(|| "none".to_owned());
        let failure_kind = self
            .failure_kind
            .map(ReceivedFailureKind::discriminant)
            .unwrap_or("none");
        let clear_history = self.clear_history;
        format!(
            "version={};name={};message_type={};topic={};occurred_after={};max_rows={};contexts={};failure_kind={};clear_history={}",
            self.version(),
            self.name(),
            self.descriptor.message_type.as_str(),
            self.descriptor.topic,
            occurred_after,
            self.max_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            self.contexts.join(","),
            failure_kind,
            clear_history,
        )
    }

    fn contexts(&self) -> &[String] {
        &self.contexts
    }

    fn dry_run_preview(&self, cfg: &ResolvedConfig) -> Result<String> {
        let table = ReceivedTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        Ok(format!(
            "would requeue ~N received rows in {}",
            table.qualified_name()
        ))
    }

    fn estimate_replay_count<'a>(
        &'a self,
        cfg: &'a ResolvedConfig,
        tx: &'a mut Transaction<'_, Postgres>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<i64>>> + Send + 'a>> {
        Box::pin(async move {
            let table = ReceivedTable::new(cfg.schema.clone(), self.descriptor.clone())?;
            let sql = replay_received_count_sql(&table, self)?;
            let count = sqlx::query_scalar::<_, i64>(&sql)
                .fetch_one(&mut **tx)
                .await?;
            Ok(Some(count))
        })
    }
}

fn replay_received_update_sql(table: &ReceivedTable, replay: &Replay) -> Result<String> {
    let max_rows = replay.max_rows_or_error()?;
    let filter = replay_received_filter_sql(replay)?;
    // Default redrive preserves attempts and error history for triage; an
    // explicit `clear_history()` resets the row to a clean slate with a full
    // retry budget.
    // A clean slate must also drop the denormalized failure columns, or a
    // history-erased row would still be matched by a `since`/`kind` filter that
    // reads them.
    let history_reset = if replay.clear_history {
        ",
            attempts = 0,
            errors = '[]'::jsonb,
            last_failed_at = NULL,
            last_failure_kind = NULL"
    } else {
        ""
    };
    Ok(format!(
        "WITH candidates AS (
         SELECT message_id FROM {name}
         WHERE {filter}
         ORDER BY {failure_order}
         LIMIT {max_rows}
        )
        UPDATE {name}
        SET status = {pending},
            next_attempt_at = NULL,
            processed_at = NULL{history_reset}
        WHERE message_id IN (SELECT message_id FROM candidates)",
        name = table.qualified_name(),
        filter = filter,
        failure_order = received_failure_order_sql(),
        max_rows = max_rows,
        pending = ReceiveStatus::Pending.sql_literal(),
        history_reset = history_reset,
    ))
}

fn replay_received_count_sql(table: &ReceivedTable, replay: &Replay) -> Result<String> {
    let max_rows = replay.max_rows_or_error()?;
    let filter = replay_received_filter_sql(replay)?;
    Ok(format!(
        "SELECT count(*) FROM (
         SELECT message_id FROM {name}
         WHERE {filter}
         ORDER BY {failure_order}
         LIMIT {max_rows}
         ) candidates",
        name = table.qualified_name(),
        filter = filter,
        failure_order = received_failure_order_sql(),
        max_rows = max_rows,
    ))
}

pub(crate) fn replay_received_filter_sql(replay: &Replay) -> Result<String> {
    // Redrive targets terminal rows. With retry backoff in place a receive row
    // reaches `Failed` only after its retry budget is exhausted; non-exhausted
    // failures stay `Retryable` with a scheduled `next_attempt_at` and recover on
    // their own, so they must not be replayed. Replay moves the exhausted rows
    // back to `Pending` for one more reprocessing pass, preserving attempts and
    // error history for triage.
    let mut filter = format!("status = {}", ReceiveStatus::Failed.sql_literal());
    if let Some(occurred_after) = replay.occurred_after {
        let formatted = occurred_after
            .format(&Rfc3339)
            .map_err(|err| Error::InvalidReplay {
                version: replay.version,
                message: err.to_string(),
            })?;
        filter.push_str(" AND ");
        filter.push_str(latest_failure_time_sql());
        filter.push_str(" >= ");
        filter.push_str(&sql_string_literal(&formatted));
        filter.push_str("::timestamptz");
    }
    if let Some(kind) = replay.failure_kind {
        filter.push_str(&latest_failure_kind_clause(kind));
    }
    Ok(filter)
}

/// Runs a received-row replay at runtime instead of as a migration changeset.
///
/// This is the same guarded statement [`Replay`] applies through the change
/// engine — terminal rows only, history preserved unless `clear_history` is set
/// — executed directly so an admin route can redrive a DLQ without writing to
/// the changelog. Operational redrive is not a schema change and must not
/// consume a changeset version.
///
/// `replay` must carry `max_rows`; an unbounded operational redrive would let a
/// single request re-enqueue an entire DLQ.
pub async fn redrive_received(pool: &PgPool, cfg: &ResolvedConfig, replay: &Replay) -> Result<u64> {
    let table = ReceivedTable::new(cfg.schema.clone(), replay.descriptor.clone())?;
    let sql = replay_received_update_sql(&table, replay)?;
    let result = sqlx::query(&sql).execute(pool).await?;
    Ok(result.rows_affected())
}
