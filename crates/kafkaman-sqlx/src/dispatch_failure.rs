//! Classifying a dispatch failure, and recording it without losing the row.

use kafkaman_core::{MarkOutcome, ReceivedFailureKind};
use sqlx::{PgPool, Postgres, Transaction};

use crate::received_rows::{record_received_failure, ReceivedFailureRecord};
use crate::{DispatchStats, Error, ReceivedTable, Result};

#[cfg(feature = "internal-hooks")]
use kafkaman_core::ReceivedRow;

#[cfg(feature = "internal-hooks")]
use crate::hooks::{DispatchFailureEvent, DispatchHookSlot, DispatchHooks};

/// How a dispatch failure should be recorded: which class it belongs to, and
/// whether another attempt could ever change the outcome.
///
/// The two are independent. `Infrastructure` covers both a transient database
/// blip (retry, and it probably succeeds) and a cache origin mismatch (retry, and
/// it fails identically forever), so the retry decision cannot be read off the
/// class alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FailureDisposition {
    pub(crate) kind: ReceivedFailureKind,
    /// `true` when the failure is a property of the stored row rather than of the
    /// environment, so the retry budget is spent for nothing and the operator
    /// signal is delayed by exactly that budget.
    pub(crate) terminal: bool,
}

impl FailureDisposition {
    pub(crate) fn retryable(kind: ReceivedFailureKind) -> Self {
        Self {
            kind,
            terminal: false,
        }
    }

    fn terminal(kind: ReceivedFailureKind) -> Self {
        Self {
            kind,
            terminal: true,
        }
    }
}

/// Classify a failure raised by kafkaman itself, around the handler.
pub(crate) fn received_failure_disposition(error: &Error) -> FailureDisposition {
    match error {
        Error::MissingHandler(_) => {
            FailureDisposition::retryable(ReceivedFailureKind::MissingHandler)
        }
        Error::Serde(_) => FailureDisposition::retryable(ReceivedFailureKind::InvalidPayload),
        Error::Sqlx(_) => FailureDisposition::retryable(ReceivedFailureKind::Infrastructure),
        Error::Handler(_) => FailureDisposition::retryable(ReceivedFailureKind::Handler),

        // Deterministic by construction, and listed explicitly so a later edit
        // cannot quietly reclassify them by widening the catch-all below.
        //
        // A cache origin mismatch means the entity's records moved topic or
        // partition, so the guard's predicate can never be true again no matter
        // how often it is retried; it needs an operator re-bootstrap, and the
        // stored detail says so. A missing entity key means the row carries no
        // resolvable convergence identity, which re-reading the same row cannot
        // supply. Retrying either burns the whole attempt budget and delays the
        // signal by exactly that long.
        //
        // Decision point 4 also calls for a consecutive-regression breaker
        // mirroring `ConsecutiveSkipLimitExceeded`. That is deliberately NOT
        // implemented here: a breaker halts the pipeline, and one entity's
        // repartition should not stop dispatch for every other entity. Failing
        // the affected row immediately achieves the breaker's actual purpose —
        // never grinding on a broken invariant — with a blast radius of one row.
        Error::CacheOriginMismatch { .. } | Error::MissingEntityKey { .. } => {
            FailureDisposition::terminal(ReceivedFailureKind::Infrastructure)
        }

        _ => FailureDisposition::retryable(ReceivedFailureKind::Infrastructure),
    }
}

/// Classify a failure the handler itself returned.
///
/// Everything defaults to `Handler`, because that is whose code failed; a
/// deserialization error is called out separately since it says the stored
/// payload does not match the handler's type, which is a different repair.
pub(crate) fn handler_failure_disposition(error: &Error) -> FailureDisposition {
    match error {
        Error::Serde(_) => FailureDisposition::retryable(ReceivedFailureKind::InvalidPayload),
        _ => FailureDisposition::retryable(ReceivedFailureKind::Handler),
    }
}

/// Undo whatever the handler wrote, then record the failure.
///
/// Rolling back to the savepoint keeps the failure record in the same
/// transaction as the claim, which is what makes attempts and error history
/// consistent with the row's status. If the savepoint rollback itself fails the
/// transaction is unusable, so the whole thing is abandoned and the failure is
/// recorded on a fresh connection — losing the atomicity, but not the record.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub(crate) async fn rollback_handler_and_record_received_failure(
    mut tx: Transaction<'_, Postgres>,
    pool: &PgPool,
    table: &ReceivedTable,
    failure: ReceivedFailureRecord,
) -> Result<MarkOutcome> {
    if rollback_to_dispatch_handler_savepoint(&mut tx)
        .await
        .is_ok()
    {
        let outcome = record_received_failure(&mut tx, table, failure).await?;
        tx.commit().await?;
        return Ok(outcome);
    }

    // The transaction is already broken; its rollback result tells us nothing
    // we can act on, and the failure below is what actually needs reporting.
    let _ = tx.rollback().await;
    let mut conn = pool.acquire().await?;
    record_received_failure(&mut conn, table, failure).await
}

#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub(crate) async fn create_dispatch_handler_savepoint(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query("SAVEPOINT kafkaman_dispatch_handler")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
async fn rollback_to_dispatch_handler_savepoint(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query("ROLLBACK TO SAVEPOINT kafkaman_dispatch_handler")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// A failed dispatch claimed a row and processed nothing; it counts as `failed`
/// only when the failure was actually recorded against the row.
pub(crate) fn dispatch_failure_stats(outcome: MarkOutcome) -> DispatchStats {
    DispatchStats {
        claimed: 1,
        processed: 0,
        failed: usize::from(outcome == MarkOutcome::Updated),
    }
}

/// Run the observer installed in `slot`, if the caller supplied any hooks.
#[cfg(feature = "internal-hooks")]
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub(crate) async fn run_dispatch_hook(
    hooks: Option<&DispatchHooks>,
    slot: DispatchHookSlot,
    row: &ReceivedRow,
    kind: ReceivedFailureKind,
    message: String,
) -> Result<()> {
    if let Some(hooks) = hooks {
        hooks
            .run(
                slot,
                DispatchFailureEvent {
                    message_id: row.message_id,
                    idempotency_key: row.idempotency_key,
                    message_type: row.message_type.clone(),
                    kind,
                    message,
                },
            )
            .await?;
    }
    Ok(())
}
