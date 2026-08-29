//! Classifying a dispatch failure, and recording it without losing the row.

use kafkaman_core::{
    FailureStage, InstrumentDb, MarkOutcome, ProblemType as _, ReceivedFailureKind,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::received_rows::{record_received_failure, ReceivedFailureRecord};
use crate::{DispatchStats, Error, ReceivedTable, Result};

#[cfg(feature = "internal-hooks")]
use kafkaman_core::ReceivedRow;

#[cfg(feature = "internal-hooks")]
use crate::hooks::{DispatchFailureEvent, DispatchHookSlot, DispatchHooks};

/// How a dispatch failure should be recorded: which class it belongs to, which
/// frame raised it, and whether another attempt could ever change the outcome.
///
/// The three are independent, which is why they are three fields.
///
/// `kind` is the *taxonomy* — what kind of thing went wrong — and is a function
/// of the error alone. `stage` is the *blame* — whose code raised it — and is a
/// function of the call site alone. They used to be one value, and the value
/// answered whichever question was asked last: a database error returned by a
/// handler recorded `handler`, erasing the fact that it was infrastructure, and
/// an operator filtering the dead-letter queue for infrastructure failures found
/// none of them. See the taxonomy/blame decision.
///
/// `terminal` is independent of both. `Infrastructure` covers a transient
/// database blip (retry, and it probably succeeds) and a cache origin mismatch
/// (retry, and it fails identically forever), so the retry decision cannot be
/// read off the class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FailureDisposition {
    pub(crate) kind: ReceivedFailureKind,
    /// Which part of the dispatch raised it.
    pub(crate) stage: FailureStage,
    /// `true` when the failure is a property of the stored row rather than of
    /// the environment, so the retry budget is spent for nothing and the
    /// operator signal is delayed by exactly that budget.
    pub(crate) terminal: bool,
}

/// Classify a dispatch failure.
///
/// One function, where there were two. The pair existed to say that an error
/// which came out of the handler frame was a `Handler` failure whatever it
/// actually was — which is the blame axis overwriting the taxonomy axis, and is
/// now recorded as `stage` instead. With that removed as an input there was
/// nothing left to distinguish them.
///
/// The class comes from the error's own `ProblemType`, through the single
/// coarsening in `kafkaman-core`, so the value stored on the row and the value
/// exported as `error.type` cannot disagree.
pub(crate) fn failure_disposition(error: &Error, stage: FailureStage) -> FailureDisposition {
    FailureDisposition {
        kind: error.failure_kind(),
        stage,
        terminal: is_terminal(error),
    }
}

/// Whether a second attempt could ever reach a different answer.
///
/// A property of the error, not of who raised it. Each of these is deterministic
/// *by construction* against the stored row: re-reading the same row runs the
/// same predicate over the same values and reaches the same conclusion, so the
/// whole attempt budget is spent for nothing and the operator signal is delayed
/// by exactly that long.
///
/// A cache origin mismatch means the entity's records moved topic or partition,
/// so the guard's predicate can never be true again and it needs an operator
/// re-bootstrap. A missing or invalid entity key means the row carries no
/// resolvable convergence identity, which re-reading it cannot supply.
///
/// Deliberately **not** `HandlerPanicked`, even though a panic is usually
/// deterministic. Unlike the three above, a panic is not deterministic by
/// construction — an `unwrap` on a value a concurrent writer had not committed
/// yet succeeds on the retry, and that is the case worth surviving.
///
/// Decision point 4 of the dispatch-error decision also calls for a
/// consecutive-regression breaker mirroring `ConsecutiveSkipLimitExceeded`. That
/// is deliberately not implemented: a breaker halts the pipeline, and one
/// entity's repartition should not stop dispatch for every other entity. Failing
/// the affected row immediately achieves the breaker's actual purpose — never
/// grinding on a broken invariant — with a blast radius of one row.
fn is_terminal(error: &Error) -> bool {
    matches!(
        error,
        Error::CacheOriginMismatch { .. }
            | Error::MissingEntityKey { .. }
            | Error::InvalidEntityKey { .. }
    )
}

/// Undo whatever the handler wrote, then record the failure.
///
/// Rolling back to the savepoint keeps the failure record in the same
/// transaction as the claim, which is what makes attempts and error history
/// consistent with the row's status. If the savepoint rollback itself fails the
/// transaction is unusable, so the whole thing is abandoned and the failure is
/// recorded on a fresh connection — losing the atomicity, but not the record.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub(crate) async fn rollback_handler_and_record_received_failure(
    mut tx: Transaction<'_, Postgres>,
    pool: &PgPool,
    table: &ReceivedTable,
    failure: ReceivedFailureRecord,
) -> Result<(MarkOutcome, FailureRecordPath)> {
    match rollback_to_dispatch_handler_savepoint(&mut tx, table).await {
        Ok(()) => {
            let outcome = record_received_failure(&mut tx, table, failure).await?;
            tx.commit()
                .instrument_db(kafkaman_core::db_span!(
                    "COMMIT",
                    table.qualified_name(),
                    "commit received failure transaction",
                ))
                .await?;
            Ok((outcome, FailureRecordPath::Savepoint))
        }
        Err(error) => {
            // Reported rather than dropped. This is the condition that costs the
            // failure record its atomicity with the claim, and until it was
            // logged it appeared in no signal at all: the `is_ok()` this replaces
            // discarded the only evidence that the fallback below was taken.
            tracing::warn!(
                error = %error,
                "dispatch handler savepoint rollback failed; abandoning the claim \
                 transaction and recording the failure on a fresh connection"
            );
            // The transaction is already broken, so its own rollback result tells
            // us nothing we can act on.
            let _ = tx.rollback().await;
            let mut conn = pool
                .acquire()
                .instrument_db(kafkaman_core::db_span!(
                    "ACQUIRE",
                    table.qualified_name(),
                    "acquire fallback failure connection",
                ))
                .await?;
            let outcome = record_received_failure(&mut conn, table, failure).await?;
            Ok((outcome, FailureRecordPath::FallbackConnection))
        }
    }
}

/// Which transaction a failure record landed in.
///
/// Exported onto `kafkaman.dispatch` as `kafkaman.failure.recorded_via`. Until
/// it existed, the atomic path and the atomicity-losing fallback produced spans
/// an operator could not tell apart: identical name, identical attributes,
/// identical status. The difference is whether the row's attempt count and the
/// claim it belongs to can still disagree after a crash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureRecordPath {
    /// Recorded in the claim transaction itself, before any handler ran.
    ClaimTransaction,
    /// Rolled back to the handler savepoint and recorded in the claim
    /// transaction. The normal path, and the atomic one.
    Savepoint,
    /// The savepoint rollback failed, so the claim transaction was abandoned and
    /// the record written on a fresh connection. The record survives; its
    /// atomicity with the claim does not.
    FallbackConnection,
}

impl FailureRecordPath {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ClaimTransaction => "claim_transaction",
            Self::Savepoint => "savepoint",
            Self::FallbackConnection => "fallback_connection",
        }
    }
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub(crate) async fn create_dispatch_handler_savepoint(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
) -> Result<()> {
    sqlx::query("SAVEPOINT kafkaman_dispatch_handler")
        .execute(&mut **tx)
        .instrument_db(kafkaman_core::db_span!(
            "SAVEPOINT",
            table.qualified_name(),
            "open dispatch handler savepoint",
        ))
        .await?;
    Ok(())
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn rollback_to_dispatch_handler_savepoint(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
) -> Result<()> {
    sqlx::query("ROLLBACK TO SAVEPOINT kafkaman_dispatch_handler")
        .execute(&mut **tx)
        .instrument_db(kafkaman_core::db_span!(
            "ROLLBACK",
            table.qualified_name(),
            "roll back dispatch handler savepoint",
        ))
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
        panicked: 0,
        panicked_message_id: None,
    }
}

/// Run the observer installed in `slot`, if the caller supplied any hooks.
#[cfg(feature = "internal-hooks")]
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
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
