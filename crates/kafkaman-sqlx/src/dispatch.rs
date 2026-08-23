//! One dispatch cycle: claim a received row, run its handler, and record what
//! happened.
//!
//! The whole cycle is one transaction so the handler's writes, the cache apply,
//! and the row's status change commit or roll back together. A handler failure
//! is unwound to a savepoint rather than aborting the transaction, so the
//! failure record itself still lands.

use kafkaman_core::{MarkOutcome, ReceivedMeta, ReceivedRow};
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;

use crate::dispatch_cache::{upsert_cache_from_received, CacheApplyOutcome};
use crate::dispatch_failure::{
    create_dispatch_handler_savepoint, dispatch_failure_stats, handler_failure_disposition,
    received_failure_disposition, rollback_handler_and_record_received_failure, FailureDisposition,
};
use crate::received_rows::{
    claim_received_row, mark_received_processed, record_received_failure, ReceivedFailureRecord,
};
use crate::{DispatchStats, Error, MessageRouter, ReceivedTable, Result};

#[cfg(feature = "internal-hooks")]
use crate::dispatch_failure::run_dispatch_hook;
#[cfg(feature = "internal-hooks")]
use crate::hooks::{DispatchHookSlot, DispatchHooks};

/// Hooks the dispatcher may call, present only under `internal-hooks`.
///
/// A zero-sized stand-in when the feature is off, so `dispatch_once_inner` has
/// one signature rather than two and the call sites keep one shape. Threading a
/// `#[cfg]`-gated *parameter* instead is what forced the two-branch wrapper this
/// replaces.
#[cfg(feature = "internal-hooks")]
type Hooks<'a> = Option<&'a DispatchHooks>;
#[cfg(not(feature = "internal-hooks"))]
type Hooks<'a> = std::marker::PhantomData<&'a ()>;

#[cfg(feature = "internal-hooks")]
const NO_HOOKS: Hooks<'static> = None;
#[cfg(not(feature = "internal-hooks"))]
const NO_HOOKS: Hooks<'static> = std::marker::PhantomData;

/// Dispatch at most one due received row.
///
/// One row per call, not a batch: the caller loops, and a per-row transaction
/// keeps a single bad handler from rolling back everything else in flight.
pub async fn dispatch_once(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
) -> Result<DispatchStats> {
    dispatch_once_inner(pool, table, router, due_at, NO_HOOKS).await
}

#[cfg(feature = "internal-hooks")]
#[doc(hidden)]
pub async fn dispatch_once_with_observer(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    hooks: &DispatchHooks,
) -> Result<DispatchStats> {
    dispatch_once_inner(pool, table, router, due_at, Some(hooks)).await
}

async fn dispatch_once_inner(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    hooks: Hooks<'_>,
) -> Result<DispatchStats> {
    let mut tx = pool.begin().await?;
    let Some(row) = claim_received_row(&mut tx, table, due_at).await? else {
        tx.commit().await?;
        return Ok(DispatchStats::default());
    };

    let Some(handler) = router.handler_for(&row.message_type) else {
        // No handler is retryable, not terminal: the usual cause is a replica
        // that has not been deployed yet, and the next one to claim the row may
        // well have it.
        return record_failure_in_claim_tx(
            tx,
            table,
            &row,
            ReceivedFailureRecord::of(
                &row,
                FailureDisposition::retryable(kafkaman_core::ReceivedFailureKind::MissingHandler),
                Error::MissingHandler(row.message_type.clone()).to_string(),
                due_at,
            ),
            hooks,
        )
        .await;
    };

    create_dispatch_handler_savepoint(&mut tx).await?;
    let meta = ReceivedMeta::from(&row);
    let handler_result = handler.handle(&mut tx, meta, row.payload.clone()).await;

    match handler_result {
        Ok(()) => match apply_successful_dispatch(&mut tx, table, &row, due_at).await {
            Ok(outcome) => {
                tx.commit().await?;
                Ok(DispatchStats {
                    claimed: 1,
                    // A stale or missing mark means another worker owns the row
                    // now; the work is not lost, but this cycle did not do it.
                    processed: usize::from(outcome == MarkOutcome::Updated),
                    failed: 0,
                })
            }
            // The handler succeeded but kafkaman's own bookkeeping did not, so
            // the handler's writes must be unwound with everything else.
            Err(err) => {
                let disposition = received_failure_disposition(&err);
                unwind_and_record(
                    pool,
                    tx,
                    table,
                    &row,
                    ReceivedFailureRecord::of(&row, disposition, err.to_string(), due_at),
                    hooks,
                )
                .await
            }
        },
        Err(err) => {
            let disposition = handler_failure_disposition(&err);
            unwind_and_record(
                pool,
                tx,
                table,
                &row,
                ReceivedFailureRecord::of(&row, disposition, err.to_string(), due_at),
                hooks,
            )
            .await
        }
    }
}

/// Record a failure in the transaction that claimed the row.
///
/// Used where nothing has run yet, so there is nothing to unwind.
async fn record_failure_in_claim_tx(
    mut tx: Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    failure: ReceivedFailureRecord,
    hooks: Hooks<'_>,
) -> Result<DispatchStats> {
    #[cfg(feature = "internal-hooks")]
    run_dispatch_hook(
        hooks,
        DispatchHookSlot::BeforeRecordFailure,
        row,
        failure.disposition.kind,
        failure.message.clone(),
    )
    .await?;
    #[cfg(not(feature = "internal-hooks"))]
    let _ = (hooks, row);

    let outcome = record_received_failure(&mut tx, table, failure).await?;
    tx.commit().await?;
    Ok(dispatch_failure_stats(outcome))
}

/// Roll the handler's writes back to the savepoint, then record the failure.
///
/// Both failing paths — the handler's own error and a failure applying its
/// result — need exactly this, and they used to spell it out twice.
async fn unwind_and_record(
    pool: &PgPool,
    tx: Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    failure: ReceivedFailureRecord,
    hooks: Hooks<'_>,
) -> Result<DispatchStats> {
    #[cfg(feature = "internal-hooks")]
    for slot in [
        DispatchHookSlot::BeforeFailureRollback,
        DispatchHookSlot::BeforeRecordFailure,
    ] {
        run_dispatch_hook(
            hooks,
            slot,
            row,
            failure.disposition.kind,
            failure.message.clone(),
        )
        .await?;
    }
    #[cfg(not(feature = "internal-hooks"))]
    let _ = (hooks, row);

    let outcome = rollback_handler_and_record_received_failure(tx, pool, table, failure).await?;
    Ok(dispatch_failure_stats(outcome))
}

/// Apply the row to the cache, then mark it processed.
///
/// In that order, and in one transaction: a row marked processed before its
/// cache apply committed would never be retried, so the state would be lost.
async fn apply_successful_dispatch(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    processed_at: OffsetDateTime,
) -> Result<MarkOutcome> {
    let outcome = upsert_cache_from_received(tx, table, row).await?;
    if outcome == CacheApplyOutcome::Ignored {
        tracing::debug!(
            message_id = %row.message_id,
            source_offset = row.source_offset,
            "cache apply ignored: record is not newer than applied state"
        );
    }
    mark_received_processed(tx, table, row.message_id, processed_at).await
}
