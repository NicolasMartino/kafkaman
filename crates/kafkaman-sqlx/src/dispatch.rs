//! One dispatch cycle: claim a received row, converge the cache, run its
//! handlers, and record what happened.
//!
//! # Order
//!
//! 1. Claim the row.
//! 2. Look the handlers up. No handler at *either* position is a retryable
//!    failure and short-circuits here, before the cache advances.
//! 3. Open the savepoint.
//! 4. Run the pre-upsert handler, if one is registered.
//! 5. Apply the cache upsert. Nothing can suppress this.
//! 6. Run the post-upsert handler, unless the pre-upsert hook asked to skip it
//!    or the cache apply was `Ignored`.
//! 7. Mark the row processed.
//!
//! The whole cycle is one transaction so the handlers' writes, the cache apply,
//! and the row's status change commit or roll back together. A handler failure
//! is unwound to a savepoint rather than aborting the transaction, so the
//! failure record itself still lands.
//!
//! # Why the savepoint opens before the upsert
//!
//! This is the one part of the ordering that is not mechanically safe. If the
//! savepoint opened just before the post-upsert handler, a failing handler would
//! roll back its own writes and leave the *cache row advanced*, with the
//! received row parked `Retryable`. The retry would then find the record at or
//! behind the applied offset, get `Ignored`, and skip the handler — permanently.
//! Covering the upsert makes the retry see the same state the first attempt did.

use kafkaman_core::{MarkOutcome, ReceivedMeta, ReceivedRow};
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use tracing::Instrument;

use crate::dispatch_cache::{upsert_cache_from_received, CacheApplyOutcome};
use crate::dispatch_failure::{
    create_dispatch_handler_savepoint, dispatch_failure_stats, handler_failure_disposition,
    received_failure_disposition, rollback_handler_and_record_received_failure, FailureDisposition,
};
use crate::received_rows::{
    claim_received_row, mark_received_processed, record_received_failure, ReceivedFailureRecord,
};
use crate::router::HandlerFlow;
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

    // The lookup stays ahead of the upsert. A replica that has not been deployed
    // yet must not silently converge a cache it has no code to derive from, so
    // an unregistered type parks its row and the cache does not advance. The
    // row is parked, not dropped, so convergence is deferred rather than broken
    // — and the deferral is the point.
    if !router.routes(&row.message_type) {
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
    }

    create_dispatch_handler_savepoint(&mut tx).await?;

    // Parented from the ingest that stored this row, across the same kind of
    // durable gap the relay crosses on the send side: the row may have been
    // ingested by another process, or before this one started. Without stored
    // context it is a root span, which is what an uningested-under-tracing row
    // should produce.
    let span = tracing::info_span!(
        "kafkaman.dispatch",
        message_type = row.message_type.as_str(),
        messaging.system = "kafka",
        messaging.destination.name = row.source_topic.as_str(),
        messaging.operation.name = "process",
        messaging.message.id = %row.message_id,
    );
    if let Some(trace) = &row.trace {
        kafkaman_core::set_parent(&span, trace);
    }

    match converge_and_dispatch(&mut tx, table, &row, router, due_at)
        .instrument(span)
        .await
    {
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
        Err(failure) => {
            let disposition = failure.disposition();
            unwind_and_record(
                pool,
                tx,
                table,
                &row,
                ReceivedFailureRecord::of(&row, disposition, failure.to_string(), due_at),
                hooks,
            )
            .await
        }
    }
}

/// Where in the cycle a dispatch failed.
///
/// The two are classified differently and must not be conflated: an application
/// handler's error is the application's problem, while a failure in kafkaman's
/// own cache apply or bookkeeping carries cases that are deterministic and
/// therefore terminal — a cache origin mismatch cannot be fixed by retrying.
/// Collapsing both into one `Error` would silently make those retryable and burn
/// the whole attempt budget before reporting them.
enum DispatchFailure {
    /// An application handler returned an error, at either position.
    Handler(Error),
    /// The cache apply or the processed mark failed.
    Bookkeeping(Error),
}

impl DispatchFailure {
    fn disposition(&self) -> FailureDisposition {
        match self {
            Self::Handler(error) => handler_failure_disposition(error),
            Self::Bookkeeping(error) => received_failure_disposition(error),
        }
    }
}

impl std::fmt::Display for DispatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handler(error) | Self::Bookkeeping(error) => write!(f, "{error}"),
        }
    }
}

/// Everything inside the savepoint: both handler positions, the cache upsert
/// between them, and the processed mark.
async fn converge_and_dispatch(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    router: &MessageRouter,
    processed_at: OffsetDateTime,
) -> std::result::Result<MarkOutcome, DispatchFailure> {
    let meta = ReceivedMeta::from(row);

    let flow = match router.before_handler_for(&row.message_type) {
        Some(before) => before
            .handle(&mut *tx, meta.clone(), row.payload.clone())
            .await
            .map_err(DispatchFailure::Handler)?,
        None => HandlerFlow::Continue,
    };

    // Unconditional, and deliberately not reachable from either hook: the cache
    // is the converged current state of every entity on the topic.
    let applied = upsert_cache_from_received(tx, table, row)
        .await
        .map_err(DispatchFailure::Bookkeeping)?;
    log_cache_apply(row, applied);

    // An `Ignored` record is at or behind the entity's applied offset, so it
    // carries no new state; re-deriving from it is wasted work at best and a
    // stale recomputation at worst. `Migrated` did advance the row, so it runs.
    let ignored = applied == CacheApplyOutcome::Ignored;
    if flow == HandlerFlow::Continue && !ignored {
        if let Some(handler) = router.handler_for(&row.message_type) {
            handler
                .handle(&mut *tx, meta, row.payload.clone())
                .await
                .map_err(DispatchFailure::Handler)?;
        }
    }

    mark_received_processed(tx, table, row.message_id, processed_at)
        .await
        .map_err(DispatchFailure::Bookkeeping)
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

/// What the cache apply did, for an operator reading logs.
fn log_cache_apply(row: &ReceivedRow, outcome: CacheApplyOutcome) {
    match outcome {
        CacheApplyOutcome::Ignored => tracing::debug!(
            message_id = %row.message_id,
            source_offset = row.source_offset,
            "cache apply ignored: record is not newer than applied state"
        ),
        // Deliberately `info` rather than `debug`: adopting a new origin resets
        // the offset guard for that entity, which is a thing an operator should
        // be able to find in a log when reconstructing what a rebuild did.
        CacheApplyOutcome::Migrated => tracing::info!(
            message_id = %row.message_id,
            source_topic = %row.source_topic,
            source_partition = row.source_partition,
            source_offset = row.source_offset,
            "cache origin migrated: adopted the declared topic and reset the offset guard"
        ),
        CacheApplyOutcome::Applied => {}
    }
}
