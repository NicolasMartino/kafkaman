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

use kafkaman_core::InstrumentDb;
use std::future::Future;
use std::pin::Pin;

use kafkaman_core::{
    FailureStage, LifecycleSampler, MarkOutcome, ReceivedFailureKind, ReceivedMeta, ReceivedRow,
};
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use tracing::Instrument;

use crate::catch_panic::{catch_handler_panic, HandlerAbort};
use crate::dispatch_cache::{upsert_cache_from_received, CacheApplyOutcome};
use crate::dispatch_failure::{
    create_dispatch_handler_savepoint, dispatch_failure_stats, failure_disposition,
    rollback_handler_and_record_received_failure, FailureDisposition, FailureRecordPath,
};
use crate::received_rows::{
    claim_received_row, mark_received_processed, record_received_failure, ReceivedFailureRecord,
};
use crate::retry_backoff::received_retry_outcome;
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
// Stays in the debug tier: one dispatch poll, work or no work.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn dispatch_once(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
) -> Result<DispatchStats> {
    dispatch_once_inner(pool, table, router, due_at, NO_HOOKS, None).await
}

/// [`dispatch_once`], emitting a per-message success event at the sampled rate.
///
/// # Why the sampler comes in here rather than staying in the loop
///
/// The event belongs *inside* `kafkaman.dispatch`, and that span does not
/// outlive this call. A dispatcher loop that samples its own successes after
/// `dispatch_once` returns emits a line that names a message type and nothing
/// else: no trace id, no span id, no way back to the row it is about. An event
/// about one message that cannot be traced to that message is a line in a log
/// file; one that can is the pivot `sample_success` exists to provide. The relay
/// carries its sampler across the same boundary for the same reason.
///
/// The sampler is borrowed mutably because its count carries across cycles:
/// sampling one in ten successes has to mean one in ten over the stream, not one
/// per call that happens to succeed.
// Stays in the debug tier: one dispatch poll, work or no work.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn dispatch_once_sampled(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    lifecycle: &mut LifecycleSampler,
) -> Result<DispatchStats> {
    dispatch_once_inner(pool, table, router, due_at, NO_HOOKS, Some(lifecycle)).await
}

#[cfg(feature = "internal-hooks")]
#[doc(hidden)]
// Stays in the debug tier: one dispatch poll, work or no work.
// See the span-depth decision for the rule.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn dispatch_once_with_observer(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    hooks: &DispatchHooks,
) -> Result<DispatchStats> {
    dispatch_once_inner(pool, table, router, due_at, Some(hooks), None).await
}

async fn dispatch_once_inner(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    hooks: Hooks<'_>,
    lifecycle: Option<&mut LifecycleSampler>,
) -> Result<DispatchStats> {
    // Debug rather than info, beside `claim_received_row`'s own span: this
    // transaction opens on every cycle whether or not a row is due, so at the
    // default filter it would be most of what an idle service exports.
    let mut tx = pool
        .begin()
        .instrument_db(kafkaman_core::db_poll_span!(
            "BEGIN",
            table.qualified_name(),
            "open received claim transaction"
        ))
        .await?;
    let Some(row) = claim_received_row(&mut tx, table, due_at).await? else {
        tx.commit()
            .instrument_db(kafkaman_core::db_poll_span!(
                "COMMIT",
                table.qualified_name(),
                "commit empty received claim transaction"
            ))
            .await?;
        return Ok(DispatchStats::default());
    };

    // Parented from the ingest that stored this row, across the same kind of
    // durable gap the relay crosses on the send side: the row may have been
    // ingested by another process, or before this one started. Without stored
    // context it is a root span, which is what an uningested-under-tracing row
    // should produce.
    let span = tracing::info_span!(
        "kafkaman.dispatch",
        "otel.kind" = "consumer",
        "otel.status_code" = tracing::field::Empty,
        "otel.status_description" = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        "kafkaman.failure.kind" = tracing::field::Empty,
        "kafkaman.failure.type" = tracing::field::Empty,
        "kafkaman.failure.stage" = tracing::field::Empty,
        "kafkaman.failure.recorded_via" = tracing::field::Empty,
        "kafkaman.retry.attempt" = tracing::field::Empty,
        "kafkaman.retry.exhausted" = tracing::field::Empty,
        message_type = row.message_type.as_str(),
        messaging.system = "kafka",
        messaging.destination.name = row.source_topic.as_str(),
        messaging.operation.name = "process",
        messaging.message.id = %row.message_id,
    );
    if let Some(trace) = &row.trace {
        kafkaman_core::set_parent(&span, trace);
    }

    // The span opens the moment there is a row to describe, and covers
    // everything done to it — not just the handler call.
    //
    // Covering only the handler is the tempting version and it loses the cases
    // an operator is actually looking for. A row whose message type has no
    // registered handler never reaches a handler, so it produced no span at all:
    // the most common deployment-order failure there is, and it was invisible in
    // the trace. A handler that fails does its failure accounting — savepoint
    // rollback, failure record, retry scheduling — after the handler returns,
    // which is exactly the part worth timing when a dispatcher is slow.
    let result = dispatch_claimed_row(
        pool, table, router, due_at, hooks, lifecycle, &span, tx, row,
    )
    .instrument(span.clone())
    .await;
    // A hard dispatcher error means the cycle did not even finish recording the
    // row-level failure. Row-level receive failures are marked inside
    // `dispatch_claimed_row`, after the failure record has landed, so APM can
    // separate successful and failed consumer transactions without counting
    // abandoned bookkeeping attempts as handled message failures.
    if let Err(err) = &result {
        kafkaman_core::record_exception(&span, err);
    }
    result
}

/// Everything that happens to a row once it is claimed.
///
/// Split out so [`dispatch_once_inner`] has one `.instrument` call covering all
/// of it, rather than a span that each branch has to remember to enter.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn dispatch_claimed_row(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
    hooks: Hooks<'_>,
    lifecycle: Option<&mut LifecycleSampler>,
    dispatch_span: &tracing::Span,
    mut tx: Transaction<'_, Postgres>,
    row: ReceivedRow,
) -> Result<DispatchStats> {
    // The lookup stays ahead of the upsert. A replica that has not been deployed
    // yet must not silently converge a cache it has no code to derive from, so
    // an unregistered type parks its row and the cache does not advance. The
    // row is parked, not dropped, so convergence is deferred rather than broken
    // — and the deferral is the point.
    if !router.routes(&row.message_type) {
        // Kept as the error rather than rendered to a `String` on the way past:
        // the span needs to ask it what class of problem it is, and a `String`
        // cannot answer that. It is also what the disposition is derived from,
        // so the class cannot be stated here and contradicted there.
        let error = Error::MissingHandler(row.message_type.clone());
        let disposition = failure_disposition(&error, FailureStage::Routing);
        let (stats, recorded_via) = record_failure_in_claim_tx(
            tx,
            table,
            &row,
            ReceivedFailureRecord::of(&row, disposition, error.to_string(), due_at),
            hooks,
        )
        .await?;
        record_retry_outcome(dispatch_span, table, &row, disposition);
        record_recorded_dispatch_failure(
            dispatch_span,
            stats.failed,
            disposition.kind,
            FailureStage::Routing,
            recorded_via,
            &error,
        );
        return Ok(stats);
    }

    create_dispatch_handler_savepoint(&mut tx, table).await?;

    match converge_and_dispatch(&mut tx, table, &row, router, due_at).await {
        Ok(outcome) => {
            tx.commit()
                .instrument_db(kafkaman_core::db_span!(
                    "COMMIT",
                    table.qualified_name(),
                    "commit dispatch transaction",
                ))
                .await?;
            // A stale or missing mark means another worker owns the row now; the
            // work is not lost, but this cycle did not do it.
            let processed = usize::from(outcome == MarkOutcome::Updated);
            if processed > 0 {
                emit_success_event(table, &row, lifecycle);
            }
            Ok(DispatchStats {
                claimed: 1,
                processed,
                failed: 0,
                panicked: 0,
                panicked_message_id: None,
            })
        }
        Err(failure) => {
            let disposition = failure.disposition();
            let panicked = usize::from(failure.is_handler_panic());
            let (mut stats, recorded_via) = unwind_and_record(
                pool,
                tx,
                table,
                &row,
                ReceivedFailureRecord::of(&row, disposition, failure.to_string(), due_at),
                hooks,
            )
            .await?;
            record_retry_outcome(dispatch_span, table, &row, disposition);
            record_recorded_dispatch_failure(
                dispatch_span,
                stats.failed,
                disposition.kind,
                disposition.stage,
                recorded_via,
                &failure,
            );
            stats.panicked = panicked;
            // Named here rather than deeper, so `dispatch_failure_stats` stays
            // panic-agnostic and the identity is set on the one path that knows
            // a panic happened.
            stats.panicked_message_id = (panicked > 0).then_some(row.message_id);
            Ok(stats)
        }
    }
}

/// A span for one application handler call.
///
/// Until this existed, a slow handler was indistinguishable from slow kafkaman
/// bookkeeping: both showed as unattributed time inside `kafkaman.dispatch`, and
/// the handler is the one of the two an operator can do anything about.
///
/// `otel.kind` is `internal` rather than `consumer`, deliberately. The consuming
/// happened at ingest; this span is the application's own work, running inside a
/// message the process already owns.
///
/// `handler.position` distinguishes the pre-upsert hook from the post-upsert
/// handler. Two values, so it stays a bounded grouping key — and the distinction
/// matters, because only the pre-upsert position can skip the one after it.
///
/// Errors are recorded here and on `kafkaman.dispatch`, deliberately. This span
/// says the handler itself failed and carries handler-specific fields; the
/// enclosing dispatch transaction carries the receive attempt outcome so APM can
/// split messaging transactions by success and failure.
///
/// `handler.outcome` is recorded only when the handler *panicked*, which is what
/// makes it useful: `handler.outcome: panicked` selects exactly the panics, and
/// nothing else has to be excluded. A returned error is already visible as
/// `otel.status_code`, and the two are stored under one
/// `ReceivedFailureKind::Handler` on purpose — see `handler_failure_disposition`.
fn handler_span(row: &ReceivedRow, position: &'static str) -> tracing::Span {
    tracing::info_span!(
        "kafkaman.handler",
        "otel.kind" = "internal",
        "otel.status_code" = tracing::field::Empty,
        "otel.status_description" = tracing::field::Empty,
        "error.type" = tracing::field::Empty,
        "kafkaman.failure.kind" = tracing::field::Empty,
        "kafkaman.failure.type" = tracing::field::Empty,
        "kafkaman.failure.stage" = tracing::field::Empty,
        message_type = row.message_type.as_str(),
        "handler.position" = position,
        "handler.outcome" = tracing::field::Empty,
    )
}

/// Report which attempt a failure is, and whether it was the last one.
///
/// Without these two a trace can say a message failed but not whether *this* is
/// the attempt that dead-lettered it — which is the difference between a retry
/// an operator can ignore and a message that has stopped moving.
fn record_retry_outcome(
    span: &tracing::Span,
    table: &ReceivedTable,
    row: &ReceivedRow,
    disposition: FailureDisposition,
) {
    let (attempt, exhausted) = received_retry_outcome(table, row.attempts, disposition);
    span.record("kafkaman.retry.attempt", u64::from(attempt));
    span.record("kafkaman.retry.exhausted", exhausted);
}

/// The failure taxonomy as the *row* records it.
///
/// These three mirror what the DLQ row and the admin API say, so they stay on
/// [`ReceivedFailureKind`]'s four persisted values. `error.type` deliberately
/// does not: it is what an APM backend groups by, and there the distinction
/// between a handler that returned an error and one that unwound is worth
/// having. See [`kafkaman_core::problem`].
fn record_failure_attrs(span: &tracing::Span, kind: ReceivedFailureKind, stage: FailureStage) {
    span.record("kafkaman.failure.kind", kind.discriminant());
    span.record("kafkaman.failure.type", kind.problem_type());
    span.record("kafkaman.failure.stage", stage.as_str());
}

/// Mark `span` failed and classify it, without reporting an error.
///
/// For the span *enclosing* one that has already reported the same failure. A
/// backend derives one error per exception event, so a handler failure reported
/// on both `kafkaman.handler` and `kafkaman.dispatch` would arrive as two
/// errors that no operator can tell apart from two failures.
fn record_failure_status<E>(
    span: &tracing::Span,
    kind: ReceivedFailureKind,
    stage: FailureStage,
    error: &E,
) where
    E: kafkaman_core::ProblemType + std::fmt::Display + ?Sized,
{
    record_failure_attrs(span, kind, stage);
    span.record("error.type", error.problem_type());
    kafkaman_core::record_error(span, &error);
}

/// Mark `span` failed, classify it, and report the failure as an error.
///
/// For the innermost span that owns the failure: `kafkaman.handler` when a
/// handler failed, `kafkaman.dispatch` when the failure was in routing or in
/// kafkaman's own bookkeeping and no narrower span saw it.
fn record_failure_exception<E>(
    span: &tracing::Span,
    kind: ReceivedFailureKind,
    stage: FailureStage,
    error: &E,
) where
    E: kafkaman_core::ProblemType + std::fmt::Display + ?Sized,
{
    record_failure_attrs(span, kind, stage);
    kafkaman_core::record_exception(span, error);
}

/// Record a durably-parked failure on the enclosing `kafkaman.dispatch` span.
///
/// `failed == 0` means the row was not actually parked — a stale claim, or a row
/// another worker owns now — and there is no handled failure to report.
fn record_recorded_dispatch_failure<E>(
    span: &tracing::Span,
    failed: usize,
    kind: ReceivedFailureKind,
    stage: FailureStage,
    recorded_via: FailureRecordPath,
    error: &E,
) where
    E: kafkaman_core::ProblemType + std::fmt::Display + ?Sized,
{
    if failed == 0 {
        return;
    }
    // Which transaction the record landed in, and so whether the row's attempt
    // count is still atomic with the claim that produced it.
    span.record("kafkaman.failure.recorded_via", recorded_via.as_str());
    match stage {
        // `run_handler` already reported this one on `kafkaman.handler`, which is
        // the narrower and more useful place for it.
        FailureStage::Handler => record_failure_status(span, kind, stage, error),
        FailureStage::Routing | FailureStage::Bookkeeping => {
            record_failure_exception(span, kind, stage, error);
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
    /// One classifier for both variants, differing only in the stage they pass.
    ///
    /// It used to be two, and which one ran decided the stored class: an error
    /// out of the handler frame became `Handler` whatever it was. The frame is
    /// now recorded as the stage instead, so the class is the error's alone.
    fn disposition(&self) -> FailureDisposition {
        match self {
            Self::Handler(error) => failure_disposition(error, FailureStage::Handler),
            Self::Bookkeeping(error) => failure_disposition(error, FailureStage::Bookkeeping),
        }
    }

    fn is_handler_panic(&self) -> bool {
        matches!(self, Self::Handler(Error::HandlerPanicked(_)))
    }
}

impl std::fmt::Display for DispatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handler(error) | Self::Bookkeeping(error) => write!(f, "{error}"),
        }
    }
}

impl kafkaman_core::ProblemType for DispatchFailure {
    /// The wrapped error's own classification. Which of the two stages a failure
    /// came from is already carried by `kafkaman.failure.stage`; repeating it
    /// here would make `handler` and `bookkeeping` grow parallel copies of every
    /// URI for no gain.
    fn problem_type(&self) -> &'static str {
        match self {
            Self::Handler(error) | Self::Bookkeeping(error) => error.problem_type(),
        }
    }
}

/// Everything inside the savepoint: both handler positions, the cache upsert
/// between them, and the processed mark.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn converge_and_dispatch(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    router: &MessageRouter,
    processed_at: OffsetDateTime,
) -> std::result::Result<MarkOutcome, DispatchFailure> {
    let meta = ReceivedMeta::from(row);

    let flow = match router.before_handler_for(&row.message_type) {
        Some(before) => {
            let span = handler_span(row, "before");
            let call = before.handle(&mut *tx, meta.clone(), row.payload.clone());
            run_handler(&span, call)
                .await
                .map_err(DispatchFailure::Handler)?
        }
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
            let span = handler_span(row, "after");
            let call = handler.handle(&mut *tx, meta, row.payload.clone());
            run_handler(&span, call)
                .await
                .map_err(DispatchFailure::Handler)?;
        }
    }

    mark_received_processed(tx, table, row.message_id, processed_at)
        .await
        .map_err(DispatchFailure::Bookkeeping)
}

/// Run one handler call inside its span, with the panic boundary applied.
///
/// Both positions go through here rather than repeating four lines twice, so the
/// boundary cannot be put on one and forgotten on the other — which is the whole
/// failure mode it exists to prevent.
///
/// Not annotated for the internal span tier: it applies a phase span, and a
/// second span around it would nest a duplicate above every `kafkaman.handler`.
async fn run_handler<T>(
    span: &tracing::Span,
    call: Pin<Box<dyn Future<Output = Result<T>> + Send + '_>>,
) -> Result<T> {
    let result = match catch_handler_panic(call).instrument(span.clone()).await {
        Ok(result) => result,
        Err(HandlerAbort::Panicked(message)) => {
            span.record("handler.outcome", "panicked");
            Err(Error::HandlerPanicked(message))
        }
        // Deliberately not `HandlerPanicked`. This is a kafkaman bug, not the
        // application's, and recording it as a panic would put it in the row's
        // failure history under the application's name and count it against the
        // dispatcher's panic breaker.
        Err(HandlerAbort::PolledAfterCompletion) => Err(Error::Handler(
            "kafkaman polled the handler boundary after it completed".to_owned(),
        )),
    };
    if let Err(error) = &result {
        let disposition = failure_disposition(error, FailureStage::Handler);
        record_failure_exception(span, disposition.kind, disposition.stage, error);
    }
    result
}

/// Emit one sampled per-message success event, inside `kafkaman.dispatch`.
///
/// Synchronous, and deliberately: [`kafkaman_core::attach`] hands back a guard
/// that is not `Send`, and a context held across an `await` would attribute
/// whatever else the runtime schedules on this thread to this message.
fn emit_success_event(
    table: &ReceivedTable,
    row: &ReceivedRow,
    lifecycle: Option<&mut LifecycleSampler>,
) {
    let Some(sampler) = lifecycle else {
        return;
    };
    if sampler.take(1) == 0 {
        return;
    }

    // Attached, not merely parented: the log appender stamps records from the
    // OpenTelemetry context, which the `tracing` span stack does not set on its
    // own. Without this the event reaches the log signal with no trace id and
    // cannot be pivoted into the trace it belongs to.
    let span = tracing::Span::current();
    let _scope = kafkaman_core::attach(&span);
    tracing::info!(
        parent: &span,
        message_type = table.descriptor.message_type.as_str(),
        message_id = %row.message_id,
        "received message processed"
    );
}

/// Record a failure in the transaction that claimed the row.
///
/// Used where nothing has run yet, so there is nothing to unwind.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn record_failure_in_claim_tx(
    mut tx: Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    failure: ReceivedFailureRecord,
    hooks: Hooks<'_>,
) -> Result<(DispatchStats, FailureRecordPath)> {
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
    tx.commit()
        .instrument_db(kafkaman_core::db_span!(
            "COMMIT",
            table.qualified_name(),
            "commit received failure transaction",
        ))
        .await?;
    Ok((
        dispatch_failure_stats(outcome),
        FailureRecordPath::ClaimTransaction,
    ))
}

/// Roll the handler's writes back to the savepoint, then record the failure.
///
/// Both failing paths — the handler's own error and a failure applying its
/// result — need exactly this, and they used to spell it out twice.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn unwind_and_record(
    pool: &PgPool,
    tx: Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
    failure: ReceivedFailureRecord,
    hooks: Hooks<'_>,
) -> Result<(DispatchStats, FailureRecordPath)> {
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

    let (outcome, recorded_via) =
        rollback_handler_and_record_received_failure(tx, pool, table, failure).await?;
    Ok((dispatch_failure_stats(outcome), recorded_via))
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
