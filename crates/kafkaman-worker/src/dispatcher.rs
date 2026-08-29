use std::collections::BTreeSet;
use std::time::Instant;

use kafkaman_core::DispatcherConfig;
use kafkaman_sqlx::{dispatch_once_sampled, DispatchStats, MessageRouter, ReceivedTable};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::metrics::DispatchMetrics;
use crate::run_loop::sleep_or_shutdown;
use crate::{Error, Result};

/// Run the receive dispatcher until `shutdown` is cancelled.
///
/// Continues immediately whenever a cycle claimed a row, so a backlog drains at
/// full speed rather than one row per poll interval, and sleeps only when there
/// was nothing due.
///
/// Returns `Err` for a configuration that can never succeed, and when the
/// handler-panic breaker trips — see [`DispatcherConfig::max_consecutive_panicking_rows`].
/// Every other failure is logged and retried, because a database blip must not
/// take a dispatcher down.
pub async fn run_dispatcher(
    pool: PgPool,
    table: ReceivedTable,
    router: MessageRouter,
    cfg: DispatcherConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    cfg.validate()?;

    let metrics = DispatchMetrics::new(table.descriptor.message_type.as_str());
    let mut lifecycle = cfg.lifecycle.sampler();
    // The rows in the current panic streak, not a count of panics. See
    // `note_panicking_row`.
    let mut panicking_rows: BTreeSet<Uuid> = BTreeSet::new();

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // A failed cycle is treated as "nothing claimed" rather than propagated:
        // a transient database error must not stop a dispatcher, and the sleep
        // that follows is also the backoff.
        let started = Instant::now();
        // The sampler goes in rather than the event coming out. A success event
        // emitted here, after the call returns, names a message type and nothing
        // else — the `kafkaman.dispatch` span it belongs to has already closed,
        // so the line carries no trace id and cannot be pivoted back to the row
        // it describes. `dispatch_once_sampled` emits it inside that span.
        let claimed = match dispatch_once_sampled(
            &pool,
            &table,
            &router,
            OffsetDateTime::now_utc(),
            &mut lifecycle,
        )
        .await
        {
            Ok(stats) => {
                metrics.cycle();
                metrics.rows("claimed", stats.claimed);
                metrics.rows("processed", stats.processed);
                metrics.rows("failed", stats.failed);
                metrics.rows("panicked", stats.panicked);
                if stats.claimed > 0 {
                    metrics.dispatched(dispatch_outcome(&stats), started.elapsed());
                }
                if stats.claimed > 0 {
                    tracing::debug!(
                        claimed = stats.claimed,
                        processed = stats.processed,
                        failed = stats.failed,
                        panicked = stats.panicked,
                        "receive dispatch cycle complete"
                    );
                }
                if stats.panicked > 0 {
                    note_panicking_row(&mut panicking_rows, &stats);
                    if panicking_rows.len() >= cfg.max_consecutive_panicking_rows {
                        let err = Error::ConsecutivePanickingRowLimitExceeded {
                            limit: cfg.max_consecutive_panicking_rows,
                            message_type: table.descriptor.message_type.as_str().to_owned(),
                            message_id: stats.panicked_message_id,
                        };
                        metrics.error();
                        tracing::error!(
                            error = %err,
                            "receive dispatcher stopped after consecutive rows panicked"
                        );
                        return Err(err);
                    }
                } else if stats.claimed > 0 {
                    // A row that got through ends the streak. An *empty* cycle
                    // does not: an idle dispatcher has proved nothing, and
                    // clearing on idle would let a slow trickle of panicking
                    // rows run forever.
                    //
                    // Which is also why the streak is unbounded in wall-clock
                    // time — the only thing that clears it is evidence of
                    // recovery, and time spent idle is not evidence. See
                    // `max_consecutive_panicking_rows`.
                    panicking_rows.clear();
                }
                stats.claimed
            }
            Err(err) => {
                metrics.error();
                tracing::error!(
                    error = %err,
                    "receive dispatch cycle failed; retrying after poll interval"
                );
                0
            }
        };

        if claimed > 0 {
            continue;
        }

        if !sleep_or_shutdown(cfg.poll_interval, &shutdown).await {
            break;
        }
    }

    Ok(())
}

/// How a dispatch that claimed a row ended, as the `outcome` attribute.
///
/// `dispatch_once` handles exactly one row per call, so these are mutually
/// exclusive rather than a summary. A claim that is neither processed nor failed
/// lost its lease to another worker mid-flight — the row is not lost, but this
/// dispatch did not complete it, and lumping that in with success would hide a
/// lease that is too short for the handler it covers.
fn dispatch_outcome(stats: &DispatchStats) -> &'static str {
    if stats.processed > 0 {
        "processed"
    } else if stats.panicked > 0 {
        "panicked"
    } else if stats.failed > 0 {
        "failed"
    } else {
        "stale"
    }
}

/// Record one panicking row against the current streak.
///
/// # Why the streak is a set of rows and not a count of panics
///
/// The breaker exists for a deploy whose handler panics on everything, which no
/// retry budget can absorb and which needs a process-level signal. It must not
/// fire for a single poison message, because absorbing that is precisely what
/// the retry budget and the dead-letter queue are for — and stopping the service
/// over one bad message is the failure the handler panic boundary was built to
/// remove.
///
/// A count cannot separate those. `dispatch_once` claims one row per cycle and
/// only a *successful* claim ends a streak, so one poison row retrying on the
/// default `max_attempts` of 10 contributes ten panics with nothing in between —
/// numerically identical to ten distinct rows panicking once each. The two were
/// once conflated, and the result was that a single poison message tripped the
/// breaker on the very attempt that dead-lettered it: the row was handled
/// exactly as designed and the dispatcher stopped anyway.
///
/// Counting the distinct rows involved removes the ambiguity instead of tuning
/// around it. One row contributes one entry however often it is retried; the set
/// is bounded by the limit, because reaching the limit stops the loop.
fn note_panicking_row(streak: &mut BTreeSet<Uuid>, stats: &DispatchStats) {
    let Some(message_id) = stats.panicked_message_id else {
        // Set one line from `panicked` in `dispatch_claimed_row`, so this is a
        // library bug rather than a state to handle. Skipping the insert makes
        // it fail open: a breaker that cannot attribute a panic should not trip
        // on it.
        debug_assert!(
            false,
            "a panicking dispatch must name the row that panicked"
        );
        return;
    };
    streak.insert(message_id);
}
