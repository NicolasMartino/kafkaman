use std::time::{Duration, Instant};

use kafkaman_core::LifecycleEmission;
use kafkaman_sqlx::{dispatch_once, DispatchStats, MessageRouter, ReceivedTable};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use crate::metrics::DispatchMetrics;
use crate::run_loop::sleep_or_shutdown;
use crate::{Error, Result};

/// Run the receive dispatcher until `shutdown` is cancelled.
///
/// Continues immediately whenever a cycle claimed a row, so a backlog drains at
/// full speed rather than one row per poll interval, and sleeps only when there
/// was nothing due.
///
/// `poll_interval` must be greater than zero: that sleep is the only thing that
/// yields between empty polls, so a zero interval turns the idle loop into a
/// busy spin against the database.
pub async fn run_dispatcher(
    pool: PgPool,
    table: ReceivedTable,
    router: MessageRouter,
    poll_interval: Duration,
    lifecycle: LifecycleEmission,
    shutdown: CancellationToken,
) -> Result<()> {
    if poll_interval.is_zero() {
        return Err(Error::InvalidDispatcherConfig {
            field: "poll_interval",
            reason: "must be greater than zero",
        });
    }

    let metrics = DispatchMetrics::new(table.descriptor.message_type.as_str());
    let mut lifecycle = lifecycle.sampler();

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // A failed cycle is treated as "nothing claimed" rather than propagated:
        // a transient database error must not stop a dispatcher, and the sleep
        // that follows is also the backoff.
        let started = Instant::now();
        let claimed = match dispatch_once(&pool, &table, &router, OffsetDateTime::now_utc()).await {
            Ok(stats) => {
                metrics.cycle();
                metrics.rows("claimed", stats.claimed);
                metrics.rows("processed", stats.processed);
                metrics.rows("failed", stats.failed);
                if stats.claimed > 0 {
                    metrics.dispatched(dispatch_outcome(&stats), started.elapsed());
                }
                for _ in 0..lifecycle.take(stats.processed) {
                    tracing::info!(
                        message_type = table.descriptor.message_type.as_str(),
                        "received message processed"
                    );
                }
                if stats.claimed > 0 {
                    tracing::debug!(
                        claimed = stats.claimed,
                        processed = stats.processed,
                        failed = stats.failed,
                        "receive dispatch cycle complete"
                    );
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

        if !sleep_or_shutdown(poll_interval, &shutdown).await {
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
    } else if stats.failed > 0 {
        "failed"
    } else {
        "stale"
    }
}
