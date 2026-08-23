use std::time::Duration;

use kafkaman_sqlx::{dispatch_once, MessageRouter, ReceivedTable};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

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
    shutdown: CancellationToken,
) -> Result<()> {
    if poll_interval.is_zero() {
        return Err(Error::InvalidDispatcherConfig {
            field: "poll_interval",
            reason: "must be greater than zero",
        });
    }

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // A failed cycle is treated as "nothing claimed" rather than propagated:
        // a transient database error must not stop a dispatcher, and the sleep
        // that follows is also the backoff.
        let claimed = match dispatch_once(&pool, &table, &router, OffsetDateTime::now_utc()).await {
            Ok(stats) => {
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
