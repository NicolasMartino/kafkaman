use kafkaman_core::PurgeConfig;
use kafkaman_sqlx::{purge_outbox_once, OutboxTable};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::run_loop::sleep_or_shutdown;
use crate::Result;

/// Reclaim terminal outbox rows until `shutdown` is cancelled.
///
/// Nothing purges any kafkaman table by default, so an outbox grows for the life of
/// the application unless something like this runs. Scope is the outbox alone —
/// received tables are the only durable record of what was done and their dedupe
/// window is their retention window; cache tables are the state.
///
/// Sweeps continuously while batches come back non-empty, then sleeps. That is the
/// same shape as [`run_dispatcher`](crate::run_dispatcher), and for the same reason:
/// a backlog should drain at full speed rather than one batch per poll interval.
///
/// Returns `Err` only for a configuration that can never succeed. A failed sweep is
/// logged and retried, because reclaiming disk must never be the thing that takes a
/// relay host down.
pub async fn run_purger(
    pool: PgPool,
    table: OutboxTable,
    cfg: PurgeConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    cfg.validate()?;

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let deleted = match purge_outbox_once(&pool, &table, &cfg).await {
            Ok(stats) => {
                if stats.deleted > 0 {
                    tracing::debug!(deleted = stats.deleted, "outbox retention batch reclaimed");
                }
                stats.deleted
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "outbox retention sweep failed; retrying after poll interval"
                );
                0
            }
        };

        if deleted > 0 {
            continue;
        }

        if !sleep_or_shutdown(cfg.poll_interval, &shutdown).await {
            break;
        }
    }

    Ok(())
}
