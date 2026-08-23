use kafkaman_core::{MarkOutcome, RelayConfig, RelayStats};
use kafkaman_sqlx::{claim_batch, mark_publish_failed, mark_published, OutboxTable};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::run_loop::sleep_or_shutdown;
use crate::{Publisher, Result};

/// Claim one batch, publish it, and record what happened to each row.
///
/// The claim commits before any publishing starts, so a slow broker does not
/// hold a transaction open across network calls. Publishing is sequential
/// because claim order is publish order, and per-entity ordering is what the
/// convergence guard downstream depends on.
pub async fn relay_once<P: Publisher>(
    pool: &PgPool,
    publisher: &P,
    table: &OutboxTable,
    cfg: &RelayConfig,
) -> Result<RelayStats> {
    cfg.validate()?;

    let mut tx = pool.begin().await.map_err(kafkaman_sqlx::Error::from)?;
    let claimed = claim_batch(
        &mut tx,
        table,
        &cfg.worker_id,
        cfg.lease_for,
        cfg.batch_limit,
    )
    .await?;
    tx.commit().await.map_err(kafkaman_sqlx::Error::from)?;

    let mut stats = RelayStats {
        claimed: claimed.len(),
        ..RelayStats::default()
    };

    for row in claimed {
        // A mark that finds no claim is not an error: the lease expired and
        // another worker owns the row now. Counted, not failed.
        let outcome = match publisher.publish(&row).await {
            Ok(_) => {
                let outcome = mark_published(pool, table, row.message_id(), row.claim_id).await?;
                if outcome == MarkOutcome::Updated {
                    stats.published += 1;
                }
                outcome
            }
            Err(err) => {
                let outcome = mark_publish_failed(
                    pool,
                    table,
                    row.message_id(),
                    row.claim_id,
                    &err.to_string(),
                    cfg.retry_after,
                )
                .await?;
                if outcome == MarkOutcome::Updated {
                    stats.failed += 1;
                }
                outcome
            }
        };

        match outcome {
            MarkOutcome::Updated => {}
            MarkOutcome::StaleClaim => stats.stale += 1,
            MarkOutcome::Missing => stats.missing += 1,
        }
    }

    Ok(stats)
}

/// Run the outbox relay until `shutdown` is cancelled.
///
/// Returns `Err` only for a configuration that can never succeed. Transient
/// per-cycle failures are logged and retried, because a database blip must not
/// take the relay down.
///
/// Unlike [`run_dispatcher`](crate::run_dispatcher) and
/// [`run_purger`](crate::run_purger), this sleeps after every cycle rather than
/// continuing immediately on a non-empty batch. That means a backlog drains at
/// one batch per poll interval. It is left as-is deliberately: changing it is a
/// throughput change with its own timing consequences, not a tidy-up.
pub async fn run<P: Publisher>(
    pool: PgPool,
    publisher: P,
    table: OutboxTable,
    cfg: RelayConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    // Validate once, up front. Previously the only validation happened inside
    // `relay_once`, so an invalid config produced an error every cycle and then
    // slept on the very `poll_interval` that had just been rejected — a zero
    // interval span the loop at full speed while logging an error each pass.
    cfg.validate()?;

    loop {
        // Checked before the cycle, not only after it. Without this a relay
        // handed an already-cancelled token still claims and publishes one
        // batch — harmless to the data, since claims are leased and marks are
        // idempotent, but it starts broker calls a shutdown has already asked
        // it not to start. `run_dispatcher` and `run_purger` both guard here.
        if shutdown.is_cancelled() {
            break;
        }

        match relay_once(&pool, &publisher, &table, &cfg).await {
            Ok(stats) if stats.claimed > 0 => tracing::debug!(
                claimed = stats.claimed,
                published = stats.published,
                failed = stats.failed,
                stale = stats.stale,
                missing = stats.missing,
                "relay cycle complete"
            ),
            Ok(_) => {}
            Err(err) => tracing::error!(
                error = %err,
                "relay cycle failed; retrying after poll interval"
            ),
        }

        if !sleep_or_shutdown(cfg.poll_interval, &shutdown).await {
            break;
        }
    }

    Ok(())
}
