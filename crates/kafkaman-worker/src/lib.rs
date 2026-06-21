use std::{error::Error as StdError, time::Duration};

use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, MarkOutcome, PublishAck, RelayConfig, RelayStats};
use kafkaman_sqlx::{
    claim_batch, dispatch_once, mark_publish_failed, mark_published, MessageRouter, OutboxTable,
    ReceivedTable,
};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

pub use kafkaman_core;
pub use kafkaman_sqlx;

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error("invalid relay config: {0}")]
    InvalidConfig(String),
}

#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError>;
}

pub async fn relay_once<P: Publisher>(
    pool: &PgPool,
    publisher: &P,
    table: &OutboxTable,
    cfg: &RelayConfig,
) -> Result<RelayStats> {
    cfg.validate().map_err(Error::InvalidConfig)?;

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
        match publisher.publish(&row).await {
            Ok(_) => match mark_published(pool, table, row.message_id(), row.claim_id).await? {
                MarkOutcome::Updated => stats.published += 1,
                MarkOutcome::StaleClaim => stats.stale += 1,
                MarkOutcome::Missing => stats.missing += 1,
            },
            Err(err) => {
                match mark_publish_failed(
                    pool,
                    table,
                    row.message_id(),
                    row.claim_id,
                    &err.to_string(),
                    cfg.retry_after,
                )
                .await?
                {
                    MarkOutcome::Updated => stats.failed += 1,
                    MarkOutcome::StaleClaim => stats.stale += 1,
                    MarkOutcome::Missing => stats.missing += 1,
                }
            }
        }
    }

    Ok(stats)
}

pub async fn run<P: Publisher>(
    pool: PgPool,
    publisher: P,
    table: OutboxTable,
    cfg: RelayConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        // A relay cycle failure (typically a transient claim/mark database
        // error) must not kill the worker. Log it and retry on the next tick;
        // only a shutdown signal ends the loop.
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

        tokio::select! {
            _ = tokio::time::sleep(cfg.poll_interval) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    Ok(())
}

pub async fn run_dispatcher(
    pool: PgPool,
    table: ReceivedTable,
    router: MessageRouter,
    poll_interval: Duration,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        if shutdown.is_cancelled() {
            break;
        }

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

        tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    Ok(())
}
