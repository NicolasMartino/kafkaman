use std::error::Error as StdError;

use async_trait::async_trait;
use kafkaman_core::{ClaimedOutboxRow, MarkOutcome, PublishAck, RelayConfig, RelayStats};
use kafkaman_sqlx::{claim_batch, mark_publish_failed, mark_published, OutboxTable};
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

    #[error("publish failed: {0}")]
    Publish(BoxError),
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
                MarkOutcome::StaleClaim | MarkOutcome::Missing => stats.stale += 1,
            },
            Err(err) => {
                let retry_at = OffsetDateTime::now_utc() + cfg.retry_after;
                match mark_publish_failed(
                    pool,
                    table,
                    row.message_id(),
                    row.claim_id,
                    &err.to_string(),
                    retry_at,
                )
                .await?
                {
                    MarkOutcome::Updated => stats.failed += 1,
                    MarkOutcome::StaleClaim | MarkOutcome::Missing => stats.stale += 1,
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
        relay_once(&pool, &publisher, &table, &cfg).await?;

        tokio::select! {
            _ = tokio::time::sleep(cfg.poll_interval) => {}
            _ = shutdown.cancelled() => break,
        }
    }

    Ok(())
}
