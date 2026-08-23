use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafkaman_core::{Envelope, KafkaMessage, ReceivedIngestFailureKind};
use kafkaman_sqlx::{
    insert_received_ingest_failure, insert_received_with_outcome, ReceivedInsertOutcome,
    ResolvedConfig,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "internal-hooks")]
use crate::hooks::{IngestCommitEvent, PostDurableWriteObserver};
use crate::ingest_record::{ingest_failure_record, record_envelope};
use crate::{Error, IngestLoopStats, IngestStats, Result};

/// Where a record sits in the log.
///
/// Read once, at the top of [`RdkafkaConsumer::ingest_once`], and then passed
/// down. The breaker error, the commit observer, and the returned stats all
/// report a position, and reading it from the message separately at each of
/// those points is how they would come to disagree about which record they are
/// describing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecordLocation {
    partition: i32,
    offset: i64,
}

impl RecordLocation {
    fn of<M: Message>(message: &M) -> Self {
        Self {
            partition: message.partition(),
            offset: message.offset(),
        }
    }
}

pub struct RdkafkaConsumer {
    consumer: StreamConsumer,
    /// Poison-free by construction: a `Mutex` here forced two `.expect()` calls
    /// on a library path, and the counter needs no invariant beyond monotonic
    /// increment-and-read.
    consecutive_skips: Arc<AtomicUsize>,
    max_consecutive_skips: usize,
    #[cfg(feature = "internal-hooks")]
    post_durable_write_observer: Option<Arc<PostDurableWriteObserver>>,
}

impl std::fmt::Debug for RdkafkaConsumer {
    /// The rdkafka consumer handle has no representation; report the breaker
    /// state, which is what an operator debugging a stalled ingest needs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdkafkaConsumer")
            .field(
                "consecutive_skips",
                &self.consecutive_skips.load(Ordering::Relaxed),
            )
            .field("max_consecutive_skips", &self.max_consecutive_skips)
            .finish_non_exhaustive()
    }
}

impl RdkafkaConsumer {
    const DEFAULT_MAX_CONSECUTIVE_SKIPS: usize = 10;

    pub fn new(consumer: StreamConsumer) -> Self {
        Self {
            consumer,
            consecutive_skips: Arc::new(AtomicUsize::new(0)),
            max_consecutive_skips: Self::DEFAULT_MAX_CONSECUTIVE_SKIPS,
            #[cfg(feature = "internal-hooks")]
            post_durable_write_observer: None,
        }
    }

    pub fn with_max_consecutive_skips(mut self, max_consecutive_skips: usize) -> Self {
        self.max_consecutive_skips = max_consecutive_skips.max(1);
        self
    }

    #[cfg(feature = "internal-hooks")]
    #[doc(hidden)]
    pub fn with_post_durable_write_observer<F>(mut self, hook: F) -> Self
    where
        F: Fn(IngestCommitEvent) -> Result<()> + Send + Sync + 'static,
    {
        self.post_durable_write_observer = Some(Arc::new(hook));
        self
    }

    pub fn from_brokers(brokers: &str, group_id: &str) -> Result<Self> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            // Offsets are committed explicitly, after the receive row is
            // durable. Auto-commit could acknowledge a record whose row was
            // never written, which is the one loss this design must not have.
            .set("enable.auto.commit", "false")
            .create()?;
        Ok(Self::new(consumer))
    }

    pub fn subscribe(&self, topics: &[&str]) -> Result<()> {
        self.consumer.subscribe(topics)?;
        Ok(())
    }

    /// Consume one record and store it durably, or quarantine it.
    ///
    /// The offset is committed only after the receive row has committed, so a
    /// crash anywhere in between redelivers rather than loses. Redelivery is
    /// safe because the insert deduplicates on the idempotency key.
    pub async fn ingest_once<P>(&self, pool: &PgPool, cfg: &ResolvedConfig) -> Result<IngestStats>
    where
        P: KafkaMessage + DeserializeOwned + Serialize,
    {
        let message = self.consumer.recv().await?;
        let at = RecordLocation::of(&message);

        match record_envelope::<P, _>(&message) {
            Ok((envelope, key)) => {
                self.store::<P>(pool, cfg, &message, at, &envelope, key)
                    .await
            }
            Err(err) => match err.ingest_failure_kind() {
                // A record that can never be read is quarantined and
                // acknowledged, so the partition advances past it instead of
                // every later record queueing behind one that will never parse.
                Some(kind) => {
                    self.quarantine::<P>(pool, cfg, &message, at, kind, err.to_string())
                        .await
                }
                None => Err(err),
            },
        }
    }

    /// Write the receive row, then acknowledge the record.
    async fn store<P>(
        &self,
        pool: &PgPool,
        cfg: &ResolvedConfig,
        message: &BorrowedMessage<'_>,
        at: RecordLocation,
        envelope: &Envelope<P>,
        key: Option<Vec<u8>>,
    ) -> Result<IngestStats>
    where
        P: KafkaMessage + Serialize,
    {
        let mut tx = pool.begin().await?;
        let outcome = insert_received_with_outcome(
            &mut tx,
            cfg,
            envelope,
            at.partition,
            at.offset,
            key.as_deref(),
        )
        .await?;

        let conflicted = outcome == ReceivedInsertOutcome::MessageIdConflict;
        if conflicted {
            // The same `message_id` already identifies a different logical
            // message. Record it in the same transaction as the (rejected)
            // insert, so the audit row exists exactly when the rejection does.
            let failure = ingest_failure_record::<P, _>(
                message,
                ReceivedIngestFailureKind::MessageIdConflict,
                format!(
                    "message_id {} conflicts with an existing receive row for another idempotency key",
                    envelope.message_id
                ),
            );
            insert_received_ingest_failure(&mut tx, cfg, &failure).await?;
        }
        tx.commit().await?;

        // A conflict is a skip, and counts against the same breaker as an
        // unparseable record: a producer emitting colliding ids is as stuck as
        // one emitting malformed payloads.
        if conflicted {
            self.count_skip(at)?;
        } else {
            self.consecutive_skips.store(0, Ordering::Relaxed);
        }

        #[cfg(feature = "internal-hooks")]
        self.run_post_durable_write_observer(IngestCommitEvent {
            partition: at.partition,
            offset: at.offset,
            outcome,
        })?;

        self.consumer.commit_message(message, CommitMode::Sync)?;

        Ok(IngestStats {
            consumed: 1,
            inserted: usize::from(outcome == ReceivedInsertOutcome::Inserted),
            duplicates: usize::from(outcome == ReceivedInsertOutcome::DuplicateIdempotencyKey),
            skipped: usize::from(conflicted),
            committed: 1,
            partition: at.partition,
            offset: at.offset,
        })
    }

    /// Store an unreadable record for triage, then acknowledge it.
    ///
    /// The quarantine row is written in its own transaction, so a breaker trip
    /// on the following line cannot roll it back — the record's diagnosis
    /// survives even when the loop stops. Redelivery after a trip re-runs this,
    /// which the `(topic, partition, offset)` primary key absorbs.
    async fn quarantine<P>(
        &self,
        pool: &PgPool,
        cfg: &ResolvedConfig,
        message: &BorrowedMessage<'_>,
        at: RecordLocation,
        kind: ReceivedIngestFailureKind,
        error: String,
    ) -> Result<IngestStats>
    where
        P: KafkaMessage,
    {
        let failure = ingest_failure_record::<P, _>(message, kind, error);
        let mut tx = pool.begin().await?;
        insert_received_ingest_failure(&mut tx, cfg, &failure).await?;
        tx.commit().await?;

        // Before the commit: a tripped breaker must leave the offset
        // uncommitted, so the operator's fix is not raced by the loop moving on.
        self.count_skip(at)?;
        self.consumer.commit_message(message, CommitMode::Sync)?;

        Ok(IngestStats {
            consumed: 1,
            inserted: 0,
            duplicates: 0,
            skipped: 1,
            committed: 1,
            partition: at.partition,
            offset: at.offset,
        })
    }

    /// Count one skip against the poison breaker, failing when it trips.
    fn count_skip(&self, at: RecordLocation) -> Result<()> {
        let skips = self.consecutive_skips.fetch_add(1, Ordering::Relaxed) + 1;
        if skips >= self.max_consecutive_skips {
            return Err(Error::ConsecutiveSkipLimitExceeded {
                limit: self.max_consecutive_skips,
                partition: at.partition,
                offset: at.offset,
            });
        }
        Ok(())
    }

    /// Runs the receive ingester until `shutdown` is cancelled or the poison breaker trips.
    ///
    /// Transient Kafka/SQL errors are logged and retried after `retry_delay`. Deterministic
    /// poison records are handled by [`Self::ingest_once`]; if the consecutive skip limit is
    /// reached, the breaker error is returned without committing the breaker record.
    pub async fn run_ingester<P>(
        &self,
        pool: &PgPool,
        cfg: &ResolvedConfig,
        retry_delay: Duration,
        shutdown: CancellationToken,
    ) -> Result<IngestLoopStats>
    where
        P: KafkaMessage + DeserializeOwned + Serialize,
    {
        let mut loop_stats = IngestLoopStats::default();

        loop {
            let result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(loop_stats),
                result = self.ingest_once::<P>(pool, cfg) => result,
            };

            match result {
                Ok(stats) => {
                    if stats.skipped > 0 {
                        tracing::warn!(
                            partition = stats.partition,
                            offset = stats.offset,
                            skipped = stats.skipped,
                            committed = stats.committed,
                            "Kafka ingest skipped and quarantined record"
                        );
                    } else {
                        tracing::debug!(
                            partition = stats.partition,
                            offset = stats.offset,
                            inserted = stats.inserted,
                            duplicates = stats.duplicates,
                            committed = stats.committed,
                            "Kafka ingest stored record"
                        );
                    }
                    loop_stats.record_cycle(&stats);
                }
                Err(err @ Error::ConsecutiveSkipLimitExceeded { .. }) => {
                    tracing::error!(error = %err, "Kafka ingest stopped after consecutive skips");
                    return Err(err);
                }
                Err(err) => {
                    loop_stats.transient_errors += 1;
                    tracing::error!(
                        error = %err,
                        retry_delay_ms = retry_delay.as_millis() as u64,
                        "Kafka ingest failed; retrying"
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Ok(loop_stats),
                        _ = tokio::time::sleep(retry_delay) => {}
                    }
                }
            }
        }
    }

    #[cfg(feature = "internal-hooks")]
    fn run_post_durable_write_observer(&self, context: IngestCommitEvent) -> Result<()> {
        if let Some(hook) = &self.post_durable_write_observer {
            hook(context)?;
        }
        Ok(())
    }
}
