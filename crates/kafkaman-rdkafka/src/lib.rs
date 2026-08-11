use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use kafkaman_core::{
    ClaimedOutboxRow, Envelope, IdempotencyIdentity, IdempotencyKey, KafkaMessage, PublishAck,
    ReceivedIngestFailureKind,
};
use kafkaman_sqlx::{
    insert_received_ingest_failure, insert_received_with_outcome, ReceivedIngestFailure,
    ReceivedInsertOutcome, ResolvedConfig,
};
use kafkaman_worker::{BoxError, Publisher};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use kafkaman_core;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "test-hooks")]
type PostDurableWriteHook = dyn Fn(IngestCommitContext) -> Result<()> + Send + Sync + 'static;

#[cfg(feature = "test-hooks")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestCommitContext {
    pub partition: i32,
    pub offset: i64,
    pub outcome: ReceivedInsertOutcome,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error("delivery failed: {0}")]
    Delivery(String),

    #[cfg(feature = "test-hooks")]
    #[error("ingest test hook failed: {0}")]
    TestHook(String),

    #[error("Kafka record had no payload")]
    MissingPayload,

    #[error("Kafka record must include kafkaman-idempotency-key")]
    MissingIdempotencyKey,

    #[error("invalid Kafka header `{name}`: {message}")]
    InvalidHeader { name: &'static str, message: String },

    #[error("Kafka record arrived on topic `{actual}` while `{expected}` was expected")]
    UnexpectedTopic {
        expected: &'static str,
        actual: String,
    },

    #[error(
        "consecutive ingest skip limit {limit} reached at partition {partition} offset {offset}"
    )]
    ConsecutiveSkipLimitExceeded {
        limit: usize,
        partition: i32,
        offset: i64,
    },
}

#[derive(Clone)]
pub struct RdkafkaPublisher {
    producer: FutureProducer,
}

pub struct RdkafkaConsumer {
    consumer: StreamConsumer,
    consecutive_skips: Arc<Mutex<usize>>,
    max_consecutive_skips: usize,
    #[cfg(feature = "test-hooks")]
    post_durable_write_hook: Option<Arc<PostDurableWriteHook>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestStats {
    pub consumed: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub skipped: usize,
    pub committed: usize,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngestLoopStats {
    pub cycles: usize,
    pub consumed: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub skipped: usize,
    pub committed: usize,
    pub transient_errors: usize,
}

impl IngestLoopStats {
    fn record_cycle(&mut self, stats: &IngestStats) {
        self.cycles += 1;
        self.consumed += stats.consumed;
        self.inserted += stats.inserted;
        self.duplicates += stats.duplicates;
        self.skipped += stats.skipped;
        self.committed += stats.committed;
    }
}

impl RdkafkaPublisher {
    pub fn new(producer: FutureProducer) -> Self {
        Self { producer }
    }

    pub fn from_brokers(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self::new(producer))
    }

    pub async fn publish_row(&self, row: &ClaimedOutboxRow) -> Result<PublishAck> {
        let payload = serde_json::to_vec(&row.row.payload)?;
        let mut headers = OwnedHeaders::new();

        for (key, value) in &row.row.headers {
            headers = headers.insert(Header {
                key: key.as_str(),
                value: Some(value.as_str()),
            });
        }

        let message_id = row.row.message_id.to_string();
        let correlation_id = row.row.correlation_id.to_string();
        headers = headers
            .insert(Header {
                key: "kafkaman-message-id",
                value: Some(message_id.as_str()),
            })
            .insert(Header {
                key: "kafkaman-correlation-id",
                value: Some(correlation_id.as_str()),
            });

        let idempotency_key;
        if let Some(key) = row.row.idempotency_key {
            idempotency_key = key.to_string();
            headers = headers.insert(Header {
                key: "kafkaman-idempotency-key",
                value: Some(idempotency_key.as_str()),
            });
        }

        let causation_id;
        if let Some(id) = row.row.causation_id {
            causation_id = id.to_string();
            headers = headers.insert(Header {
                key: "kafkaman-causation-id",
                value: Some(causation_id.as_str()),
            });
        }

        let mut record = FutureRecord::to(&row.row.topic)
            .payload(payload.as_slice())
            .headers(headers);
        if let Some(key) = row.row.partition_key.as_deref() {
            record = record.key(key);
        }

        match self.producer.send(record, Timeout::Never).await {
            Ok((partition, offset)) => Ok(PublishAck {
                topic: row.row.topic.clone(),
                partition,
                offset,
            }),
            Err((error, _message)) => Err(Error::Delivery(error.to_string())),
        }
    }
}

impl RdkafkaConsumer {
    const DEFAULT_MAX_CONSECUTIVE_SKIPS: usize = 10;

    pub fn new(consumer: StreamConsumer) -> Self {
        Self {
            consumer,
            consecutive_skips: Arc::new(Mutex::new(0)),
            max_consecutive_skips: Self::DEFAULT_MAX_CONSECUTIVE_SKIPS,
            #[cfg(feature = "test-hooks")]
            post_durable_write_hook: None,
        }
    }

    pub fn with_max_consecutive_skips(mut self, max_consecutive_skips: usize) -> Self {
        self.max_consecutive_skips = max_consecutive_skips.max(1);
        self
    }

    #[cfg(feature = "test-hooks")]
    pub fn with_post_durable_write_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(IngestCommitContext) -> Result<()> + Send + Sync + 'static,
    {
        self.post_durable_write_hook = Some(Arc::new(hook));
        self
    }

    pub fn from_brokers(brokers: &str, group_id: &str) -> Result<Self> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()?;
        Ok(Self::new(consumer))
    }

    pub fn subscribe(&self, topics: &[&str]) -> Result<()> {
        self.consumer.subscribe(topics)?;
        Ok(())
    }

    pub async fn ingest_once<P>(&self, pool: &PgPool, cfg: &ResolvedConfig) -> Result<IngestStats>
    where
        P: KafkaMessage + DeserializeOwned + Serialize,
    {
        let message = self.consumer.recv().await?;
        let partition = message.partition();
        let offset = message.offset();
        let (envelope, key) = match record_envelope::<P, _>(&message) {
            Ok(record) => record,
            Err(err) if err.is_deterministic_ingest_skip() => {
                self.quarantine_ingest_failure::<P, _>(pool, cfg, &message, &err)
                    .await?;
                let skip_count = self.record_consecutive_skip();
                if skip_count >= self.max_consecutive_skips {
                    return Err(Error::ConsecutiveSkipLimitExceeded {
                        limit: self.max_consecutive_skips,
                        partition,
                        offset,
                    });
                }
                self.consumer.commit_message(&message, CommitMode::Sync)?;
                return Ok(IngestStats {
                    consumed: 1,
                    inserted: 0,
                    duplicates: 0,
                    skipped: 1,
                    committed: 1,
                    partition,
                    offset,
                });
            }
            Err(err) => return Err(err),
        };

        let mut tx = pool.begin().await?;
        let outcome = insert_received_with_outcome(
            &mut tx,
            cfg,
            &envelope,
            message.partition(),
            message.offset(),
            key.as_deref(),
        )
        .await?;
        if outcome == ReceivedInsertOutcome::MessageIdConflict {
            let failure = ingest_failure_record::<P, _>(
                &message,
                ReceivedIngestFailureKind::MessageIdConflict,
                format!(
                    "message_id {} conflicts with an existing receive row for another idempotency key",
                    envelope.message_id
                ),
            )?;
            insert_received_ingest_failure(&mut tx, cfg, &failure).await?;
        }
        tx.commit().await?;

        if outcome == ReceivedInsertOutcome::MessageIdConflict {
            let skip_count = self.record_consecutive_skip();
            if skip_count >= self.max_consecutive_skips {
                return Err(Error::ConsecutiveSkipLimitExceeded {
                    limit: self.max_consecutive_skips,
                    partition,
                    offset,
                });
            }
        } else {
            self.reset_consecutive_skips();
        }

        #[cfg(feature = "test-hooks")]
        self.run_post_durable_write_hook(IngestCommitContext {
            partition,
            offset,
            outcome,
        })?;

        self.consumer.commit_message(&message, CommitMode::Sync)?;

        Ok(IngestStats {
            consumed: 1,
            inserted: usize::from(outcome == ReceivedInsertOutcome::Inserted),
            duplicates: usize::from(outcome == ReceivedInsertOutcome::DuplicateIdempotencyKey),
            skipped: usize::from(outcome == ReceivedInsertOutcome::MessageIdConflict),
            committed: 1,
            partition: message.partition(),
            offset: message.offset(),
        })
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

    #[cfg(feature = "test-hooks")]
    fn run_post_durable_write_hook(&self, context: IngestCommitContext) -> Result<()> {
        if let Some(hook) = &self.post_durable_write_hook {
            hook(context)?;
        }
        Ok(())
    }

    async fn quarantine_ingest_failure<P, M>(
        &self,
        pool: &PgPool,
        cfg: &ResolvedConfig,
        message: &M,
        error: &Error,
    ) -> Result<()>
    where
        P: KafkaMessage,
        M: Message,
    {
        let failure =
            ingest_failure_record::<P, _>(message, ingest_failure_kind(error), error.to_string())?;
        let mut tx = pool.begin().await?;
        insert_received_ingest_failure(&mut tx, cfg, &failure).await?;
        tx.commit().await?;
        Ok(())
    }

    fn record_consecutive_skip(&self) -> usize {
        let mut guard = self
            .consecutive_skips
            .lock()
            .expect("consecutive skip counter mutex poisoned");
        *guard += 1;
        *guard
    }

    fn reset_consecutive_skips(&self) {
        let mut guard = self
            .consecutive_skips
            .lock()
            .expect("consecutive skip counter mutex poisoned");
        *guard = 0;
    }
}

impl Error {
    fn is_deterministic_ingest_skip(&self) -> bool {
        matches!(
            self,
            Error::MissingPayload
                | Error::MissingIdempotencyKey
                | Error::InvalidHeader { .. }
                | Error::UnexpectedTopic { .. }
                | Error::Serde(_)
        )
    }
}

fn ingest_failure_kind(error: &Error) -> ReceivedIngestFailureKind {
    match error {
        Error::MissingPayload => ReceivedIngestFailureKind::MissingPayload,
        Error::MissingIdempotencyKey => ReceivedIngestFailureKind::MissingIdempotencyKey,
        Error::Serde(_) => ReceivedIngestFailureKind::InvalidPayload,
        Error::InvalidHeader { .. } => ReceivedIngestFailureKind::InvalidHeader,
        Error::UnexpectedTopic { .. } => ReceivedIngestFailureKind::UnexpectedTopic,
        _ => ReceivedIngestFailureKind::InvalidPayload,
    }
}

fn ingest_failure_record<P, M>(
    message: &M,
    kind: ReceivedIngestFailureKind,
    error: String,
) -> Result<ReceivedIngestFailure>
where
    P: KafkaMessage,
    M: Message,
{
    Ok(ReceivedIngestFailure {
        source_topic: message.topic().to_owned(),
        source_partition: message.partition(),
        source_offset: message.offset(),
        key: message.key().map(Vec::from),
        headers: all_headers(message.headers())?,
        payload: message.payload().map(Vec::from),
        message_type: P::MESSAGE_TYPE.to_owned(),
        expected_topic: P::TOPIC.to_owned(),
        kind,
        error,
    })
}

fn all_headers<H: Headers>(headers: Option<&H>) -> Result<serde_json::Value> {
    let mut output: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if let Some(headers) = headers {
        for header in headers.iter() {
            let value = header
                .value
                .map(|value| String::from_utf8_lossy(value).into_owned())
                .unwrap_or_default();
            output.entry(header.key.to_owned()).or_default().push(value);
        }
    }
    Ok(serde_json::to_value(output)?)
}

fn record_envelope<P, M>(message: &M) -> Result<(Envelope<P>, Option<Vec<u8>>)>
where
    P: KafkaMessage + DeserializeOwned,
    M: Message,
{
    if message.topic() != P::TOPIC {
        return Err(Error::UnexpectedTopic {
            expected: P::TOPIC,
            actual: message.topic().to_owned(),
        });
    }

    let payload = message.payload().ok_or(Error::MissingPayload)?;
    let payload = serde_json::from_slice::<P>(payload)?;
    let key = message.key().map(Vec::from);

    let mut envelope = Envelope::new(payload);
    envelope.headers = user_headers(message.headers());
    let idempotency_key = header_value(message.headers(), "kafkaman-idempotency-key")?
        .ok_or(Error::MissingIdempotencyKey)?;
    let idempotency_key =
        IdempotencyKey::from_hex(&idempotency_key).map_err(|err| Error::InvalidHeader {
            name: "kafkaman-idempotency-key",
            message: err.to_string(),
        })?;
    envelope.idempotency_key = Some(IdempotencyIdentity::from_key(idempotency_key));
    if let Some(message_id) = header_value(message.headers(), "kafkaman-message-id")? {
        envelope.message_id = parse_uuid_header("kafkaman-message-id", &message_id)?;
    }
    if let Some(correlation_id) = header_value(message.headers(), "kafkaman-correlation-id")? {
        envelope.correlation_id = parse_uuid_header("kafkaman-correlation-id", &correlation_id)?;
    }
    if let Some(causation_id) = header_value(message.headers(), "kafkaman-causation-id")? {
        envelope.causation_id = Some(parse_uuid_header("kafkaman-causation-id", &causation_id)?);
    }

    Ok((envelope, key))
}

fn user_headers<H: Headers>(headers: Option<&H>) -> BTreeMap<String, String> {
    let mut output = BTreeMap::new();
    let Some(headers) = headers else {
        return output;
    };
    for header in headers.iter() {
        if header
            .key
            .as_bytes()
            .get(..kafkaman_core::RESERVED_HEADER_PREFIX.len())
            .is_some_and(|prefix| {
                prefix.eq_ignore_ascii_case(kafkaman_core::RESERVED_HEADER_PREFIX.as_bytes())
            })
        {
            continue;
        }
        if let Some(value) = header.value {
            output.insert(
                header.key.to_owned(),
                String::from_utf8_lossy(value).into_owned(),
            );
        }
    }
    output
}

fn header_value<H: Headers>(headers: Option<&H>, name: &'static str) -> Result<Option<String>> {
    let Some(headers) = headers else {
        return Ok(None);
    };
    for header in headers.iter() {
        if header.key.eq_ignore_ascii_case(name) {
            return Ok(header
                .value
                .map(|value| String::from_utf8_lossy(value).into_owned()));
        }
    }
    Ok(None)
}

fn parse_uuid_header(name: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|err| Error::InvalidHeader {
        name,
        message: err.to_string(),
    })
}

#[async_trait]
impl Publisher for RdkafkaPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        self.publish_row(row)
            .await
            .map_err(|err| Box::new(err) as BoxError)
    }
}
