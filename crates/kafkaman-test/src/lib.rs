use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kafkaman_core::{
    ClaimedOutboxRow, Envelope, KafkaMessage, OutboxRow, OutboxStatus, PublishAck, PublishedRecord,
    ReceivedRow, RelayStats, SqlIdentifier,
};
use kafkaman_sqlx::{
    enqueue, insert_received, migrate, outbox_row, received_row_by_idempotency_key,
    CreateOutboxTable, CreateReceivedTable, InitSchema, MigrationContext, MigrationReport,
    OutboxTable, ReceivedTable, ResolvedConfig,
};
use kafkaman_worker::{BoxError, Publisher};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

pub use kafkaman_config;
pub use kafkaman_core;
pub use kafkaman_sqlx;
pub use kafkaman_worker;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Core(#[from] kafkaman_core::Error),

    #[error(transparent)]
    Sqlx(#[from] kafkaman_sqlx::Error),

    #[error(transparent)]
    Pool(#[from] sqlx::Error),

    #[error(transparent)]
    Worker(#[from] kafkaman_worker::Error),

    #[cfg(feature = "redpanda")]
    #[error(transparent)]
    Rdkafka(#[from] kafkaman_rdkafka::Error),

    #[error("outbox row `{0}` was not found")]
    MissingRow(Uuid),

    #[error("received row with idempotency key `{0}` was not found")]
    MissingReceivedRow(String),

    #[error("outbox row `{message_id}` expected status `{expected}` but found `{actual}`")]
    UnexpectedStatus {
        message_id: Uuid,
        expected: OutboxStatus,
        actual: OutboxStatus,
    },
}

#[derive(Clone, Debug, Default)]
pub struct CapturingPublisher {
    records: Arc<Mutex<Vec<PublishedRecord>>>,
}

impl CapturingPublisher {
    pub fn records(&self) -> Vec<PublishedRecord> {
        self.records
            .lock()
            .expect("capturing publisher mutex poisoned")
            .clone()
    }

    pub fn records_on(&self, topic: &str) -> Vec<PublishedRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.topic == topic)
            .collect()
    }
}

#[async_trait]
impl Publisher for CapturingPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        let offset = {
            let mut records = self
                .records
                .lock()
                .expect("capturing publisher mutex poisoned");
            let offset = records.len() as i64;
            records.push(PublishedRecord {
                topic: row.row.topic.clone(),
                key: row.row.partition_key.clone(),
                payload: row.row.payload.clone(),
                headers: row.row.headers.clone(),
                message_id: row.row.message_id,
            });
            offset
        };

        Ok(PublishAck {
            topic: row.row.topic.clone(),
            partition: 0,
            offset,
        })
    }
}

pub enum HarnessPublisher {
    Capturing(CapturingPublisher),
    #[cfg(feature = "redpanda")]
    Redpanda(kafkaman_rdkafka::RdkafkaPublisher),
}

#[async_trait]
impl Publisher for HarnessPublisher {
    async fn publish(&self, row: &ClaimedOutboxRow) -> std::result::Result<PublishAck, BoxError> {
        match self {
            HarnessPublisher::Capturing(publisher) => publisher.publish(row).await,
            #[cfg(feature = "redpanda")]
            HarnessPublisher::Redpanda(publisher) => publisher.publish(row).await,
        }
    }
}

pub struct Harness {
    pool: PgPool,
    cfg: Arc<Mutex<ResolvedConfig>>,
    last_migration_report: Arc<Mutex<MigrationReport>>,
    publisher: HarnessPublisher,
    // Serializes dynamic message registration + migration so a concurrent caller
    // cannot observe a freshly-registered type and obtain a table handle before
    // the migration that creates its table has committed.
    registration_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Harness {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
        let cfg = ResolvedConfig::new(schema);
        let changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
        let report = migrate(&pool, &cfg, &MigrationContext::default(), &changesets).await?;

        Ok(Self {
            pool,
            cfg: Arc::new(Mutex::new(cfg)),
            last_migration_report: Arc::new(Mutex::new(report)),
            publisher: HarnessPublisher::Capturing(CapturingPublisher::default()),
            registration_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub async fn connect_with_config(
        database_url: &str,
        config: kafkaman_config::Config,
    ) -> Result<Self> {
        let cfg = ResolvedConfig::from_config(Some(&config), std::iter::empty())?;
        let pool = PgPool::connect(database_url).await?;
        let changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
        let report = migrate(&pool, &cfg, &MigrationContext::default(), &changesets).await?;

        Ok(Self {
            pool,
            cfg: Arc::new(Mutex::new(cfg)),
            last_migration_report: Arc::new(Mutex::new(report)),
            publisher: HarnessPublisher::Capturing(CapturingPublisher::default()),
            registration_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Connect a harness that publishes to a real Redpanda/Kafka broker via
    /// [`kafkaman_rdkafka::RdkafkaPublisher`]. Assertions about what was
    /// published must come from a broker consumer, not from captured records.
    #[cfg(feature = "redpanda")]
    pub async fn connect_redpanda(database_url: &str, brokers: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
        let cfg = ResolvedConfig::new(schema);
        let changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
        let report = migrate(&pool, &cfg, &MigrationContext::default(), &changesets).await?;

        let publisher = kafkaman_rdkafka::RdkafkaPublisher::from_brokers(brokers)?;
        Ok(Self {
            pool,
            cfg: Arc::new(Mutex::new(cfg)),
            last_migration_report: Arc::new(Mutex::new(report)),
            publisher: HarnessPublisher::Redpanda(publisher),
            registration_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn config(&self) -> ResolvedConfig {
        self.cfg
            .lock()
            .expect("harness config mutex poisoned")
            .clone()
    }

    pub fn migrate_report(&self) -> MigrationReport {
        self.last_migration_report
            .lock()
            .expect("harness migration report mutex poisoned")
            .clone()
    }

    /// The capturing publisher backing this harness. Only valid for harnesses
    /// built with [`Harness::connect`]; panics in Redpanda mode, where output
    /// must be asserted through a broker consumer.
    pub fn publisher(&self) -> CapturingPublisher {
        match &self.publisher {
            HarnessPublisher::Capturing(publisher) => publisher.clone(),
            #[cfg(feature = "redpanda")]
            HarnessPublisher::Redpanda(_) => {
                panic!("publisher() is only available for the capturing harness")
            }
        }
    }

    pub async fn enqueue<P>(&self, evt: &Envelope<P>) -> Result<()>
    where
        P: KafkaMessage + Serialize,
    {
        let cfg = self.ensure_message::<P>().await?;
        let mut tx = self.pool.begin().await?;
        enqueue(&mut tx, &cfg, evt).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn relay_once<P>(&self) -> Result<RelayStats>
    where
        P: KafkaMessage + Serialize,
    {
        let cfg = self.ensure_message::<P>().await?;
        let table = OutboxTable::for_message::<P>(&cfg)?;
        let stats =
            kafkaman_worker::relay_once(&self.pool, &self.publisher, &table, &cfg.relay).await?;
        Ok(stats)
    }

    pub async fn assert_status<P>(&self, message_id: Uuid, expected: OutboxStatus) -> Result<()>
    where
        P: KafkaMessage + Serialize,
    {
        let row = self.outbox_row::<P>(message_id).await?;
        if row.status == expected {
            Ok(())
        } else {
            Err(Error::UnexpectedStatus {
                message_id,
                expected,
                actual: row.status,
            })
        }
    }

    pub async fn outbox_row<P>(&self, message_id: Uuid) -> Result<OutboxRow>
    where
        P: KafkaMessage + Serialize,
    {
        let cfg = self.ensure_message::<P>().await?;
        let table = OutboxTable::for_message::<P>(&cfg)?;
        outbox_row(&self.pool, &table, message_id)
            .await?
            .ok_or(Error::MissingRow(message_id))
    }

    pub fn published_on(&self, topic: &str) -> Vec<PublishedRecord> {
        self.publisher().records_on(topic)
    }

    pub async fn outbox_table<P>(&self) -> Result<OutboxTable>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_message::<P>().await?;
        Ok(OutboxTable::for_message::<P>(&cfg)?)
    }

    pub async fn received_table<P>(&self) -> Result<ReceivedTable>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_received_message::<P>().await?;
        Ok(ReceivedTable::for_message::<P>(&cfg)?)
    }

    pub async fn insert_received<P>(
        &self,
        evt: &Envelope<P>,
        source_partition: i32,
        source_offset: i64,
        key: Option<&[u8]>,
    ) -> Result<bool>
    where
        P: KafkaMessage + Serialize,
    {
        let cfg = self.ensure_received_message::<P>().await?;
        let mut tx = self.pool.begin().await?;
        let inserted =
            insert_received(&mut tx, &cfg, evt, source_partition, source_offset, key).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn received_row_by_idempotency_key<P>(
        &self,
        idempotency_key: &str,
    ) -> Result<ReceivedRow>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_received_message::<P>().await?;
        let table = ReceivedTable::for_message::<P>(&cfg)?;
        received_row_by_idempotency_key(&self.pool, &table, idempotency_key)
            .await?
            .ok_or_else(|| Error::MissingReceivedRow(idempotency_key.to_owned()))
    }

    async fn ensure_message<P>(&self) -> Result<ResolvedConfig>
    where
        P: KafkaMessage,
    {
        let descriptor = P::descriptor()?;
        // Hold this for the whole check-register-migrate sequence. The guard is
        // only released once migration has committed, so no concurrent caller
        // can see the type registered before its table exists.
        let _guard = self.registration_lock.lock().await;
        let next = {
            let mut cfg = self.cfg.lock().expect("harness config mutex poisoned");
            if !cfg
                .messages()
                .iter()
                .any(|candidate| candidate.message_type == descriptor.message_type)
            {
                *cfg = cfg.clone().with_message(descriptor);
            }
            cfg.clone()
        };
        let report = migrate(
            &self.pool,
            &next,
            &MigrationContext::default(),
            &changesets_for(&next),
        )
        .await?;
        *self
            .last_migration_report
            .lock()
            .expect("harness migration report mutex poisoned") = report;
        Ok(next)
    }

    async fn ensure_received_message<P>(&self) -> Result<ResolvedConfig>
    where
        P: KafkaMessage,
    {
        let descriptor = P::descriptor()?;
        let _guard = self.registration_lock.lock().await;
        let next = {
            let mut cfg = self.cfg.lock().expect("harness config mutex poisoned");
            if !cfg
                .messages()
                .iter()
                .any(|candidate| candidate.message_type == descriptor.message_type)
            {
                *cfg = cfg.clone().with_message(descriptor);
            }
            cfg.clone()
        };
        let report = migrate(
            &self.pool,
            &next,
            &MigrationContext::default(),
            &received_changesets_for(&next),
        )
        .await?;
        *self
            .last_migration_report
            .lock()
            .expect("harness migration report mutex poisoned") = report;
        Ok(next)
    }
}

fn changesets_for(cfg: &ResolvedConfig) -> Vec<Box<dyn kafkaman_sqlx::Changeset>> {
    let mut changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
    for (idx, descriptor) in cfg.messages().iter().cloned().enumerate() {
        changesets.push(Box::new(CreateOutboxTable::new(idx as i64 + 2, descriptor)));
    }
    changesets
}

fn received_changesets_for(cfg: &ResolvedConfig) -> Vec<Box<dyn kafkaman_sqlx::Changeset>> {
    let mut changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
    for (idx, descriptor) in cfg.messages().iter().cloned().enumerate() {
        changesets.push(Box::new(CreateReceivedTable::new(
            10_000 + idx as i64,
            descriptor,
        )));
    }
    changesets
}
