use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kafkaman_core::{
    ClaimedOutboxRow, Envelope, KafkaMessage, OutboxRow, OutboxStatus, PublishAck, PublishedRecord,
    RelayStats, SqlIdentifier,
};
use kafkaman_sqlx::{
    enqueue, migrate, outbox_row, CreateOutboxTable, InitSchema, OutboxTable, ResolvedConfig,
};
use kafkaman_worker::{BoxError, Publisher};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

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

    #[error("outbox row `{0}` was not found")]
    MissingRow(Uuid),

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
}

pub struct Harness {
    pool: PgPool,
    cfg: Arc<Mutex<ResolvedConfig>>,
    publisher: CapturingPublisher,
}

impl Harness {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
        let cfg = ResolvedConfig::new(schema);
        let changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
        migrate(&pool, &cfg, &changesets).await?;

        Ok(Self {
            pool,
            cfg: Arc::new(Mutex::new(cfg)),
            publisher: CapturingPublisher::default(),
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

    pub fn publisher(&self) -> CapturingPublisher {
        self.publisher.clone()
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
        self.publisher.records_on(topic)
    }

    pub async fn outbox_table<P>(&self) -> Result<OutboxTable>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_message::<P>().await?;
        Ok(OutboxTable::for_message::<P>(&cfg)?)
    }

    async fn ensure_message<P>(&self) -> Result<ResolvedConfig>
    where
        P: KafkaMessage,
    {
        let descriptor = P::descriptor()?;
        let needs_migration = {
            let mut cfg = self.cfg.lock().expect("harness config mutex poisoned");
            if cfg
                .messages()
                .iter()
                .any(|candidate| candidate.message_type == descriptor.message_type)
            {
                false
            } else {
                let next = cfg.clone().with_message(descriptor);
                *cfg = next;
                true
            }
        };

        let cfg = self
            .cfg
            .lock()
            .expect("harness config mutex poisoned")
            .clone();
        if needs_migration {
            migrate(&self.pool, &cfg, &changesets_for(&cfg)).await?;
        }
        Ok(cfg)
    }
}

fn changesets_for(cfg: &ResolvedConfig) -> Vec<Box<dyn kafkaman_sqlx::Changeset>> {
    let mut changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
    for (idx, descriptor) in cfg.messages().iter().cloned().enumerate() {
        changesets.push(Box::new(CreateOutboxTable::new(idx as i64 + 2, descriptor)));
    }
    changesets
}
