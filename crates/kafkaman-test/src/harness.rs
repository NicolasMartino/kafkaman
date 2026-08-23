use std::sync::{Arc, Mutex};

use kafkaman_core::{
    Envelope, IntoIdempotencyIdentity, KafkaMessage, OutboxRow, OutboxStatus, ReceivedRow,
    RelayStats, SqlIdentifier,
};
use kafkaman_sqlx::{
    enqueue, insert_received, migrate, outbox_row, received_row_by_idempotency_key, Changeset,
    CreateCacheTable, CreateOutboxTable, CreateReceivedTable, InitSchema, MigrationContext,
    MigrationReport, OutboxTable, ReceivedTable, ResolvedConfig,
};
use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::publisher::{CapturingPublisher, HarnessPublisher};
use crate::{Error, Result};

/// A kafkaman deployment in a schema of its own, migrating itself on demand.
///
/// Message types register lazily: the first call that needs a type's tables
/// registers it and runs the changesets that create them. That keeps a test to
/// the types it actually uses instead of a fixed changelog every test pays for.
#[derive(Debug)]
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

/// Which changesets a lazily-registered message type needs.
///
/// The two sides are separate because a test that only sends does not want a
/// received table, and one that only receives does not want an outbox — and
/// migrating both would double every test's schema work.
#[derive(Clone, Copy)]
enum Tables {
    Outbox,
    ReceivedAndCache,
}

impl Tables {
    /// Version numbers are per side and far apart, so registering a type for
    /// sending and later for receiving cannot collide in `changelog_history`.
    fn changesets(self, cfg: &ResolvedConfig) -> Vec<Box<dyn Changeset>> {
        let mut changesets: Vec<Box<dyn Changeset>> = vec![Box::new(InitSchema)];
        for (idx, descriptor) in cfg.messages().iter().cloned().enumerate() {
            let idx = idx as i64;
            match self {
                Tables::Outbox => {
                    changesets.push(Box::new(CreateOutboxTable::new(idx + 2, descriptor)));
                }
                Tables::ReceivedAndCache => {
                    let base = 10_000 + idx * 2;
                    changesets.push(Box::new(CreateReceivedTable::new(base, descriptor.clone())));
                    changesets.push(Box::new(CreateCacheTable::new(base + 1, descriptor)));
                }
            }
        }
        changesets
    }
}

impl Harness {
    /// Connect to `database_url` in a fresh, uniquely-named schema.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
        Self::start(
            database_url,
            ResolvedConfig::new(schema),
            HarnessPublisher::Capturing(CapturingPublisher::default()),
        )
        .await
    }

    /// Connect using a caller-supplied config, for tests about configuration
    /// itself.
    pub async fn connect_with_config(
        database_url: &str,
        config: kafkaman_config::Config,
    ) -> Result<Self> {
        let cfg = ResolvedConfig::from_config(Some(&config), std::iter::empty())?;
        Self::start(
            database_url,
            cfg,
            HarnessPublisher::Capturing(CapturingPublisher::default()),
        )
        .await
    }

    /// Connect a harness that publishes to a real Redpanda/Kafka broker via
    /// [`kafkaman_rdkafka::RdkafkaPublisher`]. Assertions about what was
    /// published must come from a broker consumer, not from captured records.
    #[cfg(feature = "redpanda")]
    pub async fn connect_redpanda(database_url: &str, brokers: &str) -> Result<Self> {
        let schema = SqlIdentifier::new(format!("kafkaman_test_{}", Uuid::new_v4().simple()))?;
        let publisher = kafkaman_rdkafka::RdkafkaPublisher::from_brokers(brokers)?;
        Self::start(
            database_url,
            ResolvedConfig::new(schema),
            HarnessPublisher::Redpanda(publisher),
        )
        .await
    }

    /// Open the pool and lay down the version-1 baseline.
    ///
    /// The one place the three constructors converge, so they cannot drift on
    /// what a freshly-connected harness has already migrated.
    async fn start(
        database_url: &str,
        cfg: ResolvedConfig,
        publisher: HarnessPublisher,
    ) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        let changesets: Vec<Box<dyn Changeset>> = vec![Box::new(InitSchema)];
        let report = migrate(&pool, &cfg, &MigrationContext::default(), &changesets).await?;

        Ok(Self {
            pool,
            cfg: Arc::new(Mutex::new(cfg)),
            last_migration_report: Arc::new(Mutex::new(report)),
            publisher,
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

    /// The capturing publisher backing this harness, or
    /// [`Error::NotCapturing`] on a Redpanda harness, where output must be
    /// asserted through a broker consumer.
    pub fn try_publisher(&self) -> Result<CapturingPublisher> {
        match &self.publisher {
            HarnessPublisher::Capturing(publisher) => Ok(publisher.clone()),
            #[cfg(feature = "redpanda")]
            HarnessPublisher::Redpanda(_) => Err(Error::NotCapturing),
        }
    }

    /// [`Self::try_publisher`], panicking on a Redpanda harness.
    ///
    /// # Panics
    /// If this harness publishes to a real broker.
    pub fn publisher(&self) -> CapturingPublisher {
        self.try_publisher()
            .expect("publisher() is only available for the capturing harness")
    }

    pub async fn enqueue<P>(&self, evt: &Envelope<P>) -> Result<()>
    where
        P: KafkaMessage + Serialize,
    {
        let cfg = self.ensure_message::<P>(Tables::Outbox).await?;
        let mut tx = self.pool.begin().await?;
        enqueue(&mut tx, &cfg, evt).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn relay_once<P>(&self) -> Result<RelayStats>
    where
        P: KafkaMessage + Serialize,
    {
        let cfg = self.ensure_message::<P>(Tables::Outbox).await?;
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
        let cfg = self.ensure_message::<P>(Tables::Outbox).await?;
        let table = OutboxTable::for_message::<P>(&cfg)?;
        outbox_row(&self.pool, &table, message_id)
            .await?
            .ok_or(Error::MissingRow(message_id))
    }

    pub fn published_on(&self, topic: &str) -> Vec<kafkaman_core::PublishedRecord> {
        self.publisher().records_on(topic)
    }

    pub async fn outbox_table<P>(&self) -> Result<OutboxTable>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_message::<P>(Tables::Outbox).await?;
        Ok(OutboxTable::for_message::<P>(&cfg)?)
    }

    pub async fn received_table<P>(&self) -> Result<ReceivedTable>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_message::<P>(Tables::ReceivedAndCache).await?;
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
        let cfg = self.ensure_message::<P>(Tables::ReceivedAndCache).await?;
        let mut tx = self.pool.begin().await?;
        let inserted =
            insert_received(&mut tx, &cfg, evt, source_partition, source_offset, key).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    pub async fn received_row_by_idempotency_key<P>(
        &self,
        idempotency_key: impl IntoIdempotencyIdentity,
    ) -> Result<ReceivedRow>
    where
        P: KafkaMessage,
    {
        let cfg = self.ensure_message::<P>(Tables::ReceivedAndCache).await?;
        let table = ReceivedTable::for_message::<P>(&cfg)?;
        let identity = idempotency_key.into_idempotency_identity()?;
        let key = identity.key;
        received_row_by_idempotency_key(&self.pool, &table, key)
            .await?
            .ok_or_else(|| Error::MissingReceivedRow {
                key_source: identity
                    .source
                    .as_ref()
                    .map_or_else(|| "<none>".to_owned(), |source| source.value().to_string()),
                digest: key.to_hex(),
            })
    }

    /// Register `P` if it is new, then migrate the tables `tables` names.
    ///
    /// One implementation for both sides rather than two near-identical ones:
    /// the registration and locking are the delicate part, and having them
    /// written twice is how the outbox side would get a fix the received side
    /// did not.
    async fn ensure_message<P>(&self, tables: Tables) -> Result<ResolvedConfig>
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
            // `try_with_message`, not `with_message`: re-registering one
            // `message_type` under a second topic is a configuration error that
            // production rejects, and it is already idempotent for an identical
            // descriptor. The infallible form here silently kept the first topic
            // — so a test that registered a type twice by mistake would pass
            // while publishing to a topic it never named, which is the one place
            // that failure must not be quiet.
            *cfg = cfg.clone().try_with_message(descriptor)?;
            cfg.clone()
        };

        let report = migrate(
            &self.pool,
            &next,
            &MigrationContext::default(),
            &tables.changesets(&next),
        )
        .await?;
        *self
            .last_migration_report
            .lock()
            .expect("harness migration report mutex poisoned") = report;
        Ok(next)
    }
}
