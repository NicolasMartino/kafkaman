//! The recovery entry point: resync and redrive from a declared source list.
//!
//! Behind `redpanda` because a resync verifies its topics on a real broker
//! before it touches storage.
#![cfg(feature = "redpanda")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod redrive;
mod resync;
mod selection;

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use durable_send_tests::{
    postgres, redpanda, KeylessProduct, ProductSnapshot, RegionalProduct, TestPostgres, TestResult,
};
use kafkaman::config::Config;
use kafkaman::recovery::{Recovery, RecoveryError, RecoveryInputs, Report, Source, SourceProgress};
use kafkaman_core::{Envelope, KafkaMessage, MessageDescriptor, TopicMode};
use kafkaman_rdkafka::{converge_topics, TopicAdmin};
use kafkaman_sqlx::{
    migrate, InitSchema, MigrationContext, OutboxTable, ResolvedConfig, ResyncFuture,
    ResyncProgress, Role, RoleRegistry,
};
use kafkaman_test::EnvelopeTestExt;
use sqlx::{PgConnection, PgPool};
use testcontainers::{ContainerAsync, GenericImage};
use uuid::Uuid;

/// The relay settings every resolved file needs.
const RELAY: &str = "[relay]
worker_id = \"recovery-test\"
batch_limit = 100
lease_for = \"30s\"
retry_after = \"1s\"
poll_interval = \"100ms\"
";

/// Retry defaults, which a `[retry.messages.*]` override needs beside it.
const RETRY_DEFAULTS: &str = "[retry.defaults]
max_attempts = 5
initial_backoff = \"500ms\"
max_backoff = \"30s\"
multiplier = 2.0
errors_limit = 16
dlq = \"table\"
";

/// A database with a fresh kafkaman schema, and the config that names it.
struct Fixture {
    _postgres: TestPostgres,
    pool: PgPool,
    schema: String,
}

impl Fixture {
    async fn start() -> TestResult<Self> {
        let postgres = postgres().await?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(postgres.url())
            .await?;
        let schema = format!("recovery_{}", Uuid::new_v4().simple());
        sqlx::raw_sql(
            "CREATE TABLE product_state (id text PRIMARY KEY, name text NOT NULL);
             CREATE TABLE regional_state (id text PRIMARY KEY, name text NOT NULL);
             CREATE TABLE keyless_state (id text PRIMARY KEY, name text NOT NULL);
             CREATE TABLE recovery_fault (key text PRIMARY KEY, kind text NOT NULL)",
        )
        .execute(&pool)
        .await?;
        Ok(Self {
            _postgres: postgres,
            pool,
            schema,
        })
    }

    /// The service file, with resync pages of two.
    fn config(&self) -> Config {
        self.config_with("[recovery]\nbatch_size = 2\n")
    }

    fn config_with(&self, extra: &str) -> Config {
        Config::parse(&format!(
            "{extra}[database]\nschema = \"{}\"\n{RELAY}",
            self.schema
        ))
        .unwrap()
    }

    fn resolved(&self) -> ResolvedConfig {
        ResolvedConfig::from_config(Some(&self.config()), [products(), regional(), keyless()])
            .unwrap()
    }

    async fn seed(&self, table: &str, rows: &[(&str, &str)]) -> TestResult {
        for (id, name) in rows {
            sqlx::query(&format!(
                "INSERT INTO {table} VALUES ($1, $2)
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
            ))
            .bind(id)
            .bind(name)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn fault(&self, key: &str, kind: &str) -> TestResult {
        sqlx::query("INSERT INTO recovery_fault VALUES ($1, $2)")
            .bind(key)
            .bind(kind)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn clear_faults(&self) -> TestResult {
        sqlx::query("DELETE FROM recovery_fault")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Whether `P`'s outbox table exists.
    async fn outbox_exists<P: KafkaMessage>(&self) -> TestResult<bool> {
        let table = OutboxTable::for_message::<P>(&self.resolved())?;
        Ok(
            sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
                .bind(table.qualified_name())
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// `(entity_key, name, idempotency_key)` for every outbox row, in enqueue
    /// order.
    async fn outbox<P: KafkaMessage>(&self) -> TestResult<Vec<(String, String, String)>> {
        let table = OutboxTable::for_message::<P>(&self.resolved())?;
        Ok(sqlx::query_as(&format!(
            "SELECT entity_key, payload->>'name', idempotency_key FROM {}
             ORDER BY enqueue_order",
            table.qualified_name()
        ))
        .fetch_all(&self.pool)
        .await?)
    }

    /// Tasks alive once every pool connection is back idle: the pool hands a
    /// connection back on a short task of its own.
    async fn settled_tasks(&self) -> TestResult<usize> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.pool.num_idle() < self.pool.size() as usize {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        Ok(tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks())
    }

    /// Changesets applied beyond the baseline schema.
    async fn applied_changesets(&self) -> TestResult<i64> {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("\"{}\".changelog_history", self.schema))
            .fetch_one(&self.pool)
            .await?;
        if !exists {
            return Ok(0);
        }
        Ok(sqlx::query_scalar(&format!(
            "SELECT count(*) FROM \"{}\".changelog_history WHERE version <> 1",
            self.schema
        ))
        .fetch_one(&self.pool)
        .await?)
    }

    /// Lay down the baseline schema without any message tables.
    async fn init_schema(&self) -> TestResult {
        let changesets: Vec<Box<dyn kafkaman_sqlx::Changeset>> = vec![Box::new(InitSchema)];
        migrate(
            &self.pool,
            &self.resolved(),
            &MigrationContext::default(),
            &changesets,
        )
        .await?;
        Ok(())
    }

    /// Received and cache tables for `descriptor`, as a handler migrates them.
    async fn migrate_handler(&self, descriptor: MessageDescriptor) -> TestResult {
        let cfg = self.resolved();
        let resolved = cfg
            .messages()
            .iter()
            .find(|declared| declared.message_type == descriptor.message_type)
            .cloned()
            .unwrap();
        let mut roles = RoleRegistry::new();
        roles.declare(resolved, Role::Handle)?;
        migrate(
            &self.pool,
            &cfg,
            &MigrationContext::default(),
            &roles.changelog()?,
        )
        .await?;
        Ok(())
    }

    async fn run(
        &self,
        recovery: &mut Recovery,
        brokers: Option<&str>,
        inputs: RecoveryInputs,
    ) -> Result<Report, RecoveryError> {
        self.run_with(&self.config(), recovery, brokers, inputs)
            .await
    }

    async fn run_with(
        &self,
        config: &Config,
        recovery: &mut Recovery,
        brokers: Option<&str>,
        inputs: RecoveryInputs,
    ) -> Result<Report, RecoveryError> {
        let request = recovery.request_from(config, inputs).unwrap();
        recovery
            .run(
                &self.pool,
                config,
                brokers,
                &MigrationContext::default(),
                request,
            )
            .await
    }
}

/// Two resync sources around a received one, in declaration order.
fn mixed() -> Recovery {
    Recovery::new(vec![
        product_source("products"),
        Source::received::<KeylessProduct>("keyless"),
        regional_source("regional"),
    ])
    .unwrap()
}

/// The pacing variables the Job read before the file carried its pacing, set
/// for as long as the guard lives. One guard at a time, so tests that set
/// them do not see each other's values.
struct LegacyPacing(#[allow(dead_code)] MutexGuard<'static, ()>);

impl LegacyPacing {
    fn set(batch_size: &str, pause_ms: &str) -> Self {
        static LOCK: Mutex<()> = Mutex::new(());
        let guard = LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("REPUBLISH_BATCH_SIZE", batch_size);
        std::env::set_var("REPUBLISH_PAUSE_MS", pause_ms);
        Self(guard)
    }
}

impl Drop for LegacyPacing {
    fn drop(&mut self) {
        std::env::remove_var("REPUBLISH_BATCH_SIZE");
        std::env::remove_var("REPUBLISH_PAUSE_MS");
    }
}

/// Start a stopped broker again and wait until it answers for `topics`.
async fn restart(
    broker: &ContainerAsync<GenericImage>,
    brokers: &str,
    topics: &[MessageDescriptor],
) -> TestResult {
    broker.start().await?;
    let admin = TopicAdmin::from_brokers(brokers)?;
    tokio::time::timeout(Duration::from_secs(60), async {
        while converge_topics(&admin, TopicMode::Verify, &prefixed(topics))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await?;
    Ok(())
}

/// Relay `P`'s outbox until `rows` records are on the broker.
async fn relay<P: KafkaMessage + serde::Serialize>(
    fixture: &Fixture,
    brokers: &str,
    rows: usize,
) -> TestResult {
    let cfg = fixture.resolved();
    let table = OutboxTable::for_message::<P>(&cfg)?;
    let publisher = kafkaman_rdkafka::RdkafkaPublisher::from_brokers(brokers)?;
    let mut published = 0;
    tokio::time::timeout(Duration::from_secs(60), async {
        while published < rows {
            let stats =
                kafkaman_worker::relay_once(&fixture.pool, &publisher, &table, &cfg.relay).await?;
            published += stats.published;
            if stats.published == 0 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        Ok::<_, durable_send_tests::BoxError>(())
    })
    .await??;
    Ok(())
}

/// A broker on which `topics` already exist, as an operator provisions them.
/// `topics` as a run resolves them, under any `KAFKAMAN_TOPICS_PREFIX`.
fn prefixed(topics: &[MessageDescriptor]) -> Vec<MessageDescriptor> {
    let file = Config::parse(&format!("[database]\nschema = \"kafkaman\"\n{RELAY}")).unwrap();
    ResolvedConfig::from_config(Some(&file), topics.to_vec())
        .unwrap()
        .messages()
        .to_vec()
}

async fn broker_with(
    topics: &[MessageDescriptor],
) -> TestResult<(ContainerAsync<GenericImage>, String)> {
    let (container, brokers) = redpanda().await?;
    let admin = TopicAdmin::from_brokers(&brokers)?;
    for topic in prefixed(topics) {
        admin
            .create(&topic.topic, 1, None, &topic.topic_spec)
            .await?;
    }
    Ok((container, brokers))
}

fn products() -> MessageDescriptor {
    ProductSnapshot::descriptor().unwrap()
}

fn regional() -> MessageDescriptor {
    RegionalProduct::descriptor().unwrap()
}

fn keyless() -> MessageDescriptor {
    KeylessProduct::descriptor().unwrap()
}

fn keys_from<'a>(
    table: &'static str,
    conn: &'a mut PgConnection,
    after: Option<&'a str>,
    limit: usize,
) -> ResyncFuture<'a, Vec<String>> {
    Box::pin(async move {
        Ok(sqlx::query_scalar(&format!(
            "SELECT id FROM {table} WHERE ($1::text IS NULL OR id COLLATE \"C\" > $1)
             ORDER BY id COLLATE \"C\" LIMIT $2"
        ))
        .bind(after)
        .bind(i64::try_from(limit).unwrap())
        .fetch_all(conn)
        .await?)
    })
}

/// Current state for `key`, or a planted fault: `mismatch` returns another
/// entity's envelope, `error` fails the query, `missing` finds nothing.
async fn current(
    table: &str,
    conn: &mut PgConnection,
    key: &str,
) -> kafkaman_sqlx::Result<Option<(String, String)>> {
    let fault: Option<String> =
        sqlx::query_scalar("SELECT kind FROM recovery_fault WHERE key = $1")
            .bind(key)
            .fetch_optional(&mut *conn)
            .await?;
    match fault.as_deref() {
        Some("error") => {
            sqlx::query("SELECT 1 / 0").execute(&mut *conn).await?;
            unreachable!("the planted fault fails the query");
        }
        Some("mismatch") => return Ok(Some(("someone-else".to_owned(), "wrong".to_owned()))),
        Some("missing") => return Ok(None),
        _ => {}
    }
    let name: Option<String> =
        sqlx::query_scalar(&format!("SELECT name FROM {table} WHERE id = $1"))
            .bind(key)
            .fetch_optional(conn)
            .await?;
    Ok(name.map(|name| (key.to_owned(), name)))
}

fn product_source(name: &str) -> Source {
    Source::resync::<ProductSnapshot, _, _>(
        name,
        |conn, after, limit| keys_from("product_state", conn, after, limit),
        |conn, key, run_id| {
            Box::pin(async move {
                Ok(current("product_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        ProductSnapshot::envelope(&id, &name)
                            .with_idempotency_key(format!("product:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )
}

fn regional_source(name: &str) -> Source {
    Source::resync::<RegionalProduct, _, _>(
        name,
        |conn, after, limit| keys_from("regional_state", conn, after, limit),
        |conn, key, run_id| {
            Box::pin(async move {
                Ok(current("regional_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        Envelope::new(RegionalProduct {
                            product_id: id.clone(),
                            region: "eu".to_owned(),
                            name,
                        })
                        .with_idempotency_key(format!("regional:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )
}

fn keyless_source(name: &str) -> Source {
    Source::resync::<KeylessProduct, _, _>(
        name,
        |conn, after, limit| keys_from("keyless_state", conn, after, limit),
        |conn, key, run_id| {
            Box::pin(async move {
                Ok(current("keyless_state", conn, key)
                    .await?
                    .map(|(id, name)| {
                        KeylessProduct::envelope(&id, &name)
                            .with_idempotency_key(format!("keyless:{id}:recovery:{run_id}"))
                    }))
            })
        },
    )
}

fn resync_inputs(source: Option<&str>, start_after: Option<&str>) -> RecoveryInputs {
    RecoveryInputs {
        source: source.map(str::to_owned),
        start_after: start_after.map(str::to_owned),
        ..RecoveryInputs::default()
    }
}

fn resync_progress(enqueued: u64, skipped: u64, last_key: Option<&str>) -> SourceProgress {
    SourceProgress::Resync(ResyncProgress {
        enqueued,
        skipped,
        last_key: last_key.map(str::to_owned),
    })
}

/// The `kafkaman` events a scope emitted, each as `{"fields": {...}}`.
#[derive(Clone, Default)]
struct Events(Sink);

impl Events {
    /// Capture events on this thread until the guard drops. The tests run on
    /// the current-thread runtime, so every await stays on it. One subscriber
    /// stays installed for the whole binary: a per-test default can lose a
    /// callsite another test's thread registers at the same moment.
    fn capture(&self) -> CaptureGuard {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture))
                .expect("no other global subscriber in this binary");
        });
        SINK.with(|sink| *sink.borrow_mut() = Some(self.0.clone()));
        CaptureGuard
    }

    fn messages(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| event["fields"]["message"].as_str().map(str::to_owned))
            .collect()
    }

    fn named(&self, message: &str) -> Vec<serde_json::Value> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event["fields"]["message"] == message)
            .cloned()
            .collect()
    }
}

type Sink = Arc<Mutex<Vec<serde_json::Value>>>;

thread_local! {
    static SINK: std::cell::RefCell<Option<Sink>> = const { std::cell::RefCell::new(None) };
}

struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        SINK.with(|sink| *sink.borrow_mut() = None);
    }
}

struct Capture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if !event.metadata().target().starts_with("kafkaman") {
            return;
        }
        let Some(sink) = SINK.with(|sink| sink.borrow().clone()) else {
            return;
        };
        let mut fields = Fields::default();
        event.record(&mut fields);
        sink.lock()
            .unwrap()
            .push(serde_json::json!({ "fields": fields.0 }));
    }
}

#[derive(Default)]
struct Fields(serde_json::Map<String, serde_json::Value>);

impl tracing::field::Visit for Fields {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), format!("{value:?}").into());
    }
}
