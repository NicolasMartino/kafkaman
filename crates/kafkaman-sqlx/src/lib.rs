use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use kafkaman_config::{Config, ConfigErrors, ConfigSchema};
use kafkaman_core::{
    ClaimedOutboxRow, Envelope, KafkaMessage, MarkOutcome, MessageDescriptor, OutboxRow,
    OutboxStatus, ReceiveStatus, ReceivedError, ReceivedRow, RelayConfig, SqlIdentifier,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgRow;
use sqlx::{Connection, PgConnection, PgPool, Postgres, Row, Transaction};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

pub use kafkaman_core;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Core(#[from] kafkaman_core::Error),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Config(#[from] kafkaman_config::ConfigError),

    #[error("invalid kafkaman config: {0}")]
    ConfigErrors(#[from] ConfigErrors),

    #[error("changeset versions must be unique and ordered: duplicate version {0}")]
    DuplicateChangesetVersion(i64),

    #[error("changeset versions must be declared in ascending order: {previous} before {next}")]
    DisorderedChangesetVersion { previous: i64, next: i64 },

    #[error(
        "changeset {version} checksum mismatch: history has {stored}, source declares {current}"
    )]
    ChecksumMismatch {
        version: i64,
        stored: String,
        current: String,
    },

    #[error("invalid replay changeset {version}: {message}")]
    InvalidReplay { version: i64, message: String },

    #[error("migration advisory lock is held by another migration; dry-run skipped")]
    MigrationLockBusy,

    #[error("message descriptor `{0}` is not configured")]
    UnknownMessageType(String),

    #[error("envelope header `{0}` is in the reserved `kafkaman-` namespace")]
    ReservedHeader(String),

    #[error("received message must include an idempotency key")]
    MissingIdempotencyKey,

    #[error("no handler registered for message type `{0}`")]
    MissingHandler(String),

    #[error("handler failed: {0}")]
    Handler(String),
}

#[derive(Clone, Debug)]
pub struct ResolvedConfig {
    pub schema: SqlIdentifier,
    pub relay: RelayConfig,
    messages: Vec<MessageDescriptor>,
}

impl ResolvedConfig {
    pub fn new(schema: SqlIdentifier) -> Self {
        Self {
            schema,
            relay: RelayConfig::default(),
            messages: Vec::new(),
        }
    }

    pub fn from_config<I>(cfg: Option<&Config>, messages: I) -> Result<Self>
    where
        I: IntoIterator<Item = MessageDescriptor>,
    {
        let messages = messages.into_iter().collect::<Vec<_>>();
        let Some(cfg) = cfg else {
            if messages.is_empty() {
                return Ok(Self::default());
            }

            return Err(Error::ConfigErrors(ConfigErrors::new(vec![
                kafkaman_config::ConfigIssue::new(
                    "kafkaman.toml",
                    "missing config file for registered kafkaman features",
                ),
            ])));
        };

        let schema = ConfigSchema::new()
            .require::<String>("database.schema")
            .require::<String>("relay.worker_id")
            .require::<i64>("relay.batch_limit")
            .require::<Duration>("relay.lease_for")
            .require::<Duration>("relay.retry_after")
            .require::<Duration>("relay.poll_interval");

        let mut issues = match cfg.validate(&schema) {
            Ok(()) => Vec::new(),
            Err(errors) => errors.into_issues(),
        };

        let schema = match cfg.get::<String>("database.schema") {
            Ok(schema) => Some(schema),
            Err(err) => {
                issues.push(err.into());
                None
            }
        };

        let relay = match cfg.relay() {
            Ok(relay) => match relay.into_relay_config() {
                Ok(relay) => Some(relay),
                Err(message) => {
                    issues.push(kafkaman_config::ConfigIssue::new("relay", message));
                    None
                }
            },
            Err(err) => {
                issues.push(err.into());
                None
            }
        };

        // Validate the optional retry/DLQ policy on the same boot path that opens
        // and migrates the database, so a bad policy is rejected before any DB
        // work. Runtime retry processing is M4 scope, but the config contract is
        // enforced here. Only message types registered with this resolver may
        // carry per-message overrides.
        if cfg.contains("retry") {
            let registered = messages
                .iter()
                .map(|descriptor| descriptor.message_type.as_str().to_owned())
                .collect::<Vec<_>>();
            if let Err(retry_errors) = cfg.retry_config(registered) {
                issues.extend(retry_errors.into_issues());
            }
        }

        if !issues.is_empty() {
            return Err(Error::ConfigErrors(ConfigErrors::new(issues)));
        }

        let schema = SqlIdentifier::new(schema.expect("schema was validated"))?;
        let mut resolved = Self::new(schema).with_relay(relay.expect("relay was validated"));
        for message in messages {
            resolved = resolved.with_message(message);
        }
        Ok(resolved)
    }

    /// Register a message type's outbox table. Registering the same
    /// `message_type` more than once is idempotent: the first descriptor wins
    /// and later ones are ignored, so a duplicate cannot generate a second
    /// changeset targeting the same outbox table/index names.
    pub fn with_message(mut self, descriptor: MessageDescriptor) -> Self {
        let already_registered = self
            .messages
            .iter()
            .any(|existing| existing.message_type == descriptor.message_type);
        if !already_registered {
            self.messages.push(descriptor);
        }
        self
    }

    pub fn with_relay(mut self, relay: RelayConfig) -> Self {
        self.relay = relay;
        self
    }

    pub fn schema(&self) -> &SqlIdentifier {
        &self.schema
    }

    pub fn messages(&self) -> &[MessageDescriptor] {
        &self.messages
    }

    pub fn descriptor_for<P: KafkaMessage>(&self) -> Result<MessageDescriptor> {
        let descriptor = P::descriptor()?;
        if self
            .messages
            .iter()
            .any(|configured| configured.message_type == descriptor.message_type)
        {
            Ok(descriptor)
        } else {
            Err(Error::UnknownMessageType(
                descriptor.message_type.as_str().to_owned(),
            ))
        }
    }
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        Self::new(SqlIdentifier::new("kafkaman").expect("default schema is valid"))
    }
}

#[derive(Clone, Debug)]
pub struct OutboxTable {
    pub schema: SqlIdentifier,
    pub table: SqlIdentifier,
    pub descriptor: MessageDescriptor,
}

impl OutboxTable {
    pub fn new(schema: SqlIdentifier, descriptor: MessageDescriptor) -> Result<Self> {
        let table = SqlIdentifier::new(format!("outbox_{}", descriptor.message_type.as_str()))?;
        Ok(Self {
            schema,
            table,
            descriptor,
        })
    }

    pub fn for_message<P: KafkaMessage>(cfg: &ResolvedConfig) -> Result<Self> {
        Self::new(cfg.schema.clone(), cfg.descriptor_for::<P>()?)
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema.quoted(), self.table.quoted())
    }

    fn state_index_name(&self) -> SqlIdentifier {
        let base = self.table.as_str();
        let middle_limit = SqlIdentifier::MAX_LEN - "idx_".len() - "_state".len();
        let middle = if base.len() <= middle_limit {
            base.to_owned()
        } else {
            // Two distinct long table names can share a 53-char prefix; a plain
            // truncation would collide on the same index name. Append a short
            // deterministic hash of the full name so they stay distinct. (Table
            // names are validated ASCII, so byte slicing is on a char boundary.)
            let hash = format!("{:08x}", advisory_lock_key(base) as u32);
            let keep = middle_limit - hash.len() - 1;
            format!("{}_{}", &base[..keep], hash)
        };
        SqlIdentifier::new(format!("idx_{middle}_state")).expect("index name is generated valid")
    }
}

#[derive(Clone, Debug)]
pub struct ReceivedTable {
    pub schema: SqlIdentifier,
    pub table: SqlIdentifier,
    pub descriptor: MessageDescriptor,
}

impl ReceivedTable {
    pub fn new(schema: SqlIdentifier, descriptor: MessageDescriptor) -> Result<Self> {
        let table = SqlIdentifier::new(format!("received_{}", descriptor.message_type.as_str()))?;
        Ok(Self {
            schema,
            table,
            descriptor,
        })
    }

    pub fn for_message<P: KafkaMessage>(cfg: &ResolvedConfig) -> Result<Self> {
        Self::new(cfg.schema.clone(), cfg.descriptor_for::<P>()?)
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema.quoted(), self.table.quoted())
    }

    fn index_name(&self, suffix: &str) -> SqlIdentifier {
        let base = self.table.as_str();
        let prefix = "idx_";
        let middle_limit = SqlIdentifier::MAX_LEN - prefix.len() - suffix.len();
        let middle = if base.len() <= middle_limit {
            base.to_owned()
        } else {
            let hash = format!("{:08x}", advisory_lock_key(base) as u32);
            let keep = middle_limit - hash.len() - 1;
            format!("{}_{}", &base[..keep], hash)
        };
        SqlIdentifier::new(format!("{prefix}{middle}{suffix}"))
            .expect("index name is generated valid")
    }

    fn idempotency_index_name(&self) -> SqlIdentifier {
        self.index_name("_idempotency")
    }

    fn state_index_name(&self) -> SqlIdentifier {
        self.index_name("_state")
    }
}

pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

type ErasedHandler = Arc<dyn ErasedMessageHandler>;

trait ErasedMessageHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        payload: serde_json::Value,
    ) -> HandlerFuture<'a>;
}

struct TypedMessageHandler<P, F> {
    handler: F,
    _message: std::marker::PhantomData<fn(P)>,
}

impl<P, F> ErasedMessageHandler for TypedMessageHandler<P, F>
where
    P: DeserializeOwned + Send + 'static,
    F: for<'a> Fn(&'a mut PgConnection, P) -> HandlerFuture<'a> + Send + Sync + 'static,
{
    fn handle<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        payload: serde_json::Value,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            let message = serde_json::from_value(payload)?;
            (self.handler)(conn, message).await
        })
    }
}

#[derive(Clone, Default)]
pub struct MessageRouter {
    handlers: BTreeMap<String, ErasedHandler>,
}

impl MessageRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handler<P>(
        mut self,
        handler: impl for<'a> Fn(&'a mut PgConnection, P) -> HandlerFuture<'a> + Send + Sync + 'static,
    ) -> Self
    where
        P: KafkaMessage + DeserializeOwned + Send + 'static,
    {
        let descriptor = P::descriptor().expect("KafkaMessage descriptor must be valid");
        self.handlers.insert(
            descriptor.message_type.as_str().to_owned(),
            Arc::new(TypedMessageHandler::<P, _> {
                handler,
                _message: std::marker::PhantomData,
            }),
        );
        self
    }

    fn handler_for(&self, message_type: &str) -> Option<ErasedHandler> {
        self.handlers.get(message_type).cloned()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DispatchStats {
    pub claimed: usize,
    pub processed: usize,
    pub failed: usize,
}

pub struct ChangeBuilder {
    statements: Vec<String>,
}

impl ChangeBuilder {
    pub fn new() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    pub fn push(&mut self, sql: impl Into<String>) {
        self.statements.push(sql.into());
    }

    fn into_statements(self) -> Vec<String> {
        self.statements
    }
}

impl Default for ChangeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationContext {
    contexts: BTreeSet<String>,
    applied_by: String,
}

impl MigrationContext {
    pub fn new() -> Self {
        Self {
            contexts: BTreeSet::new(),
            applied_by: "unknown".to_owned(),
        }
    }

    pub fn from_env() -> Self {
        let mut ctx = Self::new();
        if let Ok(contexts) = std::env::var("KAFKAMAN_CONTEXTS") {
            ctx.contexts = contexts
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }
        if let Ok(applied_by) = std::env::var("KAFKAMAN_APPLIED_BY") {
            let applied_by = applied_by.trim();
            if !applied_by.is_empty() {
                ctx.applied_by = applied_by.to_owned();
            }
        }
        ctx
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.contexts.insert(context.into());
        self
    }

    pub fn with_applied_by(mut self, applied_by: impl Into<String>) -> Self {
        self.applied_by = applied_by.into();
        self
    }

    pub fn applied_by(&self) -> &str {
        &self.applied_by
    }

    fn matches(&self, required: &[String]) -> bool {
        required.is_empty()
            || required
                .iter()
                .any(|required| self.contexts.contains(required))
    }
}

impl Default for MigrationContext {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MigrationReport {
    steps: Vec<MigrationStepReport>,
}

impl MigrationReport {
    pub fn push(&mut self, step: MigrationStepReport) {
        self.steps.push(step);
    }

    pub fn steps(&self) -> &[MigrationStepReport] {
        &self.steps
    }

    pub fn applied_count(&self) -> usize {
        self.steps
            .iter()
            .filter(|step| matches!(step.action, MigrationAction::Applied))
            .count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationStepReport {
    pub version: i64,
    pub name: String,
    pub action: MigrationAction,
    pub preview: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationAction {
    Applied,
    SkippedAlreadyApplied,
    SkippedContext,
    WouldApply,
    ChecksumMismatch { stored: String, current: String },
}

pub trait Changeset: Send + Sync {
    fn version(&self) -> i64;
    fn name(&self) -> &str;
    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()>;

    fn checksum_material(&self) -> String {
        format!("{}:{}", self.version(), self.name())
    }

    fn checksum(&self) -> String {
        stable_checksum(&self.checksum_material())
    }

    fn contexts(&self) -> &[String] {
        &[]
    }

    fn dry_run_preview(&self, cfg: &ResolvedConfig) -> Result<String> {
        let mut builder = ChangeBuilder::new();
        self.build(cfg, &mut builder)?;
        Ok(builder.into_statements().join("\n"))
    }

    fn estimate_replay_count<'a>(
        &'a self,
        _cfg: &'a ResolvedConfig,
        _tx: &'a mut Transaction<'_, Postgres>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<i64>>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }
}

/// Version-1 changelog baseline marker.
///
/// The schema and `changelog_history` table are created by [`migrate`]'s
/// bootstrap step, because they must exist before any changeset can be
/// version-checked. `InitSchema` therefore emits no DDL of its own — it is the
/// single, explicit version-1 entry a changelog records so application
/// changesets can start at version 2. Keeping the DDL solely in the bootstrap
/// avoids two owners of the same schema/history-table definition.
#[derive(Debug)]
pub struct InitSchema;

impl Changeset for InitSchema {
    fn version(&self) -> i64 {
        1
    }

    fn name(&self) -> &str {
        "init_schema"
    }

    fn build(&self, _cfg: &ResolvedConfig, _builder: &mut ChangeBuilder) -> Result<()> {
        Ok(())
    }

    fn checksum_material(&self) -> String {
        format!("version={};name={}", self.version(), self.name())
    }
}

#[derive(Clone, Debug)]
pub struct CreateOutboxTable {
    pub version: i64,
    pub descriptor: MessageDescriptor,
}

impl CreateOutboxTable {
    pub fn new(version: i64, descriptor: MessageDescriptor) -> Self {
        Self {
            version,
            descriptor,
        }
    }
}

impl Changeset for CreateOutboxTable {
    fn version(&self) -> i64 {
        self.version
    }

    fn name(&self) -> &str {
        "create_outbox_table"
    }

    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
        let table = OutboxTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        builder.push(create_outbox_table_sql(&table));
        builder.push(create_outbox_state_index_sql(&table));
        Ok(())
    }

    fn checksum_material(&self) -> String {
        format!(
            "version={};name={};message_type={};topic={}",
            self.version(),
            self.name(),
            self.descriptor.message_type.as_str(),
            self.descriptor.topic
        )
    }
}

#[derive(Clone, Debug)]
pub struct CreateReceivedTable {
    pub version: i64,
    pub descriptor: MessageDescriptor,
}

impl CreateReceivedTable {
    pub fn new(version: i64, descriptor: MessageDescriptor) -> Self {
        Self {
            version,
            descriptor,
        }
    }
}

impl Changeset for CreateReceivedTable {
    fn version(&self) -> i64 {
        self.version
    }

    fn name(&self) -> &str {
        "create_received_table"
    }

    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
        let table = ReceivedTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        builder.push(create_received_table_sql(&table));
        builder.push(create_received_idempotency_index_sql(&table));
        builder.push(create_received_state_index_sql(&table));
        Ok(())
    }

    fn checksum_material(&self) -> String {
        format!(
            "version={};name={};message_type={};topic={}",
            self.version(),
            self.name(),
            self.descriptor.message_type.as_str(),
            self.descriptor.topic
        )
    }
}

/// Additive changeset that brings an outbox table created before idempotency
/// support up to the current shape. Fresh tables already include the column via
/// [`create_outbox_table_sql`], so this `ALTER ... ADD COLUMN IF NOT EXISTS` is a
/// no-op there; include it in a changelog only to upgrade pre-existing tables.
#[derive(Clone, Debug)]
pub struct AddIdempotencyKey {
    pub version: i64,
    pub descriptor: MessageDescriptor,
}

impl AddIdempotencyKey {
    pub fn new(version: i64, descriptor: MessageDescriptor) -> Self {
        Self {
            version,
            descriptor,
        }
    }
}

impl Changeset for AddIdempotencyKey {
    fn version(&self) -> i64 {
        self.version
    }

    fn name(&self) -> &str {
        "add_idempotency_key"
    }

    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
        let table = OutboxTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        builder.push(add_idempotency_key_sql(&table));
        Ok(())
    }

    fn checksum_material(&self) -> String {
        format!(
            "version={};name={};message_type={};topic={}",
            self.version(),
            self.name(),
            self.descriptor.message_type.as_str(),
            self.descriptor.topic
        )
    }
}

pub fn add_idempotency_key_sql(table: &OutboxTable) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS idempotency_key TEXT",
        table.qualified_name()
    )
}

#[derive(Clone, Debug)]
pub struct Replay {
    version: i64,
    descriptor: MessageDescriptor,
    occurred_after: Option<OffsetDateTime>,
    max_rows: Option<i64>,
    contexts: Vec<String>,
}

impl Replay {
    pub fn outbox<P: KafkaMessage>(version: i64) -> Result<Self> {
        Ok(Self {
            version,
            descriptor: P::descriptor()?,
            occurred_after: None,
            max_rows: None,
            contexts: Vec::new(),
        })
    }

    pub fn since(mut self, occurred_after: OffsetDateTime) -> Self {
        self.occurred_after = Some(occurred_after);
        self
    }

    pub fn max_rows(mut self, cap: i64) -> Self {
        self.max_rows = Some(cap);
        self
    }

    pub fn contexts(mut self, contexts: &[&str]) -> Self {
        self.contexts = contexts
            .iter()
            .map(|context| (*context).to_owned())
            .collect();
        self
    }

    fn max_rows_or_error(&self) -> Result<i64> {
        match self.max_rows {
            Some(max_rows) if max_rows > 0 => Ok(max_rows),
            Some(_) => Err(Error::InvalidReplay {
                version: self.version,
                message: "max_rows must be greater than zero".to_owned(),
            }),
            None => Err(Error::InvalidReplay {
                version: self.version,
                message: "max_rows is required".to_owned(),
            }),
        }
    }
}

impl Changeset for Replay {
    fn version(&self) -> i64 {
        self.version
    }

    fn name(&self) -> &str {
        "replay_outbox"
    }

    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
        let table = OutboxTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        builder.push(replay_outbox_update_sql(&table, self)?);
        Ok(())
    }

    fn checksum_material(&self) -> String {
        let occurred_after = self
            .occurred_after
            .map(|timestamp| {
                timestamp
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| timestamp.to_string())
            })
            .unwrap_or_else(|| "none".to_owned());
        format!(
            "version={};name={};message_type={};topic={};occurred_after={};max_rows={};contexts={}",
            self.version(),
            self.name(),
            self.descriptor.message_type.as_str(),
            self.descriptor.topic,
            occurred_after,
            self.max_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            self.contexts.join(",")
        )
    }

    fn contexts(&self) -> &[String] {
        &self.contexts
    }

    fn dry_run_preview(&self, cfg: &ResolvedConfig) -> Result<String> {
        let table = OutboxTable::new(cfg.schema.clone(), self.descriptor.clone())?;
        Ok(format!(
            "would requeue ~N rows in {}",
            table.qualified_name()
        ))
    }

    fn estimate_replay_count<'a>(
        &'a self,
        cfg: &'a ResolvedConfig,
        tx: &'a mut Transaction<'_, Postgres>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<i64>>> + Send + 'a>> {
        Box::pin(async move {
            let table = OutboxTable::new(cfg.schema.clone(), self.descriptor.clone())?;
            let sql = replay_outbox_count_sql(&table, self)?;
            let count = sqlx::query_scalar::<_, i64>(&sql)
                .fetch_one(&mut **tx)
                .await?;
            Ok(Some(count))
        })
    }
}

fn replay_outbox_update_sql(table: &OutboxTable, replay: &Replay) -> Result<String> {
    let max_rows = replay.max_rows_or_error()?;
    let filter = replay_outbox_filter_sql(replay)?;
    Ok(format!(
        "WITH candidates AS (\
         SELECT message_id FROM {name} \
         WHERE {filter} \
         ORDER BY occurred_at, message_id \
         LIMIT {max_rows}\
         ) \
         UPDATE {name} \
         SET status = {pending}, \
             attempts = 0, \
             last_error = NULL, \
             next_attempt_at = now(), \
             claim_id = NULL, \
             claimed_by = NULL, \
             claim_expires_at = NULL, \
             published_at = NULL \
         WHERE message_id IN (SELECT message_id FROM candidates)",
        name = table.qualified_name(),
        filter = filter,
        max_rows = max_rows,
        pending = OutboxStatus::Pending.sql_literal(),
    ))
}

fn replay_outbox_count_sql(table: &OutboxTable, replay: &Replay) -> Result<String> {
    let max_rows = replay.max_rows_or_error()?;
    let filter = replay_outbox_filter_sql(replay)?;
    Ok(format!(
        "SELECT COUNT(*)::BIGINT FROM (\
         SELECT 1 FROM {name} \
         WHERE {filter} \
         ORDER BY occurred_at, message_id \
         LIMIT {max_rows}\
         ) AS replay_candidates",
        name = table.qualified_name(),
        filter = filter,
        max_rows = max_rows,
    ))
}

fn replay_outbox_filter_sql(replay: &Replay) -> Result<String> {
    let mut filter = format!("status = {}", OutboxStatus::Published.sql_literal());
    if let Some(occurred_after) = replay.occurred_after {
        let formatted = occurred_after
            .format(&Rfc3339)
            .map_err(|err| Error::InvalidReplay {
                version: replay.version,
                message: err.to_string(),
            })?;
        filter.push_str(" AND occurred_at >= ");
        filter.push_str(&sql_string_literal(&formatted));
        filter.push_str("::timestamptz");
    }
    Ok(filter)
}

pub async fn migrate(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    assert_changelog_order(changesets)?;

    let mut conn = pool.acquire().await?;
    let lock_key = advisory_lock_key(cfg.schema.as_str());
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await?;

    let result = run_migrations(&mut conn, cfg, ctx, changesets).await;

    let unlock_result = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await;

    match (result, unlock_result) {
        (Ok(report), Ok(_)) => Ok(report),
        (Err(err), Ok(_)) => Err(err),
        (Ok(_), Err(err)) => Err(Error::Sqlx(err)),
        (Err(err), Err(_)) => Err(err),
    }
}

pub async fn migrate_dry_run(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    assert_changelog_order(changesets)?;

    let mut conn = pool.acquire().await?;
    let lock_key = advisory_lock_key(cfg.schema.as_str());
    // A dry-run is a non-mutating preview: take the lock without blocking so a
    // long preview can never stall a real deploy migration, and a deploy
    // migration in progress short-circuits the preview instead of queueing
    // behind it.
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await?;
    if !acquired {
        return Err(Error::MigrationLockBusy);
    }

    let result = run_migrations_dry_run(&mut conn, cfg, ctx, changesets).await;

    let unlock_result = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await;

    match (result, unlock_result) {
        (Ok(report), Ok(_)) => Ok(report),
        (Err(err), Ok(_)) => Err(err),
        (Ok(_), Err(err)) => Err(Error::Sqlx(err)),
        (Err(err), Err(_)) => Err(err),
    }
}

async fn run_migrations_dry_run(
    conn: &mut PgConnection,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    // Run the whole preview inside one transaction that is always rolled back.
    // Bootstrap DDL (`CREATE SCHEMA/TABLE`, the `applied_by` backfill) and the
    // per-changeset row reads therefore leave no trace: a dry-run never mutates
    // `changelog_history`, even legacy rows.
    let mut tx = conn.begin().await?;
    let result = dry_run_in_tx(&mut tx, cfg, ctx, changesets).await;
    let rollback = tx.rollback().await;
    match (result, rollback) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(Error::Sqlx(err)),
    }
}

async fn dry_run_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    bootstrap_history(tx, cfg).await?;
    let mut report = MigrationReport::default();

    for changeset in changesets {
        let changeset = changeset.as_ref();
        if !ctx.matches(changeset.contexts()) {
            report.push(MigrationStepReport {
                version: changeset.version(),
                name: changeset.name().to_owned(),
                action: MigrationAction::SkippedContext,
                preview: Some(format!(
                    "requires one of contexts: {}",
                    changeset.contexts().join(",")
                )),
            });
            continue;
        }

        let checksum = changeset.checksum();
        if let Some(history) = history_row(cfg, tx, changeset.version()).await? {
            let action = match history.checksum {
                Some(stored) if stored != checksum => MigrationAction::ChecksumMismatch {
                    stored,
                    current: checksum,
                },
                _ => MigrationAction::SkippedAlreadyApplied,
            };
            report.push(MigrationStepReport {
                version: changeset.version(),
                name: changeset.name().to_owned(),
                action,
                preview: None,
            });
            continue;
        }

        let preview = match changeset.estimate_replay_count(cfg, tx).await? {
            Some(count) => Some(
                changeset
                    .dry_run_preview(cfg)?
                    .replace("~N", &format!("~{count}")),
            ),
            None => Some(changeset.dry_run_preview(cfg)?),
        };

        report.push(MigrationStepReport {
            version: changeset.version(),
            name: changeset.name().to_owned(),
            action: MigrationAction::WouldApply,
            preview,
        });
    }

    Ok(report)
}

async fn run_migrations(
    conn: &mut PgConnection,
    cfg: &ResolvedConfig,
    ctx: &MigrationContext,
    changesets: &[Box<dyn Changeset>],
) -> Result<MigrationReport> {
    bootstrap_history(conn, cfg).await?;
    let mut report = MigrationReport::default();

    for changeset in changesets {
        let changeset = changeset.as_ref();
        if !ctx.matches(changeset.contexts()) {
            report.push(MigrationStepReport {
                version: changeset.version(),
                name: changeset.name().to_owned(),
                action: MigrationAction::SkippedContext,
                preview: Some(format!(
                    "requires one of contexts: {}",
                    changeset.contexts().join(",")
                )),
            });
            continue;
        }

        let checksum = changeset.checksum();
        let mut tx = conn.begin().await?;
        if let Some(history) = history_row(cfg, &mut tx, changeset.version()).await? {
            if let Some(stored) = history.checksum {
                if stored != checksum {
                    return Err(Error::ChecksumMismatch {
                        version: changeset.version(),
                        stored,
                        current: checksum,
                    });
                }
            }
            report.push(MigrationStepReport {
                version: changeset.version(),
                name: changeset.name().to_owned(),
                action: MigrationAction::SkippedAlreadyApplied,
                preview: None,
            });
            tx.commit().await?;
            continue;
        }

        let mut builder = ChangeBuilder::new();
        changeset.build(cfg, &mut builder)?;
        for statement in builder.into_statements() {
            sqlx::query(&statement).execute(&mut *tx).await?;
        }
        insert_history(
            cfg,
            &mut tx,
            changeset.version(),
            changeset.name(),
            &checksum,
            ctx.applied_by(),
        )
        .await?;
        tx.commit().await?;

        report.push(MigrationStepReport {
            version: changeset.version(),
            name: changeset.name().to_owned(),
            action: MigrationAction::Applied,
            preview: None,
        });
    }

    Ok(report)
}

/// Stable FNV-1a 64-bit hash of the schema name, used as a Postgres advisory
/// lock key. Deterministic across builds so every replica of the same
/// application maps a schema to the same lock.
fn advisory_lock_key(schema: &str) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in schema.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// Stable SHA-256 (hex) checksum of a changeset's source-declared material,
/// stored as `sha256:<64 hex chars>`. The `changeset:` domain prefix keeps these
/// digests from ever colliding with hashes computed for another purpose.
fn stable_checksum(material: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"changeset:");
    hasher.update(material.as_bytes());
    let digest = hasher.finalize();

    let mut checksum = String::with_capacity("sha256:".len() + digest.len() * 2);
    checksum.push_str("sha256:");
    for byte in digest {
        checksum.push(char::from_digit(u32::from(byte >> 4), 16).expect("nibble is < 16"));
        checksum.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("nibble is < 16"));
    }
    checksum
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn bootstrap_history(conn: &mut PgConnection, cfg: &ResolvedConfig) -> Result<()> {
    let schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", cfg.schema.quoted());
    sqlx::query(&schema_sql).execute(&mut *conn).await?;

    let history = history_table_name(cfg)?;
    let history_sql = format!(
        "CREATE TABLE IF NOT EXISTS {history} (\
         version BIGINT PRIMARY KEY, \
         name TEXT NOT NULL, \
         checksum TEXT, \
         applied_by TEXT, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now()\
         )"
    );
    sqlx::query(&history_sql).execute(&mut *conn).await?;

    let checksum_sql = format!("ALTER TABLE {history} ADD COLUMN IF NOT EXISTS checksum TEXT");
    sqlx::query(&checksum_sql).execute(&mut *conn).await?;

    let applied_by_sql = format!("ALTER TABLE {history} ADD COLUMN IF NOT EXISTS applied_by TEXT");
    sqlx::query(&applied_by_sql).execute(&mut *conn).await?;

    let backfill_sql =
        format!("UPDATE {history} SET applied_by = 'unknown' WHERE applied_by IS NULL");
    sqlx::query(&backfill_sql).execute(&mut *conn).await?;

    Ok(())
}

pub fn assert_changelog_order(changesets: &[Box<dyn Changeset>]) -> Result<()> {
    let mut versions = BTreeSet::new();
    let mut previous = None;
    for changeset in changesets {
        let version = changeset.version();
        if !versions.insert(version) {
            return Err(Error::DuplicateChangesetVersion(version));
        }
        if let Some(previous) = previous {
            if version <= previous {
                return Err(Error::DisorderedChangesetVersion {
                    previous,
                    next: version,
                });
            }
        }
        previous = Some(version);
    }
    Ok(())
}

/// Build a `Vec<Box<dyn Changeset>>` and assert ascending, unique versions at
/// construction. Panics with the ordering error when the changelog is invalid;
/// use [`try_changelog!`] when you need to inspect the
/// [`Error::DuplicateChangesetVersion`] / [`Error::DisorderedChangesetVersion`]
/// detail instead of a panic.
#[macro_export]
macro_rules! changelog {
    ($($changeset:expr),* $(,)?) => {{
        match $crate::try_changelog![$($changeset),*] {
            Ok(changesets) => changesets,
            Err(err) => panic!("invalid kafkaman changelog ordering: {err}"),
        }
    }};
}

/// Fallible sibling of [`changelog!`]: returns `Result<Vec<Box<dyn Changeset>>>`
/// so callers keep the structured ordering error rather than a panic.
#[macro_export]
macro_rules! try_changelog {
    ($($changeset:expr),* $(,)?) => {{
        let changesets: Vec<Box<dyn $crate::Changeset>> = vec![$(Box::new($changeset)),*];
        $crate::assert_changelog_order(&changesets).map(|()| changesets)
    }};
}

#[derive(Clone, Debug)]
struct HistoryRow {
    checksum: Option<String>,
}

async fn history_row(
    cfg: &ResolvedConfig,
    tx: &mut Transaction<'_, Postgres>,
    version: i64,
) -> Result<Option<HistoryRow>> {
    let history = history_table_name(cfg)?;
    let sql = format!("SELECT checksum FROM {history} WHERE version = $1");
    let row = sqlx::query(&sql)
        .bind(version)
        .fetch_optional(&mut **tx)
        .await?;

    match row {
        // Decode `checksum` as an explicit nullable column: a legacy NULL becomes
        // `None`, while a genuine decode error surfaces instead of being silently
        // swallowed by `.ok()`.
        Some(row) => Ok(Some(HistoryRow {
            checksum: row.try_get::<Option<String>, _>("checksum")?,
        })),
        None => Ok(None),
    }
}

async fn insert_history(
    cfg: &ResolvedConfig,
    tx: &mut Transaction<'_, Postgres>,
    version: i64,
    name: &str,
    checksum: &str,
    applied_by: &str,
) -> Result<()> {
    let history = history_table_name(cfg)?;
    let sql = format!(
        "INSERT INTO {history} (version, name, checksum, applied_by) VALUES ($1, $2, $3, $4)"
    );
    sqlx::query(&sql)
        .bind(version)
        .bind(name)
        .bind(checksum)
        .bind(applied_by)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn history_table_name(cfg: &ResolvedConfig) -> Result<String> {
    Ok(format!(
        "{}.{}",
        cfg.schema.quoted(),
        SqlIdentifier::new("changelog_history")?.quoted()
    ))
}

pub fn create_outbox_table_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {name} (
    message_id UUID PRIMARY KEY,
    idempotency_key TEXT,
    status TEXT NOT NULL DEFAULT {pending},
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    claim_id UUID,
    claimed_by TEXT,
    claim_expires_at TIMESTAMPTZ,
    topic TEXT NOT NULL,
    partition_key TEXT,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    headers JSONB NOT NULL DEFAULT '{{}}',
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CHECK (status IN ({status_list}))
)",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
        status_list = OutboxStatus::sql_literal_list(),
    )
}

pub fn create_outbox_state_index_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (status, next_attempt_at, claim_expires_at, created_at)",
        table.state_index_name().quoted(),
        table.qualified_name()
    )
}

pub fn create_received_table_sql(table: &ReceivedTable) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {name} (\n            message_id UUID PRIMARY KEY,\n            idempotency_key TEXT NOT NULL,\n            status TEXT NOT NULL DEFAULT {pending} CHECK (status IN ({statuses})),\n            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),\n            next_attempt_at TIMESTAMPTZ,\n            errors JSONB NOT NULL DEFAULT '[]'::jsonb,\n            source_topic TEXT NOT NULL,\n            source_partition INTEGER NOT NULL,\n            source_offset BIGINT NOT NULL,\n            key BYTEA,\n            message_type TEXT NOT NULL,\n            message_version INTEGER NOT NULL DEFAULT 1 CHECK (message_version >= 1),\n            headers JSONB NOT NULL DEFAULT '{{}}'::jsonb,\n            payload JSONB NOT NULL,\n            correlation_id UUID,\n            causation_id UUID,\n            occurred_at TIMESTAMPTZ NOT NULL,\n            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),\n            processed_at TIMESTAMPTZ\n        )",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        statuses = ReceiveStatus::sql_literal_list(),
    )
}

pub fn create_received_idempotency_index_sql(table: &ReceivedTable) -> String {
    format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (idempotency_key)",
        table.idempotency_index_name().quoted(),
        table.qualified_name()
    )
}

pub fn create_received_state_index_sql(table: &ReceivedTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (status, next_attempt_at, created_at)",
        table.state_index_name().quoted(),
        table.qualified_name()
    )
}

pub async fn enqueue<P>(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
) -> Result<()>
where
    P: KafkaMessage + Serialize,
{
    let table = OutboxTable::for_message::<P>(cfg)?;
    if let Some(reserved) = kafkaman_core::reserved_header(&evt.headers) {
        return Err(Error::ReservedHeader(reserved.to_owned()));
    }
    let headers = serde_json::to_value(&evt.headers)?;
    let payload = serde_json::to_value(&evt.payload)?;
    let partition_key = evt.payload.partition_key();

    let sql = format!(
        "INSERT INTO {name} (
            message_id, idempotency_key, status, attempts, next_attempt_at, topic, partition_key,
            correlation_id, causation_id, headers, payload, occurred_at
        ) VALUES ($1, $2, {pending}, 0, now(), $3, $4, $5, $6, $7, $8, $9)",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
    );

    sqlx::query(&sql)
        .bind(evt.message_id)
        .bind(evt.idempotency_key.as_deref())
        .bind(table.descriptor.topic.clone())
        .bind(partition_key)
        .bind(evt.correlation_id)
        .bind(evt.causation_id)
        .bind(headers)
        .bind(payload)
        .bind(evt.occurred_at)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

pub async fn insert_received<P>(
    tx: &mut Transaction<'_, Postgres>,
    cfg: &ResolvedConfig,
    evt: &Envelope<P>,
    source_partition: i32,
    source_offset: i64,
    key: Option<&[u8]>,
) -> Result<bool>
where
    P: KafkaMessage + Serialize,
{
    let table = ReceivedTable::for_message::<P>(cfg)?;
    if let Some(reserved) = kafkaman_core::reserved_header(&evt.headers) {
        return Err(Error::ReservedHeader(reserved.to_owned()));
    }
    let idempotency_key = evt
        .idempotency_key
        .as_deref()
        .ok_or(Error::MissingIdempotencyKey)?;
    let headers = serde_json::to_value(&evt.headers)?;
    let payload = serde_json::to_value(&evt.payload)?;

    let sql = format!(
        "INSERT INTO {name} (
            message_id, idempotency_key, status, attempts, next_attempt_at, errors,
            source_topic, source_partition, source_offset, key, message_type, message_version,
            headers, payload, correlation_id, causation_id, occurred_at
        ) VALUES (
            $1, $2, {pending}, 0, NULL, '[]'::jsonb,
            $3, $4, $5, $6, $7, 1,
            $8, $9, $10, $11, $12
        ) ON CONFLICT (idempotency_key) DO NOTHING",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
    );

    let result = sqlx::query(&sql)
        .bind(evt.message_id)
        .bind(idempotency_key)
        .bind(table.descriptor.topic.clone())
        .bind(source_partition)
        .bind(source_offset)
        .bind(key)
        .bind(table.descriptor.message_type.as_str())
        .bind(headers)
        .bind(payload)
        .bind(evt.correlation_id)
        .bind(evt.causation_id)
        .bind(evt.occurred_at)
        .execute(&mut **tx)
        .await?;

    Ok(result.rows_affected() == 1)
}

pub async fn dispatch_once(
    pool: &PgPool,
    table: &ReceivedTable,
    router: &MessageRouter,
    due_at: OffsetDateTime,
) -> Result<DispatchStats> {
    let mut tx = pool.begin().await?;
    let Some(row) = claim_received_row(&mut tx, table, due_at).await? else {
        tx.commit().await?;
        return Ok(DispatchStats::default());
    };

    let handler = router
        .handler_for(&row.message_type)
        .ok_or_else(|| Error::MissingHandler(row.message_type.clone()))?;

    mark_received_processing(&mut tx, table, row.message_id).await?;
    let handler_result = handler.handle(&mut tx, row.payload.clone()).await;
    match handler_result {
        Ok(()) => {
            mark_received_processed(&mut tx, table, row.message_id, due_at).await?;
            tx.commit().await?;
            Ok(DispatchStats {
                claimed: 1,
                processed: 1,
                failed: 0,
            })
        }
        Err(err) => {
            let message = err.to_string();
            tx.rollback().await?;
            record_received_failure(pool, table, row.message_id, message, due_at).await?;
            Ok(DispatchStats {
                claimed: 1,
                processed: 0,
                failed: 1,
            })
        }
    }
}

async fn claim_received_row(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    due_at: OffsetDateTime,
) -> Result<Option<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {name}
         WHERE (
             status = {pending}
             AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
         ) OR (
             status = {retryable}
             AND next_attempt_at <= $1
         )
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1",
        name = table.qualified_name(),
        pending = ReceiveStatus::Pending.sql_literal(),
        retryable = ReceiveStatus::Retryable.sql_literal(),
    );
    let row = sqlx::query(&sql)
        .bind(due_at)
        .fetch_optional(&mut **tx)
        .await?;
    row.map(received_row_from_pg).transpose()
}

async fn mark_received_processing(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    message_id: Uuid,
) -> Result<()> {
    let sql = format!(
        "UPDATE {} SET status = {} WHERE message_id = $1",
        table.qualified_name(),
        ReceiveStatus::Processing.sql_literal(),
    );
    sqlx::query(&sql)
        .bind(message_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn mark_received_processed(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    message_id: Uuid,
    processed_at: OffsetDateTime,
) -> Result<()> {
    let sql = format!(
        "UPDATE {} SET status = {}, processed_at = $2 WHERE message_id = $1",
        table.qualified_name(),
        ReceiveStatus::Processed.sql_literal(),
    );
    sqlx::query(&sql)
        .bind(message_id)
        .bind(processed_at)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn record_received_failure(
    pool: &PgPool,
    table: &ReceivedTable,
    message_id: Uuid,
    message: String,
    occurred_at: OffsetDateTime,
) -> Result<()> {
    let error = serde_json::to_value(ReceivedError {
        message,
        occurred_at,
    })?;
    let sql = format!(
        "UPDATE {name}
         SET status = {retryable},
             attempts = attempts + 1,
             next_attempt_at = NULL,
             errors = COALESCE((
                 SELECT jsonb_agg(value ORDER BY ord)
                 FROM (
                     SELECT value, ord
                     FROM jsonb_array_elements(errors || jsonb_build_array($2::jsonb))
                         WITH ORDINALITY AS entries(value, ord)
                     ORDER BY ord DESC
                     LIMIT 20
                 ) kept
             ), '[]'::jsonb)
         WHERE message_id = $1",
        name = table.qualified_name(),
        retryable = ReceiveStatus::Retryable.sql_literal(),
    );
    sqlx::query(&sql)
        .bind(message_id)
        .bind(error)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn claim_batch(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    worker_id: &str,
    lease_for: Duration,
    limit: i64,
) -> Result<Vec<ClaimedOutboxRow>> {
    let select_sql = format!(
        "SELECT * FROM {name}
         WHERE (status = {pending} AND next_attempt_at <= now())
            OR (status = {publishing} AND claim_expires_at <= now())
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT $1",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    let candidates = sqlx::query(&select_sql)
        .bind(limit)
        .fetch_all(&mut **tx)
        .await?;

    // Compute the lease expiry from the database clock (now()) rather than the
    // application host clock, so lease ownership is immune to clock skew between
    // the worker and Postgres.
    let lease_secs = lease_for.as_secs_f64();
    let update_sql = format!(
        "UPDATE {name}
             SET status = {publishing},
                 attempts = attempts + 1,
                 claim_id = $2,
                 claimed_by = $3,
                 claim_expires_at = now() + make_interval(secs => $4)
             WHERE message_id = $1
             RETURNING *",
        name = table.qualified_name(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );

    let mut claimed = Vec::with_capacity(candidates.len());
    for row in candidates {
        let message_id: Uuid = row.try_get("message_id")?;
        let claim_id = Uuid::new_v4();
        let updated = sqlx::query(&update_sql)
            .bind(message_id)
            .bind(claim_id)
            .bind(worker_id)
            .bind(lease_secs)
            .fetch_one(&mut **tx)
            .await?;

        claimed.push(ClaimedOutboxRow {
            row: row_from_pg(updated)?,
            claim_id,
        });
    }

    Ok(claimed)
}

pub async fn mark_published(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
) -> Result<MarkOutcome> {
    let sql = format!(
        "UPDATE {name}
         SET status = {published},
             published_at = now(),
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE message_id = $1 AND status = {publishing} AND claim_id = $2",
        name = table.qualified_name(),
        published = OutboxStatus::Published.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    mark_with_sql(pool, table, &sql, message_id, claim_id).await
}

pub async fn mark_publish_failed(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
    error: &str,
    retry_after: Duration,
) -> Result<MarkOutcome> {
    // Schedule the retry from the database clock (now() + interval) so retry
    // eligibility shares the same clock as claim eligibility.
    let sql = format!(
        "UPDATE {name}
         SET status = {pending},
             last_error = $3,
             next_attempt_at = now() + make_interval(secs => $4),
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE message_id = $1 AND status = {publishing} AND claim_id = $2",
        name = table.qualified_name(),
        pending = OutboxStatus::Pending.sql_literal(),
        publishing = OutboxStatus::Publishing.sql_literal(),
    );
    let result = sqlx::query(&sql)
        .bind(message_id)
        .bind(claim_id)
        .bind(error)
        .bind(retry_after.as_secs_f64())
        .execute(pool)
        .await?;

    if result.rows_affected() == 1 {
        Ok(MarkOutcome::Updated)
    } else {
        mark_miss_outcome(pool, table, message_id).await
    }
}

async fn mark_with_sql(
    pool: &PgPool,
    table: &OutboxTable,
    sql: &str,
    message_id: Uuid,
    claim_id: Uuid,
) -> Result<MarkOutcome> {
    let result = sqlx::query(sql)
        .bind(message_id)
        .bind(claim_id)
        .execute(pool)
        .await?;

    if result.rows_affected() == 1 {
        Ok(MarkOutcome::Updated)
    } else {
        mark_miss_outcome(pool, table, message_id).await
    }
}

async fn mark_miss_outcome(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
) -> Result<MarkOutcome> {
    let sql = format!(
        "SELECT message_id FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let exists = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(pool)
        .await?
        .is_some();
    Ok(if exists {
        MarkOutcome::StaleClaim
    } else {
        MarkOutcome::Missing
    })
}

pub async fn outbox_row(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
) -> Result<Option<OutboxRow>> {
    let sql = format!(
        "SELECT * FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(pool)
        .await?;
    row.map(row_from_pg).transpose()
}

fn row_from_pg(row: PgRow) -> Result<OutboxRow> {
    let status: String = row.try_get("status")?;
    let headers: serde_json::Value = row.try_get("headers")?;
    let headers: BTreeMap<String, String> = serde_json::from_value(headers)?;

    Ok(OutboxRow {
        message_id: row.try_get("message_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        status: status
            .parse()
            .map_err(|err: kafkaman_core::Error| Error::Core(err))?,
        attempts: row.try_get("attempts")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        last_error: row.try_get("last_error")?,
        claim_id: row.try_get("claim_id")?,
        claimed_by: row.try_get("claimed_by")?,
        claim_expires_at: row.try_get("claim_expires_at")?,
        topic: row.try_get("topic")?,
        partition_key: row.try_get("partition_key")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        headers,
        payload: row.try_get("payload")?,
        occurred_at: row.try_get("occurred_at")?,
        created_at: row.try_get("created_at")?,
        published_at: row.try_get("published_at")?,
    })
}

pub async fn received_row(
    pool: &PgPool,
    table: &ReceivedTable,
    message_id: Uuid,
) -> Result<Option<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {} WHERE message_id = $1",
        table.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(message_id)
        .fetch_optional(pool)
        .await?;
    row.map(received_row_from_pg).transpose()
}

pub async fn received_row_by_idempotency_key(
    pool: &PgPool,
    table: &ReceivedTable,
    idempotency_key: &str,
) -> Result<Option<ReceivedRow>> {
    let sql = format!(
        "SELECT * FROM {} WHERE idempotency_key = $1",
        table.qualified_name()
    );
    let row = sqlx::query(&sql)
        .bind(idempotency_key)
        .fetch_optional(pool)
        .await?;
    row.map(received_row_from_pg).transpose()
}

fn received_row_from_pg(row: PgRow) -> Result<ReceivedRow> {
    let status: String = row.try_get("status")?;
    let headers: serde_json::Value = row.try_get("headers")?;
    let headers: BTreeMap<String, String> = serde_json::from_value(headers)?;
    let errors: serde_json::Value = row.try_get("errors")?;
    let errors: Vec<ReceivedError> = serde_json::from_value(errors)?;

    Ok(ReceivedRow {
        message_id: row.try_get("message_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        status: status
            .parse()
            .map_err(|err: kafkaman_core::Error| Error::Core(err))?,
        attempts: row.try_get("attempts")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        errors,
        source_topic: row.try_get("source_topic")?,
        source_partition: row.try_get("source_partition")?,
        source_offset: row.try_get("source_offset")?,
        key: row.try_get("key")?,
        message_type: row.try_get("message_type")?,
        message_version: row.try_get("message_version")?,
        headers,
        payload: row.try_get("payload")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        occurred_at: row.try_get("occurred_at")?,
        created_at: row.try_get("created_at")?,
        processed_at: row.try_get("processed_at")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct OrderCreated;

    impl KafkaMessage for OrderCreated {
        const MESSAGE_TYPE: &'static str = "order_created";
        const TOPIC: &'static str = "orders";
    }

    #[test]
    fn renders_outbox_ddl_with_validated_identifiers() {
        let descriptor = OrderCreated::descriptor().unwrap();
        let table = OutboxTable::new(SqlIdentifier::new("kafkaman").unwrap(), descriptor).unwrap();
        let ddl = create_outbox_table_sql(&table);

        assert!(ddl.contains("\"kafkaman\".\"outbox_order_created\""));
        assert!(ddl.contains("claim_id UUID"));
        assert!(ddl.contains("CHECK (status IN"));
    }

    #[test]
    fn long_outbox_table_names_get_distinct_state_indexes() {
        // Two message types whose outbox table names share the first 53 chars
        // would collide under plain truncation; the hash suffix keeps them apart.
        let schema = SqlIdentifier::new("kafkaman").unwrap();
        let prefix = "a".repeat(46);
        let table_a =
            OutboxTable::new(schema.clone(), descriptor(&format!("{prefix}_one"))).unwrap();
        let table_b = OutboxTable::new(schema, descriptor(&format!("{prefix}_two"))).unwrap();

        let idx_a = table_a.state_index_name();
        let idx_b = table_b.state_index_name();

        assert_ne!(idx_a.as_str(), idx_b.as_str());
        assert!(idx_a.as_str().len() <= SqlIdentifier::MAX_LEN);
        assert!(idx_b.as_str().len() <= SqlIdentifier::MAX_LEN);
    }

    fn descriptor(message_type: &str) -> MessageDescriptor {
        MessageDescriptor::new(message_type, "topic").unwrap()
    }

    #[test]
    fn registering_the_same_message_type_twice_is_idempotent() {
        let cfg = ResolvedConfig::default()
            .with_message(descriptor("order_created"))
            .with_message(descriptor("order_created"));
        assert_eq!(cfg.messages().len(), 1);
    }

    #[test]
    fn resolved_config_from_config_validates_before_runtime_use() {
        let cfg = Config::from_str(
            r#"
            [database]
            schema = "kafkaman"

            [relay]
            worker_id = "worker-a"
            batch_limit = 10
            lease_for = "30s"
            retry_after = "1s"
            poll_interval = "250ms"
            "#,
        )
        .unwrap();

        let resolved = ResolvedConfig::from_config(Some(&cfg), [descriptor("order_created")])
            .expect("valid config resolves");
        assert_eq!(resolved.schema().as_str(), "kafkaman");
        assert_eq!(resolved.relay.worker_id, "worker-a");
        assert_eq!(resolved.messages().len(), 1);

        let trivial = ResolvedConfig::from_config(None, std::iter::empty()).unwrap();
        assert!(trivial.messages().is_empty());

        let err = ResolvedConfig::from_config(None, [descriptor("order_created")]).unwrap_err();
        assert!(err.to_string().contains("missing config file"));
    }

    #[test]
    fn migration_context_and_replay_guardrails_are_explicit() {
        let ctx = MigrationContext::default()
            .with_context("staging")
            .with_applied_by("deploy-bot");
        assert_eq!(ctx.applied_by(), "deploy-bot");
        assert!(ctx.matches(&["staging".to_owned()]));
        assert!(!ctx.matches(&["prod".to_owned()]));

        let replay = Replay::outbox::<OrderCreated>(3).unwrap();
        let cfg = ResolvedConfig::default().with_message(OrderCreated::descriptor().unwrap());
        assert!(matches!(
            replay.dry_run_preview(&cfg),
            Ok(message) if message.contains("would requeue")
        ));
        assert!(matches!(
            replay.build(&cfg, &mut ChangeBuilder::new()),
            Err(Error::InvalidReplay { .. })
        ));
        assert!(matches!(
            Replay::outbox::<OrderCreated>(3)
                .unwrap()
                .max_rows(0)
                .build(&cfg, &mut ChangeBuilder::new()),
            Err(Error::InvalidReplay { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_changeset_versions() {
        let changesets: Vec<Box<dyn Changeset>> = vec![Box::new(InitSchema), Box::new(InitSchema)];
        assert!(matches!(
            assert_changelog_order(&changesets),
            Err(Error::DuplicateChangesetVersion(1))
        ));
    }
}
