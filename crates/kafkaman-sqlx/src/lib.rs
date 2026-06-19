use std::collections::BTreeMap;
use std::time::Duration;

use kafkaman_core::{
    ClaimedOutboxRow, Envelope, KafkaMessage, MarkOutcome, MessageDescriptor, OutboxRow,
    RelayConfig, SqlIdentifier,
};
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
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

    #[error("changeset versions must be unique and ordered: duplicate version {0}")]
    DuplicateChangesetVersion(i64),

    #[error("message descriptor `{0}` is not configured")]
    UnknownMessageType(String),
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

    pub fn with_message(mut self, descriptor: MessageDescriptor) -> Self {
        self.messages.push(descriptor);
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
        let middle_limit = SqlIdentifier::MAX_LEN - "idx_".len() - "_state".len();
        let middle = &self.table.as_str()[..self.table.as_str().len().min(middle_limit)];
        SqlIdentifier::new(format!("idx_{middle}_state")).expect("index name is generated valid")
    }
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

pub trait Changeset: Send + Sync {
    fn version(&self) -> i64;
    fn name(&self) -> &str;
    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()>;
}

#[derive(Debug)]
pub struct InitSchema;

impl Changeset for InitSchema {
    fn version(&self) -> i64 {
        1
    }

    fn name(&self) -> &str {
        "init_schema"
    }

    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
        builder.push(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            cfg.schema.quoted()
        ));
        builder.push(format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
            cfg.schema.quoted(),
            SqlIdentifier::new("changelog_history")?.quoted()
        ));
        Ok(())
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
}

pub async fn migrate(
    pool: &PgPool,
    cfg: &ResolvedConfig,
    changesets: &[Box<dyn Changeset>],
) -> Result<()> {
    ensure_unique_versions(changesets)?;
    bootstrap_history(pool, cfg).await?;

    let mut sorted: Vec<&Box<dyn Changeset>> = changesets.iter().collect();
    sorted.sort_by_key(|changeset| changeset.version());

    for changeset in sorted {
        let mut builder = ChangeBuilder::new();
        changeset.build(cfg, &mut builder)?;
        let statements = builder.into_statements();

        let mut tx = pool.begin().await?;
        if has_changeset(cfg, &mut tx, changeset.version()).await? {
            tx.commit().await?;
            continue;
        }

        for statement in statements {
            sqlx::query(&statement).execute(&mut *tx).await?;
        }

        insert_history(cfg, &mut tx, changeset.version(), changeset.name()).await?;
        tx.commit().await?;
    }

    Ok(())
}

async fn bootstrap_history(pool: &PgPool, cfg: &ResolvedConfig) -> Result<()> {
    let schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}", cfg.schema.quoted());
    sqlx::query(&schema_sql).execute(pool).await?;

    let history = history_table_name(cfg)?;
    let history_sql = format!(
        "CREATE TABLE IF NOT EXISTS {history} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    );
    sqlx::query(&history_sql).execute(pool).await?;
    Ok(())
}

fn ensure_unique_versions(changesets: &[Box<dyn Changeset>]) -> Result<()> {
    let mut versions = std::collections::BTreeSet::new();
    for changeset in changesets {
        if !versions.insert(changeset.version()) {
            return Err(Error::DuplicateChangesetVersion(changeset.version()));
        }
    }
    Ok(())
}

async fn has_changeset(
    cfg: &ResolvedConfig,
    tx: &mut Transaction<'_, Postgres>,
    version: i64,
) -> Result<bool> {
    let history = history_table_name(cfg)?;
    let sql = format!("SELECT version FROM {history} WHERE version = $1");
    let found = sqlx::query(&sql)
        .bind(version)
        .fetch_optional(&mut **tx)
        .await?
        .is_some();
    Ok(found)
}

async fn insert_history(
    cfg: &ResolvedConfig,
    tx: &mut Transaction<'_, Postgres>,
    version: i64,
    name: &str,
) -> Result<()> {
    let history = history_table_name(cfg)?;
    let sql = format!("INSERT INTO {history} (version, name) VALUES ($1, $2)");
    sqlx::query(&sql)
        .bind(version)
        .bind(name)
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
        "CREATE TABLE IF NOT EXISTS {} (
    message_id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'Pending',
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
    CHECK (status IN ('Pending', 'Publishing', 'Published', 'Failed'))
)",
        table.qualified_name()
    )
}

pub fn create_outbox_state_index_sql(table: &OutboxTable) -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} (status, next_attempt_at, claim_expires_at, created_at)",
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
    let headers = serde_json::to_value(&evt.headers)?;
    let payload = serde_json::to_value(&evt.payload)?;
    let partition_key = evt.payload.partition_key();
    let descriptor = P::descriptor()?;

    let sql = format!(
        "INSERT INTO {} (
            message_id, status, attempts, next_attempt_at, topic, partition_key,
            correlation_id, causation_id, headers, payload, occurred_at
        ) VALUES ($1, 'Pending', 0, now(), $2, $3, $4, $5, $6, $7, $8)",
        table.qualified_name()
    );

    sqlx::query(&sql)
        .bind(evt.message_id)
        .bind(descriptor.topic)
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

pub async fn claim_batch(
    tx: &mut Transaction<'_, Postgres>,
    table: &OutboxTable,
    worker_id: &str,
    lease_for: Duration,
    limit: i64,
) -> Result<Vec<ClaimedOutboxRow>> {
    let select_sql = format!(
        "SELECT * FROM {}
         WHERE (status = 'Pending' AND next_attempt_at <= now())
            OR (status = 'Publishing' AND claim_expires_at <= now())
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT $1",
        table.qualified_name()
    );
    let candidates = sqlx::query(&select_sql)
        .bind(limit)
        .fetch_all(&mut **tx)
        .await?;

    let mut claimed = Vec::with_capacity(candidates.len());
    let lease_until = OffsetDateTime::now_utc() + lease_for;

    for row in candidates {
        let message_id: Uuid = row.try_get("message_id")?;
        let claim_id = Uuid::new_v4();
        let update_sql = format!(
            "UPDATE {}
             SET status = 'Publishing',
                 attempts = attempts + 1,
                 claim_id = $2,
                 claimed_by = $3,
                 claim_expires_at = $4
             WHERE message_id = $1
             RETURNING *",
            table.qualified_name()
        );
        let updated = sqlx::query(&update_sql)
            .bind(message_id)
            .bind(claim_id)
            .bind(worker_id)
            .bind(lease_until)
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
        "UPDATE {}
         SET status = 'Published',
             published_at = now(),
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE message_id = $1 AND status = 'Publishing' AND claim_id = $2",
        table.qualified_name()
    );
    mark_with_sql(pool, table, &sql, message_id, claim_id).await
}

pub async fn mark_publish_failed(
    pool: &PgPool,
    table: &OutboxTable,
    message_id: Uuid,
    claim_id: Uuid,
    error: &str,
    retry_at: OffsetDateTime,
) -> Result<MarkOutcome> {
    let sql = format!(
        "UPDATE {}
         SET status = 'Pending',
             last_error = $3,
             next_attempt_at = $4,
             claim_id = NULL,
             claimed_by = NULL,
             claim_expires_at = NULL
         WHERE message_id = $1 AND status = 'Publishing' AND claim_id = $2",
        table.qualified_name()
    );
    let result = sqlx::query(&sql)
        .bind(message_id)
        .bind(claim_id)
        .bind(error)
        .bind(retry_at)
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
    fn rejects_duplicate_changeset_versions() {
        let changesets: Vec<Box<dyn Changeset>> = vec![Box::new(InitSchema), Box::new(InitSchema)];
        assert!(matches!(
            ensure_unique_versions(&changesets),
            Err(Error::DuplicateChangesetVersion(1))
        ));
    }
}
