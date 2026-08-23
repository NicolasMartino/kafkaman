use kafkaman_config::RetryPolicy;
use kafkaman_core::{KafkaMessage, MessageDescriptor, SqlIdentifier};

use crate::lock_keys::advisory_lock_key;
use crate::{ResolvedConfig, Result};

/// The outbox table for one message type: rows waiting to reach the broker.
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
        qualified_name(&self.schema, &self.table)
    }

    pub(crate) fn index_name(&self, suffix: &str) -> SqlIdentifier {
        bounded_index_name(&self.table, suffix)
    }

    pub(crate) fn state_index_name(&self) -> SqlIdentifier {
        self.index_name("_state")
    }

    pub(crate) fn entity_state_index_name(&self) -> SqlIdentifier {
        self.index_name("_entity_state")
    }
}

/// The received table for one message type: rows that arrived and must be
/// dispatched exactly once.
#[derive(Clone, Debug)]
pub struct ReceivedTable {
    pub schema: SqlIdentifier,
    pub table: SqlIdentifier,
    pub descriptor: MessageDescriptor,
    pub retry: RetryPolicy,
}

impl ReceivedTable {
    pub fn new(schema: SqlIdentifier, descriptor: MessageDescriptor) -> Result<Self> {
        Self::new_with_retry(schema, descriptor, RetryPolicy::default())
    }

    fn new_with_retry(
        schema: SqlIdentifier,
        descriptor: MessageDescriptor,
        retry: RetryPolicy,
    ) -> Result<Self> {
        let table = SqlIdentifier::new(format!("received_{}", descriptor.message_type.as_str()))?;
        Ok(Self {
            schema,
            table,
            descriptor,
            retry,
        })
    }

    pub fn for_message<P: KafkaMessage>(cfg: &ResolvedConfig) -> Result<Self> {
        let descriptor = cfg.descriptor_for::<P>()?;
        let retry = cfg.retry.policy_for(descriptor.message_type.as_str());
        Self::new_with_retry(cfg.schema.clone(), descriptor, retry)
    }

    pub fn qualified_name(&self) -> String {
        qualified_name(&self.schema, &self.table)
    }

    pub(crate) fn index_name(&self, suffix: &str) -> SqlIdentifier {
        bounded_index_name(&self.table, suffix)
    }

    pub(crate) fn idempotency_index_name(&self) -> SqlIdentifier {
        self.index_name("_idempotency")
    }

    pub(crate) fn state_index_name(&self) -> SqlIdentifier {
        self.index_name("_state")
    }
}

/// The cache table for one message type: current state, one row per entity.
#[derive(Clone, Debug)]
pub struct CacheTable {
    pub schema: SqlIdentifier,
    pub table: SqlIdentifier,
    pub descriptor: MessageDescriptor,
}

impl CacheTable {
    pub fn new(schema: SqlIdentifier, descriptor: MessageDescriptor) -> Result<Self> {
        let table = SqlIdentifier::new(format!("cache_{}", descriptor.message_type.as_str()))?;
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
        qualified_name(&self.schema, &self.table)
    }
}

/// A schema-qualified, quoted table name, safe to interpolate into generated
/// SQL because both halves are validated [`SqlIdentifier`]s.
pub(crate) fn qualified_name(schema: &SqlIdentifier, table: &SqlIdentifier) -> String {
    format!("{}.{}", schema.quoted(), table.quoted())
}

/// Build an index name for `table` that fits PostgreSQL's 63-byte identifier
/// limit.
///
/// Two distinct long table names can share a 53-character prefix, so plain
/// truncation would silently collapse them onto one index name — and
/// `CREATE INDEX IF NOT EXISTS` would then skip creating the second table's
/// index entirely. A short deterministic hash of the full name keeps them apart.
///
/// Total by construction, so it returns `SqlIdentifier` rather than a `Result`:
/// `table` is already a validated identifier, `suffix` is an ASCII literal, and
/// `middle_limit` bounds the result to `MAX_LEN`. Slicing is byte-safe because
/// validated identifiers are ASCII.
fn bounded_index_name(table: &SqlIdentifier, suffix: &str) -> SqlIdentifier {
    const PREFIX: &str = "idx_";
    let base = table.as_str();
    let middle_limit = SqlIdentifier::MAX_LEN - PREFIX.len() - suffix.len();
    let middle = if base.len() <= middle_limit {
        base.to_owned()
    } else {
        let hash = format!("{:08x}", advisory_lock_key(base) as u32);
        let keep = middle_limit - hash.len() - 1;
        format!("{}_{}", &base[..keep], hash)
    };
    SqlIdentifier::new(format!("{PREFIX}{middle}{suffix}"))
        .unwrap_or_else(|_| unreachable!("index name is generated from validated parts"))
}
