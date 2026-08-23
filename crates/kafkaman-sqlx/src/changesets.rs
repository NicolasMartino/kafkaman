use kafkaman_core::MessageDescriptor;

use crate::changeset::{ChangeBuilder, Changeset};
use crate::schema_sql::{
    add_idempotency_key_sql, add_idempotency_source_sql, add_outbox_entity_key_sql,
    add_received_entity_key_sql, create_cache_table_sql, create_outbox_entity_state_index_sql,
    create_outbox_retention_index_sql, create_outbox_state_index_sql, create_outbox_table_sql,
    create_received_idempotency_index_sql, create_received_state_index_sql,
    create_received_table_sql,
};
use crate::{CacheTable, OutboxTable, ReceivedTable, ResolvedConfig, Result};

/// Declare a changeset that targets one message type's tables.
///
/// Every such changeset is the same struct — a version and a descriptor — with
/// the same constructor and the same checksum material; only the name and the
/// statements differ. Written out, that was six near-identical copies of about
/// thirty lines each, and the copies are not merely verbose: `checksum_material`
/// is what stops an edited changeset from re-running under an already-applied
/// version, so a copy that formats its material even slightly differently
/// silently breaks migration history for the type it covers.
///
/// The body receives `cfg`, an owned `descriptor`, and `builder`, and may use
/// `?`.
macro_rules! descriptor_changeset {
    (
        $(#[$meta:meta])*
        $name:ident => $changeset_name:literal,
        |$cfg:ident, $descriptor:ident, $builder:ident| $body:block
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        pub struct $name {
            pub version: i64,
            pub descriptor: MessageDescriptor,
        }

        impl $name {
            pub fn new(version: i64, descriptor: MessageDescriptor) -> Self {
                Self {
                    version,
                    descriptor,
                }
            }
        }

        impl Changeset for $name {
            fn version(&self) -> i64 {
                self.version
            }

            fn name(&self) -> &str {
                $changeset_name
            }

            fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()> {
                let $cfg = cfg;
                let $descriptor = self.descriptor.clone();
                let $builder = builder;
                $body
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
    };
}

/// Version-1 changelog baseline marker.
///
/// The schema and `changelog_history` table are created by
/// [`migrate`](crate::migrate)'s bootstrap step, because they must exist before
/// any changeset can be version-checked. `InitSchema` therefore emits no DDL of
/// its own — it is the single, explicit version-1 entry a changelog records so
/// application changesets can start at version 2. Keeping the DDL solely in the
/// bootstrap avoids two owners of the same schema/history-table definition.
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

descriptor_changeset! {
    /// Create a message type's outbox table and its three indexes.
    CreateOutboxTable => "create_outbox_table",
    |cfg, descriptor, builder| {
        let table = OutboxTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(create_outbox_table_sql(&table));
        builder.push(create_outbox_state_index_sql(&table));
        builder.push(create_outbox_entity_state_index_sql(&table));
        builder.push(create_outbox_retention_index_sql(&table));
    }
}

descriptor_changeset! {
    /// Create a message type's received table, its dedupe index, and its claim
    /// index.
    CreateReceivedTable => "create_received_table",
    |cfg, descriptor, builder| {
        let table = ReceivedTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(create_received_table_sql(&table));
        builder.push(create_received_idempotency_index_sql(&table));
        builder.push(create_received_state_index_sql(&table));
    }
}

descriptor_changeset! {
    /// Create a message type's cache table: current state, one row per entity.
    CreateCacheTable => "create_cache_table",
    |cfg, descriptor, builder| {
        let table = CacheTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(create_cache_table_sql(&table));
    }
}

descriptor_changeset! {
    /// Additive changeset that brings an outbox table created before entity-first
    /// outbound supersede up to the current shape. Fresh tables already include the
    /// column and index via [`create_outbox_table_sql`] and
    /// [`create_outbox_entity_state_index_sql`], so this is a no-op there; include it
    /// in a changelog only to upgrade pre-existing tables.
    AddOutboxEntityKey => "add_outbox_entity_key",
    |cfg, descriptor, builder| {
        let table = OutboxTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(add_outbox_entity_key_sql(&table));
        builder.push(create_outbox_entity_state_index_sql(&table));
    }
}

descriptor_changeset! {
    /// Additive changeset that brings an outbox table created before idempotency
    /// support up to the current shape. Fresh tables already include the columns via
    /// [`create_outbox_table_sql`], so these `ALTER ... ADD COLUMN IF NOT EXISTS`
    /// statements are no-ops there; include it in a changelog only to upgrade
    /// pre-existing tables.
    AddIdempotencyKey => "add_idempotency_key",
    |cfg, descriptor, builder| {
        let table = OutboxTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(add_idempotency_key_sql(&table));
        builder.push(add_idempotency_source_sql(&table));
    }
}

descriptor_changeset! {
    /// Additive changeset bringing a received table created before the entity key
    /// became a first-class column up to the current shape. Fresh tables already
    /// include the column via [`create_received_table_sql`], so this is a no-op
    /// there; include it in a changelog only to upgrade pre-existing tables.
    ///
    /// Rows written before this changeset keep `entity_key IS NULL` and fall back to
    /// the Kafka record key when the cache is applied. A legacy row with no record
    /// key fails with [`Error::MissingEntityKey`](crate::Error::MissingEntityKey)
    /// rather than being cached under a fabricated identity.
    AddReceivedEntityKey => "add_received_entity_key",
    |cfg, descriptor, builder| {
        let table = ReceivedTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(add_received_entity_key_sql(&table));
    }
}

descriptor_changeset! {
    /// Additive changeset adding the retention index to an outbox table created
    /// before retention existed. Fresh tables get it from [`CreateOutboxTable`].
    ///
    /// Note for adopters: changesets apply inside a transaction, so this builds the
    /// index non-concurrently and blocks writes on the table for the duration. On an
    /// outbox that has grown without retention — which is the situation this changeset
    /// exists to fix — that is a real outage window, and `enqueue` runs inside the
    /// caller's business transaction, so it propagates into application requests.
    AddOutboxRetentionIndex => "add_outbox_retention_index",
    |cfg, descriptor, builder| {
        let table = OutboxTable::new(cfg.schema.clone(), descriptor)?;
        builder.push(create_outbox_retention_index_sql(&table));
    }
}
