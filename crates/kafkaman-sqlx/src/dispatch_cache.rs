use kafkaman_core::ReceivedRow;
use sqlx::{Postgres, Row, Transaction};

use crate::{CacheTable, Error, ReceivedTable, Result};

/// What the guarded cache upsert did with a record.
///
/// The distinction matters because two of these look identical at the SQL layer
/// — both leave the cache row untouched — but mean opposite things. `Ignored` is
/// the guard working: a replayed or reordered record lost to state already
/// applied. `Applied` is forward progress. A topic/partition change is neither,
/// and is reported as [`Error::CacheOriginMismatch`] rather than a variant here,
/// because it must fail the row instead of being counted as a normal skip.
///
/// It fails the row terminally on the first occurrence rather than halting the
/// pipeline: the predicate can never become true again, so retrying only spends
/// the attempt budget and delays the operator signal, while halting dispatch
/// outright would let one entity's repartition stop every other entity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheApplyOutcome {
    /// The record was newer than the cached state and was written.
    Applied,
    /// The record was at or behind the applied offset and was correctly dropped.
    Ignored,
}

pub(crate) async fn upsert_cache_from_received(
    tx: &mut Transaction<'_, Postgres>,
    table: &ReceivedTable,
    row: &ReceivedRow,
) -> Result<CacheApplyOutcome> {
    let cache = CacheTable::new(table.schema.clone(), table.descriptor.clone())?;
    let entity_key = received_entity_key(row)?;
    let sql = format!(
        "INSERT INTO {name} (
            entity_key, payload, applied_topic, applied_partition, applied_offset, deleted,
            updated_at
        ) VALUES ($1, $2, $3, $4, $5, false, now())
        ON CONFLICT (entity_key) DO UPDATE
        SET payload = EXCLUDED.payload,
            applied_topic = EXCLUDED.applied_topic,
            applied_partition = EXCLUDED.applied_partition,
            applied_offset = EXCLUDED.applied_offset,
            updated_at = now()
        WHERE {name}.applied_topic = EXCLUDED.applied_topic
          AND {name}.applied_partition = EXCLUDED.applied_partition
          AND EXCLUDED.applied_offset > {name}.applied_offset
        RETURNING entity_key",
        name = cache.qualified_name()
    );

    let applied = sqlx::query(&sql)
        .bind(entity_key.as_str())
        .bind(row.payload.clone())
        .bind(row.source_topic.as_str())
        .bind(row.source_partition)
        .bind(row.source_offset)
        .fetch_optional(&mut **tx)
        .await?;

    if applied.is_some() {
        return Ok(CacheApplyOutcome::Applied);
    }

    // The upsert wrote nothing. Offsets are comparable only within one topic and
    // partition, so before calling this a correctly-ignored stale record we have
    // to rule out that the guard is instead permanently wedged: if the entity has
    // moved topic or partition, the `DO UPDATE` predicate can never again be
    // true and this row would freeze forever with no error and no metric.
    classify_skipped_cache_apply(tx, &cache, &entity_key, row).await
}

async fn classify_skipped_cache_apply(
    tx: &mut Transaction<'_, Postgres>,
    cache: &CacheTable,
    entity_key: &str,
    row: &ReceivedRow,
) -> Result<CacheApplyOutcome> {
    let sql = format!(
        "SELECT applied_topic, applied_partition FROM {} WHERE entity_key = $1",
        cache.qualified_name()
    );
    let Some(current) = sqlx::query(&sql)
        .bind(entity_key)
        .fetch_optional(&mut **tx)
        .await?
    else {
        // No cache row at all, yet the insert did not take: the only way to get
        // here is a concurrent writer that has since applied newer state.
        return Ok(CacheApplyOutcome::Ignored);
    };

    let applied_topic: String = current.try_get("applied_topic")?;
    let applied_partition: i32 = current.try_get("applied_partition")?;
    if applied_topic != row.source_topic || applied_partition != row.source_partition {
        return Err(Error::CacheOriginMismatch {
            entity_key: entity_key.to_owned(),
            applied_topic,
            applied_partition,
            incoming_topic: row.source_topic.clone(),
            incoming_partition: row.source_partition,
        });
    }

    Ok(CacheApplyOutcome::Ignored)
}

/// Resolve the entity a received row is a snapshot of, in descending order of
/// trust:
///
/// 1. the `entity_key` column, written from the typed payload at ingest;
/// 2. the Kafka record key, which is the entity key for types that partition on
///    it — the fallback for rows written before the column existed.
///
/// There is deliberately no `kafkaman-entity-key` header tier, even though
/// [`enqueue`](crate::enqueue) publishes that header. It could never fire: ingest strips the
/// whole reserved namespace from user headers, and `insert_received_with_outcome`
/// rejects outright any envelope that carries a reserved header, so no supported
/// write path can put one in `row.headers`. The header is published for *foreign
/// consumers* that cannot deserialize the typed payload; kafkaman's own ingest
/// always can, which is why it reads the payload instead.
///
/// There is also deliberately no fallback to `message_id`. That would give every
/// message its own cache row, so the cache would grow without bound and never
/// converge — a silent failure that looks like a working cache until someone
/// reads it. An unresolvable entity key is a hard error instead.
pub(crate) fn received_entity_key(row: &ReceivedRow) -> Result<String> {
    if let Some(entity_key) = &row.entity_key {
        return Ok(entity_key.clone());
    }

    if let Some(key) = &row.key {
        return String::from_utf8(key.clone()).map_err(|_| Error::InvalidEntityKey {
            message_type: row.message_type.clone(),
        });
    }

    Err(Error::MissingEntityKey {
        message_id: row.message_id,
        message_type: row.message_type.clone(),
    })
}
