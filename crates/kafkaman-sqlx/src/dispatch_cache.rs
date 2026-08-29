use kafkaman_core::InstrumentDb;
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
///
/// An origin change is only a failure when it is *unauthorized*. A record that
/// arrives on the topic its own message type declares is authoritative by
/// definition, and is how a rebuilt topic reaches a cache that still points at
/// the old one — see [`Migrated`](CacheApplyOutcome::Migrated).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheApplyOutcome {
    /// The record was newer than the cached state and was written.
    Applied,
    /// The record was at or behind the applied offset and was correctly dropped.
    Ignored,
    /// The cached state came from a different topic, the record arrived on the
    /// type's declared topic, and the guard was reset to adopt the new origin.
    ///
    /// This is what makes a topic rebuild survivable. Offsets compare only
    /// within one topic-partition, so a cache holding an offset from a retired
    /// topic can never again satisfy the guard; without this the row would be
    /// frozen for good and every subsequent record would fail terminally.
    Migrated,
}

#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
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
        .instrument_db(kafkaman_core::db_span!(
            "UPSERT",
            cache.qualified_name(),
            "apply received row to cache",
        ))
        .await?;

    if applied.is_some() {
        return Ok(CacheApplyOutcome::Applied);
    }

    // The upsert wrote nothing. Offsets are comparable only within one topic and
    // partition, so before calling this a correctly-ignored stale record we have
    // to rule out that the guard is instead permanently wedged: if the entity has
    // moved topic or partition, the `DO UPDATE` predicate can never again be
    // true and this row would freeze forever with no error and no metric.
    classify_skipped_cache_apply(tx, &cache, &entity_key, row, &table.descriptor.topic).await
}

/// Decide what a written-nothing upsert actually meant.
///
/// Four outcomes hide behind the same empty result, and the declared topic — the
/// one this message type says it lives on — is what separates them:
///
/// 1. **same origin** — the guard did its job on a replayed or reordered record;
/// 2. **same topic, different partition** — an in-place repartition, which is
///    terminal. The supported way to change a partition count is to republish
///    every entity onto a *new* topic, precisely because offsets are comparable
///    only within one topic-partition; accepting this would bless the operation
///    the model tells you not to perform;
/// 3. **different topic, and the record is on the declared one** — the topic was
///    rebuilt and the cache still points at the retired one. Authorized: adopt
///    the new origin and reset the guard, or the row freezes for good;
/// 4. **different topic, and the record is not on the declared one** — either a
///    straggler from a topic the cache has already migrated off, which is
///    correctly dropped, or a consumer wired to the wrong topic, which must fail
///    loudly.
///
/// Case 3 is why the authorization matters. Accepting *any* origin change would
/// mean a consumer misconfigured onto the wrong topic silently overwrote a cache
/// with another domain's state.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn classify_skipped_cache_apply(
    tx: &mut Transaction<'_, Postgres>,
    cache: &CacheTable,
    entity_key: &str,
    row: &ReceivedRow,
    declared_topic: &str,
) -> Result<CacheApplyOutcome> {
    let sql = format!(
        "SELECT applied_topic, applied_partition FROM {} WHERE entity_key = $1",
        cache.qualified_name()
    );
    let Some(current) = sqlx::query(&sql)
        .bind(entity_key)
        .fetch_optional(&mut **tx)
        .instrument_db(kafkaman_core::db_span!(
            "SELECT",
            cache.qualified_name(),
            "classify skipped cache apply",
        ))
        .await?
    else {
        // No cache row at all, yet the insert did not take: the only way to get
        // here is a concurrent writer that has since applied newer state.
        return Ok(CacheApplyOutcome::Ignored);
    };

    let applied_topic: String = current.try_get("applied_topic")?;
    let applied_partition: i32 = current.try_get("applied_partition")?;

    if applied_topic == row.source_topic && applied_partition == row.source_partition {
        return Ok(CacheApplyOutcome::Ignored);
    }

    let mismatch = || Error::CacheOriginMismatch {
        entity_key: entity_key.to_owned(),
        applied_topic: applied_topic.clone(),
        applied_partition,
        incoming_topic: row.source_topic.clone(),
        incoming_partition: row.source_partition,
    };

    // Case 2. Same topic, moved partition: an in-place repartition. Not
    // authorized by a declared topic and never will be — the declaration says
    // which topic is authoritative, not which partition an entity belongs on.
    if applied_topic == row.source_topic {
        return Err(mismatch());
    }

    // Case 3.
    if row.source_topic == declared_topic {
        return migrate_cache_origin(tx, cache, entity_key, row).await;
    }

    // Case 4. The cache has already migrated onto the declared topic and this
    // record is a straggler from the retired one, still draining out of the
    // received table. Dropping it is the correct, quiet outcome — failing it
    // would fill the error table for the length of a cutover.
    if applied_topic == declared_topic {
        return Ok(CacheApplyOutcome::Ignored);
    }

    Err(mismatch())
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

/// Adopt a new topic for one entity, resetting the offset guard.
///
/// Takes the row lock the read path deliberately does not: the common case
/// reaching [`classify_skipped_cache_apply`] is an ignored replay, and making
/// every one of those lock a row to answer a question they almost never need
/// would be a real cost for a rare recovery.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
async fn migrate_cache_origin(
    tx: &mut Transaction<'_, Postgres>,
    cache: &CacheTable,
    entity_key: &str,
    row: &ReceivedRow,
) -> Result<CacheApplyOutcome> {
    let locked = sqlx::query(&format!(
        "SELECT applied_topic, applied_partition, applied_offset FROM {} \
         WHERE entity_key = $1 FOR UPDATE",
        cache.qualified_name()
    ))
    .bind(entity_key)
    .fetch_optional(&mut **tx)
    .instrument_db(kafkaman_core::db_span!(
        "SELECT",
        cache.qualified_name(),
        "lock cache origin",
    ))
    .await?;

    let Some(locked) = locked else {
        // Deleted between the unlocked read and this one.
        return Ok(CacheApplyOutcome::Ignored);
    };

    let applied_topic: String = locked.try_get("applied_topic")?;
    let applied_partition: i32 = locked.try_get("applied_partition")?;
    let applied_offset: i64 = locked.try_get("applied_offset")?;

    // Another dispatcher may have migrated this entity between the two reads,
    // in which case the origin now matches and ordinary offset ordering applies
    // again.
    let same_origin =
        applied_topic == row.source_topic && applied_partition == row.source_partition;
    if same_origin && row.source_offset <= applied_offset {
        return Ok(CacheApplyOutcome::Ignored);
    }

    sqlx::query(&format!(
        "UPDATE {} SET payload = $2, applied_topic = $3, applied_partition = $4, \
         applied_offset = $5, deleted = false, updated_at = now() WHERE entity_key = $1",
        cache.qualified_name()
    ))
    .bind(entity_key)
    .bind(row.payload.clone())
    .bind(row.source_topic.as_str())
    .bind(row.source_partition)
    .bind(row.source_offset)
    .execute(&mut **tx)
    .instrument_db(kafkaman_core::db_span!(
        "UPDATE",
        cache.qualified_name(),
        "migrate cache origin",
    ))
    .await?;

    Ok(if same_origin {
        CacheApplyOutcome::Applied
    } else {
        CacheApplyOutcome::Migrated
    })
}
