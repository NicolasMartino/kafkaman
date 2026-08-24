//! Deriving a kafkaman changelog from declared roles.
//!
//! A hand-written changelog makes the service author pick the version integers,
//! which makes *registration order* the durable identity of a schema change.
//! That works exactly until two services, or two branches, or one refactor
//! disagree about the order — and then the migration engine, which keys history
//! on the version, either re-runs a create or reports a checksum mismatch for a
//! changeset nobody touched.
//!
//! So generated changesets take their identity from what they actually are:
//! `(table kind, message type, template version)`.
//!
//! # Why table kind and not the registration role
//!
//! `cache::<T>()`, `handle::<T>()`, and `handle_before::<T>()` are three ways to
//! say the same thing about the schema: this service consumes `T`, so it needs a
//! received table and a cache table. They differ only in what application code
//! runs during dispatch, which is not a schema fact at all. Keying identity on
//! the registration role would give the same DDL a different version depending
//! on which of the three the author wrote, so swapping `cache::<T>()` for
//! `handle::<T>()` — adding a handler to a type already being consumed — would
//! present as a brand-new changeset against an already-created table.
//!
//! The three schema roles are therefore the three table kinds, and the four
//! registration roles map onto them: `publish` to `outbox`, and any consuming
//! role to `received` plus `cache`.
//!
//! # Band allocation
//!
//! ```text
//! band  = first 40 bits of sha256("kafkaman:changeset-band:v1\0" || kind || "\0" || message_type)
//! version = RESERVED_CEILING + band * BAND_WIDTH + template_version
//! ```
//!
//! - **Reserved range.** Everything below [`RESERVED_CEILING`] belongs to library
//!   singletons and hand-written changelogs. [`InitSchema`](crate::InitSchema) is
//!   hardcoded to `1` and must keep sorting first.
//! - **Width.** [`BAND_WIDTH`] slots per table leave room for 1024 template
//!   versions, and the widest possible version stays below `2^51` — comfortably
//!   inside the `BIGINT` primary key of `changelog_history`.
//! - **Hash.** A fixed, explicitly-versioned SHA-256, never `DefaultHasher`,
//!   whose output `std` does not promise to keep stable across releases. The
//!   digest is part of the durable contract: changing the `v1` prefix is a
//!   breaking migration change, not a refactor.
//! - **Collisions.** At 2^40 bands a collision is vanishingly unlikely and still
//!   possible, so it is detected rather than assumed away — see
//!   [`Error::ChangesetBandCollision`].
//!
//! Inter-table order is arbitrary and must not be relied on. Only the slot order
//! *within* one table carries meaning.

use std::collections::BTreeMap;

use kafkaman_core::MessageDescriptor;
use sha2::{Digest, Sha256};

use crate::changelog::assert_changelog_order;
use crate::changeset::Changeset;
use crate::changesets::{CreateCacheTable, CreateOutboxTable, CreateReceivedTable, InitSchema};
use crate::schema_sql::{
    CACHE_TEMPLATE_VERSION, OUTBOX_TEMPLATE_VERSION, RECEIVED_TEMPLATE_VERSION,
};
use crate::{Error, Result};

/// Versions below this belong to library singletons and hand-written changelogs.
pub const RESERVED_CEILING: i64 = 1_000;

/// Template slots per table, and therefore the spacing between two bands.
pub const BAND_WIDTH: i64 = 1_024;

/// Bands are drawn from a 40-bit space.
const BAND_BITS: u32 = 40;

/// Domain separator for the band digest. Bumping this renumbers every generated
/// changeset in every database, so it is a breaking migration change.
const BAND_DOMAIN: &[u8] = b"kafkaman:changeset-band:v1\0";

/// A kind of table kafkaman generates, which is also the unit of changeset
/// identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableKind {
    /// Written by [`enqueue`](crate::enqueue), drained by the relay.
    Outbox,
    /// The durable inbound ledger and dedupe window.
    Received,
    /// The converged current state of every entity on a topic.
    Cache,
}

impl TableKind {
    /// Every kind, in a fixed order. Used for generation and exhaustiveness.
    pub const ALL: [Self; 3] = [Self::Outbox, Self::Received, Self::Cache];

    /// The kinds a consumed message type needs.
    pub const CONSUMED: [Self; 2] = [Self::Received, Self::Cache];

    /// The ASCII name hashed into the band. Part of the durable contract:
    /// renaming one renumbers that kind's changesets everywhere.
    #[must_use]
    pub const fn band_name(self) -> &'static str {
        match self {
            Self::Outbox => "outbox",
            Self::Received => "received",
            Self::Cache => "cache",
        }
    }

    /// The current template slot for this kind.
    #[must_use]
    pub const fn template_version(self) -> i64 {
        match self {
            Self::Outbox => OUTBOX_TEMPLATE_VERSION,
            Self::Received => RECEIVED_TEMPLATE_VERSION,
            Self::Cache => CACHE_TEMPLATE_VERSION,
        }
    }
}

/// The band a table kind and message type are allocated.
#[must_use]
pub fn band(kind: TableKind, message_type: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(BAND_DOMAIN);
    hasher.update(kind.band_name().as_bytes());
    hasher.update(b"\0");
    hasher.update(message_type.as_bytes());
    let digest = hasher.finalize();

    // Big-endian over the leading bytes, masked to `BAND_BITS`. Reading a fixed
    // prefix rather than folding the whole digest keeps the mapping trivially
    // reproducible by anything that can run SHA-256, which matters because the
    // numbers end up as primary keys in a user's database.
    let mut raw = 0_u64;
    for byte in digest.iter().take(8) {
        raw = (raw << 8) | u64::from(*byte);
    }
    let masked = raw >> (64 - BAND_BITS);

    // `masked` is 40 bits, so this cannot overflow `i64` and cannot be negative.
    i64::try_from(masked).unwrap_or(0)
}

/// The changeset version for one table kind, message type, and template slot.
#[must_use]
pub fn changeset_version(kind: TableKind, message_type: &str, template_version: i64) -> i64 {
    RESERVED_CEILING + band(kind, message_type) * BAND_WIDTH + template_version
}

/// The band a generated version belongs to. Inverse of [`changeset_version`].
#[must_use]
pub fn band_of(version: i64) -> i64 {
    (version - RESERVED_CEILING) / BAND_WIDTH
}

/// Upgrade changesets for a table kind, for slots `1..=template_version`.
///
/// Empty for all three kinds today: every shipped template is at slot 0. This is
/// the function a template bump extends — see the note at the top of
/// `schema_sql.rs`. It exists now, rather than being added with the first
/// upgrade, so that the slot arithmetic it depends on is exercised and pinned
/// before anything relies on it.
fn upgrades(kind: TableKind, _descriptor: &MessageDescriptor) -> Vec<Box<dyn Changeset>> {
    match kind {
        TableKind::Outbox | TableKind::Received | TableKind::Cache => Vec::new(),
    }
}

/// The create changeset for one table kind, at slot 0 of the given band.
fn create(kind: TableKind, descriptor: MessageDescriptor, band: i64) -> Box<dyn Changeset> {
    let version = RESERVED_CEILING + band * BAND_WIDTH;
    match kind {
        TableKind::Outbox => Box::new(CreateOutboxTable::new(version, descriptor)),
        TableKind::Received => Box::new(CreateReceivedTable::new(version, descriptor)),
        TableKind::Cache => Box::new(CreateCacheTable::new(version, descriptor)),
    }
}

/// One table's generated changesets: the create at slot 0, then any upgrades.
fn changesets_for(
    kind: TableKind,
    descriptor: &MessageDescriptor,
    band: i64,
) -> Vec<Box<dyn Changeset>> {
    let mut changesets = vec![create(kind, descriptor.clone(), band)];
    changesets.extend(upgrades(kind, descriptor));
    changesets
}

/// Build the kafkaman-owned changelog for a set of `(kind, descriptor)` pairs.
///
/// Sorts by version and detects band collisions *before*
/// [`assert_changelog_order`] sees the slice: that function validates the slice
/// is ascending, not that the set is unique, so an unsorted set of valid
/// versions would fail as a disorder error and a genuine collision would surface
/// as [`Error::DuplicateChangesetVersion`] — which reads like a library bug to
/// the one user who ever hits it.
pub(crate) fn build_changelog(
    tables: impl IntoIterator<Item = (TableKind, MessageDescriptor)>,
) -> Result<Vec<Box<dyn Changeset>>> {
    build_changelog_with_bands(tables, band)
}

/// [`build_changelog`], with the band allocator injected.
///
/// The seam exists for exactly one reason: a band collision is the failure this
/// module's error handling is written for, and the real 40-bit digest cannot be
/// made to collide on demand. Testing the collision path by constructing the
/// error value instead would assert the message and prove nothing about whether
/// the code ever reaches it.
fn build_changelog_with_bands(
    tables: impl IntoIterator<Item = (TableKind, MessageDescriptor)>,
    band_of_table: impl Fn(TableKind, &str) -> i64,
) -> Result<Vec<Box<dyn Changeset>>> {
    let mut owners: BTreeMap<i64, (TableKind, String)> = BTreeMap::new();
    let mut generated: Vec<Box<dyn Changeset>> = Vec::new();

    for (kind, descriptor) in tables {
        let message_type = descriptor.message_type.as_str().to_owned();
        let claimed = band_of_table(kind, &message_type);

        if let Some((other_kind, other_type)) = owners.get(&claimed) {
            if (*other_kind, other_type.as_str()) != (kind, message_type.as_str()) {
                return Err(Error::ChangesetBandCollision {
                    band: claimed,
                    first: format!("{}/{other_type}", other_kind.band_name()),
                    second: format!("{}/{message_type}", kind.band_name()),
                });
            }
            // Same table declared twice: roles are deduplicated upstream, but
            // tolerating it here keeps this function usable on its own.
            continue;
        }

        owners.insert(claimed, (kind, message_type));
        generated.extend(changesets_for(kind, &descriptor, claimed));
    }

    let mut changelog: Vec<Box<dyn Changeset>> = vec![Box::new(InitSchema)];
    generated.sort_by_key(|changeset| changeset.version());
    changelog.extend(generated);

    assert_changelog_order(&changelog)?;
    Ok(changelog)
}

#[cfg(test)]
pub(crate) fn build_changelog_with_forced_band(
    tables: impl IntoIterator<Item = (TableKind, MessageDescriptor)>,
    forced: i64,
) -> Result<Vec<Box<dyn Changeset>>> {
    build_changelog_with_bands(tables, move |_, _| forced)
}
