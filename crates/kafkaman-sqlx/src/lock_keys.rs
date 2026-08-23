//! Deterministic Postgres advisory lock keys.
//!
//! These values are permanent. Every replica of an application must map the same
//! input to the same lock, and a rolling deploy has two binaries live at once —
//! so a build that hashes differently from its predecessor lets two writers that
//! should have serialized run concurrently. Changing anything here is a
//! coordination change, not a refactor.

use crate::OutboxTable;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// One FNV-1a 64-bit round over `bytes`, continuing from `hash`.
fn fnv1a(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

/// Lock key serializing migrations of one schema.
pub(crate) fn advisory_lock_key(schema: &str) -> i64 {
    fnv1a(FNV_OFFSET_BASIS, schema.as_bytes()) as i64
}

/// Lock key serializing enqueues of one entity of one message type.
pub(crate) fn outbox_entity_lock_key(table: &OutboxTable, entity_key: &str) -> i64 {
    [
        "outbox_entity",
        table.schema.as_str(),
        table.descriptor.message_type.as_str(),
        entity_key,
    ]
    .into_iter()
    // A delimiter after each part, so `("ab", "c")` cannot hash to the same key
    // as `("a", "bc")` and collapse two entities onto one lock.
    .fold(FNV_OFFSET_BASIS, |hash, part| {
        fnv1a(fnv1a(hash, part.as_bytes()), &[0xff])
    }) as i64
}
