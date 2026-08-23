use kafkaman_core::SqlIdentifier;

use crate::lock_keys::{advisory_lock_key, outbox_entity_lock_key};
use crate::tests::descriptor;
use crate::OutboxTable;

#[test]
fn advisory_lock_keys_are_stable_and_entity_scoped() {
    // Replicas must agree on the schema lock, or two migrations run at once.
    assert_eq!(advisory_lock_key("kafkaman"), advisory_lock_key("kafkaman"));
    assert_ne!(
        advisory_lock_key("kafkaman"),
        advisory_lock_key("kafkaman2")
    );

    let table = OutboxTable::new(SqlIdentifier::new("kafkaman").unwrap(), descriptor("a")).unwrap();
    let other = OutboxTable::new(SqlIdentifier::new("kafkaman").unwrap(), descriptor("b")).unwrap();
    // Different entities of one type must not serialize against each other.
    assert_ne!(
        outbox_entity_lock_key(&table, "e-1"),
        outbox_entity_lock_key(&table, "e-2")
    );
    // Nor may the same entity key of two message types collide.
    assert_ne!(
        outbox_entity_lock_key(&table, "e-1"),
        outbox_entity_lock_key(&other, "e-1")
    );
    // The delimiter must stop concatenation confusion between the parts.
    assert_ne!(
        outbox_entity_lock_key(&table, "xy"),
        outbox_entity_lock_key(&table, "x")
    );
}

#[test]
fn advisory_lock_key_values_are_pinned() {
    // These are wire values, not implementation detail. Every replica of an
    // application must map a schema to the same lock, and a rolling deploy has
    // two binaries live at once — so a build that hashes differently from its
    // predecessor lets two migrations of one schema run concurrently, which is
    // exactly what the lock exists to prevent. Changing these is a coordinated
    // rollout, so it must not be possible to do it by accident.
    assert_eq!(advisory_lock_key("kafkaman"), 7_849_300_636_957_484_259_i64);
}
