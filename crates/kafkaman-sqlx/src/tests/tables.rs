use kafkaman_core::SqlIdentifier;

use crate::tests::descriptor;
use crate::{OutboxTable, ReceivedTable};

#[test]
fn long_outbox_table_names_get_distinct_state_indexes() {
    // Two message types whose outbox table names share the first 53 chars
    // would collide under plain truncation; the hash suffix keeps them apart.
    let schema = SqlIdentifier::new("kafkaman").unwrap();
    let prefix = "a".repeat(46);
    let table_a = OutboxTable::new(schema.clone(), descriptor(&format!("{prefix}_one"))).unwrap();
    let table_b = OutboxTable::new(schema, descriptor(&format!("{prefix}_two"))).unwrap();

    let idx_a = table_a.state_index_name();
    let idx_b = table_b.state_index_name();

    assert_ne!(idx_a.as_str(), idx_b.as_str());
    assert!(idx_a.as_str().len() <= SqlIdentifier::MAX_LEN);
    assert!(idx_b.as_str().len() <= SqlIdentifier::MAX_LEN);
}

#[test]
fn long_received_table_names_get_distinct_bounded_indexes() {
    // Same collision hazard as the outbox side: PostgreSQL truncates
    // identifiers at 63 bytes, so two long names must not collapse onto one
    // index name.
    let schema = SqlIdentifier::new("kafkaman").unwrap();
    let prefix = "b".repeat(46);
    let a = ReceivedTable::new(schema.clone(), descriptor(&format!("{prefix}_one"))).unwrap();
    let b = ReceivedTable::new(schema, descriptor(&format!("{prefix}_two"))).unwrap();

    for name in [
        a.idempotency_index_name(),
        a.state_index_name(),
        b.idempotency_index_name(),
        b.state_index_name(),
    ] {
        assert!(name.as_str().len() <= SqlIdentifier::MAX_LEN, "{name}");
    }
    assert_ne!(
        a.idempotency_index_name().as_str(),
        b.idempotency_index_name().as_str()
    );
    assert_ne!(a.state_index_name().as_str(), b.state_index_name().as_str());
}

#[test]
fn table_names_are_derived_per_message_type() {
    let schema = SqlIdentifier::new("app").unwrap();
    let outbox = OutboxTable::new(schema.clone(), descriptor("order_created")).unwrap();
    let received = ReceivedTable::new(schema, descriptor("order_created")).unwrap();

    assert_eq!(outbox.qualified_name(), "\"app\".\"outbox_order_created\"");
    assert_eq!(
        received.qualified_name(),
        "\"app\".\"received_order_created\""
    );
}
