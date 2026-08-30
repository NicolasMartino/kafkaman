use kafkaman_core::{KafkaMessage, ReceivedFailureKind, SqlIdentifier};

use crate::schema_sql::{
    create_outbox_table_sql, create_received_failed_index_sql,
    received_failure_kind_sql_literal_list, sql_string_literal,
};
use crate::tests::OrderCreated;
use crate::{OutboxTable, ReceivedTable};

fn received_table() -> ReceivedTable {
    ReceivedTable::new(
        SqlIdentifier::new("kafkaman").unwrap(),
        OrderCreated::descriptor().unwrap(),
    )
    .unwrap()
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
fn sql_string_literal_escapes_embedded_quotes() {
    assert_eq!(sql_string_literal("plain"), "'plain'");
    assert_eq!(sql_string_literal("O'Brien"), "'O''Brien'");
    assert_eq!(sql_string_literal("''"), "''''''");
    // Backslashes are literal under `standard_conforming_strings = on`
    // (the PostgreSQL default) and must not be doubled.
    assert_eq!(sql_string_literal(r"a\b"), r"'a\b'");
}

#[test]
fn received_failure_kind_check_list_covers_every_variant() {
    // The CHECK constraint is generated from this list; a variant missing
    // here would make a legitimate failure kind unwritable at runtime.
    let list = received_failure_kind_sql_literal_list();
    for kind in ReceivedFailureKind::ALL {
        assert!(
            list.contains(kind.discriminant()),
            "{list} is missing {}",
            kind.discriminant()
        );
    }
}

#[test]
fn the_dlq_index_carries_the_kind_after_the_ordering_chain() {
    // Order is the whole point. Leading with `last_failure_kind` would hand
    // filtered pages a range scan and send unfiltered ones back to sorting;
    // trailing, it filters inside the index and leaves the ordering alone.
    let sql = create_received_failed_index_sql(&received_table());
    assert!(sql.contains("(last_failed_at, created_at, message_id, last_failure_kind)"));
    assert!(sql.contains("WHERE status = 'Failed'"));
}
