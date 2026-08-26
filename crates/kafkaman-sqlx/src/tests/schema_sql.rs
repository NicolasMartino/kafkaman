use kafkaman_core::{KafkaMessage, ReceivedFailureKind, SqlIdentifier};

use crate::schema_sql::{
    backfill_received_failure_metadata_sql, create_outbox_table_sql,
    create_received_failed_index_sql, received_failure_kind_sql_literal_list, sql_string_literal,
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
fn the_failure_backfill_speaks_every_spelling_a_stored_kind_can_have() {
    // The migration reads what past binaries wrote, which is two vocabularies:
    // the RFC 9457 `type` URI in use now and the bare discriminant that predates
    // it. Missing either leaves rows NULL and silently unreachable by a filtered
    // redrive, which is the failure this whole statement exists to prevent.
    let [kind_sql, timestamp_sql] = backfill_received_failure_metadata_sql(&received_table());
    for kind in ReceivedFailureKind::ALL {
        assert!(
            kind_sql.contains(kind.problem_type()),
            "{kind_sql} cannot recognize {}",
            kind.problem_type()
        );
        assert!(kind_sql.contains(kind.discriminant()));
    }

    // Guarded so each pass converges and so neither can touch what a current
    // binary wrote.
    assert!(kind_sql.contains("last_failure_kind IS NULL"));
    assert!(kind_sql.contains("jsonb_typeof(errors) = 'array'"));
    assert!(timestamp_sql.contains("last_failed_at IS NULL"));
    assert!(timestamp_sql.contains("jsonb_typeof(errors) = 'array'"));

    // The shape test is a value filter — it keeps `infinity` and friends, which
    // cast happily, from being read as a failure time. It is *not* what makes
    // the cast safe: `2026-99-99T…` is date-shaped and still raises. The
    // subtransaction is what keeps one malformed audit entry from aborting the
    // transaction and taking the changelog with it.
    assert!(
        timestamp_sql.contains("~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ]'"),
        "the RFC 9557 timestamp is cast only when it looks like one: {timestamp_sql}"
    );
    assert!(
        timestamp_sql.contains("EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow"),
        "a date-shaped non-date must cost only its own row: {timestamp_sql}"
    );
    assert!(
        !kind_sql.contains("::timestamptz"),
        "the kind pass must not carry the cast that can raise: {kind_sql}"
    );
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
