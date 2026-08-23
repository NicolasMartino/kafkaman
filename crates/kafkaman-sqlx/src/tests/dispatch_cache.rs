use crate::dispatch_cache::received_entity_key;
use crate::tests::received_row_fixture;
use crate::Error;

#[test]
fn entity_key_resolution_prefers_the_column_then_the_record_key() {
    // The column is written from the typed payload at ingest and is the only
    // source that survives a broker round trip intact, so it must win.
    let mut row = received_row_fixture();
    row.entity_key = Some("from-column".to_owned());
    row.key = Some(b"from-record-key".to_vec());
    assert_eq!(received_entity_key(&row).unwrap(), "from-column");

    // Without the column — a row written before it existed — the record key
    // is the entity key for every type that partitions on it.
    row.entity_key = None;
    assert_eq!(received_entity_key(&row).unwrap(), "from-record-key");
}

#[test]
fn entity_key_resolution_ignores_the_reserved_header() {
    // This assertion used to run the other way, and passed only because the
    // fixture builds the row struct directly. No supported write path can
    // produce it: ingest strips the whole reserved namespace from user
    // headers, and `insert_received_with_outcome` rejects any envelope that
    // carries one. Consulting the header was therefore dead code wearing a
    // green test, so the tier is gone and this pins its absence.
    //
    // The header is still *published*, for foreign consumers that cannot
    // deserialize the typed payload. kafkaman's own ingest always can.
    let mut row = received_row_fixture();
    row.headers
        .insert("kafkaman-entity-key".to_owned(), "from-header".to_owned());

    row.key = Some(b"from-record-key".to_vec());
    assert_eq!(
        received_entity_key(&row).unwrap(),
        "from-record-key",
        "the header must not outrank the record key"
    );

    row.key = None;
    assert!(
        matches!(
            received_entity_key(&row),
            Err(Error::MissingEntityKey { .. })
        ),
        "a header-only row has no resolvable entity key"
    );
}

#[test]
fn entity_key_resolution_errors_rather_than_fabricating_a_key() {
    // Falling back to `message_id` would give every message its own cache
    // row: the cache would grow without bound and never converge, and it
    // would look like a working cache until someone read it.
    let row = received_row_fixture();
    assert!(matches!(
        received_entity_key(&row),
        Err(Error::MissingEntityKey { .. })
    ));
}

#[test]
fn entity_key_resolution_rejects_a_non_utf8_record_key() {
    let mut row = received_row_fixture();
    row.key = Some(vec![0xff, 0xfe]);
    assert!(matches!(
        received_entity_key(&row),
        Err(Error::InvalidEntityKey { .. })
    ));
}
