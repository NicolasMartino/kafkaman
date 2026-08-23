use time::{OffsetDateTime, UtcOffset};

use crate::rfc9557;

#[test]
fn renders_rfc9557_annotated_timestamps() {
    let value = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    assert_eq!(rfc9557::render(value).unwrap(), "2023-11-14T22:13:20Z[UTC]");
}

#[test]
fn normalizes_offsets_to_utc_before_annotating() {
    // The annotation must never disagree with the offset it accompanies, so
    // a non-UTC input is converted rather than labelled `[UTC]` in place.
    let value = OffsetDateTime::from_unix_timestamp(1_700_000_000)
        .unwrap()
        .to_offset(UtcOffset::from_hms(2, 0, 0).unwrap());
    assert_eq!(rfc9557::render(value).unwrap(), "2023-11-14T22:13:20Z[UTC]");
}

#[test]
fn parses_rfc9557_with_and_without_annotation() {
    let expected = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    // Annotated, as kafkaman writes it.
    assert_eq!(
        rfc9557::parse("2023-11-14T22:13:20Z[UTC]").unwrap(),
        expected
    );
    // Bare RFC 3339, which RFC 9557 defines as valid IXDTF.
    assert_eq!(rfc9557::parse("2023-11-14T22:13:20Z").unwrap(), expected);
    // A foreign producer's zone annotation, with a non-UTC offset.
    assert_eq!(
        rfc9557::parse("2023-11-15T00:13:20+02:00[Europe/Berlin]").unwrap(),
        expected
    );
}

#[test]
fn every_rendered_timestamp_parses_back_to_itself() {
    // Round-tripping is the property the audit trail depends on: a stored
    // `occurred_at` that renders but cannot be read back makes the row it
    // belongs to undeserializable, which takes the whole error history with it.
    for offset_seconds in [0, 1, -1, 86_399, -86_399, 1_700_000_000, -1_000_000] {
        let value = OffsetDateTime::from_unix_timestamp(offset_seconds).unwrap();
        let rendered = rfc9557::render(value).unwrap();
        assert_eq!(rfc9557::parse(&rendered).unwrap(), value, "{rendered}");
    }
}
