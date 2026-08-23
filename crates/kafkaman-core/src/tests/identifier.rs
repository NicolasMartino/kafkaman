use crate::identifier::reserved_words;
use crate::SqlIdentifier;

#[test]
fn validates_sql_identifier() {
    assert!(SqlIdentifier::new("order_created").is_ok());
    assert!(SqlIdentifier::new("OrderCreated").is_err());
    assert!(SqlIdentifier::new("type").is_err());
    assert!(SqlIdentifier::new("order-created").is_err());
    assert!(SqlIdentifier::new("").is_err());
    assert!(SqlIdentifier::new("1st").is_err());
}

#[test]
fn identifier_length_is_bounded_at_postgres_limit() {
    // Postgres truncates past 63 bytes rather than erroring, so two long names
    // sharing a prefix would silently collapse onto one table.
    let longest = "a".repeat(SqlIdentifier::MAX_LEN);
    assert!(SqlIdentifier::new(longest).is_ok());

    let too_long = "a".repeat(SqlIdentifier::MAX_LEN + 1);
    assert!(SqlIdentifier::new(too_long).is_err());
}

#[test]
fn reserved_words_are_sorted() {
    // The lookup is a binary search. An out-of-order entry would make the
    // search miss it silently, letting a reserved word through as a table name
    // and producing a syntax error in generated DDL instead of a clear
    // validation failure.
    let words = reserved_words();
    assert!(
        words.windows(2).all(|pair| pair[0] < pair[1]),
        "RESERVED_WORDS must stay sorted: {words:?}"
    );
}
