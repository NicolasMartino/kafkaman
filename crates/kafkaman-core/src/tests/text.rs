use std::panic;

use crate::panic_message;
use crate::text::truncate_on_char_boundary;

#[test]
fn a_string_under_the_limit_is_returned_whole() {
    assert_eq!(truncate_on_char_boundary("short", 256), "short");
}

#[test]
fn a_string_exactly_at_the_limit_is_not_cut() {
    let text = "a".repeat(256);
    assert_eq!(truncate_on_char_boundary(&text, 256).len(), 256);
}

/// The whole reason this is not `&text[..max]`: a cut through a multi-byte
/// character does not produce a `str`, it panics.
#[test]
fn a_cut_lands_on_a_character_boundary() {
    // Ten bytes, five characters, so byte 5 is mid-character.
    let text = "ééééé";
    let cut = truncate_on_char_boundary(text, 5);
    assert_eq!(cut, "éé", "the cut walks back to the boundary below 5");
    assert_eq!(cut.len(), 4);
}

/// A limit below the first character has no boundary to walk back to except the
/// start, and an empty string is the honest answer.
#[test]
fn a_limit_smaller_than_the_first_character_yields_nothing() {
    assert_eq!(truncate_on_char_boundary("é", 1), "");
}

#[test]
fn a_literal_panic_payload_keeps_its_message() {
    let payload = panic::catch_unwind(|| panic!("boom")).expect_err("the panic is the fixture");
    assert_eq!(panic_message(payload), "boom");
}

#[test]
fn a_formatted_panic_payload_keeps_its_message() {
    let attempt = 3;
    let payload = panic::catch_unwind(|| panic!("failed on attempt {attempt}"))
        .expect_err("the panic is the fixture");
    assert_eq!(panic_message(payload), "failed on attempt 3");
}

/// `panic_any` with a type that is neither `&str` nor `String` has no
/// displayable form. Saying so beats an empty string, which reads as a panic
/// that carried nothing.
#[test]
fn a_non_string_payload_says_so_rather_than_vanishing() {
    let payload =
        panic::catch_unwind(|| panic::panic_any(42_u32)).expect_err("the panic is the fixture");
    assert_eq!(panic_message(payload), "panicked with a non-string payload");
}

/// 512 bytes plus the marker. The number is a documented compatibility surface —
/// it bounds what reaches a received row's error history and the DLQ route.
#[test]
fn a_long_panic_message_is_truncated_and_marked() {
    let long = "é".repeat(2000);
    let payload =
        panic::catch_unwind(move || panic!("{long}")).expect_err("the panic is the fixture");
    let message = panic_message(payload);

    assert!(message.len() <= 512 + 3, "length was {}", message.len());
    assert!(
        message.ends_with("..."),
        "the cut should be marked: {message}"
    );
    assert!(
        message.chars().all(|c| c == 'é' || c == '.'),
        "cutting mid-character would not have produced a String at all"
    );
}

/// The marker is only added when something was actually removed, so a message
/// that happens to end in an ellipsis is not mistaken for a truncated one.
#[test]
fn a_message_within_the_limit_gains_no_marker() {
    let payload =
        panic::catch_unwind(|| panic!("no cut here")).expect_err("the panic is the fixture");
    assert_eq!(panic_message(payload), "no cut here");
}
