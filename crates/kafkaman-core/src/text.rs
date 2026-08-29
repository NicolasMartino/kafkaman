//! Bounding untrusted text before it is recorded.
//!
//! Both users of this module write strings they did not author into somewhere
//! that keeps them: a span's status description, and a received row's error
//! history. Neither string has a bound of its own, so the bound lives here.
//!
//! Named `text` rather than `panic` because the truncation is the general part
//! and the panic payload is one caller of it. A `mod panic` would also read as
//! [`std::panic`] at every use site.

use std::any::Any;

/// The longest panic message kept.
///
/// Unlike a handler's returned error, a panic message is not authored for
/// storage: `assert_eq!` on two large structures produces kilobytes of `Debug`
/// output, and this string is written to a received row's error history,
/// retained `errors_limit` deep, and reported by the DLQ route.
const MAX_PANIC_MESSAGE: usize = 512;

/// The longest prefix of `text` that fits in `max` bytes and ends on a character
/// boundary.
///
/// Byte-bounded rather than character-bounded, because the limits it serves are
/// storage limits. Walking back to a boundary is what keeps the result a `str`:
/// a cut through a multi-byte character would not be one.
pub(crate) fn truncate_on_char_boundary(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The message a panic carried, as far as it can be recovered, bounded for
/// storage.
///
/// `panic!` produces a `&'static str` for a literal and a `String` for anything
/// formatted. Everything else — `panic_any` with a custom type — has no
/// displayable form, and saying so is more useful than an empty string.
///
/// Lives in this crate rather than beside either caller because both the
/// dispatch-side handler boundary and the ingest-side decode boundary need it,
/// and they are in sibling crates that share only this one.
pub fn panic_message(payload: Box<dyn Any + Send>) -> String {
    let message = payload
        .downcast_ref::<&'static str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panicked with a non-string payload".to_owned());

    let truncated = truncate_on_char_boundary(&message, MAX_PANIC_MESSAGE);
    if truncated.len() == message.len() {
        return message;
    }
    // The marker matters: without it a truncated `Debug` dump reads as a
    // complete one, and an operator compares it against a value that never
    // ended there.
    format!("{truncated}...")
}
