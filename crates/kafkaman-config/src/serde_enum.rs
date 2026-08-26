//! Deserializing a string into a closed set of variants.
//!
//! Shared rather than repeated: every enum here rejects the same way, and the
//! expectation string is what makes the error name the legal values instead of
//! just saying the input was wrong.
//!
//! # The spelling is exact, and that is the point
//!
//! Every caller matches lowercase, hyphenated literals: `warn`, `per-message`,
//! `kafkaman-only`. `Warn` and `per_message` are rejected, and deliberately —
//! accepting variant spellings case-insensitively means two config files that
//! read differently behave identically, and the first person to grep for
//! `per-message` across a fleet misses half of it. The cost of being strict is
//! one error at boot; the cost of being lenient is paid later and by somebody
//! else.
//!
//! What makes strictness bearable is that the error names every legal value, so
//! the operator fixes it from the message rather than from the source. That is
//! what `expected` is for, and why it is a parameter rather than a generic
//! "invalid value".
use serde::de::{self, Unexpected};
use serde::{Deserialize, Deserializer};

pub(crate) fn string_enum<'de, D, T>(
    deserializer: D,
    expected: &'static str,
    parse: impl FnOnce(&str) -> Option<T>,
) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse(value.as_str())
        .ok_or_else(|| de::Error::invalid_value(Unexpected::Str(&value), &expected))
}
