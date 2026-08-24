//! Deserializing a string into a closed set of variants.
//!
//! Shared rather than repeated: every enum here rejects the same way, and the
//! expectation string is what makes the error name the legal values instead of
//! just saying the input was wrong.
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
