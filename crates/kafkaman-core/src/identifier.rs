use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A table, column, or index name that is safe to interpolate into generated
/// SQL without escaping.
///
/// kafkaman builds its DDL and its queries by `format!`, because table names are
/// derived per message type and cannot be bound as parameters. That makes this
/// type the boundary the whole crate's SQL safety rests on: the accepted
/// alphabet — lowercase ASCII, digits, underscore, no leading digit, no reserved
/// word, at most 63 bytes — contains nothing that can terminate a quoted
/// identifier or open a comment.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    /// PostgreSQL truncates identifiers past this many bytes, silently. Two long
    /// names sharing a prefix would then collapse onto one, so the limit is
    /// enforced here rather than discovered later.
    pub const MAX_LEN: usize = 63;

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }
}

impl fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for SqlIdentifier {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl FromStr for SqlIdentifier {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    let invalid = |reason: &'static str| Error::InvalidIdentifier {
        value: value.to_owned(),
        reason,
    };

    // Taking the first character up front makes emptiness a pattern match
    // rather than a separate check followed by an `expect` that the earlier
    // check already made unreachable.
    let mut rest = value.chars();
    let Some(first) = rest.next() else {
        return Err(invalid("must not be empty"));
    };

    if value.len() > SqlIdentifier::MAX_LEN {
        return Err(invalid("must be at most 63 bytes"));
    }
    if !first.is_ascii_lowercase() {
        return Err(invalid("must start with a lowercase ASCII letter"));
    }
    if !rest.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return Err(invalid(
            "may contain only lowercase ASCII letters, digits, and underscores",
        ));
    }
    if RESERVED_WORDS.binary_search(&value).is_ok() {
        return Err(invalid("must not be a reserved SQL word"));
    }

    Ok(())
}

/// SQL words that would need quoting to be used as identifiers.
///
/// Sorted, because the lookup is a binary search — `reserved_words_are_sorted`
/// in the crate's tests pins that, since an out-of-order entry would make the
/// search silently miss it and let a reserved word through.
const RESERVED_WORDS: &[&str] = &[
    "all", "alter", "and", "as", "by", "case", "create", "delete", "drop", "from", "group",
    "insert", "into", "join", "limit", "not", "null", "or", "order", "select", "set", "table",
    "type", "update", "where",
];

/// The reserved-word list, for the test that pins its ordering.
#[cfg(test)]
pub(crate) fn reserved_words() -> &'static [&'static str] {
    RESERVED_WORDS
}
