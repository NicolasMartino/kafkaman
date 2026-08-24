//! Serde support for RFC 9557 (IXDTF) timestamps.
//!
//! RFC 9557 extends RFC 3339 with a bracketed annotation suffix. kafkaman
//! normalizes to UTC before formatting, so it always emits `[UTC]`. Parsing
//! tolerates a missing annotation, so plain RFC 3339 values — which RFC 9557
//! defines as valid IXDTF — and values from foreign producers both round-trip.
//!
//! Note that PostgreSQL cannot cast an annotated timestamp to `timestamptz`.
//! This format is therefore only ever used inside audit JSON that SQL does not
//! parse; queries filter and order on real `timestamptz` columns instead.
use serde::de::Error as _;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serializer};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

/// Render `value` as an RFC 9557 timestamp annotated `[UTC]`.
pub fn render(value: OffsetDateTime) -> Result<String, time::error::Format> {
    let utc = value.to_offset(UtcOffset::UTC);
    Ok(format!("{}[UTC]", utc.format(&Rfc3339)?))
}

/// Parse an RFC 9557 timestamp, tolerating an absent annotation.
pub fn parse(value: &str) -> Result<OffsetDateTime, time::error::Parse> {
    let base = value.split_once('[').map_or(value, |(base, _)| base);
    OffsetDateTime::parse(base, &Rfc3339)
}

pub fn serialize<S>(value: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&render(*value).map_err(S::Error::custom)?)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse(&raw).map_err(D::Error::custom)
}

/// The same format for `Option<OffsetDateTime>` fields.
///
/// `serde(with = ...)` dispatches on the field's declared type, so an
/// optional timestamp cannot reuse the scalar adapter above: it needs its
/// own pair that maps `None` to `null`. Nullable operational columns —
/// `claim_expires_at`, `next_attempt_at`, `processed_at` — all need this.
pub mod option {
    use super::{parse, render};
    use serde::de::Error as _;
    use serde::ser::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};
    use time::OffsetDateTime;

    pub fn serialize<S>(value: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(value) => serializer.serialize_str(&render(*value).map_err(S::Error::custom)?),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<String>::deserialize(deserializer)? {
            Some(raw) => parse(&raw).map(Some).map_err(D::Error::custom),
            None => Ok(None),
        }
    }
}
