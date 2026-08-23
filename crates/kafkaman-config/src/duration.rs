use std::time::Duration;

use serde::{de, Deserialize, Deserializer};

pub(crate) fn deserialize_duration<'de, D>(
    deserializer: D,
) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_duration(&value).map_err(de::Error::custom)
}

pub(crate) fn deserialize_optional_duration<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value {
        Some(value) => parse_duration(&value).map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

/// The units a duration may carry, and how many milliseconds each is worth.
///
/// A table rather than a `match` with one overflow arm per unit: the arms were
/// identical apart from the multiplier, and adding a unit meant copying an
/// overflow check that is easy to copy wrong.
const UNITS: &[(&str, u128)] = &[
    ("ms", 1),
    ("s", 1_000),
    ("m", 60_000),
    ("h", 3_600_000),
    ("d", 86_400_000),
];

/// Parse a duration of the form `<integer><unit>`, e.g. `250ms`, `30s`, `1h`.
///
/// Zero is accepted. `RelayConfig::validate` documents `retry_after = 0` as a
/// legitimate setting (retry immediately), so rejecting zero here would make a
/// supported value inexpressible in config.
///
/// Zero is not universally safe, though, and this parser is the wrong place to
/// decide: each field's validator owns its own rule. `lease_for` and
/// `poll_interval` are rejected by `RelayConfig::validate`; `initial_backoff` is
/// rejected by [`validate_policy`](crate::retry::validate_policy), because a
/// zero first backoff turns a permanently failing row into a hot loop against
/// the database.
pub(crate) fn parse_duration(input: &str) -> std::result::Result<Duration, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("duration must not be empty".to_owned());
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "duration must include a unit (ms, s, m, h, d)".to_owned())?;
    let (number, unit) = trimmed.split_at(split_at);
    if number.is_empty() {
        // Reached by a leading sign as well as by leading letters, so the
        // message names both rather than only the case that looks likelier.
        return Err(
            "duration must start with an unsigned integer, e.g. `250ms` or `30s`".to_owned(),
        );
    }

    let amount = number
        .parse::<u128>()
        .map_err(|_| "duration amount is too large".to_owned())?;
    let millis_per_unit = UNITS
        .iter()
        .find(|(name, _)| *name == unit)
        .map(|(_, millis)| *millis)
        .ok_or_else(|| "duration unit must be one of ms, s, m, h, d".to_owned())?;
    let millis = amount
        .checked_mul(millis_per_unit)
        .ok_or_else(|| "duration overflows milliseconds".to_owned())?;

    u64::try_from(millis)
        .map(Duration::from_millis)
        .map_err(|_| "duration overflows Duration".to_owned())
}
