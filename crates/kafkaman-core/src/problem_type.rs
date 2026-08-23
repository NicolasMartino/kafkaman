//! Serde support for [`ReceivedFailureKind`] as an RFC 9457 `type` URI.
use crate::ReceivedFailureKind;
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<S>(kind: &ReceivedFailureKind, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(kind.problem_type())
}

/// An unrecognized `type` degrades to the default kind rather than failing
/// the read. A stored audit record must stay readable by an older binary
/// that predates a newer failure class, for the same reason ingest tolerates
/// unknown enum variants instead of quarantining them.
pub fn deserialize<'de, D>(deserializer: D) -> Result<ReceivedFailureKind, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Ok(ReceivedFailureKind::from_problem_type(&raw).unwrap_or_default())
}
