use std::fmt;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey([u8; 32]);

pub const LEGACY_STRING_IDEMPOTENCY_NAMESPACE: &str = "kafkaman:legacy-string:v1";

impl IdempotencyKey {
    pub const HEX_LEN: usize = 64;

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(value: &str) -> Result<Self> {
        if value.len() != Self::HEX_LEN {
            return Err(Error::InvalidIdempotencyKey {
                value: value.to_owned(),
                reason: "must be 64 hexadecimal characters",
            });
        }

        let mut bytes = [0_u8; 32];
        let raw = value.as_bytes();
        for index in 0..32 {
            let high =
                decode_hex_nibble(raw[index * 2]).ok_or_else(|| Error::InvalidIdempotencyKey {
                    value: value.to_owned(),
                    reason: "must contain only hexadecimal characters",
                })?;
            let low = decode_hex_nibble(raw[index * 2 + 1]).ok_or_else(|| {
                Error::InvalidIdempotencyKey {
                    value: value.to_owned(),
                    reason: "must contain only hexadecimal characters",
                }
            })?;
            bytes[index] = (high << 4) | low;
        }

        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(Self::HEX_LEN);
        for byte in self.0 {
            output.push(hex_digit(byte >> 4));
            output.push(hex_digit(byte & 0x0f));
        }
        output
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for IdempotencyKey {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::from_hex(value)
    }
}

impl Serialize for IdempotencyKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for IdempotencyKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdempotencySource(serde_json::Value);

impl IdempotencySource {
    pub fn new(source: impl Serialize) -> Result<Self> {
        let value = serde_json::to_value(source)
            .map_err(|err| Error::InvalidIdempotencySource(err.to_string()))?;
        Ok(Self(value))
    }

    pub fn value(&self) -> &serde_json::Value {
        &self.0
    }

    pub fn into_value(self) -> serde_json::Value {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyIdentity {
    pub key: IdempotencyKey,
    pub source: Option<IdempotencySource>,
}

impl IdempotencyIdentity {
    pub fn derive(namespace: impl AsRef<str>, source: impl Serialize) -> Result<Self> {
        let namespace = namespace.as_ref();
        if namespace.trim().is_empty() {
            return Err(Error::InvalidIdempotencyNamespace {
                value: namespace.to_owned(),
                reason: "must not be empty or whitespace",
            });
        }
        let source = IdempotencySource::new(source)?;
        let canonical_source = serde_json::to_vec(source.value())
            .map_err(|err| Error::InvalidIdempotencySource(err.to_string()))?;

        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0_u8]);
        hasher.update(canonical_source);
        let digest = hasher.finalize();
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&digest);

        Ok(Self {
            key: IdempotencyKey::from_bytes(bytes),
            source: Some(source),
        })
    }

    pub fn from_parts(key: IdempotencyKey, source: IdempotencySource) -> Self {
        Self {
            key,
            source: Some(source),
        }
    }

    pub fn from_key(key: IdempotencyKey) -> Self {
        Self { key, source: None }
    }

    pub fn derive_legacy_string(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.trim().is_empty() {
            return Err(Error::InvalidIdempotencySource(
                "legacy string source must not be empty or whitespace".to_owned(),
            ));
        }
        Self::derive(LEGACY_STRING_IDEMPOTENCY_NAMESPACE, value)
    }
}

pub trait IntoIdempotencyIdentity {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity>;
}

impl IntoIdempotencyIdentity for IdempotencyIdentity {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        Ok(self)
    }
}

impl IntoIdempotencyIdentity for &str {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        IdempotencyIdentity::derive_legacy_string(self)
    }
}

impl IntoIdempotencyIdentity for String {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        IdempotencyIdentity::derive_legacy_string(self)
    }
}

impl IntoIdempotencyIdentity for &String {
    fn into_idempotency_identity(self) -> Result<IdempotencyIdentity> {
        IdempotencyIdentity::derive_legacy_string(self)
    }
}

/// Render a 4-bit value as a lowercase hex digit. Total by construction: the
/// caller masks to a nibble, so there is no error case to propagate.
fn hex_digit(nibble: u8) -> char {
    debug_assert!(nibble < 16);
    b"0123456789abcdef"[(nibble & 0x0f) as usize] as char
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
