use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::duration::parse_duration;
use crate::{
    ConfigError, ConfigErrors, ConfigSchema, RelaySection, Result, RetentionSection, RetryConfig,
    RetrySection,
};

/// A parsed `kafkaman.toml`.
#[derive(Debug)]
pub struct Config {
    table: toml::Table,
}

impl Config {
    /// Search upward from the working directory for `kafkaman.toml`.
    ///
    /// Returns `Ok(None)` when there is no such file anywhere up the tree, which
    /// is a valid state: an application that registers no kafkaman message types
    /// needs no config.
    pub fn discover() -> Result<Option<Self>> {
        let mut current = std::env::current_dir().map_err(|source| ConfigError::Io {
            path: PathBuf::from("."),
            source,
        })?;

        loop {
            let candidate = current.join("kafkaman.toml");
            if candidate.exists() {
                return Self::from_path(candidate).map(Some);
            }

            if !current.pop() {
                return Ok(None);
            }
        }
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&content)
    }

    /// Parse TOML source.
    ///
    /// Named `parse`, with [`FromStr`](std::str::FromStr) implemented alongside
    /// it, rather than an inherent `from_str` that looks like the trait method
    /// and is not one. The two now agree by construction, and `"...".parse()`
    /// works.
    pub fn parse(toml: &str) -> Result<Self> {
        let table = toml.parse::<toml::Table>()?;
        Ok(Self { table })
    }

    pub fn get<T: FromConfigValue>(&self, key: &str) -> Result<T> {
        match self.value_at(key) {
            Some(value) => T::from_config_value(key, value),
            None => Err(ConfigError::MissingKey {
                key: key.to_owned(),
            }),
        }
    }

    pub fn get_opt<T: FromConfigValue>(&self, key: &str) -> Result<Option<T>> {
        match self.value_at(key) {
            Some(value) => T::from_config_value(key, value).map(Some),
            None => Ok(None),
        }
    }

    /// The `[relay]` section. Required: without it nothing knows how to claim.
    pub fn relay(&self) -> Result<RelaySection> {
        self.required_section("relay")
    }

    /// The `[retention]` section, if present.
    ///
    /// Optional where `relay` is required, and deliberately so: absent config means
    /// no purger runs and the outbox keeps growing, which is the status quo. Making
    /// it required would turn an upgrade into a silent deletion.
    pub fn retention(&self) -> Result<Option<RetentionSection>> {
        self.section("retention")
    }

    /// The `[retry]` section.
    pub fn retry(&self) -> Result<RetrySection> {
        self.required_section("retry")
    }

    /// Deserialize a top-level section, or `None` when it is absent.
    fn section<T: DeserializeOwned>(&self, key: &'static str) -> Result<Option<T>> {
        let Some(value) = self.value_at(key).cloned() else {
            return Ok(None);
        };
        value
            .try_into()
            .map(Some)
            .map_err(|err| ConfigError::InvalidValue {
                key: key.to_owned(),
                message: err.to_string(),
            })
    }

    /// Deserialize a top-level section that must be present.
    fn required_section<T: DeserializeOwned>(&self, key: &'static str) -> Result<T> {
        self.section(key)?.ok_or_else(|| ConfigError::MissingKey {
            key: key.to_owned(),
        })
    }

    /// Resolve `[retry]` against the message types actually registered.
    pub fn retry_config<I, S>(
        &self,
        registered_messages: I,
    ) -> std::result::Result<RetryConfig, ConfigErrors>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let registered = registered_messages
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();

        let retry = match self.retry() {
            Ok(retry) => retry,
            Err(err) => return Err(ConfigErrors::new(vec![err.into()])),
        };

        retry.resolve(&registered)
    }

    /// Check every key the schema requires, reporting all problems at once.
    pub fn validate(&self, schema: &ConfigSchema) -> std::result::Result<(), ConfigErrors> {
        schema.check(|key| self.value_at(key))
    }

    /// Whether a dotted key is present, regardless of its type. Used to treat
    /// optional sections (such as `[retry]`) as present-or-absent before
    /// validating their contents.
    pub fn contains(&self, key: &str) -> bool {
        self.value_at(key).is_some()
    }

    fn value_at(&self, key: &str) -> Option<&toml::Value> {
        let mut parts = key.split('.');
        let first = parts.next()?;
        let mut value = self.table.get(first)?;
        for part in parts {
            value = value.as_table()?.get(part)?;
        }
        Some(value)
    }
}

impl std::str::FromStr for Config {
    type Err = ConfigError;

    fn from_str(toml: &str) -> Result<Self> {
        Self::parse(toml)
    }
}

/// A type a single config key can hold.
///
/// Implemented for the scalars an application is likely to require by name;
/// whole sections deserialize through serde instead.
pub trait FromConfigValue: Sized {
    const EXPECTED: &'static str;

    fn from_config_value(key: &str, value: &toml::Value) -> Result<Self>;
}

impl FromConfigValue for String {
    const EXPECTED: &'static str = "string";

    fn from_config_value(key: &str, value: &toml::Value) -> Result<Self> {
        value
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| wrong_type::<Self>(key, value))
    }
}

impl FromConfigValue for Duration {
    const EXPECTED: &'static str = "duration string";

    fn from_config_value(key: &str, value: &toml::Value) -> Result<Self> {
        let text = value
            .as_str()
            .ok_or_else(|| wrong_type::<Self>(key, value))?;
        parse_duration(text).map_err(|message| ConfigError::InvalidValue {
            key: key.to_owned(),
            message,
        })
    }
}

impl FromConfigValue for i64 {
    const EXPECTED: &'static str = "integer";

    fn from_config_value(key: &str, value: &toml::Value) -> Result<Self> {
        value
            .as_integer()
            .ok_or_else(|| wrong_type::<Self>(key, value))
    }
}

impl FromConfigValue for f64 {
    const EXPECTED: &'static str = "float or integer";

    fn from_config_value(key: &str, value: &toml::Value) -> Result<Self> {
        if let Some(float) = value.as_float() {
            return Ok(float);
        }

        let integer = value
            .as_integer()
            .ok_or_else(|| wrong_type::<Self>(key, value))?;
        // Reject rather than silently round. An integer past 2^53 cannot be
        // represented exactly, and a `multiplier` that quietly becomes a
        // different number than the file says is worse than a rejected config.
        const EXACT_LIMIT: i64 = 1 << 53;
        if !(-EXACT_LIMIT..=EXACT_LIMIT).contains(&integer) {
            return Err(ConfigError::InvalidValue {
                key: key.to_owned(),
                message: format!("integer {integer} cannot be represented exactly as a float"),
            });
        }
        Ok(integer as f64)
    }
}

impl FromConfigValue for bool {
    const EXPECTED: &'static str = "boolean";

    fn from_config_value(key: &str, value: &toml::Value) -> Result<Self> {
        value
            .as_bool()
            .ok_or_else(|| wrong_type::<Self>(key, value))
    }
}

fn wrong_type<T: FromConfigValue>(key: &str, value: &toml::Value) -> ConfigError {
    ConfigError::WrongType {
        key: key.to_owned(),
        expected: T::EXPECTED,
        found: value_type(value),
    }
}

fn value_type(value: &toml::Value) -> &'static str {
    match value {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}
