use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use kafkaman_core::RelayConfig;
use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer};
use thiserror::Error;

pub type Result<T, E = ConfigError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid TOML config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("missing config key `{key}`")]
    MissingKey { key: String },
    #[error("config key `{key}` expected {expected}, found {found}")]
    WrongType {
        key: String,
        expected: &'static str,
        found: &'static str,
    },
    #[error("config key `{key}` is invalid: {message}")]
    InvalidValue { key: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigIssue {
    pub key: String,
    pub message: String,
}

impl ConfigIssue {
    pub fn new(key: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigErrors {
    issues: Vec<ConfigIssue>,
}

impl ConfigErrors {
    pub fn new(issues: Vec<ConfigIssue>) -> Self {
        Self { issues }
    }

    pub fn push(&mut self, issue: ConfigIssue) {
        self.issues.push(issue);
    }

    pub fn extend(&mut self, other: ConfigErrors) {
        self.issues.extend(other.issues);
    }

    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn issues(&self) -> &[ConfigIssue] {
        &self.issues
    }

    pub fn into_issues(self) -> Vec<ConfigIssue> {
        self.issues
    }
}

impl fmt::Display for ConfigErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, issue) in self.issues.iter().enumerate() {
            if idx > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{}: {}", issue.key, issue.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigErrors {}

impl From<ConfigError> for ConfigIssue {
    fn from(value: ConfigError) -> Self {
        match value {
            ConfigError::Io { path, source } => ConfigIssue::new(
                path.display().to_string(),
                format!("failed to read config: {source}"),
            ),
            ConfigError::Parse(source) => ConfigIssue::new("toml", source.to_string()),
            ConfigError::MissingKey { key } => ConfigIssue::new(key, "missing required key"),
            ConfigError::WrongType {
                key,
                expected,
                found,
            } => ConfigIssue::new(key, format!("expected {expected}, found {found}")),
            ConfigError::InvalidValue { key, message } => ConfigIssue::new(key, message),
        }
    }
}

pub struct Config {
    table: toml::Table,
}

impl Config {
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
        Self::from_str(&content)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(toml: &str) -> Result<Self> {
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

    pub fn relay(&self) -> Result<RelaySection> {
        let value = self
            .value_at("relay")
            .ok_or_else(|| ConfigError::MissingKey {
                key: "relay".to_owned(),
            })?
            .clone();
        value.try_into().map_err(|err| ConfigError::InvalidValue {
            key: "relay".to_owned(),
            message: err.to_string(),
        })
    }

    pub fn retry(&self) -> Result<RetrySection> {
        let value = self
            .value_at("retry")
            .ok_or_else(|| ConfigError::MissingKey {
                key: "retry".to_owned(),
            })?
            .clone();
        value.try_into().map_err(|err| ConfigError::InvalidValue {
            key: "retry".to_owned(),
            message: err.to_string(),
        })
    }

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

    pub fn validate(&self, schema: &ConfigSchema) -> std::result::Result<(), ConfigErrors> {
        let mut issues = Vec::new();
        for required in &schema.required {
            match self.value_at(&required.key) {
                Some(value) => {
                    if let Err(err) = (required.validate)(&required.key, value) {
                        issues.push(err.into());
                    }
                }
                None => issues.push(ConfigIssue::new(
                    required.key.clone(),
                    format!("missing required key; expected {}", required.expected),
                )),
            }
        }

        if issues.is_empty() {
            Ok(())
        } else {
            Err(ConfigErrors::new(issues))
        }
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
        value
            .as_float()
            .or_else(|| value.as_integer().map(|integer| integer as f64))
            .ok_or_else(|| wrong_type::<Self>(key, value))
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

type Validator = fn(&str, &toml::Value) -> Result<()>;

pub struct ConfigSchema {
    required: Vec<RequiredKey>,
}

impl ConfigSchema {
    pub fn new() -> Self {
        Self {
            required: Vec::new(),
        }
    }

    pub fn require<T: FromConfigValue + 'static>(mut self, key: impl Into<String>) -> Self {
        self.required.push(RequiredKey {
            key: key.into(),
            expected: T::EXPECTED,
            validate: |key, value| T::from_config_value(key, value).map(|_| ()),
        });
        self
    }
}

impl Default for ConfigSchema {
    fn default() -> Self {
        Self::new()
    }
}

struct RequiredKey {
    key: String,
    expected: &'static str,
    validate: Validator,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelaySection {
    pub worker_id: String,
    pub batch_limit: i64,
    #[serde(deserialize_with = "deserialize_duration")]
    pub lease_for: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub retry_after: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub poll_interval: Duration,
}

impl RelaySection {
    pub fn into_relay_config(self) -> std::result::Result<RelayConfig, String> {
        let cfg = RelayConfig {
            worker_id: self.worker_id,
            batch_limit: self.batch_limit,
            lease_for: self.lease_for,
            retry_after: self.retry_after,
            poll_interval: self.poll_interval,
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetrySection {
    pub defaults: RetryPolicy,
    #[serde(default)]
    pub messages: BTreeMap<String, RetryPolicyOverride>,
}

impl RetrySection {
    pub fn resolve(
        self,
        registered_messages: &BTreeSet<String>,
    ) -> std::result::Result<RetryConfig, ConfigErrors> {
        let mut issues = Vec::new();
        validate_policy("retry.defaults", &self.defaults, &mut issues);

        let mut overrides = BTreeMap::new();
        for (message_type, override_policy) in self.messages {
            if !registered_messages.contains(&message_type) {
                issues.push(ConfigIssue::new(
                    format!("retry.messages.{message_type}"),
                    "message type is not registered",
                ));
            }

            let merged = override_policy.clone().apply_to(self.defaults.clone());
            validate_policy(
                &format!("retry.messages.{message_type}"),
                &merged,
                &mut issues,
            );
            overrides.insert(message_type, override_policy);
        }

        if issues.is_empty() {
            Ok(RetryConfig {
                defaults: self.defaults,
                overrides,
            })
        } else {
            Err(ConfigErrors::new(issues))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    pub defaults: RetryPolicy,
    pub overrides: BTreeMap<String, RetryPolicyOverride>,
}

impl RetryConfig {
    pub fn policy_for(&self, message_type: &str) -> RetryPolicy {
        match self.overrides.get(message_type) {
            Some(override_policy) => override_policy.clone().apply_to(self.defaults.clone()),
            None => self.defaults.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    #[serde(deserialize_with = "deserialize_duration")]
    pub initial_backoff: Duration,
    #[serde(deserialize_with = "deserialize_duration")]
    pub max_backoff: Duration,
    pub multiplier: f64,
    pub errors_limit: u32,
    pub dlq: DlqMode,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicyOverride {
    pub max_attempts: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub initial_backoff: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub max_backoff: Option<Duration>,
    pub multiplier: Option<f64>,
    pub errors_limit: Option<u32>,
    pub dlq: Option<DlqMode>,
}

impl RetryPolicyOverride {
    pub fn apply_to(self, mut base: RetryPolicy) -> RetryPolicy {
        if let Some(value) = self.max_attempts {
            base.max_attempts = value;
        }
        if let Some(value) = self.initial_backoff {
            base.initial_backoff = value;
        }
        if let Some(value) = self.max_backoff {
            base.max_backoff = value;
        }
        if let Some(value) = self.multiplier {
            base.multiplier = value;
        }
        if let Some(value) = self.errors_limit {
            base.errors_limit = value;
        }
        if let Some(value) = self.dlq {
            base.dlq = value;
        }
        base
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqMode {
    Table,
}

impl<'de> Deserialize<'de> for DlqMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DlqModeVisitor;

        impl Visitor<'_> for DlqModeVisitor {
            type Value = DlqMode;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("the string `table`")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "table" => Ok(DlqMode::Table),
                    other => Err(E::invalid_value(Unexpected::Str(other), &self)),
                }
            }
        }

        deserializer.deserialize_str(DlqModeVisitor)
    }
}

fn validate_policy(path: &str, policy: &RetryPolicy, issues: &mut Vec<ConfigIssue>) {
    if policy.max_attempts == 0 {
        issues.push(ConfigIssue::new(
            format!("{path}.max_attempts"),
            "must be greater than or equal to 1",
        ));
    }
    if policy.initial_backoff > policy.max_backoff {
        issues.push(ConfigIssue::new(
            format!("{path}.initial_backoff"),
            "must be less than or equal to max_backoff",
        ));
    }
    if !policy.multiplier.is_finite() || policy.multiplier < 1.0 {
        issues.push(ConfigIssue::new(
            format!("{path}.multiplier"),
            "must be finite and greater than or equal to 1.0",
        ));
    }
    if policy.errors_limit == 0 {
        issues.push(ConfigIssue::new(
            format!("{path}.errors_limit"),
            "must be greater than or equal to 1",
        ));
    }
}

fn deserialize_duration<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_duration(&value).map_err(de::Error::custom)
}

fn deserialize_optional_duration<'de, D>(
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

fn parse_duration(input: &str) -> std::result::Result<Duration, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("duration must not be empty".to_owned());
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "duration must include a unit (ms, s, m, h, d)".to_owned())?;
    let (number, unit) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err("duration must start with a positive integer".to_owned());
    }
    let amount = number
        .parse::<u128>()
        .map_err(|_| "duration amount is too large".to_owned())?;
    if amount == 0 {
        return Err("duration must be greater than zero".to_owned());
    }

    let millis = match unit {
        "ms" => amount,
        "s" => amount
            .checked_mul(1_000)
            .ok_or_else(|| "duration overflows milliseconds".to_owned())?,
        "m" => amount
            .checked_mul(60_000)
            .ok_or_else(|| "duration overflows milliseconds".to_owned())?,
        "h" => amount
            .checked_mul(3_600_000)
            .ok_or_else(|| "duration overflows milliseconds".to_owned())?,
        "d" => amount
            .checked_mul(86_400_000)
            .ok_or_else(|| "duration overflows milliseconds".to_owned())?,
        _ => return Err("duration unit must be one of ms, s, m, h, d".to_owned()),
    };

    if millis > u64::MAX as u128 {
        return Err("duration overflows Duration".to_owned());
    }

    Ok(Duration::from_millis(millis as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "worker-a"
        batch_limit = 25
        lease_for = "30s"
        retry_after = "500ms"
        poll_interval = "250ms"

        [retry.defaults]
        max_attempts = 5
        initial_backoff = "100ms"
        max_backoff = "30s"
        multiplier = 2.0
        errors_limit = 16
        dlq = "table"

        [retry.messages.order_created]
        max_attempts = 7
        initial_backoff = "250ms"
    "#;

    #[test]
    fn dotted_access_is_typed() {
        let cfg = Config::from_str(VALID).unwrap();

        assert_eq!(
            cfg.get::<Duration>("relay.retry_after").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(cfg.get::<i64>("relay.batch_limit").unwrap(), 25);
        assert!(cfg.get::<i64>("relay.retry_after").is_err());
        assert!(cfg.get_opt::<String>("missing.key").unwrap().is_none());
    }

    #[test]
    fn path_loading_and_common_scalar_access_work() {
        let path =
            std::env::temp_dir().join(format!("kafkaman-config-test-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            format!("{VALID}\n[feature]\nenabled = true\nratio = 1.25\nlabel = \"alpha\"\n"),
        )
        .unwrap();

        let cfg = Config::from_path(&path).unwrap();
        assert!(cfg.get::<bool>("feature.enabled").unwrap());
        assert_eq!(cfg.get::<f64>("feature.ratio").unwrap(), 1.25);
        assert_eq!(cfg.get::<String>("feature.label").unwrap(), "alpha");
        assert_eq!(cfg.relay().unwrap().worker_id, "worker-a");
        cfg.relay().unwrap().into_relay_config().unwrap();
        assert!(cfg
            .get::<String>("feature.missing")
            .unwrap_err()
            .to_string()
            .contains("feature.missing"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn validate_reports_all_required_key_problems() {
        let cfg = Config::from_str(
            r#"
            [relay]
            retry_after = 3
            "#,
        )
        .unwrap();
        let schema = ConfigSchema::new()
            .require::<String>("database.schema")
            .require::<Duration>("relay.retry_after");

        let err = cfg.validate(&schema).unwrap_err();

        assert_eq!(err.issues().len(), 2);
        assert!(err.to_string().contains("database.schema"));
        assert!(err.to_string().contains("relay.retry_after"));
    }

    #[test]
    fn typed_relay_section_denies_unknown_fields() {
        let cfg = Config::from_str(
            r#"
            [relay]
            worker_id = "worker-a"
            batch_limit = 25
            lease_for = "30s"
            retry_after = "500ms"
            poll_interval = "250ms"
            surprise = true
            "#,
        )
        .unwrap();

        assert!(cfg.relay().unwrap_err().to_string().contains("surprise"));
    }

    #[test]
    fn retry_config_merges_and_validates_registered_messages() {
        let cfg = Config::from_str(VALID).unwrap();
        let retry = cfg.retry_config(["order_created"]).unwrap();

        let policy = retry.policy_for("order_created");
        assert_eq!(policy.max_attempts, 7);
        assert_eq!(policy.initial_backoff, Duration::from_millis(250));
        assert_eq!(policy.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn retry_config_rejects_invalid_policy_and_unknown_message() {
        let cfg = Config::from_str(
            r#"
            [retry.defaults]
            max_attempts = 0
            initial_backoff = "60s"
            max_backoff = "1s"
            multiplier = 0.9
            errors_limit = 0
            dlq = "table"

            [retry.messages.unregistered]
            max_attempts = 3
            "#,
        )
        .unwrap();

        let err = cfg.retry_config(["order_created"]).unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("retry.defaults.max_attempts"));
        assert!(rendered.contains("retry.defaults.initial_backoff"));
        assert!(rendered.contains("retry.defaults.multiplier"));
        assert!(rendered.contains("retry.defaults.errors_limit"));
        assert!(rendered.contains("retry.messages.unregistered"));
    }

    #[test]
    fn retry_config_rejects_bad_variant_and_duration_overflow() {
        assert!(Config::from_str(
            r#"
            [retry.defaults]
            max_attempts = 3
            initial_backoff = "999999999999999999999999999999999999999999h"
            max_backoff = "1s"
            multiplier = 2.0
            errors_limit = 1
            dlq = "table"
            "#
        )
        .unwrap()
        .retry()
        .is_err());

        assert!(Config::from_str(
            r#"
            [retry.defaults]
            max_attempts = 3
            initial_backoff = "1s"
            max_backoff = "2s"
            multiplier = 2.0
            errors_limit = 1
            dlq = "topic"
            "#
        )
        .unwrap()
        .retry()
        .is_err());
    }
}
