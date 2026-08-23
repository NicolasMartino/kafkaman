use std::fmt;
use std::path::PathBuf;

use thiserror::Error;

/// One thing wrong with the config.
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

/// One problem, located at the config key that has it.
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

/// Every problem found in one pass over the config.
///
/// Validation accumulates rather than short-circuiting, so an operator fixing a
/// config sees all of it at once instead of rediscovering one more mistake per
/// deploy.
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

    pub fn issues(&self) -> &[ConfigIssue] {
        &self.issues
    }

    pub fn into_issues(self) -> Vec<ConfigIssue> {
        self.issues
    }
}

/// Read access to the issues, so callers get the whole slice API — `len`,
/// `is_empty`, `iter`, indexing — without this type re-exporting each of them by
/// hand.
impl std::ops::Deref for ConfigErrors {
    type Target = [ConfigIssue];

    fn deref(&self) -> &Self::Target {
        &self.issues
    }
}

impl IntoIterator for ConfigErrors {
    type Item = ConfigIssue;
    type IntoIter = std::vec::IntoIter<ConfigIssue>;

    fn into_iter(self) -> Self::IntoIter {
        self.issues.into_iter()
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
