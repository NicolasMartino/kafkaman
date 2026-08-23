use crate::config::FromConfigValue;
use crate::{ConfigErrors, ConfigIssue, Result};

/// The keys a config file must contain, and what type each must hold.
///
/// Separate from the typed `[section]` structs: those describe sections that
/// deserialize as a unit, while this covers individual dotted keys an
/// application requires — including ones kafkaman itself knows nothing about.
#[derive(Debug)]
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

    /// Check `lookup` against every required key, reporting all problems at once
    /// rather than stopping at the first.
    pub(crate) fn check<'a>(
        &self,
        lookup: impl Fn(&str) -> Option<&'a toml::Value>,
    ) -> std::result::Result<(), ConfigErrors> {
        let mut issues = Vec::new();
        for required in &self.required {
            match lookup(&required.key) {
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
}

impl Default for ConfigSchema {
    fn default() -> Self {
        Self::new()
    }
}

/// A required key, with its type erased behind the validator that checks it.
///
/// The function pointer is what lets one `Vec` hold requirements for keys of
/// different types without the schema being generic over all of them.
#[derive(Debug)]
struct RequiredKey {
    key: String,
    expected: &'static str,
    validate: Validator,
}

type Validator = fn(&str, &toml::Value) -> Result<()>;
