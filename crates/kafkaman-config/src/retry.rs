use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use crate::duration::{deserialize_duration, deserialize_optional_duration};
use crate::ConfigIssue;

/// The resolved retry policy for every registered message type.
#[derive(Debug, Clone, Default, PartialEq)]
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

/// How many times a failing message is retried, and how far apart.
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

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
            multiplier: 2.0,
            errors_limit: 20,
            dlq: DlqMode::Table,
        }
    }
}

/// A per-message-type override: every field optional, absent meaning "inherit".
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
    /// Layer this override onto `base`, field by field.
    ///
    /// Written as a struct literal rather than a chain of `if let Some`
    /// assignments so it is *exhaustive*: adding a field to [`RetryPolicy`]
    /// fails to compile here. The assignment form silently ignored a new
    /// field's override instead, and the only symptom would be a per-type
    /// setting that quietly did nothing.
    pub fn apply_to(self, base: RetryPolicy) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.max_attempts.unwrap_or(base.max_attempts),
            initial_backoff: self.initial_backoff.unwrap_or(base.initial_backoff),
            max_backoff: self.max_backoff.unwrap_or(base.max_backoff),
            multiplier: self.multiplier.unwrap_or(base.multiplier),
            errors_limit: self.errors_limit.unwrap_or(base.errors_limit),
            dlq: self.dlq.unwrap_or(base.dlq),
        }
    }
}

/// Where a message goes once its retry budget is exhausted.
///
/// One variant today: the row stays in its received table, marked `Failed`,
/// which is what [`Replay::received`] redrives. The enum exists so adding a
/// second destination is not a breaking config change.
///
/// [`Replay::received`]: https://docs.rs/kafkaman-sqlx
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DlqMode {
    Table,
}

/// Reject a retry policy that cannot work, appending to `issues` rather than
/// returning, so one pass reports every problem in the file.
pub(crate) fn validate_policy(path: &str, policy: &RetryPolicy, issues: &mut Vec<ConfigIssue>) {
    if policy.max_attempts == 0 {
        issues.push(ConfigIssue::new(
            format!("{path}.max_attempts"),
            "must be greater than or equal to 1",
        ));
    }
    // A zero first backoff makes every retry due the instant it is scheduled, so
    // the dispatcher's "claimed something, poll again immediately" fast path never
    // yields and a permanently failing row spins against the database until its
    // attempts are exhausted. `parse_duration` accepts zero because `retry_after`
    // legitimately means "retry immediately"; the fields where zero is unsafe are
    // rejected here and in `RelayConfig::validate`.
    //
    // `max_backoff` needs no separate check: a zero max with a non-zero initial is
    // caught by the ordering rule below, and a zero max with a zero initial is
    // caught by this one.
    if policy.initial_backoff.is_zero() {
        issues.push(ConfigIssue::new(
            format!("{path}.initial_backoff"),
            "must be greater than zero",
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
