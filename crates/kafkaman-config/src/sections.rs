use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kafkaman_core::{LifecycleEmission, PurgeConfig, RelayConfig, TopicMode};
use serde::Deserialize;

use crate::duration::deserialize_duration;
use crate::observability::{validate_observability_override, validate_observability_policy};
use crate::retry::validate_policy;
use crate::{
    ConfigErrors, ConfigIssue, ObservabilityConfig, ObservabilityPolicy,
    ObservabilityPolicyOverride, RetryConfig, RetryPolicy, RetryPolicyOverride,
};

/// The `[relay]` section of `kafkaman.toml`.
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
            // The relay section carries no lifecycle knobs of its own; callers
            // overlay the resolved per-message-type policy with
            // `RelayConfig::lifecycle`.
            lifecycle: LifecycleEmission::default(),
        };
        cfg.validate().map_err(|err| err.to_string())?;
        Ok(cfg)
    }
}

/// The `[retention]` section of `kafkaman.toml`.
///
/// Scoped to the outbox. See the outbox retention decision for why the received
/// and cache tables are not reclaimable.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetentionSection {
    #[serde(deserialize_with = "deserialize_duration")]
    pub outbox_after: Duration,
    pub batch_size: i64,
    #[serde(deserialize_with = "deserialize_duration")]
    pub poll_interval: Duration,
    /// Reclaim `Failed` rows too. Defaults to false: they are the invalid-send
    /// audit trail, and unlike other terminal rows nothing else carries the record
    /// that the work was rejected.
    #[serde(default)]
    pub include_failed: bool,
}

impl RetentionSection {
    pub fn into_purge_config(self) -> std::result::Result<PurgeConfig, String> {
        let cfg = PurgeConfig {
            older_than: self.outbox_after,
            batch_size: self.batch_size,
            poll_interval: self.poll_interval,
            include_failed: self.include_failed,
        };
        cfg.validate().map_err(|err| err.to_string())?;
        Ok(cfg)
    }
}

/// The `[retry]` section of `kafkaman.toml`: defaults plus per-type overrides.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetrySection {
    pub defaults: RetryPolicy,
    #[serde(default)]
    pub messages: BTreeMap<String, RetryPolicyOverride>,
}

impl RetrySection {
    /// Validate the defaults and every override, reporting all problems at once.
    ///
    /// An override naming an unregistered message type is an error rather than a
    /// warning: it means someone configured a retry policy that will never be
    /// consulted, most likely because they typoed the type name, and a silent
    /// no-op is exactly the outcome they will not notice.
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

            // Validate the merged policy, not the override alone: an override
            // is only ever used layered onto the defaults, and a field that is
            // fine in isolation can still be invalid against what it inherits.
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

/// The `[topics]` section.
///
/// `TopicMode` itself lives in `kafkaman-core` beside the `reconcile` function
/// that gives each variant meaning, so the decision table has one home.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicsSection {
    #[serde(default)]
    pub mode: TopicMode,
}

/// The `[observability]` section of `kafkaman.toml`: defaults plus per-type
/// overrides.
///
/// `Default` is what an absent section means, and it is deliberately the same
/// value as an empty `[observability]` table: no overrides anywhere, so every
/// policy falls through to [`ObservabilityPolicy::default`]. Writing the section
/// and leaving it empty is therefore indistinguishable from omitting it, which
/// is the property that lets the section be optional without a second code path.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObservabilitySection {
    #[serde(default)]
    pub defaults: ObservabilityPolicyOverride,
    #[serde(default)]
    pub messages: BTreeMap<String, ObservabilityPolicyOverride>,
}

impl ObservabilitySection {
    pub fn resolve(
        self,
        registered_messages: &BTreeSet<String>,
    ) -> std::result::Result<ObservabilityConfig, ConfigErrors> {
        let mut issues = Vec::new();
        let defaults = self.defaults.apply_to(ObservabilityPolicy::default());
        validate_observability_policy("observability.defaults", &defaults, &mut issues);

        let mut overrides = BTreeMap::new();
        for (message_type, override_policy) in self.messages {
            if !registered_messages.contains(&message_type) {
                issues.push(ConfigIssue::new(
                    format!("observability.messages.{message_type}"),
                    "message type is not registered",
                ));
            }

            // Validate the override's own fields, not the merged result. A bad
            // default is one mistake in one place; reporting it again under
            // every message type that inherits it buries the actual fix under
            // repetitions of it.
            validate_observability_override(
                &format!("observability.messages.{message_type}"),
                &override_policy,
                &mut issues,
            );
            overrides.insert(message_type, override_policy);
        }

        if issues.is_empty() {
            Ok(ObservabilityConfig {
                defaults,
                overrides,
            })
        } else {
            Err(ConfigErrors::new(issues))
        }
    }
}
