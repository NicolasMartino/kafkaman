//! Resolved observability policy: what schedulers log, how often, and when a
//! row counts as overdue.
use std::collections::BTreeMap;
use std::time::Duration;

use kafkaman_core::LifecycleEmission;
use serde::{Deserialize, Deserializer};

use crate::duration::deserialize_optional_duration;
use crate::serde_enum::string_enum;
use crate::ConfigIssue;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObservabilityConfig {
    pub defaults: ObservabilityPolicy,
    pub overrides: BTreeMap<String, ObservabilityPolicyOverride>,
}

impl ObservabilityConfig {
    /// The effective policy for one message type: its override merged over the
    /// defaults, or the defaults when it has none.
    ///
    /// Called per record on the ingest path, which is why both policy types are
    /// `Copy`: resolving one is a map lookup and a memcpy, with nothing to
    /// allocate and nothing to drop.
    pub fn policy_for(&self, message_type: &str) -> ObservabilityPolicy {
        match self.overrides.get(message_type) {
            Some(override_policy) => override_policy.apply_to(self.defaults),
            None => self.defaults,
        }
    }
}

/// Resolved observability policy for one message type.
///
/// Not every field is wired to behavior yet. The ones that are:
///
/// - `lifecycle` and `sample_success` drive per-message success events in the
///   relay and dispatcher loops, via [`ObservabilityPolicy::lifecycle_emission`].
/// - `kafka_trace_handoff` controls whether consumer ingest spans link to the
///   propagated producer context or continue it as their parent.
/// - `stuck_after` is the overdue threshold used by the stuck-row queries.
/// - `max_queue_age` sets the `over_max_queue_age` flag on depth summaries.
///
/// `level`, `payload`, and `headers` are **reserved**: they parse and validate,
/// and they are part of the config contract, but nothing reads them yet. They
/// are accepted now so that adding the redaction hook later is not a breaking
/// config change. Each field documents this individually so it cannot be
/// mistaken for a live knob at the use site.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ObservabilityPolicy {
    /// **Reserved.** Intended minimum level for kafkaman's own diagnostics.
    /// kafkaman emits through `tracing` and the host owns the subscriber, so
    /// filtering today is the subscriber's job — set an `EnvFilter` directive on
    /// the `kafkaman` targets instead.
    pub level: ObservabilityLevel,
    /// Whether schedulers emit a per-message success event or only per-cycle
    /// summaries. Live.
    pub lifecycle: LifecycleLogging,
    /// **Reserved.** Intended payload exposure policy. No kafkaman code path
    /// emits payload bodies today, so this is `Off` in effect regardless of the
    /// configured value; it exists so a future redaction hook has a home.
    pub payload: PayloadLogging,
    /// **Reserved.** Intended user-header exposure policy. As with `payload`,
    /// no current code path emits arbitrary user headers.
    pub headers: HeaderLogging,
    /// How a Kafka consumer span relates to the producer context propagated on
    /// the record. `Linked` is the OpenTelemetry messaging default and remains
    /// safest for batch-shaped consumers; `Parented` is an opt-in for
    /// single-record processing when a backend should show one distributed
    /// waterfall across the broker hop.
    pub kafka_trace_handoff: KafkaTraceHandoff,
    /// Fraction of successes that get a lifecycle event, `0.0..=1.0`. Live when
    /// `lifecycle` is `per-message`.
    pub sample_success: f64,
    /// How long an expired claim or overdue row must stay overdue before the
    /// stuck-row queries report it. Live.
    pub stuck_after: Duration,
    /// Age at which queued work sets `over_max_queue_age` on a depth summary.
    /// Live.
    pub max_queue_age: Duration,
}

impl ObservabilityPolicy {
    /// The runtime lifecycle knobs, for handing to the worker loops.
    ///
    /// Only `lifecycle` and `sample_success` cross this boundary. The rest of
    /// the policy is consumed where it applies: `stuck_after` and
    /// `max_queue_age` by the inspection routes, and `level`/`payload`/`headers`
    /// nowhere yet — see their documentation.
    pub fn lifecycle_emission(&self) -> LifecycleEmission {
        LifecycleEmission::new(
            matches!(self.lifecycle, LifecycleLogging::PerMessage),
            self.sample_success,
        )
    }
}

impl Default for ObservabilityPolicy {
    fn default() -> Self {
        Self {
            level: ObservabilityLevel::Warn,
            lifecycle: LifecycleLogging::Summary,
            payload: PayloadLogging::Off,
            headers: HeaderLogging::KafkamanOnly,
            kafka_trace_handoff: KafkaTraceHandoff::Linked,
            sample_success: 0.0,
            stuck_after: Duration::from_secs(60),
            max_queue_age: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityPolicyOverride {
    pub level: Option<ObservabilityLevel>,
    pub lifecycle: Option<LifecycleLogging>,
    pub payload: Option<PayloadLogging>,
    pub headers: Option<HeaderLogging>,
    pub kafka_trace_handoff: Option<KafkaTraceHandoff>,
    pub sample_success: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub stuck_after: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_optional_duration")]
    pub max_queue_age: Option<Duration>,
}

impl ObservabilityPolicyOverride {
    pub fn apply_to(self, mut base: ObservabilityPolicy) -> ObservabilityPolicy {
        if let Some(value) = self.level {
            base.level = value;
        }
        if let Some(value) = self.lifecycle {
            base.lifecycle = value;
        }
        if let Some(value) = self.payload {
            base.payload = value;
        }
        if let Some(value) = self.headers {
            base.headers = value;
        }
        if let Some(value) = self.kafka_trace_handoff {
            base.kafka_trace_handoff = value;
        }
        if let Some(value) = self.sample_success {
            base.sample_success = value;
        }
        if let Some(value) = self.stuck_after {
            base.stuck_after = value;
        }
        if let Some(value) = self.max_queue_age {
            base.max_queue_age = value;
        }
        base
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservabilityLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl<'de> Deserialize<'de> for ObservabilityLevel {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        string_enum(
            deserializer,
            "error, warn, info, debug, or trace",
            |value| match value {
                "error" => Some(Self::Error),
                "warn" => Some(Self::Warn),
                "info" => Some(Self::Info),
                "debug" => Some(Self::Debug),
                "trace" => Some(Self::Trace),
                _ => None,
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleLogging {
    Summary,
    PerMessage,
}

impl<'de> Deserialize<'de> for LifecycleLogging {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        string_enum(
            deserializer,
            "summary or per-message",
            |value| match value {
                "summary" => Some(Self::Summary),
                "per-message" => Some(Self::PerMessage),
                _ => None,
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadLogging {
    Off,
    Redacted,
    Sampled,
    Full,
}

impl<'de> Deserialize<'de> for PayloadLogging {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        string_enum(
            deserializer,
            "off, redacted, sampled, or full",
            |value| match value {
                "off" => Some(Self::Off),
                "redacted" => Some(Self::Redacted),
                "sampled" => Some(Self::Sampled),
                "full" => Some(Self::Full),
                _ => None,
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderLogging {
    Off,
    KafkamanOnly,
    All,
}

impl<'de> Deserialize<'de> for HeaderLogging {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        string_enum(
            deserializer,
            "off, kafkaman-only, or all",
            |value| match value {
                "off" => Some(Self::Off),
                "kafkaman-only" => Some(Self::KafkamanOnly),
                "all" => Some(Self::All),
                _ => None,
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaTraceHandoff {
    Linked,
    Parented,
}

impl<'de> Deserialize<'de> for KafkaTraceHandoff {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        string_enum(deserializer, "linked or parented", |value| match value {
            "linked" => Some(Self::Linked),
            "parented" => Some(Self::Parented),
            _ => None,
        })
    }
}

pub(crate) fn validate_observability_policy(
    path: &str,
    policy: &ObservabilityPolicy,
    issues: &mut Vec<ConfigIssue>,
) {
    check_sample_success(path, policy.sample_success, issues);
    check_positive_duration(path, "stuck_after", policy.stuck_after, issues);
    check_positive_duration(path, "max_queue_age", policy.max_queue_age, issues);
}

/// Validates only the fields a per-message override actually sets.
///
/// Inherited values were already checked once against
/// `[observability.defaults]`, and an error is only actionable at the place it
/// is written.
pub(crate) fn validate_observability_override(
    path: &str,
    override_policy: &ObservabilityPolicyOverride,
    issues: &mut Vec<ConfigIssue>,
) {
    if let Some(sample_success) = override_policy.sample_success {
        check_sample_success(path, sample_success, issues);
    }
    if let Some(stuck_after) = override_policy.stuck_after {
        check_positive_duration(path, "stuck_after", stuck_after, issues);
    }
    if let Some(max_queue_age) = override_policy.max_queue_age {
        check_positive_duration(path, "max_queue_age", max_queue_age, issues);
    }
}

fn check_sample_success(path: &str, sample_success: f64, issues: &mut Vec<ConfigIssue>) {
    if !sample_success.is_finite() || !(0.0..=1.0).contains(&sample_success) {
        issues.push(ConfigIssue::new(
            format!("{path}.sample_success"),
            "must be finite and between 0.0 and 1.0",
        ));
    }
}

fn check_positive_duration(
    path: &str,
    field: &str,
    value: Duration,
    issues: &mut Vec<ConfigIssue>,
) {
    if value.is_zero() {
        issues.push(ConfigIssue::new(
            format!("{path}.{field}"),
            "must be greater than zero",
        ));
    }
}
