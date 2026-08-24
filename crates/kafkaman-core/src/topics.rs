//! What an entity topic must look like, and how to tell whether it does.
//!
//! Every in-purview kafkaman message is a compact entity snapshot, so the topic
//! it travels on has to be log-compacted. That is a property of the model rather
//! than of any individual contract — which is why [`TopicSpec`] defaults to
//! compacted and a message type declares nothing unless it is overriding
//! partitioning.
//!
//! Nothing here performs I/O. The comparison between what a type requires and
//! what a broker reports is a pure function over [`TopicSpec`] and
//! [`ObservedTopic`], so the interesting cases are unit-testable without a
//! broker; the transport crate supplies the observation.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A topic's log-retention policy, as Kafka reports it.
///
/// Carries a catch-all for the same reason the wire enums do: a broker may know
/// a policy this build does not, and failing to parse it would be a worse
/// failure than reporting it verbatim in a mismatch error.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
#[non_exhaustive]
pub enum CleanupPolicy {
    /// `compact` alone. The only policy an entity topic may have.
    Compact,
    /// `delete` alone — the broker default, and the one that silently breaks
    /// rebuild-from-log.
    Delete,
    /// Both. Rejected for entity topics: compaction keeps the latest record per
    /// key, but `delete` still ages records out by time, so the log stops being
    /// a complete source for rebuilding an entity.
    CompactAndDelete,
    /// A policy this build does not recognise, kept verbatim.
    Unrecognized(String),
}

impl CleanupPolicy {
    /// Parse a broker's `cleanup.policy` value.
    ///
    /// The value is a comma-separated list whose order is not significant, so
    /// `compact,delete` and `delete, compact` are the same policy.
    pub fn parse(raw: &str) -> Self {
        let (mut compact, mut delete, mut unknown) = (false, false, false);
        for part in raw.split(',') {
            match part.trim().to_ascii_lowercase().as_str() {
                "compact" => compact = true,
                "delete" => delete = true,
                "" => {}
                _ => unknown = true,
            }
        }

        if unknown {
            return Self::Unrecognized(raw.trim().to_owned());
        }
        match (compact, delete) {
            (true, true) => Self::CompactAndDelete,
            (true, false) => Self::Compact,
            (false, true) => Self::Delete,
            // An empty or whitespace-only value. Reporting it verbatim beats
            // guessing at the broker's default.
            (false, false) => Self::Unrecognized(raw.trim().to_owned()),
        }
    }

    /// The value as a broker would spell it.
    pub fn as_broker_value(&self) -> &str {
        match self {
            Self::Compact => "compact",
            Self::Delete => "delete",
            Self::CompactAndDelete => "compact,delete",
            Self::Unrecognized(raw) => raw,
        }
    }

    /// Whether this is `compact` and nothing else, which is what an entity topic
    /// requires.
    pub fn is_compact_only(&self) -> bool {
        matches!(self, Self::Compact)
    }
}

impl fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_broker_value())
    }
}

impl From<String> for CleanupPolicy {
    fn from(value: String) -> Self {
        Self::parse(&value)
    }
}

impl From<CleanupPolicy> for String {
    fn from(value: CleanupPolicy) -> Self {
        value.as_broker_value().to_owned()
    }
}

/// The configuration an entity topic is required to have.
///
/// `partitions` and `replication_factor` are optional because verification does
/// not need them — but creation does, and refuses to proceed without a partition
/// count. See [`TopicSpec::partitions`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicSpec {
    pub cleanup_policy: CleanupPolicy,
    /// Deliberately optional, and deliberately not defaulted.
    ///
    /// Changing a keyed compacted topic's partition count changes which
    /// partition each entity hashes to, and cache convergence compares offsets
    /// only within one topic-partition. So the count cannot be quietly guessed
    /// on the way to creating a topic: the recovery is republishing every entity
    /// onto a new topic, which is affordable but is not free. Creation requires
    /// this to be set; verification only warns when it drifts.
    pub partitions: Option<i32>,
    pub replication_factor: Option<i16>,
}

impl Default for TopicSpec {
    fn default() -> Self {
        Self::compacted()
    }
}

impl TopicSpec {
    /// The spec every in-purview entity topic has: compacted, with partitioning
    /// left to the deployment.
    pub fn compacted() -> Self {
        Self {
            cleanup_policy: CleanupPolicy::Compact,
            partitions: None,
            replication_factor: None,
        }
    }

    pub fn with_partitions(mut self, partitions: i32) -> Result<Self> {
        if partitions < 1 {
            return Err(Error::InvalidTopicSpec {
                field: "partitions",
                reason: "must be at least 1",
            });
        }
        self.partitions = Some(partitions);
        Ok(self)
    }

    pub fn with_replication_factor(mut self, replication_factor: i16) -> Result<Self> {
        if replication_factor < 1 {
            return Err(Error::InvalidTopicSpec {
                field: "replication_factor",
                reason: "must be at least 1",
            });
        }
        self.replication_factor = Some(replication_factor);
        Ok(self)
    }

    /// Compare this spec against what a broker reports.
    ///
    /// A wrong cleanup policy is an error, because it breaks the model. A
    /// partition count that differs from the declared one is only a warning:
    /// failing would turn an intentional, already-completed repartition into a
    /// boot failure across every service that reads the topic, long after the
    /// migration succeeded.
    pub fn check(&self, observed: &ObservedTopic) -> Result<Option<PartitionDrift>> {
        if self.cleanup_policy != observed.cleanup_policy {
            return Err(Error::TopicPolicyMismatch {
                topic: observed.name.clone(),
                expected: self.cleanup_policy.to_string(),
                found: observed.cleanup_policy.to_string(),
            });
        }

        match self.partitions {
            Some(declared) if declared != observed.partitions => Ok(Some(PartitionDrift {
                topic: observed.name.clone(),
                declared,
                found: observed.partitions,
            })),
            _ => Ok(None),
        }
    }

    /// The partition count to create this topic with, or an error naming what is
    /// missing. Never guesses.
    pub fn partitions_for_create(&self, topic: &str) -> Result<i32> {
        self.partitions
            .ok_or_else(|| Error::TopicPartitionsUndeclared {
                topic: topic.to_owned(),
            })
    }
}

/// What boot is allowed to do about a topic that is missing or misconfigured.
///
/// Lives here rather than in the config crate because [`reconcile`] is the thing
/// that gives each variant meaning, and keeping the two together means the
/// decision table has exactly one home.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    /// Read the broker's topic configuration and fail boot if it does not match
    /// what the registered message types require. Never writes.
    ///
    /// The default, because application principals are routinely denied
    /// `CreateTopics` by ACL — a library that created topics at boot would be
    /// unusable in those clusters.
    #[default]
    Verify,
    /// Create missing topics to spec, and still fail on an existing topic whose
    /// cleanup policy is wrong.
    Create,
    /// Do nothing. For clusters that deny metadata reads outright.
    Off,
}

/// What convergence decided to do about one topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TopicAction {
    /// The topic is already correct, or checking is switched off.
    Satisfied,
    /// The topic is missing and the mode permits creating it.
    Create {
        partitions: i32,
        replication_factor: Option<i16>,
    },
}

/// The result of reconciling one topic: what to do, and anything worth saying
/// out loud while not failing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicOutcome {
    pub action: TopicAction,
    pub drift: Option<PartitionDrift>,
}

/// Decide what to do about one topic, given what the broker reports.
///
/// Pure on purpose. The transport supplies `observed` and executes whatever
/// action comes back, which keeps every branch of this decision — including the
/// ones that are awkward to provoke against a real broker, like an ACL-denied
/// create or a topic that is compacted but wrongly partitioned — testable
/// without one.
pub fn reconcile(
    topic: &str,
    spec: &TopicSpec,
    observed: Option<&ObservedTopic>,
    mode: TopicMode,
) -> Result<TopicOutcome> {
    if mode == TopicMode::Off {
        return Ok(TopicOutcome {
            action: TopicAction::Satisfied,
            drift: None,
        });
    }

    let Some(observed) = observed else {
        return match mode {
            // Creating requires a partition count, and will not invent one.
            TopicMode::Create => Ok(TopicOutcome {
                action: TopicAction::Create {
                    partitions: spec.partitions_for_create(topic)?,
                    replication_factor: spec.replication_factor,
                },
                drift: None,
            }),
            // Auto-creation would produce a `delete` topic, so an absent topic
            // is a failure rather than something to shrug at and let the broker
            // handle on first publish.
            _ => Err(Error::TopicMissing {
                topic: topic.to_owned(),
            }),
        };
    };

    // An existing topic is held to the same standard in every mode: `create`
    // means "create what is missing", never "repair what is wrong". Silently
    // rewriting a live topic's cleanup policy at boot is not a thing a library
    // should do.
    Ok(TopicOutcome {
        action: TopicAction::Satisfied,
        drift: spec.check(observed)?,
    })
}

/// A topic's configuration as a broker actually reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedTopic {
    pub name: String,
    pub cleanup_policy: CleanupPolicy,
    pub partitions: i32,
}

/// The declared partition count and the broker's disagree.
///
/// Reported rather than raised: see [`TopicSpec::check`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionDrift {
    pub topic: String,
    pub declared: i32,
    pub found: i32,
}

impl fmt::Display for PartitionDrift {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "topic `{}` declares {} partitions but the broker reports {}; \
             cache convergence compares offsets within one topic-partition, so \
             a repartition requires republishing every entity onto a new topic",
            self.topic, self.declared, self.found
        )
    }
}
