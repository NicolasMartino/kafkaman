//! Reading and writing entity-topic configuration on a broker.
//!
//! The decision about what a topic *should* look like lives in
//! `kafkaman_core::topics` and is a pure function. This module only supplies the
//! observation and executes the action, which is why the interesting branches
//! are tested there rather than here.

use std::time::Duration;

use kafkaman_core::{
    reconcile, CleanupPolicy, MessageDescriptor, ObservedTopic, TopicAction, TopicMode, TopicSpec,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::types::RDKafkaRespErr;

use crate::{Error, Result};

/// How long to wait for the broker on each admin round trip.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The Kafka topic property carrying the log-retention policy.
const CLEANUP_POLICY: &str = "cleanup.policy";

/// Reads and writes topic configuration.
pub struct TopicAdmin {
    client: AdminClient<DefaultClientContext>,
    timeout: Duration,
}

impl std::fmt::Debug for TopicAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicAdmin")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl TopicAdmin {
    pub fn from_brokers(brokers: &str) -> Result<Self> {
        let client = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // Load-bearing, and the reason this module cannot simply ask the
            // broker about one topic by name. A metadata request that names a
            // topic is enough to make a broker with `auto.create.topics.enable`
            // create it — with its default `cleanup.policy=delete`, which is
            // precisely the misconfiguration this code exists to detect. So the
            // client is told not to, and `observe` reads the whole metadata set
            // and looks the topic up locally rather than naming it on the wire.
            .set("allow.auto.create.topics", "false")
            .create()?;
        Ok(Self {
            client,
            timeout: ADMIN_TIMEOUT,
        })
    }

    /// What the broker reports for `topic`, or `None` if it does not exist.
    // Stays in the debug tier: a topic poll, convergence or no convergence.
    // See the span-depth decision for the rule.
    #[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
    pub async fn observe(&self, topic: &str) -> Result<Option<ObservedTopic>> {
        let Some(partitions) = self.partition_count(topic)? else {
            return Ok(None);
        };
        let cleanup_policy = self.cleanup_policy(topic).await?;
        Ok(Some(ObservedTopic {
            name: topic.to_owned(),
            cleanup_policy,
            partitions,
        }))
    }

    /// Partition count, or `None` when the topic does not exist.
    ///
    /// Fetches metadata for *every* topic rather than naming this one — see the
    /// note in [`TopicAdmin::from_brokers`].
    fn partition_count(&self, topic: &str) -> Result<Option<i32>> {
        let metadata = self
            .client
            .inner()
            .fetch_metadata(None, self.timeout)
            .map_err(Error::Kafka)?;

        Ok(metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .filter(|candidate| {
                // A broker will list a topic it knows nothing about with an
                // error rather than omitting it, so a named-but-erroring entry
                // is an absent topic.
                !matches!(
                    candidate.error(),
                    Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
                )
            })
            .map(|candidate| candidate.partitions().len() as i32))
    }

    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
    async fn cleanup_policy(&self, topic: &str) -> Result<CleanupPolicy> {
        let resources = self
            .client
            .describe_configs([&ResourceSpecifier::Topic(topic)], &self.options())
            .await
            .map_err(Error::Kafka)?;

        let resource = resources
            .into_iter()
            .next()
            .ok_or_else(|| Error::TopicAdmin {
                topic: topic.to_owned(),
                message: "broker returned no config for the topic".to_owned(),
            })?
            .map_err(|code| Self::admin_error(topic, code))?;

        let entry = resource
            .entries
            .into_iter()
            .find(|entry| entry.name == CLEANUP_POLICY)
            .ok_or_else(|| Error::TopicAdmin {
                topic: topic.to_owned(),
                message: format!("broker reported no `{CLEANUP_POLICY}` for the topic"),
            })?;

        Ok(CleanupPolicy::parse(entry.value.as_deref().unwrap_or("")))
    }

    /// Create `topic`. An existing topic is not an error: two services booting
    /// against the same cluster race here by construction.
    #[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
    pub async fn create(
        &self,
        topic: &str,
        partitions: i32,
        replication_factor: Option<i16>,
        spec: &TopicSpec,
    ) -> Result<()> {
        let policy = spec.cleanup_policy.as_broker_value().to_owned();
        let new_topic = NewTopic::new(
            topic,
            partitions,
            // -1 asks the broker for its own default, which is the right
            // behaviour when a replication factor was not declared: unlike
            // partition count, changing it later is an operational task rather
            // than a rebuild.
            TopicReplication::Fixed(replication_factor.map_or(-1, i32::from)),
        )
        .set(CLEANUP_POLICY, &policy);

        let results = self
            .client
            .create_topics([&new_topic], &self.options())
            .await
            .map_err(Error::Kafka)?;

        for result in results {
            match result {
                Ok(_) => {}
                Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((_, code)) => return Err(Self::admin_error(topic, code)),
            }
        }
        Ok(())
    }

    fn options(&self) -> AdminOptions {
        AdminOptions::new().request_timeout(Some(self.timeout))
    }

    /// Translate a broker error code, naming an authorization failure for what
    /// it is rather than letting it read as a generic broker fault.
    fn admin_error(topic: &str, code: RDKafkaErrorCode) -> Error {
        let message = match code {
            RDKafkaErrorCode::TopicAuthorizationFailed => format!(
                "the configured principal is not authorized for this topic ({code}); \
                 clusters commonly deny CreateTopics to application principals, in which \
                 case provision the topic out of band and use `[topics] mode = \"verify\"`"
            ),
            other => other.to_string(),
        };
        Error::TopicAdmin {
            topic: topic.to_owned(),
            message,
        }
    }
}

/// Bring every registered entity topic into line with what its message type
/// requires, before any loop is spawned.
///
/// Returns the partition drifts worth warning about; a policy mismatch is an
/// error rather than a return value.
#[tracing::instrument(level = "info", target = "kafkaman::internal", skip_all)]
pub async fn converge_topics(
    admin: &TopicAdmin,
    mode: TopicMode,
    descriptors: &[MessageDescriptor],
) -> Result<Vec<kafkaman_core::PartitionDrift>> {
    if mode == TopicMode::Off {
        tracing::warn!(
            "topic convergence is disabled (`[topics] mode = \"off\"`); entity topics are not \
             checked for `cleanup.policy=compact`, so a non-compacted topic will not be reported"
        );
        return Ok(Vec::new());
    }

    let mut drifts = Vec::new();
    // Descriptors are per message type, but two types can share a topic, so the
    // same topic is only reconciled once.
    let mut seen: Vec<&str> = Vec::new();

    for descriptor in descriptors {
        let topic = descriptor.topic.as_str();
        if seen.contains(&topic) {
            continue;
        }
        seen.push(topic);

        let observed = admin.observe(topic).await?;
        let outcome = reconcile(topic, &descriptor.topic_spec, observed.as_ref(), mode)
            .map_err(Error::Core)?;

        if let TopicAction::Create {
            partitions,
            replication_factor,
        } = outcome.action
        {
            tracing::info!(
                topic,
                partitions,
                cleanup_policy = %descriptor.topic_spec.cleanup_policy,
                "creating entity topic"
            );
            admin
                .create(
                    topic,
                    partitions,
                    replication_factor,
                    &descriptor.topic_spec,
                )
                .await?;
        }

        if let Some(drift) = outcome.drift {
            tracing::warn!(%drift, "entity topic partition count differs from the declared one");
            drifts.push(drift);
        }
    }

    Ok(drifts)
}
