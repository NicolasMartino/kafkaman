use std::time::Duration;

use kafkaman_config::{
    Config, ConfigErrors, ConfigIssue, ConfigSchema, ObservabilityConfig, RetryConfig,
};
use kafkaman_core::{
    DispatcherConfig, KafkaMessage, MessageDescriptor, RelayConfig, SqlIdentifier, TopicMode,
};

use crate::{Error, Result};

/// Everything kafkaman needs to run, resolved and validated once at startup.
///
/// Resolution is deliberately fallible and eager. Every problem the config can
/// have — a missing key, a mistyped duration, a retry override naming a type
/// nobody registered — is reported here, before a pool is opened, rather than
/// surfacing later as a query against a table that was never created.
#[derive(Clone, Debug)]
pub struct ResolvedConfig {
    pub schema: SqlIdentifier,
    pub relay: RelayConfig,
    /// How the receive dispatcher paces itself and when its panic breaker trips.
    ///
    /// `poll_interval` falls back to the relay's when `[dispatcher]` does not
    /// set one, which is what the dispatcher read before the section existed.
    pub dispatcher: DispatcherConfig,
    pub retry: RetryConfig,
    pub observability: ObservabilityConfig,
    /// What boot may do about the topics the registered types declare.
    ///
    /// Resolved here rather than read from the file at the call site so that a
    /// mistyped mode is reported alongside every other config problem, in the
    /// one error this crate promises to raise before a pool is opened. The
    /// transport crate consumes it — `kafkaman_rdkafka::converge_topics` takes
    /// this and [`Self::messages`] — but the *decision to check* is
    /// configuration, and configuration resolves in one place.
    pub topics: TopicMode,
    messages: Vec<MessageDescriptor>,
}

impl ResolvedConfig {
    pub fn new(schema: SqlIdentifier) -> Self {
        Self {
            schema,
            relay: RelayConfig::default(),
            dispatcher: DispatcherConfig::default(),
            retry: RetryConfig::default(),
            observability: ObservabilityConfig::default(),
            topics: TopicMode::default(),
            messages: Vec::new(),
        }
    }

    /// Resolve a config file against the message types an application
    /// registers, accumulating every problem rather than stopping at the first.
    pub fn from_config<I>(cfg: Option<&Config>, messages: I) -> Result<Self>
    where
        I: IntoIterator<Item = MessageDescriptor>,
    {
        let messages = messages.into_iter().collect::<Vec<_>>();
        let Some(cfg) = cfg else {
            if messages.is_empty() {
                return Ok(Self::default());
            }

            return Err(Error::ConfigErrors(ConfigErrors::new(vec![
                ConfigIssue::new(
                    "kafkaman.toml",
                    "missing config file for registered kafkaman features",
                ),
            ])));
        };

        let schema = ConfigSchema::new()
            .require::<String>("database.schema")
            .require::<String>("relay.worker_id")
            .require::<i64>("relay.batch_limit")
            .require::<Duration>("relay.lease_for")
            .require::<Duration>("relay.retry_after")
            .require::<Duration>("relay.poll_interval");

        let mut issues = match cfg.validate(&schema) {
            Ok(()) => Vec::new(),
            Err(errors) => errors.into_issues(),
        };

        let schema = match cfg.get::<String>("database.schema") {
            Ok(schema) => Some(schema),
            Err(err) => {
                issues.push(err.into());
                None
            }
        };

        let relay = match cfg.relay() {
            Ok(relay) => match relay.into_relay_config() {
                Ok(relay) => Some(relay),
                Err(message) => {
                    issues.push(ConfigIssue::new("relay", message));
                    None
                }
            },
            Err(err) => {
                issues.push(err.into());
                None
            }
        };

        let retry = if cfg.contains("retry") {
            let registered = messages
                .iter()
                .map(|descriptor| descriptor.message_type.as_str().to_owned())
                .collect::<Vec<_>>();
            match cfg.retry_config(registered) {
                Ok(retry) => Some(retry),
                Err(retry_errors) => {
                    issues.extend(retry_errors.into_issues());
                    None
                }
            }
        } else {
            Some(RetryConfig::default())
        };

        // No `contains` guard: the section is optional and every field within it
        // is too, so `dispatcher()` defaults an absent one to exactly the
        // behaviour that predates it.
        //
        // Resolved after `relay` because the fallback for an unset
        // `poll_interval` is the relay's, which is where the dispatcher read it
        // from before this section existed.
        let dispatcher = match (cfg.dispatcher(), relay.as_ref()) {
            (Ok(section), Some(relay)) => {
                match section.into_dispatcher_config(relay.lifecycle, relay.poll_interval) {
                    Ok(dispatcher) => Some(dispatcher),
                    Err(reason) => {
                        issues.push(ConfigIssue::new("dispatcher", reason));
                        None
                    }
                }
            }
            (Err(err), _) => {
                issues.push(err.into());
                None
            }
            // `[relay]` already failed and reported its own issue; there is no
            // fallback to resolve against and no second complaint worth making.
            (Ok(_), None) => None,
        };

        // Absent is not the same as off: `topics()` supplies `verify` for a
        // missing section, and only an unparseable one lands here as an issue.
        let topics = match cfg.topics() {
            Ok(section) => Some(section.mode),
            Err(err) => {
                issues.push(err.into());
                None
            }
        };

        // No `contains` guard, unlike `[relay]` and `[retry]` above:
        // `observability_config` defaults the absent section itself, so the
        // section is optional in one place instead of in each caller.
        let observability = {
            let registered = messages
                .iter()
                .map(|descriptor| descriptor.message_type.as_str().to_owned())
                .collect::<Vec<_>>();
            match cfg.observability_config(registered) {
                Ok(observability) => Some(observability),
                Err(observability_errors) => {
                    issues.extend(observability_errors.into_issues());
                    None
                }
            }
        };

        if !issues.is_empty() {
            return Err(Error::ConfigErrors(ConfigErrors::new(issues)));
        }

        // `issues` is empty, so each of these is `Some`. Expressed as a fallible
        // unwrap anyway: a library must not abort the caller's process if that
        // reasoning is ever invalidated by an edit above.
        let missing = |key: &str| {
            Error::ConfigErrors(ConfigErrors::new(vec![ConfigIssue::new(
                key,
                "internal error: value missing after validation reported no issues",
            )]))
        };
        let schema = SqlIdentifier::new(schema.ok_or_else(|| missing("database.schema"))?)?;
        let mut resolved = Self::new(schema)
            .with_relay(relay.ok_or_else(|| missing("relay"))?)
            .with_dispatcher(dispatcher.ok_or_else(|| missing("dispatcher"))?)
            .with_retry(retry.ok_or_else(|| missing("retry"))?)
            .with_observability(observability.ok_or_else(|| missing("observability"))?)
            .with_topics(topics.ok_or_else(|| missing("topics"))?);
        for message in messages {
            // Fallible on purpose. A `message_type` re-registered under a
            // different topic is a misconfiguration, not a duplicate: keeping the
            // first topic silently publishes a type to a topic nobody asked for,
            // and the only symptom is a consumer that quietly stops receiving.
            // This is the path every application takes, so the check has to live
            // here and not only in the helper.
            resolved = resolved.try_with_message(message)?;
        }
        Ok(resolved)
    }

    /// Register a message type's tables.
    ///
    /// Re-registering an identical descriptor is idempotent, so a duplicate
    /// cannot generate a second changeset targeting the same table and index
    /// names. Re-registering the same `message_type` under a *different* topic
    /// is a configuration error rather than a duplicate, and is reported by
    /// [`Self::try_with_message`]; this infallible form keeps the first
    /// registration.
    pub fn with_message(mut self, descriptor: MessageDescriptor) -> Self {
        if self.registered(&descriptor).is_none() {
            self.messages.push(descriptor);
        }
        self
    }

    /// Fallible [`Self::with_message`]: rejects a `message_type` that is already
    /// registered under a different topic.
    ///
    /// Silently keeping the first topic would route messages to a topic the
    /// caller never asked for, which is invisible until a consumer somewhere
    /// stops receiving them.
    pub fn try_with_message(mut self, descriptor: MessageDescriptor) -> Result<Self> {
        match self.registered(&descriptor) {
            Some(existing) if existing.topic != descriptor.topic => {
                Err(Error::ConflictingMessageType {
                    message_type: descriptor.message_type.as_str().to_owned(),
                    registered: existing.topic.clone(),
                    conflicting: descriptor.topic,
                })
            }
            Some(_) => Ok(self),
            None => {
                self.messages.push(descriptor);
                Ok(self)
            }
        }
    }

    fn registered(&self, descriptor: &MessageDescriptor) -> Option<&MessageDescriptor> {
        self.messages
            .iter()
            .find(|existing| existing.message_type == descriptor.message_type)
    }

    pub fn with_relay(mut self, relay: RelayConfig) -> Self {
        self.relay = relay;
        self
    }

    pub fn with_dispatcher(mut self, dispatcher: DispatcherConfig) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn with_topics(mut self, topics: TopicMode) -> Self {
        self.topics = topics;
        self
    }

    pub fn with_observability(mut self, observability: ObservabilityConfig) -> Self {
        self.observability = observability;
        self
    }

    pub fn schema(&self) -> &SqlIdentifier {
        &self.schema
    }

    pub fn messages(&self) -> &[MessageDescriptor] {
        &self.messages
    }

    /// The descriptor for `P`, rejecting a type nobody registered.
    ///
    /// Refusing an unregistered type is what stops a query being built against
    /// a table no changeset ever created.
    pub fn descriptor_for<P: KafkaMessage>(&self) -> Result<MessageDescriptor> {
        let descriptor = P::descriptor()?;
        if self
            .messages
            .iter()
            .any(|configured| configured.message_type == descriptor.message_type)
        {
            Ok(descriptor)
        } else {
            Err(Error::UnknownMessageType(
                descriptor.message_type.as_str().to_owned(),
            ))
        }
    }
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        // The literal is a valid identifier; `unreachable!` documents that
        // rather than leaving a bare panic on a library path.
        Self::new(
            SqlIdentifier::new("kafkaman")
                .unwrap_or_else(|_| unreachable!("the default schema literal is valid")),
        )
    }
}
