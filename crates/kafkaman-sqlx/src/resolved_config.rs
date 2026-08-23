use std::time::Duration;

use kafkaman_config::{Config, ConfigErrors, ConfigIssue, ConfigSchema, RetryConfig};
use kafkaman_core::{KafkaMessage, MessageDescriptor, RelayConfig, SqlIdentifier};

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
    pub retry: RetryConfig,
    messages: Vec<MessageDescriptor>,
}

impl ResolvedConfig {
    pub fn new(schema: SqlIdentifier) -> Self {
        Self {
            schema,
            relay: RelayConfig::default(),
            retry: RetryConfig::default(),
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
            .with_retry(retry.ok_or_else(|| missing("retry"))?);
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

    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
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
