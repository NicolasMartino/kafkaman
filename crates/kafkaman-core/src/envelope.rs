use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{IdempotencyIdentity, IntoIdempotencyIdentity, Result};

/// A payload plus the metadata that travels with it: identity, causation, and
/// event time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope<P> {
    pub message_id: Uuid,
    pub idempotency_key: Option<IdempotencyIdentity>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub headers: BTreeMap<String, String>,
    pub payload: P,
    pub occurred_at: OffsetDateTime,
}

impl<P> Envelope<P> {
    pub fn new(payload: P) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            idempotency_key: None,
            correlation_id: Uuid::new_v4(),
            causation_id: None,
            headers: BTreeMap::new(),
            payload,
            occurred_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn with_message_id(mut self, message_id: Uuid) -> Self {
        self.message_id = message_id;
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    /// Attach an already-derived identity. Infallible, because
    /// [`IdempotencyIdentity`] can only be constructed through a checked
    /// derivation.
    ///
    /// The panicking `with_idempotency_key` that used to sit here was a library
    /// API that aborted the caller's process on bad input; it existed purely for
    /// test ergonomics and now lives in `kafkaman-test` as an extension trait.
    pub fn with_idempotency_identity(mut self, identity: IdempotencyIdentity) -> Self {
        self.idempotency_key = Some(identity);
        self
    }

    pub fn try_with_idempotency_key(
        mut self,
        idempotency_key: impl IntoIdempotencyIdentity,
    ) -> Result<Self> {
        self.idempotency_key = Some(idempotency_key.into_idempotency_identity()?);
        Ok(self)
    }
}
