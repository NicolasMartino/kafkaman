use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use kafkaman_core::{KafkaMessage, ReceivedMeta};
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::Result;

/// The future a dispatch handler returns.
///
/// Boxed because handlers are stored behind a trait object keyed by message
/// type, which cannot be generic over each handler's own future type.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub(crate) type ErasedHandler = Arc<dyn ErasedMessageHandler>;

/// A handler with its payload type erased, so one map can hold handlers for
/// every registered message type.
pub(crate) trait ErasedMessageHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        meta: ReceivedMeta,
        payload: serde_json::Value,
    ) -> HandlerFuture<'a>;
}

struct TypedMessageHandler<P, F> {
    handler: F,
    _message: std::marker::PhantomData<fn(P)>,
}

impl<P, F> ErasedMessageHandler for TypedMessageHandler<P, F>
where
    P: DeserializeOwned + Send + 'static,
    F: for<'a> Fn(&'a mut PgConnection, ReceivedMeta, P) -> HandlerFuture<'a>
        + Send
        + Sync
        + 'static,
{
    fn handle<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        meta: ReceivedMeta,
        payload: serde_json::Value,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            let message = serde_json::from_value(payload)?;
            (self.handler)(conn, meta, message).await
        })
    }
}

/// Which handler runs for which message type.
#[derive(Clone, Default)]
pub struct MessageRouter {
    handlers: BTreeMap<String, ErasedHandler>,
}

impl std::fmt::Debug for MessageRouter {
    /// Handlers are boxed closures with no useful representation, so list the
    /// message types that are routed — which is the thing worth seeing when a
    /// dispatch reports `MissingHandler`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageRouter")
            .field("message_types", &self.handlers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl MessageRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handler<P>(
        mut self,
        handler: impl for<'a> Fn(&'a mut PgConnection, ReceivedMeta, P) -> HandlerFuture<'a>
            + Send
            + Sync
            + 'static,
    ) -> Self
    where
        P: KafkaMessage + DeserializeOwned + Send + 'static,
    {
        // Key on the constant directly instead of building a `MessageDescriptor`,
        // which is fallible and previously panicked here. Validation already
        // happens where it matters: `ReceivedTable::for_message::<P>` refuses an
        // invalid type, so no rows of one can exist for this entry to match.
        self.handlers.insert(
            P::MESSAGE_TYPE.to_owned(),
            Arc::new(TypedMessageHandler::<P, _> {
                handler,
                _message: std::marker::PhantomData,
            }),
        );
        self
    }

    pub(crate) fn handler_for(&self, message_type: &str) -> Option<ErasedHandler> {
        self.handlers.get(message_type).cloned()
    }
}

/// What one dispatch cycle did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DispatchStats {
    pub claimed: usize,
    pub processed: usize,
    pub failed: usize,
}
