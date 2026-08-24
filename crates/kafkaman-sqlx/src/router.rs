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

/// The future a pre-upsert handler returns.
///
/// Differs from [`HandlerFuture`] only in yielding a [`HandlerFlow`], because
/// the pre-upsert position is the one that gets to decide whether the expensive
/// downstream derivation is worth running.
pub type BeforeHandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<HandlerFlow>> + Send + 'a>>;

/// What a pre-upsert handler wants to happen next.
///
/// Note what is *not* here: there is no variant that suppresses the cache
/// upsert. The cache is the converged current state of every entity on a topic,
/// so a handler that could filter a record out would leave a stale row that
/// nothing will ever correct — the correcting message being precisely the one
/// that was filtered. Selection belongs at query time. Skipping downstream
/// *work* is a cost decision the application is entitled to make; skipping
/// convergence is not.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HandlerFlow {
    /// Apply the cache upsert and run the post-upsert handler as usual.
    #[default]
    Continue,
    /// Apply the cache upsert, then skip the post-upsert handler for this
    /// record. The row is still marked processed.
    SkipHandler,
}

pub(crate) type ErasedHandler = Arc<dyn ErasedMessageHandler>;
pub(crate) type ErasedBeforeHandler = Arc<dyn ErasedBeforeMessageHandler>;

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

/// The pre-upsert twin of [`ErasedMessageHandler`].
pub(crate) trait ErasedBeforeMessageHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        meta: ReceivedMeta,
        payload: serde_json::Value,
    ) -> BeforeHandlerFuture<'a>;
}

struct TypedMessageHandler<P, F> {
    handler: F,
    _message: std::marker::PhantomData<fn(P)>,
}

struct TypedBeforeMessageHandler<P, F> {
    handler: F,
    _message: std::marker::PhantomData<fn(P)>,
}

impl<P, F> ErasedBeforeMessageHandler for TypedBeforeMessageHandler<P, F>
where
    P: DeserializeOwned + Send + 'static,
    F: for<'a> Fn(&'a mut PgConnection, ReceivedMeta, P) -> BeforeHandlerFuture<'a>
        + Send
        + Sync
        + 'static,
{
    fn handle<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        meta: ReceivedMeta,
        payload: serde_json::Value,
    ) -> BeforeHandlerFuture<'a> {
        Box::pin(async move {
            let message = serde_json::from_value(payload)?;
            (self.handler)(conn, meta, message).await
        })
    }
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
///
/// Two positions, keyed independently: `handlers` run *after* the cache upsert
/// and `before_handlers` run before it. Registering both for one message type is
/// the only legal two-handler registration, and the dispatcher runs at most one
/// of each per record.
#[derive(Clone, Default)]
pub struct MessageRouter {
    handlers: BTreeMap<String, ErasedHandler>,
    before_handlers: BTreeMap<String, ErasedBeforeHandler>,
}

impl std::fmt::Debug for MessageRouter {
    /// Handlers are boxed closures with no useful representation, so list the
    /// message types that are routed — which is the thing worth seeing when a
    /// dispatch reports `MissingHandler`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageRouter")
            .field("message_types", &self.handlers.keys().collect::<Vec<_>>())
            .field(
                "before_message_types",
                &self.before_handlers.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl MessageRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the handler that runs *after* the cache upsert.
    ///
    /// This is the default position and the one a derivation wants: the
    /// incoming message is already applied, so a handler recomputing state from
    /// its own cache sees the new value rather than a row exactly one version
    /// stale.
    ///
    /// It is not guaranteed to run once per received row. A record at or behind
    /// the entity's applied offset yields
    /// [`CacheApplyOutcome::Ignored`](crate::CacheApplyOutcome::Ignored) and the
    /// handler is skipped, because an ignored record carries no new state.
    /// Auditing every delivery belongs in [`MessageRouter::handler_before`].
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

    /// Register the handler that runs *before* the cache upsert.
    ///
    /// The explicit opt-in for handlers that need the entity's previous version,
    /// which is unrecoverable once the upsert lands. It returns a
    /// [`HandlerFlow`] and may use it to skip the post-upsert handler — but
    /// never the upsert.
    ///
    /// Reach for this rarely. Change detection genuinely cannot recover the
    /// pre-image, and auditing needs a hook the `Ignored` skip does not bypass;
    /// almost everything else is expressible post-upsert, idempotently, at
    /// higher cost.
    pub fn handler_before<P>(
        mut self,
        handler: impl for<'a> Fn(&'a mut PgConnection, ReceivedMeta, P) -> BeforeHandlerFuture<'a>
            + Send
            + Sync
            + 'static,
    ) -> Self
    where
        P: KafkaMessage + DeserializeOwned + Send + 'static,
    {
        self.before_handlers.insert(
            P::MESSAGE_TYPE.to_owned(),
            Arc::new(TypedBeforeMessageHandler::<P, _> {
                handler,
                _message: std::marker::PhantomData,
            }),
        );
        self
    }

    pub(crate) fn handler_for(&self, message_type: &str) -> Option<ErasedHandler> {
        self.handlers.get(message_type).cloned()
    }

    pub(crate) fn before_handler_for(&self, message_type: &str) -> Option<ErasedBeforeHandler> {
        self.before_handlers.get(message_type).cloned()
    }

    /// Whether this message type has a handler at either position.
    ///
    /// What the dispatcher checks before it does anything else. A type routed
    /// only pre-upsert is legitimately registered, so the missing-handler
    /// short-circuit must not fire for it.
    pub(crate) fn routes(&self, message_type: &str) -> bool {
        self.handlers.contains_key(message_type) || self.before_handlers.contains_key(message_type)
    }
}

/// What one dispatch cycle did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DispatchStats {
    pub claimed: usize,
    pub processed: usize,
    pub failed: usize,
}
