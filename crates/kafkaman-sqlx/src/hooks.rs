use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use kafkaman_core::{IdempotencyKey, ReceivedFailureKind};
use uuid::Uuid;

use crate::Result;

type DispatchHookFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// One installed observer. Both slots below hold this same shape; they were two
/// separately-declared aliases of identical type, which is a distinction the
/// compiler cannot enforce and a reader has to check character by character.
type DispatchHook = dyn Fn(DispatchFailureEvent) -> DispatchHookFuture + Send + Sync + 'static;

/// Where in the failure path an observer runs.
///
/// Naming the slot rather than giving each one its own runner is what keeps the
/// two paths from drifting: there is one body, and it cannot be fixed for one
/// slot and left stale for the other.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DispatchHookSlot {
    /// Before the handler's writes are unwound to the savepoint.
    BeforeFailureRollback,
    /// Before the failure is appended to the row's audit trail.
    BeforeRecordFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct DispatchFailureEvent {
    pub message_id: Uuid,
    pub idempotency_key: IdempotencyKey,
    pub message_type: String,
    pub kind: ReceivedFailureKind,
    pub message: String,
}

#[derive(Clone, Default)]
#[doc(hidden)]
pub struct DispatchHooks {
    before_failure_rollback: Option<Arc<DispatchHook>>,
    before_record_failure: Option<Arc<DispatchHook>>,
}

impl std::fmt::Debug for DispatchHooks {
    /// Hooks are boxed closures with no useful representation, so report which
    /// slots are occupied.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchHooks")
            .field(
                "before_failure_rollback",
                &self.before_failure_rollback.is_some(),
            )
            .field(
                "before_record_failure",
                &self.before_record_failure.is_some(),
            )
            .finish()
    }
}

impl DispatchHooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn before_record_failure<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(DispatchFailureEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.before_record_failure = Some(Arc::new(move |context| Box::pin(hook(context))));
        self
    }

    pub fn before_failure_rollback<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(DispatchFailureEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.before_failure_rollback = Some(Arc::new(move |context| Box::pin(hook(context))));
        self
    }

    /// Run the observer in `slot`, if one is installed.
    #[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
    pub(crate) async fn run(
        &self,
        slot: DispatchHookSlot,
        context: DispatchFailureEvent,
    ) -> Result<()> {
        let hook = match slot {
            DispatchHookSlot::BeforeFailureRollback => &self.before_failure_rollback,
            DispatchHookSlot::BeforeRecordFailure => &self.before_record_failure,
        };
        if let Some(hook) = hook {
            hook(context).await?;
        }
        Ok(())
    }
}
