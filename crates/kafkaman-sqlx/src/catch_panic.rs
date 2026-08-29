//! Running an application handler's future so that a panic inside it becomes an
//! error rather than unwinding into kafkaman's dispatch loop.
//!
//! # Why this is here and not in a dependency
//!
//! `futures::FutureExt::catch_unwind` does exactly this. Depending on `futures`
//! for it would widen the graph of a crate every adopter links, for thirty lines
//! that need no `unsafe` — the handler futures are already
//! [`Pin<Box<dyn Future>>`](std::pin::Pin), which is `Unpin`, so polling through
//! the box needs no structural pinning. The workspace sets
//! `unsafe_code = "forbid"`, and this stays inside that.
//!
//! # What it does not do
//!
//! It catches nothing of kafkaman's own. A panic in a dispatch loop, a relay, or
//! ingester bookkeeping is a bug in this library, and swallowing it would leave
//! a process that looks healthy while a loop is dead. This module wraps the two
//! handler calls in `dispatch.rs` and, through
//! [`catch_application_panic`], the synchronous calls into
//! application-implemented traits on the storage path; ingest wraps its own
//! decode edge separately.
//!
//! A panic is also not caught when the process is built with `panic = "abort"`,
//! because there is no unwind to catch. That is a deployment's choice and it
//! reinstates the old behaviour exactly.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use kafkaman_core::panic_message;

use crate::{Error, Result};

/// Why a handler call produced no value.
///
/// Two variants rather than one string, because they are not the same event and
/// must not be recorded as one. A panic is the application's fault and belongs in
/// the row's failure history labelled as a panic; a poll after completion is
/// *kafkaman's* fault, and labelling it `handler panicked` would put a library
/// bug in the application's error history, stamp `handler.outcome: panicked` on
/// its span, and feed the dispatcher's panic breaker.
#[derive(Debug)]
pub(crate) enum HandlerAbort {
    /// The handler future unwound, either while being polled or while being
    /// dropped. A panicking destructor in handler code is a handler panic.
    Panicked(String),
    /// The boundary was polled again after it had already returned `Ready`.
    ///
    /// Unreachable through [`run_handler`](crate::dispatch), which awaits once.
    /// It exists because the alternative is panicking here, and a panic inside
    /// the thing that exists to contain panics would unwind into the loop.
    PolledAfterCompletion,
}

/// Await `future`, converting a panic into [`HandlerAbort::Panicked`].
///
/// The future is consumed either way. Its destructor is run inside the same
/// unwind boundary on completion, so a guard dropped after `poll` returns cannot
/// unwind into the dispatcher loop.
pub(crate) fn catch_handler_panic<T>(
    future: Pin<Box<dyn Future<Output = T> + Send + '_>>,
) -> CatchPanic<'_, T> {
    CatchPanic {
        inner: Some(future),
    }
}

/// The future returned by [`catch_handler_panic`].
pub(crate) struct CatchPanic<'a, T> {
    inner: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
}

impl<T> Future for CatchPanic<'_, T> {
    type Output = std::result::Result<T, HandlerAbort>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Sound without `unsafe`: every field is `Unpin` — `Box` is `Unpin`
        // regardless of what it holds, so `Pin<Box<_>>` is too — which is what
        // makes the whole struct `Unpin` and `get_mut` safe.
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            debug_assert!(false, "the panic boundary was polled after completion");
            return Poll::Ready(Err(HandlerAbort::PolledAfterCompletion));
        };
        // `AssertUnwindSafe` is the honest assertion here rather than a
        // suppression: the state that could be observed after a caught panic is
        // the transaction, and the caller treats a panic exactly as it treats a
        // handler error — savepoint rollback, or abandon the transaction and
        // record the failure on a fresh connection. Nothing reads through to
        // half-written handler state.
        match catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => match this.drop_inner() {
                Ok(()) => Poll::Ready(Ok(value)),
                Err(message) => Poll::Ready(Err(HandlerAbort::Panicked(format!(
                    "handler future drop panicked: {message}"
                )))),
            },
            Err(payload) => {
                let message = panic_message(payload);
                let message = match this.drop_inner() {
                    Ok(()) => message,
                    Err(drop_message) => format!(
                        "{message}; handler future drop panicked while cleaning up: {drop_message}"
                    ),
                };
                Poll::Ready(Err(HandlerAbort::Panicked(message)))
            }
        }
    }
}

impl<T> CatchPanic<'_, T> {
    fn drop_inner(&mut self) -> std::result::Result<(), String> {
        let Some(inner) = self.inner.take() else {
            return Ok(());
        };
        catch_unwind(AssertUnwindSafe(|| drop(inner))).map_err(panic_message)
    }
}

impl<T> Drop for CatchPanic<'_, T> {
    fn drop(&mut self) {
        if let Err(message) = self.drop_inner() {
            tracing::error!(
                error = %message,
                "handler future drop panicked after the panic boundary was cancelled"
            );
        }
    }
}

/// Run one synchronous call into application-implemented code, converting a
/// panic into [`Error::ApplicationPanicked`].
///
/// The asynchronous handler boundary above is the well-known one, but it is not
/// the only place kafkaman calls code it does not own. Both directions of the
/// durable path invoke [`KafkaMessage`] methods and `Serialize` impls the
/// application wrote, on loops that must not die of them: on the receive side a
/// panic here would take the ingest loop down with an uncommitted offset, and on
/// the send side it would unwind through `enqueue` into whatever called it.
///
/// [`KafkaMessage`]: kafkaman_core::KafkaMessage
pub(crate) fn catch_application_panic<T>(
    message_type: &str,
    operation: &'static str,
    call: impl FnOnce() -> T,
) -> Result<T> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|payload| Error::ApplicationPanicked {
        message_type: message_type.to_owned(),
        operation,
        message: panic_message(payload),
    })
}
