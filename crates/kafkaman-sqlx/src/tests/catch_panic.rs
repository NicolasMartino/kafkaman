//! What a caught panic turns into, before any of it reaches a row.

use std::future::{ready, Future};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::catch_panic::{catch_handler_panic, HandlerAbort};

fn panic_hook_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Await `future` under the panic boundary, with the default hook silenced.
///
/// The hook prints every caught panic to stderr, which is right in a running
/// service — a panicking handler should stay as loud as it ever was — and pure
/// noise in a test that is expecting one.
async fn caught<T>(
    future: Pin<Box<dyn Future<Output = T> + Send>>,
) -> std::result::Result<T, HandlerAbort> {
    let _guard = panic_hook_lock().lock().await;
    let previous = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let outcome = catch_handler_panic(future).await;
    panic::set_hook(previous);
    outcome
}

/// The panic message from an abort that should be one.
///
/// `expect_err` alone would accept `PolledAfterCompletion`, which is the variant
/// these tests exist to keep separate from a real panic.
fn panicked<T: std::fmt::Debug>(outcome: std::result::Result<T, HandlerAbort>) -> String {
    match outcome.expect_err("the panic should be caught") {
        HandlerAbort::Panicked(message) => message,
        other => panic!("expected a caught panic, got {other:?}"),
    }
}

#[tokio::test]
async fn a_future_that_returns_is_passed_through_untouched() {
    assert!(matches!(caught(Box::pin(ready(7))).await, Ok(7)));
}

#[tokio::test]
async fn a_literal_panic_keeps_its_message() {
    let outcome = caught::<()>(Box::pin(async { panic!("boom") })).await;
    assert_eq!(panicked(outcome), "boom");
}

#[tokio::test]
async fn a_formatted_panic_keeps_its_message() {
    let attempt = 3;
    let outcome = caught::<()>(Box::pin(
        async move { panic!("failed on attempt {attempt}") },
    ))
    .await;
    assert_eq!(panicked(outcome), "failed on attempt 3");
}

#[tokio::test]
async fn a_non_string_payload_says_so_rather_than_vanishing() {
    let outcome = caught::<()>(Box::pin(async { panic::panic_any(42_u32) })).await;
    assert_eq!(panicked(outcome), "panicked with a non-string payload");
}

/// A panic message is written to a column and retained `errors_limit` deep, so
/// it is bounded — unlike a returned error, it was not authored for storage.
#[tokio::test]
async fn a_very_long_panic_message_is_truncated_on_a_character_boundary() {
    let long = "é".repeat(2000);
    let outcome = caught::<()>(Box::pin(async move { panic!("{long}") })).await;
    let message = panicked(outcome);

    assert!(message.len() <= 512 + 3, "length was {}", message.len());
    assert!(
        message.ends_with("..."),
        "the cut should be marked: {message}"
    );
    // The whole point of cutting on a character boundary: a `String` that split
    // a multi-byte character would not be a `String` at all, and every one of
    // these is two bytes.
    assert!(message.chars().all(|c| c == 'é' || c == '.'));
}

/// The boundary yields rather than blocking, so a handler that awaits is not
/// forced to complete inside one poll.
#[tokio::test]
async fn a_future_that_pends_before_panicking_is_still_caught() {
    let outcome = caught::<()>(Box::pin(async {
        tokio::task::yield_now().await;
        panic!("after a yield");
    }))
    .await;
    assert_eq!(panicked(outcome), "after a yield");
}

struct PanicOnDrop;

impl Future for PanicOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("drop exploded");
    }
}

#[tokio::test]
async fn a_future_drop_panic_is_caught_before_it_reaches_the_loop() {
    let outcome = caught::<()>(Box::pin(PanicOnDrop)).await;
    let message = panicked(outcome);
    assert!(
        message.contains("handler future drop panicked"),
        "{message}"
    );
    assert!(message.contains("drop exploded"), "{message}");
}

#[tokio::test]
async fn dropping_the_boundary_before_completion_does_not_unwind() {
    struct PendingDropPanic;

    impl Future for PendingDropPanic {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingDropPanic {
        fn drop(&mut self) {
            panic!("cancel drop exploded");
        }
    }

    let _guard = panic_hook_lock().lock().await;
    let previous = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        drop(catch_handler_panic(Box::pin(PendingDropPanic)));
    }));
    panic::set_hook(previous);

    assert!(
        outcome.is_ok(),
        "a cancelled handler future with a panicking destructor must not unwind"
    );
}
