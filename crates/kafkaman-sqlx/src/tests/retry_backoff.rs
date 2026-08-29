use std::time::Duration;

use kafkaman_config::{DlqMode, RetryPolicy};
use kafkaman_core::ReceivedFailureKind;
use rand::SeedableRng;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::dispatch_failure::{failure_disposition, FailureDisposition};
use crate::retry_backoff::{jittered, received_failure_schedule, retry_backoff};
use crate::tests::received_table;
use crate::Error;

/// The disposition most tests want: an ordinary retryable failure.
const RETRYABLE: FailureDisposition = FailureDisposition {
    kind: ReceivedFailureKind::Handler,
    stage: kafkaman_core::FailureStage::Handler,
    terminal: false,
};

#[test]
fn retry_backoff_grows_exponentially_and_saturates_at_max() {
    let policy = RetryPolicy {
        max_attempts: 10,
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(8),
        multiplier: 2.0,
        errors_limit: 20,
        dlq: DlqMode::Table,
    };

    // The first failure waits exactly the initial backoff, not initial*multiplier.
    assert_eq!(retry_backoff(&policy, 0), Duration::from_secs(1));
    assert_eq!(retry_backoff(&policy, 1), Duration::from_secs(2));
    assert_eq!(retry_backoff(&policy, 2), Duration::from_secs(4));
    // Saturation, not overflow: a long-failing row must not schedule itself
    // years into the future.
    assert_eq!(retry_backoff(&policy, 3), Duration::from_secs(8));
    assert_eq!(retry_backoff(&policy, 60), Duration::from_secs(8));
    assert_eq!(retry_backoff(&policy, u32::MAX), Duration::from_secs(8));
}

#[test]
fn jitter_stays_within_half_of_the_base_delay() {
    // The delay must never grow past the computed backoff (which is already
    // capped at `max_backoff`) nor collapse toward zero, or the backoff
    // curve would stop meaning anything.
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let base = Duration::from_secs(8);
    for _ in 0..1_000 {
        let delay = jittered(base, &mut rng);
        assert!(delay >= base / 2, "{delay:?} below half of {base:?}");
        assert!(delay <= base, "{delay:?} above {base:?}");
    }

    // A sub-nanosecond base has no room to jitter and must pass through
    // unchanged rather than becoming zero.
    assert_eq!(jittered(Duration::ZERO, &mut rng), Duration::ZERO);
    assert_eq!(
        jittered(Duration::from_nanos(1), &mut rng),
        Duration::from_nanos(1)
    );
}

#[test]
fn failure_schedule_exhausts_exactly_at_max_attempts() {
    let table = received_table("product_snapshot");
    let max = table.retry.max_attempts;
    let now = OffsetDateTime::UNIX_EPOCH;

    // Attempt N-1 has budget left: it is scheduled, not terminal.
    let penultimate = received_failure_schedule(&table, max as i32 - 2, now, RETRYABLE);
    assert!(!penultimate.exhausted);
    assert!(penultimate.next_attempt_at.is_some());

    // The attempt that reaches max_attempts is terminal and unscheduled, so
    // a Failed row can never be re-claimed by a due-time query.
    let last = received_failure_schedule(&table, max as i32 - 1, now, RETRYABLE);
    assert!(last.exhausted);
    assert!(last.next_attempt_at.is_none());
}

#[test]
fn deterministic_cache_errors_are_terminal_whichever_frame_raised_them() {
    use kafkaman_core::FailureStage;

    // Retrying either of these re-reads the same row and fails identically,
    // so spending the attempt budget buys nothing and delays the only signal
    // an operator gets by exactly that budget.
    //
    // Asserted across every stage, because that is the change: terminality used
    // to live only in the classifier kafkaman used for its *own* failures, so
    // the identical error returned by a handler quietly kept its retries. It is
    // a property of the error — the guard's predicate cannot become true again
    // no matter who noticed it could not.
    let mismatch = Error::CacheOriginMismatch {
        entity_key: "p-1".to_owned(),
        applied_topic: "products".to_owned(),
        applied_partition: 0,
        incoming_topic: "products".to_owned(),
        incoming_partition: 1,
    };
    let missing = Error::MissingEntityKey {
        message_id: Uuid::nil(),
        message_type: "product_snapshot".to_owned(),
    };

    for stage in FailureStage::ALL {
        assert!(
            failure_disposition(&mismatch, stage).terminal,
            "a cache origin mismatch is terminal from {stage:?}"
        );
        assert!(
            failure_disposition(&missing, stage).terminal,
            "a missing entity key is terminal from {stage:?}"
        );
        assert!(
            !failure_disposition(&Error::MissingHandler("x".to_owned()), stage).terminal,
            "an unregistered type is retryable from {stage:?} — a replica that \
             has not deployed yet will register it"
        );
    }

    // A transient database error shares the `Infrastructure` class with them,
    // which is exactly why terminality cannot be read off the class.
    let disposition = failure_disposition(&mismatch, FailureStage::Bookkeeping);
    assert_eq!(disposition.kind, ReceivedFailureKind::Infrastructure);

    // And terminality really does short-circuit the schedule, on attempt zero
    // of a policy with attempts to spare.
    let table = received_table("product_snapshot");
    assert!(
        table.retry.max_attempts > 1,
        "fixture must have retries left"
    );
    let schedule = received_failure_schedule(&table, 0, OffsetDateTime::UNIX_EPOCH, disposition);
    assert!(schedule.exhausted);
    assert!(
        schedule.next_attempt_at.is_none(),
        "a terminal row must not be re-claimable by a due-time query"
    );
}

#[test]
fn failure_schedule_clamps_a_negative_attempt_count() {
    // `attempts` is CHECK-constrained non-negative, but the conversion must
    // not panic or wrap if a row ever violates it.
    let table = received_table("product_snapshot");
    let schedule = received_failure_schedule(&table, -5, OffsetDateTime::UNIX_EPOCH, RETRYABLE);
    assert!(!schedule.exhausted);
    assert!(schedule.errors_limit >= 1);
}
