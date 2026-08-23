//! When a failed row is tried again.
//!
//! Pure arithmetic over a [`RetryPolicy`], separated from the transaction
//! handling that calls it so it can be reasoned about — and tested — without a
//! database.

use std::time::Duration;

use kafkaman_config::RetryPolicy;
use time::OffsetDateTime;

use crate::dispatch_failure::FailureDisposition;
use crate::ReceivedTable;

/// The retry decision for one failure.
pub(crate) struct ReceivedFailureSchedule {
    /// The row has no attempts left (or never had any) and becomes terminal.
    pub(crate) exhausted: bool,
    /// When to try again, or `None` when exhausted — which is what keeps a
    /// `Failed` row out of every due-time query.
    pub(crate) next_attempt_at: Option<OffsetDateTime>,
    /// How many entries of the stored error history to keep.
    pub(crate) errors_limit: i64,
}

pub(crate) fn received_failure_schedule(
    table: &ReceivedTable,
    current_attempts: i32,
    occurred_at: OffsetDateTime,
    disposition: FailureDisposition,
) -> ReceivedFailureSchedule {
    // `attempts` is CHECK-constrained non-negative, but a negative value must
    // not panic or wrap if a row ever violates that.
    let current_attempts = u32::try_from(current_attempts).unwrap_or(0);
    let next_attempts = current_attempts.saturating_add(1);
    // A terminal failure skips straight to the end of the retry budget: another
    // attempt would read the same row and fail identically, so scheduling one only
    // delays the operator signal.
    let exhausted = disposition.terminal || next_attempts >= table.retry.max_attempts;
    let next_attempt_at = if exhausted {
        None
    } else {
        let base = retry_backoff(&table.retry, current_attempts);
        Some(occurred_at + duration_to_time(jittered(base, &mut rand::thread_rng())))
    };

    ReceivedFailureSchedule {
        exhausted,
        next_attempt_at,
        errors_limit: i64::from(table.retry.errors_limit.max(1)),
    }
}

/// Exponential backoff, saturating at `max_backoff`.
///
/// The first failure waits exactly `initial_backoff`, not `initial * multiplier`
/// — a row that has failed once has not yet earned a longer wait than the policy
/// nominally starts at.
pub(crate) fn retry_backoff(policy: &RetryPolicy, current_attempts: u32) -> Duration {
    let multiplier = if current_attempts == 0 {
        1.0
    } else {
        policy
            .multiplier
            .powi(current_attempts.min(i32::MAX as u32) as i32)
    };
    let max = policy.max_backoff.as_secs_f64();
    let candidate = policy.initial_backoff.as_secs_f64() * multiplier;
    // A long-failing row overflows the multiplication to infinity long before it
    // exhausts its attempts; saturate rather than schedule itself past the end
    // of representable time.
    if !candidate.is_finite() {
        return policy.max_backoff;
    }
    Duration::from_secs_f64(candidate.min(max))
}

/// Spread a computed backoff over `[base/2, base]`.
///
/// Without this, every row failed by one shared outage retries at the same
/// instant, so the recovering dependency is hit by the whole backlog at once and
/// the batch fails together again — the classic retry thundering herd. "Equal
/// jitter" keeps the exponential growth curve (the delay never shrinks below
/// half the intended value) while decorrelating the instants.
pub(crate) fn jittered<R: rand::Rng>(base: Duration, rng: &mut R) -> Duration {
    let half = base / 2;
    if half.is_zero() {
        return base;
    }
    half + rng.gen_range(Duration::ZERO..=half)
}

fn duration_to_time(duration: Duration) -> time::Duration {
    time::Duration::new(
        duration.as_secs().min(i64::MAX as u64) as i64,
        duration.subsec_nanos() as i32,
    )
}
