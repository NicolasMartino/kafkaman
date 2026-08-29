use std::time::Duration;

use crate::{Error, LifecycleEmission, Result};

/// How the receive dispatcher paces itself, and when it gives up.
///
/// Here rather than in `kafkaman-worker` for the same reason [`RelayConfig`] and
/// [`PurgeConfig`] are: the loop lives in the worker crate, but the resolved
/// configuration is assembled in `kafkaman-sqlx`, which the worker depends on
/// rather than the other way round. This crate is the one both can see.
///
/// [`RelayConfig`]: crate::RelayConfig
/// [`PurgeConfig`]: crate::PurgeConfig
#[derive(Clone, Debug)]
pub struct DispatcherConfig {
    /// Sleep between polls that found nothing due.
    pub poll_interval: Duration,
    /// Per-message success event policy. Defaults to silent.
    pub lifecycle: LifecycleEmission,
    /// How many *distinct* received rows may panic in a row before the
    /// dispatcher stops.
    ///
    /// # Why distinct rows and not panics
    ///
    /// A breaker exists to catch a deploy whose handler panics on everything,
    /// which is a process-level fault no retry budget can absorb. One poison
    /// message is the opposite: it is exactly what the retry budget and the
    /// dead-letter queue are for, and stopping the service over it is the
    /// failure this whole boundary was built to remove.
    ///
    /// Counting panics cannot tell those apart. One row retrying on the default
    /// `max_attempts` of 10 produces ten panics with no successful claim between
    /// them, so an attempt counter reaches ten either way. Counting the distinct
    /// rows involved separates them exactly: the poison row contributes one no
    /// matter how often it is retried.
    ///
    /// # "Consecutive" is ordinal, not wall-clock
    ///
    /// The streak is bounded by *events*, not by elapsed time: it clears when a
    /// cycle claims a row and none of it panicked, and by nothing else. Ten
    /// distinct rows that panic over ten months, with only empty polls between
    /// them, trip this breaker exactly as ten in one second would.
    ///
    /// That is deliberate, and it is the same reasoning as counting rows rather
    /// than panics. A time window would have to be picked against a traffic rate
    /// the library cannot see: on a queue that receives one message an hour,
    /// every window short enough to catch a broken deploy is also short enough
    /// to expire before the second message arrives, so the breaker never trips.
    /// Ordinal counting needs no such guess — a service that has processed
    /// nothing successfully since the last panic has produced no evidence that
    /// it recovered, however long it spent not producing it.
    ///
    /// The cost is the case this trades for: a service healthy enough to be
    /// idle, with a rare poison row arriving now and then, eventually
    /// accumulates a streak and stops. Raise this value where that shape is
    /// expected. An *empty* cycle deliberately does not clear the streak, for
    /// the reason given at the call site.
    pub max_consecutive_panicking_rows: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            lifecycle: LifecycleEmission::default(),
            max_consecutive_panicking_rows: 10,
        }
    }
}

impl DispatcherConfig {
    /// Reject configurations that would spin the worker or disable the breaker.
    pub fn validate(&self) -> Result<()> {
        if self.poll_interval.is_zero() {
            return Err(Error::InvalidDispatcherConfig {
                field: "poll_interval",
                reason: "must be greater than zero; it is the only thing that yields between \
                         empty polls",
            });
        }
        // Rejected rather than clamped. Zero reads as "never trip", and a
        // breaker that trips before the first row is the opposite — an operator
        // who wrote it meaning the former would get a dispatcher that stops
        // immediately, silently, on a value the library accepted.
        if self.max_consecutive_panicking_rows == 0 {
            return Err(Error::InvalidDispatcherConfig {
                field: "max_consecutive_panicking_rows",
                reason: "must be greater than zero; there is no way to spell \"never stop\" here, \
                         because a handler that panics on every row is not something to keep \
                         retrying in silence",
            });
        }
        Ok(())
    }
}
