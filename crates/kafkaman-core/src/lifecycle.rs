//! Whether a scheduler emits an event per successfully handled message, and how
//! often.
//!
//! This is the runtime half of the `[observability]` lifecycle and
//! `sample_success` settings. It lives in core rather than in the config crate
//! so the worker loops can read it without depending on TOML parsing;
//! `ObservabilityPolicy` converts into it.

/// The default is silent. A healthy relay publishes continuously, so
/// per-message success logging is off unless an operator asks for it and gives
/// it a rate.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LifecycleEmission {
    /// Set by `lifecycle = "per-message"`. When false, schedulers emit only
    /// per-cycle summaries.
    pub per_message: bool,
    /// Fraction of successes to emit, `0.0..=1.0`.
    pub sample_success: f64,
}

impl LifecycleEmission {
    pub fn new(per_message: bool, sample_success: f64) -> Self {
        Self {
            per_message,
            sample_success,
        }
    }

    /// Whether any success event can be emitted at all.
    ///
    /// `per-message` with a zero rate is silent by construction, and checking it
    /// here keeps the sampler off the hot path entirely in the default case.
    pub fn emits_success_event(&self) -> bool {
        self.per_message && self.sample_success > 0.0 && self.sample_success.is_finite()
    }

    /// One event in every `n` successes, or `None` when disabled.
    fn interval(&self) -> Option<u64> {
        if !self.emits_success_event() {
            return None;
        }
        if self.sample_success >= 1.0 {
            return Some(1);
        }
        // `sample_success` is validated to `0.0..=1.0` and non-zero here, so the
        // reciprocal is finite and at least 1.
        let every = (1.0 / self.sample_success).round();
        Some(if every < 1.0 { 1 } else { every as u64 })
    }

    /// A sampler that realizes this rate.
    pub fn sampler(&self) -> LifecycleSampler {
        LifecycleSampler {
            interval: self.interval(),
            seen: 0,
        }
    }
}

/// Decides which successes get an event, at the configured rate.
///
/// Deterministic rather than random: it emits every `n`-th success instead of
/// rolling a die per message. That hits the requested rate exactly over any
/// window, needs no RNG dependency in the worker, and — unlike Bernoulli
/// sampling — cannot go a long stretch emitting nothing on a low rate, which is
/// precisely when an operator turned sampling on to see *something*.
///
/// Counting is per sampler, so each scheduler samples its own stream.
#[derive(Clone, Debug)]
pub struct LifecycleSampler {
    /// `None` disables emission entirely.
    interval: Option<u64>,
    seen: u64,
}

impl LifecycleSampler {
    /// Records `successes` and returns how many events to emit for them.
    ///
    /// Takes a batch rather than a single message because schedulers work in
    /// cycles; carrying `seen` across calls is what keeps the rate honest when
    /// batches are smaller than the sampling interval.
    pub fn take(&mut self, successes: usize) -> usize {
        let Some(interval) = self.interval else {
            return 0;
        };
        let successes = successes as u64;
        if successes == 0 {
            return 0;
        }
        let before = self.seen / interval;
        self.seen = self.seen.saturating_add(successes);
        let after = self.seen / interval;
        usize::try_from(after - before).unwrap_or(usize::MAX)
    }
}
