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

/// The fixed denominator every rate is expressed against.
///
/// `sample_success` is a float, but *counting* with one is a mistake that hides
/// well: adding 0.1 ten times gives 0.9999999999999999, so a one-in-ten rate
/// would emit its tenth event on the eleventh message and stay a message behind
/// for the life of the process. Converting the rate to an exact ratio once and
/// then counting in integers hits the requested rate for as long as the loop
/// runs.
///
/// A million is far finer than any rate an operator writes in a config file —
/// six decimal places — and leaves `seen * numerator` nowhere near overflowing
/// the `u128` it is computed in.
const RATE_DENOMINATOR: u64 = 1_000_000;

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

    /// The rate as successes emitted per [`RATE_DENOMINATOR`] successes seen, or
    /// `None` when disabled.
    ///
    /// Rounded rather than truncated, and floored at one: a positive rate that
    /// samples nothing is indistinguishable from `lifecycle = "summary"`, and
    /// the operator who set it asked to see *something*.
    fn numerator(&self) -> Option<u64> {
        if !self.emits_success_event() {
            return None;
        }
        if self.sample_success >= 1.0 {
            return Some(RATE_DENOMINATOR);
        }
        let scaled = (self.sample_success * RATE_DENOMINATOR as f64).round();
        Some((scaled as u64).max(1))
    }

    /// A sampler that realizes this rate.
    pub fn sampler(&self) -> LifecycleSampler {
        LifecycleSampler {
            numerator: self.numerator(),
            seen: 0,
            emitted: 0,
        }
    }
}

/// Decides which successes get an event, at the configured rate.
///
/// Deterministic rather than random: it keeps the running count of successes and
/// of events, and emits whatever the rate says is owed. After `N` successes it
/// has emitted `⌊N × rate⌋` events — never ahead of the rate and never a whole
/// event behind it, at any `N`. That needs no RNG dependency in the worker and —
/// unlike Bernoulli sampling — cannot go a long stretch emitting nothing on a
/// low rate, which is precisely when an operator turned sampling on to see
/// *something*.
///
/// # Why owed-count rather than every `n`-th
///
/// The obvious implementation samples every `n`-th success, with `n` the rounded
/// reciprocal of the rate. It silently rounds the operator's request to the
/// nearest ratio expressible that way: `0.75` becomes every 1st — everything —
/// and `0.66` becomes every 2nd, which is half. Both are wrong in the direction
/// that costs money, and neither is visible from the config file. Tracking the
/// count owed instead honours any rate in `0.0..=1.0`.
///
/// Counting is per sampler, so each scheduler samples its own stream.
#[derive(Clone, Debug)]
pub struct LifecycleSampler {
    /// `None` disables emission entirely.
    numerator: Option<u64>,
    seen: u64,
    emitted: u64,
}

impl LifecycleSampler {
    /// Records `successes` and returns how many events to emit for them.
    ///
    /// Takes a batch rather than a single message because schedulers work in
    /// cycles; carrying the counts across calls is what keeps the rate honest
    /// when batches are smaller than one event's worth of successes.
    pub fn take(&mut self, successes: usize) -> usize {
        let Some(numerator) = self.numerator else {
            return 0;
        };
        if successes == 0 {
            return 0;
        }

        self.seen = self
            .seen
            .saturating_add(u64::try_from(successes).unwrap_or(u64::MAX));
        // In `u128` so the multiply cannot overflow at any `seen` a process can
        // reach, and integer division so the result never drifts from the rate.
        let owed = u128::from(self.seen) * u128::from(numerator) / u128::from(RATE_DENOMINATOR);
        let owed = u64::try_from(owed).unwrap_or(u64::MAX);

        let emit = owed.saturating_sub(self.emitted);
        self.emitted = owed;
        usize::try_from(emit).unwrap_or(usize::MAX)
    }
}
