use crate::LifecycleEmission;

#[test]
fn lifecycle_emission_is_silent_by_default() {
    let emission = LifecycleEmission::default();
    assert!(!emission.emits_success_event());
    assert_eq!(emission.sampler().take(1_000), 0);
}

#[test]
fn per_message_without_a_rate_stays_silent() {
    // `lifecycle = "per-message"` alone is not a request for every message;
    // `sample_success` is what opens the tap.
    let emission = LifecycleEmission::new(true, 0.0);
    assert!(!emission.emits_success_event());
    assert_eq!(emission.sampler().take(500), 0);
}

#[test]
fn summary_lifecycle_ignores_a_configured_rate() {
    let emission = LifecycleEmission::new(false, 1.0);
    assert!(!emission.emits_success_event());
    assert_eq!(emission.sampler().take(10), 0);
}

#[test]
fn a_full_rate_emits_once_per_success() {
    let mut sampler = LifecycleEmission::new(true, 1.0).sampler();
    assert_eq!(sampler.take(3), 3);
    assert_eq!(sampler.take(1), 1);
}

#[test]
fn sampling_hits_the_configured_rate_across_cycles() {
    // 5% means one in twenty. Batches smaller than the interval must still
    // accumulate — the whole point of carrying state across cycles is that a
    // relay handling one message per poll is not silent forever.
    let mut sampler = LifecycleEmission::new(true, 0.05).sampler();
    let emitted: usize = (0..100).map(|_| sampler.take(1)).sum();
    assert_eq!(emitted, 5);

    let mut batched = LifecycleEmission::new(true, 0.05).sampler();
    assert_eq!(
        batched.take(100),
        5,
        "one batch of 100 matches 100 batches of 1"
    );
}

#[test]
fn sampling_does_not_double_count_across_batch_boundaries() {
    let mut sampler = LifecycleEmission::new(true, 0.5).sampler();
    // 3 successes at 1-in-2 yields one event, and the leftover carries.
    assert_eq!(sampler.take(3), 1);
    assert_eq!(sampler.take(1), 1);
    assert_eq!(sampler.take(0), 0);
}

#[test]
fn a_rate_that_is_not_a_reciprocal_is_still_honoured() {
    // The rates that break an every-`n`-th sampler. 0.75 rounds its reciprocal
    // to 1 and emits everything; 0.66 rounds to 2 and emits half. Both are the
    // operator quietly getting a rate they did not ask for, in the direction
    // that costs money.
    for (rate, expected) in [(0.75, 75), (0.66, 66), (0.99, 99), (0.4, 40), (0.01, 1)] {
        let mut sampler = LifecycleEmission::new(true, rate).sampler();
        let emitted: usize = (0..100).map(|_| sampler.take(1)).sum();
        assert_eq!(
            emitted, expected,
            "a rate of {rate} should emit {expected} events in 100 successes"
        );
    }
}

#[test]
fn a_rate_does_not_drift_over_a_long_stream() {
    // The reason the count is kept as an exact ratio rather than a float
    // accumulator: adding 0.1 ten times gives 0.9999999999999999, so a float
    // sampler emits 999 events here instead of 1000 and stays one behind for
    // the life of the process.
    let mut sampler = LifecycleEmission::new(true, 0.1).sampler();
    let emitted: usize = (0..10_000).map(|_| sampler.take(1)).sum();
    assert_eq!(emitted, 1_000);
}

#[test]
fn a_rate_too_small_to_express_still_emits() {
    // Below one part per million the ratio would round to zero, which is
    // silence — and silence is what `lifecycle = "summary"` already means. An
    // operator who wrote a positive rate asked to see something.
    let mut sampler = LifecycleEmission::new(true, 1e-9).sampler();
    assert_eq!(sampler.take(1_000_000), 1);
}

#[test]
fn a_rate_above_one_is_treated_as_every_success() {
    // Config validation rejects these, but `LifecycleEmission::new` is public
    // and a saturating rate must not wrap into silence.
    let mut sampler = LifecycleEmission::new(true, 4.0).sampler();
    assert_eq!(sampler.take(7), 7);
}

#[test]
fn a_non_finite_rate_is_silent_rather_than_panicking() {
    for rate in [f64::NAN, f64::INFINITY, -1.0] {
        let emission = LifecycleEmission::new(true, rate);
        assert!(!emission.emits_success_event(), "{rate} should not emit");
        assert_eq!(emission.sampler().take(1_000), 0);
    }
}
