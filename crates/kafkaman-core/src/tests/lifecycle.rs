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
