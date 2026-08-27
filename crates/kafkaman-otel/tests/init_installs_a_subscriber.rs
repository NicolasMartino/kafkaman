//! The `init` path, which the unit tests cannot reach.
//!
//! `init` installs the global `tracing` subscriber, and a process has exactly
//! one of those. A unit test that called it would seize the subscriber for every
//! other test in the same binary and make their output depend on ordering. An
//! integration test is its own binary, so this one owns a process and can
//! exercise the real entry point rather than the builder underneath it.

// Tests assert; a library returns. Same opt-out the crate uses under `cfg(test)`.
#![allow(clippy::expect_used, clippy::panic)]

/// One test, not three: each step depends on the global installed by the one
/// before it, so they cannot be allowed to run on separate threads.
#[test]
fn init_installs_a_subscriber_once_and_reports_a_second_attempt() {
    for name in [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    ] {
        std::env::remove_var(name);
    }

    let telemetry = kafkaman_otel::init("init-test-service").expect("the first init must succeed");

    assert!(
        !telemetry.is_enabled(),
        "no endpoint is configured, so init installs a subscriber and no provider"
    );
    assert_eq!(telemetry.service_name(), "init-test-service");

    // The subscriber is real, not a no-op: this reaches the registry `init`
    // installed. It asserts nothing by itself — it is here because a panic or a
    // "no subscriber" warning would mean the registry was not installed at all.
    tracing::info!("emitted through the subscriber init installed");

    // A second `init` cannot win the global, and the error says which global was
    // lost rather than surfacing as a silent no-op. An adopter whose framework
    // already installed a subscriber meets exactly this, and the message is what
    // points them at `builder` instead.
    let second = kafkaman_otel::init("init-test-service-again");
    let err = match second {
        Err(err) => err,
        Ok(_) => panic!("a second init must not install a second global subscriber"),
    };
    assert!(
        matches!(err, kafkaman_otel::Error::Subscriber(_)),
        "the second init must fail on the subscriber, not somewhere else: {err}"
    );

    telemetry
        .shutdown()
        .expect("shutting down a pipeline that installed no provider must succeed");
}
