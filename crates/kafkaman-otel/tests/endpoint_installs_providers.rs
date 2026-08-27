//! A configured endpoint reaches the providers, not just `endpoint_configured`.
//!
//! The unit tests cover the environment *decision* — which variable enables
//! which signal — but stop short of building anything, because building installs
//! global providers and exporter threads that would outlive the test. This is
//! its own binary, so it can follow the decision through to the installation it
//! is supposed to cause.
//!
//! The endpoint is `127.0.0.1:1`: a port nothing listens on, so every export
//! fails immediately with a refused connection rather than hanging on a timeout.
//! Nothing here asserts on export success — the subject is what gets installed.

// Tests assert; a library returns. Same opt-out the crate uses under `cfg(test)`.
#![allow(clippy::expect_used)]

const DEAD_ENDPOINT: &str = "http://127.0.0.1:1";

const ENDPOINTS: [&str; 4] = [
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
];

fn clear() {
    for name in ENDPOINTS {
        std::env::remove_var(name);
    }
}

/// One test, not two: both halves install global providers, so running them on
/// separate threads would race on the environment they each set up.
#[test]
fn endpoints_install_the_providers_they_select() {
    clear();

    // The generic endpoint: all three signals, and both layers available to
    // compose. `is_enabled` is the coarse answer; the layers are the useful one,
    // because a `None` layer is how a missing provider actually reaches a host.
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", DEAD_ENDPOINT);
    let all = kafkaman_otel::builder("endpoint-test-service")
        .build()
        .expect("a configured endpoint must build");

    assert!(all.is_enabled(), "a configured endpoint installs providers");
    assert!(
        all.trace_layer::<tracing_subscriber::Registry>().is_some(),
        "the generic endpoint enables traces, so there is a layer to compose"
    );
    assert!(
        all.log_layer::<tracing_subscriber::Registry>().is_some(),
        "the generic endpoint enables logs, so there is a layer to compose"
    );

    // Returns rather than hangs, against an endpoint that refuses every
    // connection. The result is deliberately not asserted: a failed final export
    // is a legitimate outcome here, and the failure this guards against is a
    // shutdown that never comes back.
    drop(all.shutdown());

    // A signal-specific endpoint enables only its own signal. This is the half
    // the unit tests could not reach: `endpoint_configured` returning `false` for
    // logs proves the decision, not that `build` acted on it.
    clear();
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", DEAD_ENDPOINT);
    let traces_only = kafkaman_otel::builder("endpoint-test-service")
        .build()
        .expect("a traces-only endpoint must build");

    assert!(traces_only.is_enabled());
    assert!(
        traces_only
            .trace_layer::<tracing_subscriber::Registry>()
            .is_some(),
        "traces were the configured signal"
    );
    assert!(
        traces_only
            .log_layer::<tracing_subscriber::Registry>()
            .is_none(),
        "a traces-only endpoint must not install a logger provider"
    );

    // `without_*` still wins over a configured endpoint, on the built handle
    // rather than only on the flag.
    clear();
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", DEAD_ENDPOINT);
    let opted_out = kafkaman_otel::builder("endpoint-test-service")
        .without_metrics()
        .without_traces()
        .without_logs()
        .build()
        .expect("building with every signal disabled must succeed");

    assert!(
        !opted_out.is_enabled(),
        "every signal was disabled explicitly, so the endpoint must be ignored"
    );

    clear();
    drop(traces_only.shutdown());
    opted_out
        .shutdown()
        .expect("a handle that installed nothing must shut down cleanly");
}
