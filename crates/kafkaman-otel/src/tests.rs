use std::sync::{Mutex, MutexGuard};

use tracing_subscriber::layer::SubscriberExt as _;

use super::{
    builder, endpoint_configured, env_is_nonempty, METRIC_EXPORT_INTERVAL, OTLP_ENDPOINT,
    OTLP_LOGS_ENDPOINT, OTLP_METRICS_ENDPOINT, OTLP_TRACES_ENDPOINT,
};

/// Serializes every test in this module.
///
/// Cargo runs these on threads of one process and the environment is shared
/// across all of them, so a test that sets an endpoint and a test that asserts
/// none is set will disagree about the truth depending on scheduling. That is
/// not hypothetical — it showed up as a different number of failures on
/// consecutive runs. Every test takes this guard for its whole body.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// The variables every test in this module is allowed to disturb.
const MANAGED: [&str; 5] = [
    OTLP_ENDPOINT,
    OTLP_METRICS_ENDPOINT,
    OTLP_TRACES_ENDPOINT,
    OTLP_LOGS_ENDPOINT,
    METRIC_EXPORT_INTERVAL,
];

/// Holds [`ENV_LOCK`] and puts the environment back the way it was found.
///
/// Restoring matters because the process is shared: a developer running
/// `cargo test` with `OTEL_EXPORTER_OTLP_ENDPOINT` exported would otherwise have
/// it deleted out from under every later test in this binary, and the failure
/// would depend on test ordering.
struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<String>)>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in &self.saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

/// Take the lock, record the environment, and start from a known-empty one.
///
/// The guard is returned so it lives for the caller's body. Poisoning is
/// recovered rather than propagated: one test panicking should report itself,
/// not turn every later test into a confusing lock error.
fn exclusive_env() -> EnvGuard {
    let lock = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let saved = MANAGED
        .iter()
        .map(|name| (*name, std::env::var(name).ok()))
        .collect();
    clear_endpoints();
    EnvGuard { _lock: lock, saved }
}

/// Clear every OTLP endpoint variable this crate reads.
///
/// Tests cannot be trusted to start from a clean environment — a developer with
/// `OTEL_EXPORTER_OTLP_ENDPOINT` exported in their shell would otherwise see
/// different results than CI.
fn clear_endpoints() {
    for name in MANAGED {
        std::env::remove_var(name);
    }
}

#[test]
fn nothing_configured_installs_nothing_and_shuts_down_cleanly() {
    let _env = exclusive_env();

    let telemetry = builder("test-service")
        .build()
        .expect("building with no endpoint configured must succeed");

    assert!(
        !telemetry.is_enabled(),
        "no endpoint is configured, so no provider should be installed"
    );
    assert_eq!(telemetry.service_name(), "test-service");
    assert!(
        telemetry
            .trace_layer::<tracing_subscriber::Registry>()
            .is_none(),
        "traces are off, so there is no layer to compose"
    );
    assert!(
        telemetry
            .log_layer::<tracing_subscriber::Registry>()
            .is_none(),
        "logs are off, so there is no layer to compose"
    );

    telemetry
        .shutdown()
        .expect("shutting down a pipeline that installed nothing must succeed");
}

/// Signal selection, start to finish.
///
/// Grouped into one test rather than three because each step depends on the
/// environment left by the previous one; [`ENV_LOCK`] keeps it from colliding
/// with the others.
#[test]
fn endpoint_variables_select_signals() {
    let _env = exclusive_env();

    // Nothing set: no signal is enabled.
    assert!(!endpoint_configured(OTLP_METRICS_ENDPOINT));
    assert!(!endpoint_configured(OTLP_TRACES_ENDPOINT));
    assert!(!endpoint_configured(OTLP_LOGS_ENDPOINT));

    // The generic endpoint enables all three. This is the OTLP specification's
    // behaviour and the reason one variable is enough for the common case.
    std::env::set_var(OTLP_ENDPOINT, "http://localhost:4318");
    assert!(endpoint_configured(OTLP_METRICS_ENDPOINT));
    assert!(endpoint_configured(OTLP_TRACES_ENDPOINT));
    assert!(endpoint_configured(OTLP_LOGS_ENDPOINT));

    // Blank counts as unset, which is what keeps a compose stack that declares
    // the variable with a `${VAR:-}` default from installing exporters pointed
    // at nothing.
    std::env::set_var(OTLP_ENDPOINT, "");
    assert!(!endpoint_configured(OTLP_METRICS_ENDPOINT));
    std::env::set_var(OTLP_ENDPOINT, "   ");
    assert!(
        !endpoint_configured(OTLP_METRICS_ENDPOINT),
        "whitespace is blank too"
    );
    assert!(!env_is_nonempty(OTLP_ENDPOINT));

    // A signal-specific variable enables only its own signal.
    clear_endpoints();
    std::env::set_var(OTLP_TRACES_ENDPOINT, "http://localhost:4318/v1/traces");
    assert!(endpoint_configured(OTLP_TRACES_ENDPOINT));
    assert!(
        !endpoint_configured(OTLP_METRICS_ENDPOINT),
        "a traces-only endpoint must not enable metrics"
    );
    assert!(
        !endpoint_configured(OTLP_LOGS_ENDPOINT),
        "a traces-only endpoint must not enable logs"
    );

    clear_endpoints();
}

/// `without_*` wins over a configured endpoint.
///
/// All three are turned off so the test builds no exporter and opens no socket,
/// while still proving the override is consulted after the environment.
#[test]
fn explicit_opt_out_beats_a_configured_endpoint() {
    let _env = exclusive_env();
    std::env::set_var(OTLP_ENDPOINT, "http://localhost:4318");

    let telemetry = builder("test-service")
        .without_metrics()
        .without_traces()
        .without_logs()
        .build()
        .expect("building with every signal disabled must succeed");

    assert!(
        !telemetry.is_enabled(),
        "every signal was disabled explicitly, so the endpoint must be ignored"
    );

    clear_endpoints();
    telemetry.shutdown().expect("shutdown must succeed");
}

/// The layers compose into a registry the host built.
///
/// The assertion that matters is that this compiles: `trace_layer` and
/// `log_layer` are generic over the subscriber they compose into, and getting
/// those bounds wrong is the failure mode tier two has. `set_default` rather
/// than `set_global_default` so the guard drops at the end of the test and the
/// other tests in this binary are unaffected.
#[test]
fn layers_compose_into_a_host_owned_registry() {
    let _env = exclusive_env();

    let telemetry = builder("test-service").build().expect("build must succeed");

    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry.trace_layer())
        .with(telemetry.log_layer());

    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::info!("composed into a host-owned registry");

    telemetry.shutdown().expect("shutdown must succeed");
}
