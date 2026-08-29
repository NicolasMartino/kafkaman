//! Unit tests, split to mirror the module they cover.

mod config_file;
mod dispatcher;
mod duration;
mod observability;
mod retention;
mod retry;
mod schema;
mod topics;

/// A minimal config satisfying every required key, so a test can vary only the
/// thing it is about.
pub(crate) const VALID: &str = r#"
    [database]
    schema = "kafkaman"

    [relay]
    worker_id = "worker-a"
    batch_limit = 25
    lease_for = "30s"
    retry_after = "500ms"
    poll_interval = "250ms"

    [retry.defaults]
    max_attempts = 5
    initial_backoff = "100ms"
    max_backoff = "30s"
    multiplier = 2.0
    errors_limit = 16
    dlq = "table"

    [retry.messages.order_created]
    max_attempts = 7
    initial_backoff = "250ms"
"#;
