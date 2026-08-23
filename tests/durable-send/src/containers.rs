use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::TestResult;

/// Labels every container this suite starts carries, so a stray one can be
/// found and reaped by project, suite, and service rather than by guessing at
/// image names.
pub const CONTAINER_LABEL_PROJECT: &str = "com.kafkaman.project";
pub const CONTAINER_LABEL_PROJECT_VALUE: &str = "kafkaman";
pub const CONTAINER_LABEL_MANAGED_BY: &str = "com.kafkaman.managed-by";
pub const CONTAINER_LABEL_MANAGED_BY_VALUE: &str = "testcontainers";
pub const CONTAINER_LABEL_SUITE: &str = "com.kafkaman.test-suite";
pub const CONTAINER_LABEL_SUITE_VALUE: &str = "durable-send";
pub const CONTAINER_LABEL_SERVICE: &str = "com.kafkaman.test-service";

/// Owns a PostgreSQL testcontainer for one test.
///
/// The container stops when this drops, so it must outlive every pool connected
/// to it.
#[derive(Debug)]
pub struct TestPostgres {
    _container: ContainerAsync<GenericImage>,
    url: String,
}

impl TestPostgres {
    /// The connection URL, borrowed so the owning guard cannot be dropped while
    /// it is in use.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }
}

/// Start an owned PostgreSQL testcontainer for this suite.
pub async fn postgres() -> TestResult<TestPostgres> {
    postgres_for_suite(CONTAINER_LABEL_SUITE_VALUE).await
}

/// Start an owned PostgreSQL testcontainer labelled for a named suite.
///
/// The label is what lets a stray container be traced back to the binary that
/// started it, so a second suite reusing this helper has to say which one it is
/// rather than inheriting a name that would point at the wrong tests.
pub async fn postgres_for_suite(suite: &str) -> TestResult<TestPostgres> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_label(CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE)
        .with_label(CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE)
        .with_label(CONTAINER_LABEL_SUITE, suite)
        .with_label(CONTAINER_LABEL_SERVICE, "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await?;

    let host = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(port).await?;
    let url = format!("postgres://postgres:postgres@{host}:{host_port}/postgres");

    Ok(TestPostgres {
        _container: container,
        url,
    })
}

/// Start Redpanda on a host port chosen before the container exists.
///
/// The port has to be known in advance because `--advertise-kafka-addr` is baked
/// into the command line, and a client told to connect to an address the broker
/// cannot actually be reached on hangs rather than fails. Choosing a free port
/// means binding it and letting go, which leaves a window where something else
/// can take it — so a lost race is retried rather than reported. The window is
/// small, but on a busy CI runner "small" is not "never", and this used to
/// surface as an unexplained flake.
pub async fn redpanda() -> TestResult<(ContainerAsync<GenericImage>, String)> {
    const ATTEMPTS: usize = 5;

    let mut last_error: Option<crate::BoxError> = None;
    for _ in 0..ATTEMPTS {
        let port = available_host_port()?;
        match start_redpanda_on(port).await {
            Ok(started) => return Ok(started),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| "redpanda did not start".into()))
}

async fn start_redpanda_on(kafka_port: u16) -> TestResult<(ContainerAsync<GenericImage>, String)> {
    let advertised = format!("PLAINTEXT://127.0.0.1:{kafka_port}");
    let listener = format!("PLAINTEXT://0.0.0.0:{kafka_port}");
    let container = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.7")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
        .with_label(CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE)
        .with_label(CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE)
        .with_label(CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE)
        .with_label(CONTAINER_LABEL_SERVICE, "redpanda")
        .with_mapped_port(kafka_port, kafka_port.tcp())
        .with_startup_timeout(std::time::Duration::from_secs(120))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--kafka-addr",
            &listener,
            "--advertise-kafka-addr",
            &advertised,
        ])
        .start()
        .await?;

    Ok((container, format!("127.0.0.1:{kafka_port}")))
}

/// A host port nothing is listening on right now.
///
/// "Right now" is the caveat [`redpanda`] retries around.
fn available_host_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
