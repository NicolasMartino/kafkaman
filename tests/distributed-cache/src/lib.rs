//! Test-support crate: assertion helpers legitimately panic, so the workspace's
//! no-panic lints are relaxed here.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Scaffolding for the two-service distributed-cache test.
//!
//! # What this fixture is for
//!
//! Every other test in the workspace reaches the library through
//! `kafkaman_test::Harness` and asserts against the database. That is the right
//! altitude for library behaviour and the wrong one for *wiring*: a dispatcher
//! that is never spawned, a `CreateCacheTable` missing from a changelog, or a
//! topic that does not match passes every one of those tests. This fixture
//! starts both example services the way their binaries do and drives them
//! over HTTP only.
//!
//! # What it deliberately does not cover
//!
//! It calls `example_order::start` and `example_product::start_with` rather than
//! running the binaries, so it never executes `main.rs` — which is where
//! `kafkaman_otel::init` is called, the signal is handled, and
//! `Telemetry::shutdown()` is sequenced after the drain. Telemetry assertions
//! therefore do not belong here, and adding them would make this suite worse:
//! both services run in one process, OpenTelemetry subscribers and providers are
//! process-global, and installing them here would test a topology no operator
//! runs. `tests/example-telemetry` owns that proof and runs the binaries as
//! child processes.
//!
//! # Container ownership
//!
//! Testcontainers cleans up through `ContainerAsync`'s `Drop`, and a value in a
//! `static` never drops at process exit — which leaks a container per binary,
//! per run, forever. So the containers are owned by [`Cluster`], which the test
//! holds until it is done, and the labels exist only as a manual fallback for
//! runs that were interrupted before `Drop` could fire.

use std::error::Error as StdError;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use kafkaman::config::Config;
use serde::de::DeserializeOwned;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

pub type BoxError = Box<dyn StdError + Send + Sync>;
pub type TestResult<T = ()> = Result<T, BoxError>;

pub const CONTAINER_LABEL_PROJECT: &str = "com.kafkaman.project";
pub const CONTAINER_LABEL_PROJECT_VALUE: &str = "kafkaman";
pub const CONTAINER_LABEL_MANAGED_BY: &str = "com.kafkaman.managed-by";
pub const CONTAINER_LABEL_MANAGED_BY_VALUE: &str = "testcontainers";
pub const CONTAINER_LABEL_SUITE: &str = "com.kafkaman.test-suite";
pub const CONTAINER_LABEL_SUITE_VALUE: &str = "distributed-cache";
pub const CONTAINER_LABEL_SERVICE: &str = "com.kafkaman.test-service";

/// The infrastructure both services share: one PostgreSQL server holding a
/// database per service, and one broker.
#[derive(Debug)]
pub struct Cluster {
    _postgres: ContainerAsync<GenericImage>,
    _redpanda: ContainerAsync<GenericImage>,
    postgres_base_url: String,
    brokers: String,
}

impl Cluster {
    /// Start PostgreSQL and Redpanda.
    pub async fn start() -> TestResult<Self> {
        let (postgres, postgres_base_url) = start_postgres().await?;
        let (redpanda, brokers) = start_redpanda().await?;
        Ok(Self {
            _postgres: postgres,
            _redpanda: redpanda,
            postgres_base_url,
            brokers,
        })
    }

    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Create a database and return its connection URL.
    ///
    /// **Two databases, not two schemas.** Neither service can read the other's
    /// tables even by accident, so "distributed" is enforced by the connection
    /// string rather than asserted in a comment. It costs nothing over two
    /// schemas in one database.
    ///
    /// Goes through `example_provision` rather than issuing its own
    /// `CREATE DATABASE`, so the name validation and duplicate handling this
    /// test relies on are the ones the shipped binary actually performs.
    pub async fn create_database(&self, name: &str) -> TestResult<String> {
        example_provision::ensure_databases(&self.admin_url(), &[name.to_owned()]).await?;
        Ok(format!("{}/{name}", self.postgres_base_url))
    }

    /// Create the entity topics, exactly as `examples/provision` does.
    ///
    /// Separate from [`Cluster::start`] so a test can deliberately *not* call
    /// it and observe what a service does against an unprovisioned broker —
    /// which, since the services verify their topics at boot, is the whole
    /// point of having a provisioner.
    pub async fn provision_topics(&self) -> TestResult {
        let topics = example_provision::entity_topics(example_provision::DEFAULT_PARTITIONS)?;
        example_provision::provision_topics(self.brokers(), &topics).await?;
        Ok(())
    }

    /// A connection string for the server's own `postgres` database.
    ///
    /// `CREATE DATABASE` needs a session, and it cannot be on the database
    /// being created.
    pub fn admin_url(&self) -> String {
        format!("{}/postgres", self.postgres_base_url)
    }
}

async fn start_postgres() -> TestResult<(ContainerAsync<GenericImage>, String)> {
    let port: ContainerPort = 5432.tcp();
    let container = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(port)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_label(CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE)
        .with_label(CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE)
        .with_label(CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE)
        .with_label(CONTAINER_LABEL_SERVICE, "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await?;
    let host = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(port).await?;
    Ok((
        container,
        format!("postgres://postgres:postgres@{host}:{host_port}"),
    ))
}

/// Start Redpanda on a port picked at runtime.
///
/// Redpanda has to advertise an address the host-side client can dial, so the
/// container port and the host port must be the same number — which is why the
/// existing full-loop test hardcodes one. A fixed port is safe only for a single
/// test binary; `cargo test --workspace` runs several at once, and the
/// process-global mutex that guards the other one cannot see across processes.
/// Reserving an ephemeral port from the OS and mapping *that* removes the
/// collision instead of serializing around it.
async fn start_redpanda() -> TestResult<(ContainerAsync<GenericImage>, String)> {
    let port = reserve_port().await?;
    let advertised = format!("PLAINTEXT://127.0.0.1:{port}");
    let listener = format!("PLAINTEXT://0.0.0.0:{port}");
    let container = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v24.2.7")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda!"))
        .with_label(CONTAINER_LABEL_PROJECT, CONTAINER_LABEL_PROJECT_VALUE)
        .with_label(CONTAINER_LABEL_MANAGED_BY, CONTAINER_LABEL_MANAGED_BY_VALUE)
        .with_label(CONTAINER_LABEL_SUITE, CONTAINER_LABEL_SUITE_VALUE)
        .with_label(CONTAINER_LABEL_SERVICE, "redpanda")
        .with_mapped_port(port, port.tcp())
        .with_startup_timeout(Duration::from_secs(180))
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
    Ok((container, format!("127.0.0.1:{port}")))
}

/// Ask the OS for a free port and release it immediately.
///
/// A race remains in principle — nothing stops another process taking the port
/// between the bind and the container's — but it is a far smaller window than a
/// constant that is guaranteed to collide with the other Redpanda test.
async fn reserve_port() -> TestResult<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn ephemeral() -> SocketAddr {
    // Port 0: the OS picks. Two services in one process, and several test
    // binaries in one `cargo test`, would otherwise have to agree on numbers.
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// The repository-relative path of a service's `kafkaman.toml`.
fn service_config_path(service: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(service)
        .join("kafkaman.toml")
}

/// Load a service's real `kafkaman.toml` from disk.
///
/// `Config::discover()` walks up from the process's current directory, so it
/// cannot serve two services running in one process — each package's own tests
/// cover that path instead, where the current directory *is* the package root.
/// What matters here is that this is the same file the binary reads, not a
/// string literal: a config that only ever exists inside a test is a config that
/// silently stops matching the one shipped.
pub fn service_config(service: &str) -> TestResult<Config> {
    let path = service_config_path(service);
    Ok(Config::from_path(&path)?)
}

/// Both example services, running against their own databases and a shared
/// broker, with an HTTP client for each.
#[derive(Debug)]
pub struct Services {
    pub product: example_product::RunningService,
    pub order: example_order::RunningService,
    pub http: reqwest::Client,
}

impl Services {
    /// Start `product` and `order` exactly as their binaries do.
    pub async fn start(cluster: &Cluster) -> TestResult<Self> {
        Self::start_with(cluster, example_product::BootMode::Builder).await
    }

    /// Start both services, booting `product` through the named path.
    ///
    /// `product` boots two ways — declared roles through `RuntimeBuilder`, or
    /// hand-assembled from the low-level primitives — and the suite runs against
    /// both. That is what makes "the escape hatch still produces an equivalent
    /// runtime" a tested claim rather than a documented intention.
    ///
    /// Only `product` is parameterised. `order` consumes with `cache::<T>()`,
    /// which is trivially equivalent to a hand-registered no-op handler and
    /// would prove nothing; `product` derives state, republishes inside the
    /// dispatch transaction, and is where an inequivalence would actually show.
    pub async fn start_with(
        cluster: &Cluster,
        product_boot: example_product::BootMode,
    ) -> TestResult<Self> {
        // Provision before either service, mirroring the ordering
        // `examples/compose.yaml` enforces with
        // `condition: service_completed_successfully`. Both services verify
        // their topics at boot and refuse to start on a missing one, so this is
        // a prerequisite rather than a convenience — and putting it here means
        // the test would notice if that ordering requirement ever went away.
        cluster.provision_topics().await?;

        // Unique per run so a repeated test against a reused server cannot
        // inherit converged state and pass for the wrong reason.
        let run = Uuid::new_v4().simple().to_string();
        let product_db = cluster.create_database(&format!("product_{run}")).await?;
        let order_db = cluster.create_database(&format!("order_{run}")).await?;

        let product = example_product::start_with(
            product_boot,
            example_product::ServiceOptions {
                database_url: product_db,
                brokers: cluster.brokers().to_owned(),
                bind: ephemeral(),
                consumer_group: format!("product-service-{run}"),
                config: Some(service_config("product")?),
            },
        )
        .await?;

        let order = example_order::start(example_order::ServiceOptions {
            database_url: order_db,
            brokers: cluster.brokers().to_owned(),
            bind: ephemeral(),
            consumer_group: format!("order-service-{run}"),
            config: Some(service_config("order")?),
        })
        .await?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            product,
            order,
            http,
        })
    }

    pub fn product_url(&self, path: &str) -> String {
        format!("{}{path}", self.product.base_url())
    }

    pub fn order_url(&self, path: &str) -> String {
        format!("{}{path}", self.order.base_url())
    }

    pub async fn shutdown(self) -> TestResult {
        // `order` first: it is the one holding a cache of what `product`
        // publishes, so stopping the producer first would leave its ingester
        // waiting on a broker connection that is going away.
        self.order.shutdown().await?;
        self.product.shutdown().await?;
        Ok(())
    }
}

/// A JSON response together with its status, so a test can assert on a rejection
/// without unwrapping a body that is not there.
#[derive(Debug)]
pub struct HttpResponse {
    pub status: reqwest::StatusCode,
    pub body: serde_json::Value,
}

impl HttpResponse {
    pub fn json<T: DeserializeOwned>(&self) -> TestResult<T> {
        Ok(serde_json::from_value(self.body.clone())?)
    }

    pub fn error_message(&self) -> String {
        self.body
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }
}

pub async fn post(
    client: &reqwest::Client,
    url: &str,
    body: serde_json::Value,
) -> TestResult<HttpResponse> {
    let response = client.post(url).json(&body).send().await?;
    into_response(response).await
}

pub async fn post_empty(client: &reqwest::Client, url: &str) -> TestResult<HttpResponse> {
    let response = client.post(url).send().await?;
    into_response(response).await
}

pub async fn get(client: &reqwest::Client, url: &str) -> TestResult<HttpResponse> {
    let response = client.get(url).send().await?;
    into_response(response).await
}

async fn into_response(response: reqwest::Response) -> TestResult<HttpResponse> {
    let status = response.status();
    let text = response.text().await?;
    let body = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    Ok(HttpResponse { status, body })
}
