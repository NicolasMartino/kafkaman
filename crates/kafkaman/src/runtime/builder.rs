//! Declaring what a service does, and turning that into a runtime.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kafkaman_config::Config;
use kafkaman_core::{DispatcherConfig, KafkaMessage, MessageDescriptor, PurgeConfig};
use kafkaman_rdkafka::{converge_topics, RdkafkaConsumer, RdkafkaPublisher, TopicAdmin};
use kafkaman_sqlx::{
    migrate, BeforeHandlerFuture, HandlerFuture, MessageRouter, MigrationContext, OutboxTable,
    ReceivedTable, ResolvedConfig, Role, RoleRegistry,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::context::{HandlerCtx, RuntimeContext};
use super::error::{BuildError, RuntimeError};
use super::subsystems::Subsystems;
use super::tasks::{BoxError, LoopFuture, RuntimeTasks};

/// Installs one registered handler into the router, once the config is resolved.
///
/// Handlers are declared before `ResolvedConfig` exists — that is the whole
/// point of a builder — but a handler context needs it. Deferring installation
/// keeps the typed closure at the call site while letting the context be built
/// from the config the runtime actually resolved.
type RouterInstaller = Box<dyn FnOnce(MessageRouter, &Arc<ResolvedConfig>) -> MessageRouter + Send>;

/// Starts the ingest loop for one consumed type, whose payload type is erased.
type IngestStarter = Box<
    dyn FnOnce(
            RdkafkaConsumer,
            PgPool,
            Arc<ResolvedConfig>,
            Duration,
            CancellationToken,
        ) -> LoopFuture
        + Send,
>;

struct ConsumedType {
    descriptor: MessageDescriptor,
    start_ingest: IngestStarter,
}

/// Declare what a service publishes, caches, and handles; get back a runtime.
///
/// # What the builder owns
///
/// Deriving kafkaman's own invariants from the declared roles: which tables to
/// create, which changesets identify them, which topics to converge, which loops
/// to start, and how to cancel and drain them. A service author should not have
/// to remember that publishing means an outbox table plus a relay, or that
/// caching means a received table *and* a cache table *and* two loops.
///
/// # What the host keeps
///
/// The Tokio runtime, the `PgPool` and its sizing, business schema, signal
/// handling, process exit, telemetry installation, config discovery, database
/// creation, and the *authority* over topic creation. The builder runs
/// convergence in exactly the mode the host's config selects; it never defaults
/// to `create`, never upgrades a mode, and exposes no knob of its own.
///
/// That list is normative rather than illustrative. A builder is precisely where
/// convenience creeps in, and every entry is something a well-meaning change
/// would add — a signal handler, a `database_url` shortcut, a
/// `tracing_subscriber` default, a config discovery fallback — while taking
/// something away from the caller.
///
/// # Example
///
/// ```no_run
/// # use kafkaman::RuntimeBuilder;
/// # async fn boot(
/// #     config: kafkaman::config::Config,
/// #     pool: sqlx::PgPool,
/// # ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct OrderSnapshot;
/// # impl kafkaman::KafkaMessage for OrderSnapshot {
/// #     const MESSAGE_TYPE: &'static str = "order_snapshot";
/// #     const TOPIC: &'static str = "orders";
/// #     fn entity_key(&self) -> String { String::new() }
/// # }
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct ProductSnapshot;
/// # impl kafkaman::KafkaMessage for ProductSnapshot {
/// #     const MESSAGE_TYPE: &'static str = "product_snapshot";
/// #     const TOPIC: &'static str = "products";
/// #     fn entity_key(&self) -> String { String::new() }
/// # }
/// let runtime = RuntimeBuilder::new()
///     .config(config)
///     .pool(pool)
///     .brokers("127.0.0.1:19092")
///     .consumer_group("order-service")
///     .publish::<OrderSnapshot>()
///     .cache::<ProductSnapshot>()
///     .build()
///     .await?;
///
/// let mut tasks = runtime.into_tasks()?;
/// let first = tasks.wait().await;
/// let drained = tasks.shutdown().await;
/// first.and(drained)?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct RuntimeBuilder {
    config: Option<Config>,
    pool: Option<PgPool>,
    brokers: Option<String>,
    consumer_group: Option<String>,
    migration_context: Option<MigrationContext>,
    subsystems: Subsystems,
    roles: RoleRegistry,
    installers: Vec<RouterInstaller>,
    consumed: BTreeMap<String, ConsumedType>,
    errors: Vec<BuildError>,
}

impl std::fmt::Debug for RuntimeBuilder {
    /// Closures have no useful representation; the declared roles are what a
    /// caller debugging a boot failure actually wants.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeBuilder")
            .field("roles", &self.roles)
            .field("brokers", &self.brokers)
            .field("consumer_group", &self.consumer_group)
            .field("subsystems", &self.subsystems)
            .field("errors", &self.errors.len())
            .finish_non_exhaustive()
    }
}

impl RuntimeBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The parsed `kafkaman.toml`. Required.
    ///
    /// There is deliberately no discovery fallback. `Config::discover()` walks
    /// up from the process's current directory, which makes it a host decision
    /// and makes it unable to serve two runtimes in one process.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// The pool the loops will share. Required.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// `bootstrap.servers`. Required.
    #[must_use]
    pub fn brokers(mut self, brokers: impl Into<String>) -> Self {
        self.brokers = Some(brokers.into());
        self
    }

    /// The Kafka consumer group. Required once anything is consumed.
    #[must_use]
    pub fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    /// Who is applying migrations, and under which contexts.
    ///
    /// Defaults to [`MigrationContext::from_env`], which reads
    /// `KAFKAMAN_CONTEXTS` and `KAFKAMAN_APPLIED_BY` — kafkaman's own env
    /// namespace, not a config file, so this is not the discovery fallback the
    /// host-owned list rules out.
    #[must_use]
    pub fn migration_context(mut self, ctx: MigrationContext) -> Self {
        self.migration_context = Some(ctx);
        self
    }

    /// Which runtime loops this process should start.
    ///
    /// Roles still decide the schema, topics, and table handles. Subsystems only
    /// decide the running topology. The default is [`Subsystems::all`];
    /// [`Subsystems::PURGE`] requires an explicit `[retention]` section before
    /// any purger loop is started, which is what makes that default safe.
    #[must_use]
    pub fn subsystems(mut self, subsystems: Subsystems) -> Self {
        self.subsystems = subsystems;
        self
    }

    /// This service produces `P`: an outbox table and a relay loop.
    #[must_use]
    pub fn publish<P: KafkaMessage>(mut self) -> Self {
        self.declare::<P>(Role::Publish);
        self
    }

    /// This service consumes `P` for its cache and nothing more.
    ///
    /// The most common role, and deliberately the least ceremonious: a received
    /// table, a cache table, an ingester, a dispatcher, and kafkaman's own no-op
    /// handler — so a cache-only type can never report a missing handler.
    #[must_use]
    pub fn cache<P>(mut self) -> Self
    where
        P: KafkaMessage + DeserializeOwned + Serialize + Send + Sync + 'static,
    {
        self.declare::<P>(Role::Cache);
        self.register_consumer::<P>();
        self.installers.push(Box::new(|router, _cfg| {
            router.handler::<P>(|_conn, _meta, _payload| Box::pin(async move { Ok(()) }))
        }));
        self
    }

    /// This service consumes `P` and derives from it *after* the cache upsert.
    ///
    /// The default handler position. The incoming record is already applied, so
    /// a handler recomputing state from its own cache sees the new value rather
    /// than one exactly one version stale.
    ///
    /// Not guaranteed to run once per received row: a record at or behind the
    /// entity's applied offset is `Ignored` and the handler is skipped. Auditing
    /// every delivery belongs in [`handle_before`](Self::handle_before).
    #[must_use]
    pub fn handle<P, F>(mut self, handler: F) -> Self
    where
        P: KafkaMessage + DeserializeOwned + Serialize + Send + Sync + 'static,
        F: for<'a> Fn(P, HandlerCtx<'a>) -> HandlerFuture<'a> + Send + Sync + 'static,
    {
        self.declare::<P>(Role::Handle);
        self.register_consumer::<P>();
        self.installers.push(Box::new(move |router, cfg| {
            let cfg = Arc::clone(cfg);
            router.handler::<P>(move |conn, meta, payload| {
                handler(payload, HandlerCtx::new(conn, Arc::clone(&cfg), meta))
            })
        }));
        self
    }

    /// This service consumes `P` and needs the entity's *previous* version.
    ///
    /// The explicit opt-in, because the pre-image is unrecoverable once the
    /// upsert lands. Returns a
    /// [`HandlerFlow`](kafkaman_sqlx::HandlerFlow) that may skip the post-upsert
    /// handler — never the upsert.
    ///
    /// Reach for it rarely. Change detection genuinely cannot recover the
    /// pre-image and auditing needs a hook the `Ignored` skip does not bypass,
    /// but almost everything else is expressible post-upsert, idempotently, at
    /// higher cost.
    #[must_use]
    pub fn handle_before<P, F>(mut self, handler: F) -> Self
    where
        P: KafkaMessage + DeserializeOwned + Serialize + Send + Sync + 'static,
        F: for<'a> Fn(P, HandlerCtx<'a>) -> BeforeHandlerFuture<'a> + Send + Sync + 'static,
    {
        self.declare::<P>(Role::HandleBefore);
        self.register_consumer::<P>();
        self.installers.push(Box::new(move |router, cfg| {
            let cfg = Arc::clone(cfg);
            router.handler_before::<P>(move |conn, meta, payload| {
                handler(payload, HandlerCtx::new(conn, Arc::clone(&cfg), meta))
            })
        }));
        self
    }

    /// Record a role, deferring the error so the chain stays fluent.
    fn declare<P: KafkaMessage>(&mut self, role: Role) {
        let declared = P::descriptor()
            .map_err(kafkaman_sqlx::Error::Core)
            .and_then(|descriptor| self.roles.declare(descriptor, role));
        if let Err(err) = declared {
            self.errors.push(BuildError::Roles(err));
        }
    }

    /// Remember how to start the ingest loop for `P`, once per type.
    ///
    /// `handle_before::<T>` plus `handle::<T>` is a legal pair, and both consume
    /// the same topic; a second ingester would be a second consumer in the same
    /// group competing for the same partitions.
    fn register_consumer<P>(&mut self)
    where
        P: KafkaMessage + DeserializeOwned + Serialize + Send + Sync + 'static,
    {
        let Ok(descriptor) = P::descriptor() else {
            // The descriptor error is already recorded by `declare`, which every
            // caller of this runs first.
            return;
        };
        let message_type = descriptor.message_type.as_str().to_owned();
        if self.consumed.contains_key(&message_type) {
            return;
        }

        self.consumed.insert(
            message_type,
            ConsumedType {
                descriptor,
                start_ingest: Box::new(
                    |consumer: RdkafkaConsumer, pool, cfg, retry_delay, shutdown| {
                        Box::pin(async move {
                            consumer
                                .run_ingester::<P>(&pool, &cfg, retry_delay, shutdown)
                                .await
                                .map(|_stats| ())
                                .map_err(|err| Box::new(err) as BoxError)
                        })
                    },
                ),
            },
        );
    }

    /// Validate, converge topics, migrate, and assemble — without starting a
    /// single loop.
    ///
    /// Loop construction is deferred to [`Runtime::into_tasks`] so `build()`
    /// stays callable from tests that never run one, and so the point at which
    /// telemetry instruments would bind is adjacent to a call the host writes
    /// rather than hidden inside this one.
    ///
    /// Ordering is deliberate. Every purely local check runs before any I/O, and
    /// topic convergence runs before the migration: boot is the last moment at
    /// which refusing to start is still cheap, and there is no point creating
    /// tables for a topic this service is going to refuse.
    pub async fn build(self) -> Result<Runtime, BuildError> {
        // Reports the first role error rather than all of them. Conflicts arrive
        // one at a time in practice, and a caller fixing one line at a time is
        // better served by a single unambiguous message.
        if let Some(error) = self.errors.into_iter().next() {
            return Err(error);
        }

        // Declaration mistakes are reported before missing host wiring, and the
        // order is deliberate. A caller who declared conflicting roles *and*
        // forgot `.pool(..)` has two problems, but only one of them is about
        // what their service does; being told about the pool first would send
        // them to fix the easy half and rediscover the real one on the next run.
        if self.roles.is_empty() {
            return Err(BuildError::NoRoles);
        }
        let consumer_group = match self.consumed.values().next() {
            // Dispatch-only and migration-only workers need the consumed tables,
            // handlers, and topics, but they never join Kafka. Requiring a
            // group for them would make the topology selector cosmetic rather
            // than operational.
            Some(_) if !self.subsystems.contains(Subsystems::INGEST) => self.consumer_group,
            Some(consumed) => {
                Some(
                    self.consumer_group
                        .ok_or_else(|| BuildError::MissingConsumerGroup {
                            message_type: consumed.descriptor.message_type.as_str().to_owned(),
                        })?,
                )
            }
            // A publish-only runtime never joins a group, and demanding one
            // would make the simplest possible service supply a value with
            // nothing to name.
            None => self.consumer_group,
        };

        let config = self.config.ok_or(BuildError::MissingConfig)?;
        let pool = self.pool.ok_or(BuildError::MissingPool)?;
        let brokers = self.brokers.ok_or(BuildError::MissingBrokers)?;

        let retention = config
            .retention()
            .map_err(|err| BuildError::Config(err.into()))?
            .map(kafkaman_config::RetentionSection::into_purge_config)
            .transpose()
            .map_err(BuildError::Retention)?;

        let cfg = ResolvedConfig::from_config(Some(&config), self.roles.descriptors())
            .map_err(BuildError::Config)?;

        // Every registered type is checked, inbound and outbound alike: a topic
        // carrying entity snapshots must be compacted whichever end of it this
        // service is on, or the entities cannot be rebuilt from it — and that
        // failure is invisible until the day someone needs the rebuild.
        //
        // A partition count that disagrees with the broker is warned about
        // inside `converge_topics` rather than returned, deliberately: failing
        // would turn an intentional, already-completed repartition into a
        // fleet-wide boot failure long after the migration succeeded.
        let admin = TopicAdmin::from_brokers(&brokers).map_err(|source| BuildError::Transport {
            brokers: brokers.clone(),
            source,
        })?;
        converge_topics(&admin, cfg.topics, cfg.messages())
            .await
            .map_err(BuildError::Topics)?;

        let changelog = self.roles.changelog().map_err(BuildError::Roles)?;
        let migration_context = self
            .migration_context
            .unwrap_or_else(MigrationContext::from_env);
        let report = migrate(&pool, &cfg, &migration_context, &changelog)
            .await
            .map_err(BuildError::Migrate)?;
        tracing::info!(
            applied = report.applied_count(),
            "kafkaman schema converged"
        );

        let cfg = Arc::new(cfg);
        let mut router = MessageRouter::new();
        for install in self.installers {
            router = install(router, &cfg);
        }

        let mut published = Vec::new();
        for descriptor in self.roles.published() {
            published.push(table_for(&cfg, descriptor, OutboxTable::for_descriptor)?);
        }

        let mut consumed = Vec::new();
        for (_, entry) in self.consumed {
            // `for_descriptor`, not `new`: it is what attaches this message
            // type's resolved retry policy to the table the dispatcher fails
            // rows against. `new` fills in `RetryPolicy::default()`, which threw
            // away `max_attempts`, `initial_backoff`, `max_backoff`,
            // `multiplier`, `errors_limit` and `dlq` from the config file — and
            // silently, because a default policy retries perfectly well, just
            // not the way the operator asked.
            let received = table_for(&cfg, &entry.descriptor, ReceivedTable::for_descriptor)?;
            consumed.push(ConsumedPlan {
                message_type: entry.descriptor.message_type.as_str().to_owned(),
                topic: entry.descriptor.topic.clone(),
                received,
                start_ingest: entry.start_ingest,
            });
        }

        Ok(Runtime {
            context: RuntimeContext::new(pool, cfg),
            brokers,
            consumer_group,
            retention,
            subsystems: self.subsystems,
            router,
            published,
            consumed,
        })
    }
}

/// Resolve one table, attributing the failure to the message type that caused it.
///
/// `build` takes the whole `ResolvedConfig` rather than just the schema, because
/// a `ReceivedTable` carries its message type's resolved retry policy and
/// `ReceivedTable::new` substitutes the library defaults for it. Handing the
/// constructor a schema was enough to make the shorter one fit, and a builder-
/// booted service therefore ignored every `[retry]` key in its own config file.
fn table_for<T>(
    cfg: &ResolvedConfig,
    descriptor: &MessageDescriptor,
    build: impl Fn(&ResolvedConfig, MessageDescriptor) -> kafkaman_sqlx::Result<T>,
) -> Result<T, BuildError> {
    build(cfg, descriptor.clone()).map_err(|source| BuildError::Table {
        message_type: descriptor.message_type.as_str().to_owned(),
        source,
    })
}

struct ConsumedPlan {
    /// Carried explicitly: `ReceivedTable` exposes no message type, and an error
    /// should name what the caller wrote rather than a derived table name.
    message_type: String,
    topic: String,
    received: ReceivedTable,
    start_ingest: IngestStarter,
}

/// An assembled runtime: migrated, converged, and not yet running.
pub struct Runtime {
    context: RuntimeContext,
    brokers: String,
    consumer_group: Option<String>,
    retention: Option<PurgeConfig>,
    subsystems: Subsystems,
    router: MessageRouter,
    published: Vec<OutboxTable>,
    consumed: Vec<ConsumedPlan>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("brokers", &self.brokers)
            .field("consumer_group", &self.consumer_group)
            .field("subsystems", &self.subsystems)
            .field("published", &self.published.len())
            .field("consumed", &self.consumed.len())
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

impl Runtime {
    /// The pool, resolved config, and typed table accessors.
    #[must_use]
    pub fn context(&self) -> &RuntimeContext {
        &self.context
    }

    /// The router these roles produced.
    ///
    /// An ordinary [`MessageRouter`], deliberately: a future wrapping hook needs
    /// no change to roles, and a host that wants to inspect or wrap what it
    /// declared can.
    #[must_use]
    pub fn router(&self) -> &MessageRouter {
        &self.router
    }

    /// Construct every loop and start it, under a fresh cancellation token.
    ///
    /// The default loop set is one relay per published type, one ingester and
    /// one dispatcher per consumed type, a purger when `[retention]` is
    /// configured, and — under the `metrics` feature, when this runtime owns any
    /// outbox or received table — a single queue-depth sampler covering all of
    /// them. [`RuntimeBuilder::subsystems`] can narrow that set for worker-role
    /// binaries. Count on the roles and subsystems you declared, not on a fixed
    /// number: the sampler in particular appears or not depending on a feature.
    pub fn into_tasks(self) -> Result<RuntimeTasks, BuildError> {
        self.into_tasks_with(CancellationToken::new())
    }

    /// Construct every loop and start it, under a token the host owns.
    ///
    /// Use this to compose the runtime's drain with something else — an HTTP
    /// server's graceful shutdown, or a signal handler the host installed.
    ///
    /// See [`into_tasks`](Self::into_tasks) for which loops are assembled.
    pub fn into_tasks_with(self, shutdown: CancellationToken) -> Result<RuntimeTasks, BuildError> {
        let pool = self.context.pool().clone();
        let cfg = Arc::clone(self.context.config());
        let subsystems = self.subsystems;
        let mut loops: Vec<(String, LoopFuture)> = Vec::new();

        // Cloned before the role loops below take ownership of the originals.
        //
        // Gated with the sampler they feed: `run_queue_metrics` lives behind
        // kafkaman-worker's `metrics` feature, and cloning two vectors for a loop
        // that will not be built is waste the compiler would warn about.
        //
        // The queue sampler is derived here rather than left to the host because
        // it takes `OutboxTable` and `ReceivedTable` handles, and naming those is
        // exactly what `tests/distributed-cache/tests/boot_surface.rs` forbids a
        // blessed boot file from doing. A host that wanted queue depth could not
        // obtain it without giving up the property the builder exists to provide,
        // which is why the example ran without these gauges for as long as it did.
        #[cfg(feature = "metrics")]
        let queue_outbox: Vec<OutboxTable> = if subsystems.contains(Subsystems::QUEUE_METRICS) {
            self.published.clone()
        } else {
            Vec::new()
        };
        #[cfg(feature = "metrics")]
        let queue_received: Vec<ReceivedTable> = if subsystems.contains(Subsystems::QUEUE_METRICS) {
            self.consumed
                .iter()
                .map(|plan| plan.received.clone())
                .collect()
        } else {
            Vec::new()
        };

        for outbox in self.published {
            let message_type = outbox.descriptor.message_type.as_str().to_owned();
            let retention = self.retention.clone();
            let purge_table = outbox.clone();

            if subsystems.contains(Subsystems::RELAY) {
                let publisher =
                    RdkafkaPublisher::from_brokers(&self.brokers).map_err(|source| {
                        BuildError::Transport {
                            brokers: self.brokers.clone(),
                            source,
                        }
                    })?;
                let relay_cfg = cfg.relay.clone();
                let pool = pool.clone();
                let shutdown = shutdown.clone();
                loops.push((
                    format!("relay:{message_type}"),
                    Box::pin(async move {
                        kafkaman_worker::run(pool, publisher, outbox, relay_cfg, shutdown)
                            .await
                            .map_err(|err| Box::new(err) as BoxError)
                    }),
                ));
            }

            // Only when `[retention]` is configured. Absent means nothing is
            // deleted, which is the status quo and the safe default.
            if subsystems.contains(Subsystems::PURGE) {
                if let Some(purge_cfg) = retention {
                    let pool = pool.clone();
                    let shutdown = shutdown.clone();
                    loops.push((
                        format!("purger:{message_type}"),
                        Box::pin(async move {
                            kafkaman_worker::run_purger(pool, purge_table, purge_cfg, shutdown)
                                .await
                                .map_err(|err| Box::new(err) as BoxError)
                        }),
                    ));
                }
            }
        }

        for consumed in self.consumed {
            if subsystems.contains(Subsystems::INGEST) {
                let group = self.consumer_group.as_deref().ok_or_else(|| {
                    BuildError::MissingConsumerGroup {
                        message_type: consumed.message_type.clone(),
                    }
                })?;
                let consumer =
                    RdkafkaConsumer::from_brokers(&self.brokers, group).map_err(|source| {
                        BuildError::Transport {
                            brokers: self.brokers.clone(),
                            source,
                        }
                    })?;
                consumer
                    .subscribe(&[consumed.topic.as_str()])
                    .map_err(|source| BuildError::Transport {
                        brokers: self.brokers.clone(),
                        source,
                    })?;

                loops.push((
                    format!("ingester:{}", consumed.message_type),
                    (consumed.start_ingest)(
                        consumer,
                        pool.clone(),
                        Arc::clone(&cfg),
                        cfg.relay.retry_after,
                        shutdown.clone(),
                    ),
                ));
            }

            if subsystems.contains(Subsystems::DISPATCH) {
                let pool = pool.clone();
                let router = self.router.clone();
                // Pacing and the panic breaker come from `[dispatcher]`; the
                // lifecycle policy is per message type, so it is layered on here
                // rather than resolved once for every dispatcher.
                let dispatcher_cfg = DispatcherConfig {
                    lifecycle: cfg
                        .observability
                        .policy_for(&consumed.message_type)
                        .lifecycle_emission(),
                    ..cfg.dispatcher.clone()
                };
                let received = consumed.received;
                let shutdown = shutdown.clone();
                loops.push((
                    format!("dispatcher:{}", consumed.message_type),
                    Box::pin(async move {
                        kafkaman_worker::run_dispatcher(
                            pool,
                            received,
                            router,
                            dispatcher_cfg,
                            shutdown,
                        )
                        .await
                        .map_err(|err| Box::new(err) as BoxError)
                    }),
                ));
            }
        }

        // Queue depth and age, sampled once for every table this runtime derived.
        //
        // Behind `metrics` because the sampler is: a build with the feature off
        // has no gauges to feed and must not pay for the Postgres queries.
        //
        // Skipped when the runtime declared no roles at all, so a runtime with
        // nothing to sample does not open a Postgres connection every interval to
        // ask about no tables.
        #[cfg(feature = "metrics")]
        if subsystems.contains(Subsystems::QUEUE_METRICS)
            && (!queue_outbox.is_empty() || !queue_received.is_empty())
        {
            // `pool` is not cloned: this is its last use, and the sampler is the
            // final loop assembled.
            let shutdown = shutdown.clone();
            // `max_queue_age` is shared with the inspection routes deliberately:
            // the flag it sets means the same thing in both places, and two
            // spellings of one threshold is one too many.
            let queue_cfg = kafkaman_worker::QueueMetricsConfig {
                max_queue_age: cfg.observability.defaults.max_queue_age,
                ..Default::default()
            };
            loops.push((
                "queue-metrics".to_owned(),
                Box::pin(async move {
                    match kafkaman_worker::run_queue_metrics(
                        pool,
                        queue_outbox,
                        queue_received,
                        queue_cfg,
                        shutdown.clone(),
                    )
                    .await
                    {
                        Ok(()) => Ok(()),
                        // The gauges register process-wide, so a second runtime in
                        // one process cannot have its own sampler — which is the
                        // case `tests/distributed-cache` creates by starting both
                        // example services in one test binary. The first runtime's
                        // series stay correct and keep exporting; only this
                        // runtime's tables go uncovered. Failing here would turn a
                        // reduction in telemetry into an outage, so the loop parks
                        // until shutdown like any other.
                        Err(kafkaman_worker::Error::QueueMetricsAlreadyRunning) => {
                            tracing::warn!(
                                "another kafkaman runtime in this process is already sampling \
                                 queue depth; this runtime's outbox and received tables are not \
                                 covered by the queue gauges"
                            );
                            shutdown.cancelled().await;
                            Ok(())
                        }
                        Err(err) => Err(Box::new(err) as BoxError),
                    }
                }),
            ));
        }

        Ok(RuntimeTasks::spawn(shutdown, loops))
    }

    /// Start every loop, wait for the first to exit, then drain.
    ///
    /// Returns rather than exiting the process. What a dead relay means is the
    /// host's decision, and a library that called `std::process::exit` would be
    /// taking it.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), RuntimeError> {
        let mut tasks = self
            .into_tasks_with(shutdown)
            .map_err(RuntimeError::Build)?;
        let first = tasks.wait().await;
        let drained = tasks.shutdown().await;
        first.and(drained)
    }
}
