//! Rebuilding outgoing snapshots and returning dead-lettered rows to dispatch,
//! for a service that declares its sources once.
//!
//! A [`Source`] is either a resync (keys plus a loader of current state) or a
//! received table to redrive. [`Recovery::request_from`] checks a run's inputs
//! against the declared list and the `[recovery]` section before any I/O;
//! [`Recovery::run`] prepares storage, runs the selected sources in order and
//! stops at the first failure. No background task is started.
//!
//! Events go to the `kafkaman` target and carry the run id, operation and
//! source. The host owns the subscriber and the process exit.
//!
//! ```no_run
//! # use kafkaman::{Envelope, KafkaMessage};
//! # use kafkaman::recovery::{Recovery, RecoveryInputs, Source};
//! # #[derive(serde::Serialize)]
//! # struct Snapshot;
//! # impl KafkaMessage for Snapshot {
//! # const MESSAGE_TYPE: &'static str = "snapshot";
//! # const TOPIC: &'static str = "snapshots";
//! # fn entity_key(&self) -> String { "entity".into() }
//! # }
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct Order;
//! # impl KafkaMessage for Order {
//! # const MESSAGE_TYPE: &'static str = "order";
//! # const TOPIC: &'static str = "orders";
//! # fn entity_key(&self) -> String { "order".into() }
//! # }
//! # async fn load(_: &mut sqlx::PgConnection, _: &str, _: uuid::Uuid)
//! # -> kafkaman::sqlx::Result<Option<Envelope<Snapshot>>> { Ok(None) }
//! # async fn example(pool: &sqlx::PgPool, file: &str, brokers: Option<&str>)
//! # -> Result<(), Box<dyn std::error::Error>> {
//! let mut recovery = Recovery::new(vec![
//!     Source::resync::<Snapshot, _, _>(
//!         "snapshots",
//!         |conn, after, limit| Box::pin(async move {
//!             Ok(sqlx::query_scalar(
//!                 "SELECT id FROM entities WHERE ($1::text IS NULL OR id COLLATE \"C\" > $1) \
//!                  ORDER BY id COLLATE \"C\" LIMIT $2"
//!             ).bind(after).bind(limit as i64).fetch_all(conn).await?)
//!         }),
//!         |conn, key, run_id| Box::pin(load(conn, key, run_id)),
//!     ),
//!     Source::received::<Order>("orders"),
//! ])?;
//! let config = kafkaman::config::Config::parse(file)?;
//! let inputs = RecoveryInputs {
//!     source: Some("snapshots".into()),
//!     start_after: Some("entity-0042".into()),
//!     ..RecoveryInputs::default()
//! };
//! let request = recovery.request_from(&config, inputs)?;
//! let report = recovery
//!     .run(pool, &config, brokers, &Default::default(), request)
//!     .await?;
//! println!("run {} completed {} sources", report.run_id, report.completed.len());
//! # Ok(()) }
//! ```

use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafkaman_config::{Config, ConfigError};
use kafkaman_core::{Envelope, KafkaMessage, MessageDescriptor, TopicMode};
use kafkaman_rdkafka::{converge_topics, TopicAdmin};
use kafkaman_sqlx::{
    migrate, redrive_received, resync, service_table_access, MigrationContext, ReceivedTable,
    Replay, ResolvedConfig, ResyncFuture, ResyncOptions, ResyncProgress, Role, RoleRegistry,
    TableAccess,
};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

/// One declared recovery source.
pub struct Source {
    name: String,
    descriptor: kafkaman_core::Result<MessageDescriptor>,
    kind: SourceKind,
}

enum SourceKind {
    Resync(Box<dyn ResyncRun>),
    Received,
}

/// A source `Recovery::new` accepted.
struct Declared {
    name: String,
    descriptor: MessageDescriptor,
    kind: SourceKind,
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Source")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for Declared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Source")
            .field("name", &self.name)
            .field("operation", &self.operation())
            .finish_non_exhaustive()
    }
}

impl Declared {
    fn operation(&self) -> Operation {
        match self.kind {
            SourceKind::Resync(_) => Operation::Resync,
            SourceKind::Received => Operation::Redrive,
        }
    }
}

impl Source {
    /// Rebuild `M`'s outbox from current rows. `keys` pages through entity
    /// keys after a cursor; `load` reads one entity's current state while
    /// kafkaman holds its entity lock, and receives the run id for the
    /// envelope's identity.
    pub fn resync<M, Keys, Load>(name: impl Into<String>, keys: Keys, load: Load) -> Self
    where
        M: KafkaMessage + Serialize + Send + Sync + 'static,
        Keys: for<'a> FnMut(
                &'a mut PgConnection,
                Option<&'a str>,
                usize,
            ) -> ResyncFuture<'a, Vec<String>>
            + Send
            + 'static,
        Load: for<'a> FnMut(
                &'a mut PgConnection,
                &'a str,
                Uuid,
            ) -> ResyncFuture<'a, Option<Envelope<M>>>
            + Send
            + 'static,
    {
        Self {
            name: name.into(),
            descriptor: M::descriptor(),
            kind: SourceKind::Resync(Box::new(TypedResync::<M, Keys, Load> {
                keys,
                load,
                message: PhantomData,
            })),
        }
    }

    /// Move `M`'s `Failed` received rows back to `Pending`.
    pub fn received<M: KafkaMessage>(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            descriptor: M::descriptor(),
            kind: SourceKind::Received,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

trait ResyncRun: Send {
    fn run<'a>(
        &'a mut self,
        pool: &'a PgPool,
        cfg: &'a ResolvedConfig,
        options: ResyncOptions,
        run_id: Uuid,
        progress: &'a mut (dyn FnMut(&ResyncProgress, bool) + Send),
    ) -> ResyncFuture<'a, ResyncProgress>;
}

struct TypedResync<M, Keys, Load> {
    keys: Keys,
    load: Load,
    message: PhantomData<fn() -> M>,
}

impl<M, Keys, Load> ResyncRun for TypedResync<M, Keys, Load>
where
    M: KafkaMessage + Serialize + Send + Sync + 'static,
    Keys: for<'a> FnMut(&'a mut PgConnection, Option<&'a str>, usize) -> ResyncFuture<'a, Vec<String>>
        + Send
        + 'static,
    Load: for<'a> FnMut(&'a mut PgConnection, &'a str, Uuid) -> ResyncFuture<'a, Option<Envelope<M>>>
        + Send
        + 'static,
{
    fn run<'a>(
        &'a mut self,
        pool: &'a PgPool,
        cfg: &'a ResolvedConfig,
        options: ResyncOptions,
        run_id: Uuid,
        progress: &'a mut (dyn FnMut(&ResyncProgress, bool) + Send),
    ) -> ResyncFuture<'a, ResyncProgress> {
        let keys = &mut self.keys;
        let load = &mut self.load;
        // Keys left in the current page, so the commit that ends it is known.
        let left = Arc::new(AtomicUsize::new(0));
        let page = left.clone();
        Box::pin(resync::<M, _, _, _>(
            pool,
            cfg,
            options,
            move |conn: &mut PgConnection, after: Option<&str>, limit| {
                let listed = keys(conn, after, limit);
                let page = page.clone();
                Box::pin(async move {
                    let keys = listed.await?;
                    page.store(keys.len(), Ordering::SeqCst);
                    Ok(keys)
                }) as ResyncFuture<'_, Vec<String>>
            },
            move |conn, key| load(conn, key, run_id),
            move |done: &ResyncProgress| {
                let ends_page = left.fetch_sub(1, Ordering::SeqCst) == 1;
                progress(done, ends_page);
            },
        ))
    }
}

/// What a run does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Resync,
    Redrive,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Resync => "resync",
            Self::Redrive => "redrive",
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A run's raw inputs, as the host read them. Unset is `None`; kafkaman
/// decides what a blank value means.
#[derive(Clone, Debug, Default)]
pub struct RecoveryInputs {
    /// `resync` (the default) or `redrive`.
    pub operation: Option<String>,
    /// One declared source name. Unset resyncs every resync source.
    pub source: Option<String>,
    /// Exclusive resume cursor for one named resync source.
    pub start_after: Option<String>,
    /// Rows a redrive may move; required for redrive, refused for resync.
    pub max_rows: Option<String>,
}

/// A validated run, ready for [`Recovery::run`].
#[derive(Clone, Debug)]
pub struct Request {
    operation: Operation,
    selected: Vec<String>,
    start_after: Option<String>,
    max_rows: i64,
    batch_size: std::num::NonZeroUsize,
    pause: Duration,
}

impl Request {
    pub fn operation(&self) -> Operation {
        self.operation
    }

    pub fn start_after(&self) -> Option<&str> {
        self.start_after.as_deref()
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size.get()
    }

    pub fn pause(&self) -> Duration {
        self.pause
    }

    /// The redrive cap; zero for a resync.
    pub fn max_rows(&self) -> i64 {
        self.max_rows
    }
}

/// What one source did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceProgress {
    Resync(ResyncProgress),
    Redrive { moved: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Outcome {
    pub source: String,
    pub progress: SourceProgress,
}

/// A run's result: every completed source, the one that failed if any, and
/// the selected sources that never ran, in order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    pub run_id: Uuid,
    pub operation: Operation,
    pub completed: Vec<Outcome>,
    pub failed: Option<Outcome>,
    pub not_run: Vec<String>,
}

/// Why a source failed after the run started.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    #[error("configuration did not resolve: {0}")]
    Resolve(#[source] kafkaman_sqlx::Error),
    #[error("broker client could not be built: {0}")]
    Transport(#[source] kafkaman_rdkafka::Error),
    #[error("topic verification failed: {0}")]
    Topics(#[source] kafkaman_rdkafka::Error),
    #[error("migration failed: {0}")]
    Migrate(#[source] kafkaman_sqlx::Error),
    #[error("resync failed: {0}")]
    Resync(#[source] kafkaman_sqlx::Error),
    #[error("{table} cannot be redriven: {}", access.repair())]
    NotRedrivable { table: String, access: TableAccess },
    #[error("redrive failed: {0}")]
    Redrive(#[source] kafkaman_sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("no recovery sources are declared")]
    NoSources,
    #[error("a recovery source name is blank")]
    BlankSourceName,
    #[error("recovery source `{0}` is declared twice")]
    DuplicateSource(String),
    #[error("recovery source `{source_name}` is invalid: {cause}")]
    InvalidSource {
        source_name: String,
        #[source]
        cause: Box<kafkaman_sqlx::Error>,
    },
    #[error("invalid [recovery] configuration: {0}")]
    Config(#[source] ConfigError),
    #[error("unknown operation `{0}`; expected `resync` or `redrive`")]
    UnknownOperation(String),
    #[error("the source name is blank")]
    BlankSource,
    #[error("unknown source `{name}`; declared sources: {}", valid.join(", "))]
    UnknownSource { name: String, valid: Vec<String> },
    #[error("source `{source_name}` cannot {operation}")]
    IncompatibleSource {
        source_name: String,
        operation: Operation,
    },
    #[error("no resync sources are declared")]
    NoResyncSources,
    #[error("a redrive needs one named received source")]
    RedriveNeedsSource,
    #[error("a resume cursor needs exactly one named resync source")]
    StartAfterNeedsSource,
    #[error("a redrive needs max_rows")]
    MissingMaxRows,
    #[error("max_rows `{value}` must be between 1 and {ceiling}")]
    InvalidMaxRows { value: String, ceiling: i64 },
    #[error("max_rows applies to a redrive, not a resync")]
    MaxRowsOnResync,
    #[error("a resync needs broker addresses")]
    MissingBrokers,
    #[error(
        "recovery run {} failed on source `{source_name}`{}: {cause}",
        report.run_id,
        last_key_suffix(report)
    )]
    Failed {
        source_name: String,
        report: Box<Report>,
        #[source]
        cause: Box<Failure>,
    },
}

fn last_key_suffix(report: &Report) -> String {
    match report.failed.as_ref().map(|failed| &failed.progress) {
        Some(SourceProgress::Resync(ResyncProgress {
            last_key: Some(key),
            ..
        })) => format!(" after last_key `{key}`"),
        _ => String::new(),
    }
}

/// A validated, ordered list of sources.
#[derive(Debug)]
pub struct Recovery {
    sources: Vec<Declared>,
}

impl Recovery {
    /// Refuse an empty list, blank or repeated names, invalid descriptors and
    /// conflicting declarations, before any I/O.
    pub fn new(sources: Vec<Source>) -> Result<Self, RecoveryError> {
        if sources.is_empty() {
            return Err(RecoveryError::NoSources);
        }
        let mut names = BTreeSet::new();
        let mut roles = RoleRegistry::new();
        let mut declared = Vec::with_capacity(sources.len());
        for source in sources {
            if source.name.trim().is_empty() {
                return Err(RecoveryError::BlankSourceName);
            }
            if !names.insert(source.name.clone()) {
                return Err(RecoveryError::DuplicateSource(source.name));
            }
            let invalid = |cause: kafkaman_sqlx::Error| RecoveryError::InvalidSource {
                source_name: source.name.clone(),
                cause: Box::new(cause),
            };
            let descriptor = source.descriptor.map_err(|err| invalid(err.into()))?;
            let role = match source.kind {
                SourceKind::Resync(_) => Role::Publish,
                SourceKind::Received => Role::Handle,
            };
            roles.declare(descriptor.clone(), role).map_err(invalid)?;
            declared.push(Declared {
                name: source.name,
                descriptor,
                kind: source.kind,
            });
        }
        Ok(Self { sources: declared })
    }

    pub fn source_names(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(|source| source.name.clone())
            .collect()
    }

    /// Check a run's inputs and the `[recovery]` section, without I/O.
    pub fn request_from(
        &self,
        config: &Config,
        inputs: RecoveryInputs,
    ) -> Result<Request, RecoveryError> {
        let recovery = config.recovery().map_err(RecoveryError::Config)?;
        let operation = match inputs.operation.as_deref() {
            None => Operation::Resync,
            Some("resync") => Operation::Resync,
            Some("redrive") => Operation::Redrive,
            Some(other) => return Err(RecoveryError::UnknownOperation(other.to_owned())),
        };

        let named = match inputs.source.as_deref() {
            None => None,
            Some(name) if name.trim().is_empty() => return Err(RecoveryError::BlankSource),
            Some(name) => {
                let index = self
                    .sources
                    .iter()
                    .position(|source| source.name == name)
                    .ok_or_else(|| RecoveryError::UnknownSource {
                        name: name.to_owned(),
                        valid: self.source_names(),
                    })?;
                if self.sources[index].operation() != operation {
                    return Err(RecoveryError::IncompatibleSource {
                        source_name: name.to_owned(),
                        operation,
                    });
                }
                Some(name.to_owned())
            }
        };

        let start_after = inputs
            .start_after
            .filter(|cursor| !cursor.trim().is_empty());
        let max_rows = inputs.max_rows.filter(|rows| !rows.trim().is_empty());

        match operation {
            Operation::Resync => {
                if max_rows.is_some() {
                    return Err(RecoveryError::MaxRowsOnResync);
                }
                let selected = match named {
                    Some(name) => vec![name],
                    None => {
                        if start_after.is_some() {
                            return Err(RecoveryError::StartAfterNeedsSource);
                        }
                        let all = self
                            .sources
                            .iter()
                            .filter(|source| source.operation() == Operation::Resync)
                            .map(|source| source.name.clone())
                            .collect::<Vec<_>>();
                        if all.is_empty() {
                            return Err(RecoveryError::NoResyncSources);
                        }
                        all
                    }
                };
                Ok(Request {
                    operation,
                    selected,
                    start_after,
                    max_rows: 0,
                    batch_size: recovery.batch_size,
                    pause: recovery.pause,
                })
            }
            Operation::Redrive => {
                let name = named.ok_or(RecoveryError::RedriveNeedsSource)?;
                let raw = max_rows.ok_or(RecoveryError::MissingMaxRows)?;
                let ceiling = recovery.max_redrive_rows;
                let rows = raw
                    .trim()
                    .parse::<i64>()
                    .ok()
                    .filter(|rows| (1..=ceiling).contains(rows))
                    .ok_or(RecoveryError::InvalidMaxRows {
                        value: raw.clone(),
                        ceiling,
                    })?;
                Ok(Request {
                    operation,
                    selected: vec![name],
                    // A redrive has no cursor; one inherited from a resync
                    // environment means nothing here.
                    start_after: None,
                    max_rows: rows,
                    batch_size: recovery.batch_size,
                    pause: recovery.pause,
                })
            }
        }
    }

    /// Run `request`. A resync verifies every selected topic, then migrates
    /// each selected outbox, then rebuilds the sources in order. A redrive
    /// probes the received table and moves at most `max_rows` rows once.
    /// Each source the request names is looked up in this list, so a request
    /// built from another list runs what its names mean here or is refused.
    pub async fn run(
        &mut self,
        pool: &PgPool,
        config: &Config,
        brokers: Option<&str>,
        migration_context: &MigrationContext,
        request: Request,
    ) -> Result<Report, RecoveryError> {
        for name in &request.selected {
            let source = self
                .sources
                .iter()
                .find(|source| source.name == *name)
                .ok_or_else(|| RecoveryError::UnknownSource {
                    name: name.clone(),
                    valid: self.source_names(),
                })?;
            if source.operation() != request.operation {
                return Err(RecoveryError::IncompatibleSource {
                    source_name: name.clone(),
                    operation: request.operation,
                });
            }
        }
        let brokers = match request.operation {
            Operation::Resync => Some(
                brokers
                    .filter(|brokers| !brokers.trim().is_empty())
                    .ok_or(RecoveryError::MissingBrokers)?,
            ),
            Operation::Redrive => None,
        };
        let run = Run::start(self, &request);
        match brokers {
            Some(brokers) => run.resync(pool, config, brokers, migration_context).await,
            None => run.redrive(pool, config).await,
        }
    }
}

/// One run's state: the outcome of every selected source so far.
struct Run<'r> {
    run_id: Uuid,
    request: &'r Request,
    /// Every declared descriptor: the file is checked against all of them.
    catalog: Vec<MessageDescriptor>,
    sources: Vec<&'r mut Declared>,
    progress: Vec<SourceProgress>,
}

impl<'r> Run<'r> {
    fn start(recovery: &'r mut Recovery, request: &'r Request) -> Self {
        let run_id = Uuid::now_v7();
        let catalog = recovery
            .sources
            .iter()
            .map(|source| source.descriptor.clone())
            .collect();
        let sources = recovery
            .sources
            .iter_mut()
            .filter(|source| request.selected.contains(&source.name))
            .collect::<Vec<_>>();
        // Seeded before any I/O, so a failure in preparation still reports
        // the cursor it was given.
        let progress = sources
            .iter()
            .map(|_| match request.operation {
                Operation::Resync => SourceProgress::Resync(ResyncProgress {
                    last_key: request.start_after.clone(),
                    ..ResyncProgress::default()
                }),
                Operation::Redrive => SourceProgress::Redrive { moved: 0 },
            })
            .collect();
        let names = sources
            .iter()
            .map(|source| source.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        tracing::info!(
            target: "kafkaman",
            %run_id,
            operation = request.operation.as_str(),
            sources = %names,
            start_after = request.start_after.as_deref(),
            "recovery started"
        );
        Self {
            run_id,
            request,
            catalog,
            sources,
            progress,
        }
    }

    fn report(&self, failed: Option<usize>, completed: usize) -> Report {
        let outcome = |index: usize| Outcome {
            source: self.sources[index].name.clone(),
            progress: self.progress[index].clone(),
        };
        Report {
            run_id: self.run_id,
            operation: self.request.operation,
            completed: (0..completed).map(outcome).collect(),
            failed: failed.map(outcome),
            not_run: (completed..self.sources.len())
                .filter(|index| Some(*index) != failed)
                .map(|index| self.sources[index].name.clone())
                .collect(),
        }
    }

    /// Fail source `index`; `completed` sources before it finished.
    fn fail(&self, index: usize, completed: usize, cause: Failure) -> RecoveryError {
        let report = self.report(Some(index), completed);
        let source_name = self.sources[index].name.clone();
        let (enqueued, skipped, last_key, moved) = match &self.progress[index] {
            SourceProgress::Resync(progress) => (
                progress.enqueued,
                progress.skipped,
                progress.last_key.clone(),
                0,
            ),
            SourceProgress::Redrive { moved } => (0, 0, None, *moved),
        };
        tracing::error!(
            target: "kafkaman",
            run_id = %self.run_id,
            operation = self.request.operation.as_str(),
            source = %source_name,
            enqueued,
            skipped,
            moved,
            last_key = last_key.as_deref(),
            not_run = %report.not_run.join(","),
            error = %cause,
            "recovery failed"
        );
        RecoveryError::Failed {
            source_name,
            report: Box::new(report),
            cause: Box::new(cause),
        }
    }

    fn finish(self) -> Report {
        let report = self.report(None, self.sources.len());
        for outcome in &report.completed {
            match &outcome.progress {
                SourceProgress::Resync(progress) => tracing::info!(
                    target: "kafkaman",
                    run_id = %self.run_id,
                    operation = "resync",
                    source = %outcome.source,
                    enqueued = progress.enqueued,
                    skipped = progress.skipped,
                    last_key = progress.last_key.as_deref(),
                    "recovery source completed"
                ),
                SourceProgress::Redrive { moved } => tracing::info!(
                    target: "kafkaman",
                    run_id = %self.run_id,
                    operation = "redrive",
                    source = %outcome.source,
                    moved,
                    "recovery source completed; rows requeued for dispatch"
                ),
            }
        }
        report
    }

    /// The resolved (prefixed) descriptor for source `index`.
    fn resolved(&self, cfg: &ResolvedConfig, index: usize) -> Option<MessageDescriptor> {
        let declared = &self.sources[index].descriptor;
        cfg.messages()
            .iter()
            .find(|descriptor| descriptor.message_type == declared.message_type)
            .cloned()
    }

    async fn resync(
        mut self,
        pool: &PgPool,
        config: &Config,
        brokers: &str,
        migration_context: &MigrationContext,
    ) -> Result<Report, RecoveryError> {
        let cfg = match ResolvedConfig::from_config(Some(config), self.catalog.clone()) {
            Ok(cfg) => cfg,
            Err(err) => return Err(self.fail(0, 0, Failure::Resolve(err))),
        };
        let admin = match TopicAdmin::from_brokers(brokers) {
            Ok(admin) => admin,
            Err(err) => return Err(self.fail(0, 0, Failure::Transport(err))),
        };

        // Every topic before any table: a run that is going to refuse one
        // source must not have created storage for another.
        for index in 0..self.sources.len() {
            let descriptors = self.resolved(&cfg, index).into_iter().collect::<Vec<_>>();
            if let Err(err) = converge_topics(&admin, TopicMode::Verify, &descriptors).await {
                return Err(self.fail(index, 0, Failure::Topics(err)));
            }
        }
        for index in 0..self.sources.len() {
            let mut roles = RoleRegistry::new();
            let changelog = self
                .resolved(&cfg, index)
                .map(|descriptor| roles.declare(descriptor, Role::Publish))
                .transpose()
                .and_then(|_| roles.changelog());
            let result = match changelog {
                Ok(changelog) => migrate(pool, &cfg, migration_context, &changelog).await,
                Err(err) => Err(err),
            };
            if let Err(err) = result {
                return Err(self.fail(index, 0, Failure::Migrate(err)));
            }
        }

        let batch_size = self.request.batch_size;
        for index in 0..self.sources.len() {
            let options = ResyncOptions {
                batch_size,
                pause: self.request.pause,
                start_after: match &self.progress[index] {
                    SourceProgress::Resync(progress) => progress.last_key.clone(),
                    SourceProgress::Redrive { .. } => None,
                },
            };
            let run_id = self.run_id;
            let name = self.sources[index].name.clone();
            let mut observed = match &self.progress[index] {
                SourceProgress::Resync(progress) => progress.clone(),
                SourceProgress::Redrive { .. } => ResyncProgress::default(),
            };
            let SourceKind::Resync(run) = &mut self.sources[index].kind else {
                continue;
            };
            let mut record = |progress: &ResyncProgress, ends_page: bool| {
                observed = progress.clone();
                if ends_page {
                    tracing::info!(
                        target: "kafkaman",
                        %run_id,
                        operation = "resync",
                        source = %name,
                        enqueued = progress.enqueued,
                        skipped = progress.skipped,
                        last_key = progress.last_key.as_deref(),
                        "recovery progress"
                    );
                }
            };
            let result = run.run(pool, &cfg, options, run_id, &mut record).await;
            match result {
                Ok(done) => self.progress[index] = SourceProgress::Resync(done),
                Err(err) => {
                    self.progress[index] = SourceProgress::Resync(observed);
                    return Err(self.fail(index, index, Failure::Resync(err)));
                }
            }
        }
        Ok(self.finish())
    }

    async fn redrive(mut self, pool: &PgPool, config: &Config) -> Result<Report, RecoveryError> {
        let cfg = match ResolvedConfig::from_config(Some(config), self.catalog.clone()) {
            Ok(cfg) => cfg,
            Err(err) => return Err(self.fail(0, 0, Failure::Resolve(err))),
        };
        let Some(descriptor) = self.resolved(&cfg, 0) else {
            return Err(self.fail(
                0,
                0,
                Failure::Resolve(kafkaman_sqlx::Error::UnknownMessageType(
                    self.sources[0].name.clone(),
                )),
            ));
        };
        let table = match ReceivedTable::for_descriptor(&cfg, descriptor.clone()) {
            Ok(table) => table,
            Err(err) => return Err(self.fail(0, 0, Failure::Resolve(err))),
        };
        // `UPDATE` as well as `SELECT` because redrive writes, and the probe
        // also asks for schema `USAGE`: without it the table grants pass and
        // the statement still fails.
        let access =
            match service_table_access(pool, &table.schema, &table.table, &["SELECT", "UPDATE"])
                .await
            {
                Ok(access) => access,
                Err(err) => return Err(self.fail(0, 0, Failure::Redrive(err))),
            };
        if !access.is_ready() {
            let table = table.qualified_name();
            return Err(self.fail(0, 0, Failure::NotRedrivable { table, access }));
        }
        let replay = Replay::received_descriptor(Replay::RUNTIME_VERSION, descriptor)
            .max_rows(self.request.max_rows);
        match redrive_received(pool, &cfg, &replay).await {
            Ok(moved) => {
                self.progress[0] = SourceProgress::Redrive { moved };
                Ok(self.finish())
            }
            Err(err) => Err(self.fail(0, 0, Failure::Redrive(err))),
        }
    }
}
