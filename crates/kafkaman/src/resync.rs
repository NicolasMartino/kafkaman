//! An internal HTTP route for rebuilding a service's declared snapshots.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use ::axum::extract::{Path, State};
use ::axum::http::StatusCode;
use ::axum::routing::{get, post};
use ::axum::{Json, Router};
use kafkaman_config::Config;
use kafkaman_sqlx::{MigrationContext, ResyncProgress};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::recovery::{Outcome, Recovery, RecoveryInputs, SourceProgress};

type RunFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
type Runner = dyn Fn(ResyncRequest, ProgressReporter) -> RunFuture + Send + Sync;

/// A source and optional exclusive cursor for a resync run.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResyncRequest {
    pub source: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SourceStatus {
    source: String,
    enqueued: u64,
    skipped: u64,
    last_key: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Running,
    Finished,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize)]
struct RunStatus {
    run_id: Uuid,
    state: Phase,
    progress: Vec<SourceStatus>,
    error: Option<String>,
}

#[derive(Default)]
struct Runs {
    active: Option<Uuid>,
    status: HashMap<Uuid, RunStatus>,
    handles: HashMap<Uuid, JoinHandle<()>>,
}

/// Records committed progress without waiting for the HTTP task to poll.
#[derive(Clone)]
pub struct ProgressReporter {
    runs: Arc<Mutex<Runs>>,
    run_id: Uuid,
}

impl std::fmt::Debug for ProgressReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressReporter")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

impl ProgressReporter {
    pub fn record(&self, outcome: Outcome) {
        let SourceProgress::Resync(progress) = outcome.progress else {
            return;
        };
        self.resync(outcome.source, progress);
    }

    fn resync(&self, source: String, progress: ResyncProgress) {
        let mut runs = lock(&self.runs);
        let Some(status) = runs.status.get_mut(&self.run_id) else {
            return;
        };
        if status.state != Phase::Running {
            return;
        }
        let update = SourceStatus {
            source: source.clone(),
            enqueued: progress.enqueued,
            skipped: progress.skipped,
            last_key: progress.last_key,
        };
        if let Some(previous) = status
            .progress
            .iter_mut()
            .find(|item| item.source == source)
        {
            *previous = update;
        } else {
            status.progress.push(update);
        }
    }
}

fn lock(runs: &Mutex<Runs>) -> std::sync::MutexGuard<'_, Runs> {
    runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// State shared by the resync routes; only one run executes at a time.
#[derive(Clone)]
pub struct ResyncState {
    runs: Arc<Mutex<Runs>>,
    runner: Arc<Runner>,
}

impl std::fmt::Debug for ResyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResyncState").finish_non_exhaustive()
    }
}

impl ResyncState {
    /// Supply a run function; it receives a progress reporter and owns its future.
    pub fn new<F, Fut>(runner: F) -> Self
    where
        F: Fn(ResyncRequest, ProgressReporter) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        Self {
            runs: Arc::new(Mutex::new(Runs::default())),
            runner: Arc::new(move |request, progress| Box::pin(runner(request, progress))),
        }
    }

    /// Bind a declared recovery list to the pool and configuration already used by the service.
    pub fn for_recovery(
        recovery: Recovery,
        pool: PgPool,
        config: Config,
        brokers: String,
        migration_context: MigrationContext,
    ) -> Self {
        let recovery = Arc::new(tokio::sync::Mutex::new(recovery));
        let config = Arc::new(config);
        let migration_context = Arc::new(migration_context);
        Self::new(move |request, reporter| {
            let recovery = Arc::clone(&recovery);
            let config = Arc::clone(&config);
            let migration_context = Arc::clone(&migration_context);
            let pool = pool.clone();
            let brokers = brokers.clone();
            async move {
                let mut recovery = recovery.lock().await;
                let inputs = RecoveryInputs {
                    source: request.source,
                    start_after: request.start_after,
                    ..RecoveryInputs::default()
                };
                let run = recovery
                    .request_from(&config, inputs)
                    .map_err(|err| err.to_string())?;
                recovery
                    .run_with_progress(
                        &pool,
                        &config,
                        Some(&brokers),
                        &migration_context,
                        run,
                        &mut |outcome| reporter.record(outcome),
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                Ok(())
            }
        })
    }
}

/// Mount only on an internal listener: these routes are unauthenticated and write to the outbox.
pub fn resync_router(state: ResyncState) -> Router {
    Router::new()
        .route("/resync", post(start))
        .route("/resync/{run_id}", get(status).delete(cancel))
        .with_state(state)
}

async fn start(
    State(state): State<ResyncState>,
    Json(request): Json<ResyncRequest>,
) -> Result<(StatusCode, Json<RunStatus>), (StatusCode, String)> {
    if request.start_after.is_some() && request.source.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "start_after needs one named source".to_owned(),
        ));
    }
    let mut runs = lock(&state.runs);
    if runs.active.is_some() {
        return Err((
            StatusCode::CONFLICT,
            "a resync is already running".to_owned(),
        ));
    }
    if runs.status.len() >= 32 {
        if let Some(oldest) = runs.status.keys().min().copied() {
            runs.status.remove(&oldest);
            runs.handles.remove(&oldest);
        }
    }
    let run_id = Uuid::now_v7();
    let status = RunStatus {
        run_id,
        state: Phase::Running,
        progress: Vec::new(),
        error: None,
    };
    runs.status.insert(run_id, status.clone());
    runs.active = Some(run_id);
    let shared = Arc::clone(&state.runs);
    let reporter = ProgressReporter {
        runs: Arc::clone(&shared),
        run_id,
    };
    let runner = Arc::clone(&state.runner);
    let handle = tokio::spawn(async move {
        let result = runner(request, reporter).await;
        let mut runs = lock(&shared);
        if let Some(status) = runs.status.get_mut(&run_id) {
            if status.state == Phase::Running {
                match result {
                    Ok(()) => status.state = Phase::Finished,
                    Err(error) => {
                        status.state = Phase::Failed;
                        status.error = Some(error);
                    }
                }
                runs.active = None;
            }
        }
        runs.handles.remove(&run_id);
    });
    runs.handles.insert(run_id, handle);
    Ok((StatusCode::ACCEPTED, Json(status)))
}

async fn status(
    State(state): State<ResyncState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<RunStatus>, StatusCode> {
    lock(&state.runs)
        .status
        .get(&run_id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn cancel(
    State(state): State<ResyncState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<RunStatus>, StatusCode> {
    let mut runs = lock(&state.runs);
    let status = runs.status.get_mut(&run_id).ok_or(StatusCode::NOT_FOUND)?;
    if status.state != Phase::Running {
        return Err(StatusCode::CONFLICT);
    }
    status.state = Phase::Cancelled;
    let result = status.clone();
    runs.active = None;
    if let Some(handle) = runs.handles.remove(&run_id) {
        handle.abort();
    }
    Ok(Json(result))
}
