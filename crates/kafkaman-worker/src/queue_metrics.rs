//! Queue depth and age as scraped series rather than polled routes.
//!
//! `outbox_status_summary` and `received_status_summary` already answer "how
//! much work is waiting, and how old is the oldest of it" against real Postgres,
//! and the admin routes already expose them. What they cannot do is show up on a
//! dashboard by themselves: somebody has to remember to curl them. Registering
//! the same queries as observable gauges is the difference between data that
//! exists and data somebody looks at.
//!
//! # Why a background loop rather than a query in the callback
//!
//! An OpenTelemetry observable-gauge callback is synchronous, so it cannot await
//! a database round trip — and even if it could, it must not. Callbacks run on
//! the SDK's collection schedule, which means the database would be queried on
//! an interval the host configures for telemetry rather than for load, and a
//! slow query would stall the whole collection cycle. That stall would land
//! precisely when the database is already struggling, which is exactly when
//! somebody is looking at the dashboard.
//!
//! So this loop owns the queries and the callbacks read a snapshot it maintains.
//! The refresh is bounded by a timeout, and a refresh that fails leaves the
//! previous snapshot in place.
//!
//! # Staleness is a series, not a gap
//!
//! The obvious design is to stop observing once a snapshot is too old, so a
//! stalled sampler shows as a gap rather than a flat line at a number that has
//! quietly stopped being true. It does not work, and the reason is worth stating
//! because it is invisible from this side of the API: an asynchronous gauge
//! under cumulative temporality republishes its last recorded value on every
//! collection cycle, whether or not the callback observed anything. Observing
//! nothing therefore produces the flat line anyway. `tests/observability`
//! demonstrates it.
//!
//! `kafkaman.queue.sample_age` is the answer instead — the age of the snapshot
//! behind every other series here. A depth of 2 means nothing on its own; a
//! depth of 2 next to a sample age of 400 seconds says plainly that the number
//! is four hundred seconds stale, and that is the condition worth alerting on.
//!
//! # These are aggregate queries
//!
//! Each refresh is a full `GROUP BY status` over each table, which is index-only
//! at best. `refresh_interval` is the knob that decides how often that runs, and
//! it is deliberately independent of the SDK's collection interval so that
//! scraping faster does not query harder.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kafkaman_core::{OutboxStatus, ReceiveStatus};
use kafkaman_sqlx::{outbox_status_summary, received_status_summary, OutboxTable, ReceivedTable};
use opentelemetry::metrics::{AsyncInstrument, ObservableGauge};
use opentelemetry::{global, KeyValue};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use crate::run_loop::sleep_or_shutdown;
use crate::{Error, Result};

/// How the queue-depth sampler paces itself.
#[derive(Clone, Debug)]
pub struct QueueMetricsConfig {
    /// How often the summary queries run.
    ///
    /// Independent of the SDK's collection interval on purpose: a host that
    /// scrapes every second must not thereby query Postgres every second.
    pub refresh_interval: Duration,

    /// How long one refresh may take before it is abandoned.
    ///
    /// A refresh that outlives this is dropped rather than awaited, because the
    /// next one will be along shortly and a queue of stacked aggregate queries
    /// helps nothing.
    pub query_timeout: Duration,

    /// Forwarded to the summary queries, which use it only for the
    /// `over_max_queue_age` flag.
    ///
    /// The gauges do not read that flag — a threshold belongs in the alert, not
    /// in the series — but the queries take it, and giving it the same value as
    /// the admin routes keeps one number in one place.
    pub max_queue_age: Duration,
}

impl Default for QueueMetricsConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(15),
            query_timeout: Duration::from_secs(5),
            max_queue_age: Duration::from_secs(300),
        }
    }
}

impl QueueMetricsConfig {
    /// Reject a configuration no interval can rescue, once, up front.
    pub fn validate(&self) -> Result<()> {
        if self.refresh_interval.is_zero() {
            return Err(Error::InvalidQueueMetricsConfig {
                field: "refresh_interval",
                reason: "must be greater than zero",
            });
        }
        if self.query_timeout.is_zero() {
            return Err(Error::InvalidQueueMetricsConfig {
                field: "query_timeout",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

/// One `(message type, status)` bucket, ready to observe.
#[derive(Debug)]
struct Sample {
    /// `[message_type, status]`, built at refresh time so the callback does no
    /// allocation on the collection path.
    attrs: [KeyValue; 2],
    depth: u64,
    /// Absent when the bucket is empty. The age of no rows is not zero — zero
    /// would read as "a row arrived just now", which is the opposite of true.
    oldest_age_seconds: Option<f64>,
}

/// What the callbacks read, and when it was true.
#[derive(Debug, Default)]
struct Snapshot {
    outbox: Vec<Sample>,
    received: Vec<Sample>,
    refreshed_at: Option<Instant>,
}

impl Snapshot {
    /// How long ago this snapshot was taken, or `None` before the first refresh.
    ///
    /// Before the first refresh there is no age to report — reporting zero would
    /// claim a freshness that does not exist yet — so the series simply starts
    /// when the first query returns.
    fn age(&self) -> Option<Duration> {
        Some(self.refreshed_at?.elapsed())
    }
}

/// The registered gauges, kept alive for as long as the loop runs.
///
/// Held rather than dropped: these are the handles the callbacks hang from, and
/// a loop that registered them and then dropped them would be reporting on
/// borrowed time.
#[derive(Debug)]
struct Gauges {
    _outbox_depth: ObservableGauge<u64>,
    _outbox_oldest_age: ObservableGauge<f64>,
    _received_depth: ObservableGauge<u64>,
    _received_oldest_age: ObservableGauge<f64>,
    _sample_age: ObservableGauge<f64>,
}

/// Sample queue depth and age until `shutdown` is cancelled.
///
/// Register one of these per process. The gauges are registered with the global
/// meter when this starts, so two concurrent loops covering the same tables
/// report the same rows twice — the callbacks are additive, and OpenTelemetry
/// has no way to tell that two of them mean the same thing.
///
/// Like every other loop here, a failed cycle is logged and retried: a database
/// blip must not take the sampler down. The difference between "the queue is
/// empty" and "the sampler is broken" is carried by
/// `kafkaman.queue.sample_age`, not by the sampler exiting.
///
/// Returns `Err` only for a configuration that can never succeed.
pub async fn run_queue_metrics(
    pool: PgPool,
    outbox_tables: Vec<OutboxTable>,
    received_tables: Vec<ReceivedTable>,
    cfg: QueueMetricsConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    cfg.validate()?;

    let shared = Arc::new(Mutex::new(Snapshot::default()));
    // Registered before the first refresh, so the instruments bind to whatever
    // provider is installed when the loop starts rather than to whatever exists
    // whenever the first query happens to finish.
    let _gauges = register(Arc::clone(&shared));

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        refresh(&pool, &outbox_tables, &received_tables, &cfg, &shared).await;

        if !sleep_or_shutdown(cfg.refresh_interval, &shutdown).await {
            break;
        }
    }

    Ok(())
}

/// Run one bounded refresh and publish it, or log why it did not happen.
///
/// Failures deliberately leave the previous snapshot in place with its original
/// timestamp, so the depths keep their last known values and
/// `kafkaman.queue.sample_age` keeps climbing. That pair is the signal: the
/// numbers are still there, and the age says how much to trust them.
async fn refresh(
    pool: &PgPool,
    outbox_tables: &[OutboxTable],
    received_tables: &[ReceivedTable],
    cfg: &QueueMetricsConfig,
    shared: &Mutex<Snapshot>,
) {
    let collected = tokio::time::timeout(
        cfg.query_timeout,
        collect(pool, outbox_tables, received_tables, cfg.max_queue_age),
    )
    .await;

    let (outbox, received) = match collected {
        Ok(Ok(samples)) => samples,
        Ok(Err(err)) => {
            tracing::error!(
                error = %err,
                "queue metrics refresh failed; the previous sample stands and its age keeps climbing"
            );
            return;
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = cfg.query_timeout.as_millis(),
                "queue metrics refresh timed out; the previous sample stands and its age keeps climbing"
            );
            return;
        }
    };

    match shared.lock() {
        Ok(mut snapshot) => {
            snapshot.outbox = outbox;
            snapshot.received = received;
            snapshot.refreshed_at = Some(Instant::now());
        }
        Err(err) => {
            // A poisoned mutex means a callback panicked while holding it. The
            // sampler keeps running rather than propagating: the sample age
            // stops advancing, which is the same signal as any stalled refresh.
            tracing::error!(error = %err, "queue metrics snapshot lock is poisoned");
        }
    }
}

/// Query every table once.
async fn collect(
    pool: &PgPool,
    outbox_tables: &[OutboxTable],
    received_tables: &[ReceivedTable],
    max_queue_age: Duration,
) -> Result<(Vec<Sample>, Vec<Sample>)> {
    // One `now` for the whole refresh, so every age in a snapshot is measured
    // from the same instant and two tables cannot disagree about the present.
    let now = OffsetDateTime::now_utc();

    let mut outbox = Vec::new();
    for table in outbox_tables {
        let summary = outbox_status_summary(pool, table, now, max_queue_age).await?;
        let message_type = table.descriptor.message_type.as_str();
        for status in OutboxStatus::ALL {
            let bucket = summary.iter().find(|entry| entry.status == status);
            outbox.push(Sample {
                attrs: attrs(message_type, status.as_str()),
                depth: depth(bucket.map(|entry| entry.count)),
                oldest_age_seconds: bucket
                    .and_then(|entry| entry.oldest_age_ms)
                    .map(age_seconds),
            });
        }
    }

    let mut received = Vec::new();
    for table in received_tables {
        let summary = received_status_summary(pool, table, now, max_queue_age).await?;
        let message_type = table.descriptor.message_type.as_str();
        for status in ReceiveStatus::ALL {
            let bucket = summary.iter().find(|entry| entry.status == status);
            received.push(Sample {
                attrs: attrs(message_type, status.as_str()),
                depth: depth(bucket.map(|entry| entry.count)),
                oldest_age_seconds: bucket
                    .and_then(|entry| entry.oldest_age_ms)
                    .map(age_seconds),
            });
        }
    }

    Ok((outbox, received))
}

fn attrs(message_type: &str, status: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("message_type", message_type.to_owned()),
        KeyValue::new("status", status),
    ]
}

/// A status the `GROUP BY` did not return has no rows, which is a depth of zero
/// rather than an absent series.
///
/// Reporting the zero explicitly is what lets a dashboard show a queue draining
/// to empty instead of the line simply stopping.
fn depth(count: Option<i64>) -> u64 {
    u64::try_from(count.unwrap_or(0)).unwrap_or(0)
}

fn age_seconds(age_ms: u64) -> f64 {
    age_ms as f64 / 1000.0
}

/// Register the gauges against the global meter.
fn register(shared: Arc<Mutex<Snapshot>>) -> Gauges {
    let meter = global::meter("kafkaman");

    let outbox_depth_source = Arc::clone(&shared);
    let outbox_depth = meter
        .u64_observable_gauge("kafkaman.outbox.depth")
        .with_description("Outbox rows in each status")
        .with_unit("{row}")
        .with_callback(move |observer| {
            observe_depth(observer, &outbox_depth_source, Queue::Outbox);
        })
        .build();

    let outbox_age_source = Arc::clone(&shared);
    let outbox_oldest_age = meter
        .f64_observable_gauge("kafkaman.outbox.oldest_age")
        .with_description("Age of the oldest outbox row in each status")
        .with_unit("s")
        .with_callback(move |observer| {
            observe_age(observer, &outbox_age_source, Queue::Outbox);
        })
        .build();

    let received_depth_source = Arc::clone(&shared);
    let received_depth = meter
        .u64_observable_gauge("kafkaman.received.depth")
        .with_description("Received rows in each status")
        .with_unit("{row}")
        .with_callback(move |observer| {
            observe_depth(observer, &received_depth_source, Queue::Received);
        })
        .build();

    let received_age_source = Arc::clone(&shared);
    let received_oldest_age = meter
        .f64_observable_gauge("kafkaman.received.oldest_age")
        .with_description("Age of the oldest received row in each status")
        .with_unit("s")
        .with_callback(move |observer| {
            observe_age(observer, &received_age_source, Queue::Received);
        })
        .build();

    let sample_age_source = Arc::clone(&shared);
    let sample_age = meter
        .f64_observable_gauge("kafkaman.queue.sample_age")
        .with_description("Age of the snapshot behind the kafkaman queue gauges")
        .with_unit("s")
        .with_callback(move |observer| {
            let Ok(snapshot) = sample_age_source.lock() else {
                return;
            };
            if let Some(age) = snapshot.age() {
                observer.observe(age.as_secs_f64(), &[]);
            }
        })
        .build();

    Gauges {
        _outbox_depth: outbox_depth,
        _outbox_oldest_age: outbox_oldest_age,
        _received_depth: received_depth,
        _received_oldest_age: received_oldest_age,
        _sample_age: sample_age,
    }
}

/// Which half of the snapshot a callback reads.
#[derive(Clone, Copy, Debug)]
enum Queue {
    Outbox,
    Received,
}

/// Read one half of the snapshot for a callback.
///
/// A poisoned lock reports nothing: it means a panic interrupted a write, and
/// recovering the data by ignoring the poison would publish a snapshot that is
/// half old and half new. `kafkaman.queue.sample_age` stops advancing, which is
/// the same signal a stalled refresh gives.
fn with_samples<F>(shared: &Mutex<Snapshot>, queue: Queue, mut visit: F)
where
    F: FnMut(&Sample),
{
    let Ok(snapshot) = shared.lock() else {
        return;
    };
    let samples = match queue {
        Queue::Outbox => &snapshot.outbox,
        Queue::Received => &snapshot.received,
    };
    for sample in samples {
        visit(sample);
    }
}

fn observe_depth(observer: &dyn AsyncInstrument<u64>, shared: &Mutex<Snapshot>, queue: Queue) {
    with_samples(shared, queue, |sample| {
        observer.observe(sample.depth, &sample.attrs);
    });
}

fn observe_age(observer: &dyn AsyncInstrument<f64>, shared: &Mutex<Snapshot>, queue: Queue) {
    with_samples(shared, queue, |sample| {
        if let Some(age) = sample.oldest_age_seconds {
            observer.observe(age, &sample.attrs);
        }
    });
}
