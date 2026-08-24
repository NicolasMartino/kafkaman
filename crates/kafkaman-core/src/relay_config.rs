use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, LifecycleEmission, Result};

/// How the outbox relay claims and publishes.
#[derive(Clone, Debug)]
pub struct RelayConfig {
    pub worker_id: String,
    pub batch_limit: i64,
    pub lease_for: Duration,
    pub retry_after: Duration,
    pub poll_interval: Duration,
    /// Per-message success event policy. Defaults to silent.
    pub lifecycle: LifecycleEmission,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", Uuid::new_v4()),
            batch_limit: 100,
            lease_for: Duration::from_secs(30),
            retry_after: Duration::from_secs(1),
            poll_interval: Duration::from_millis(250),
            lifecycle: LifecycleEmission::default(),
        }
    }
}

impl RelayConfig {
    /// Reject configurations that would break relay correctness or spin the
    /// worker.
    ///
    /// Each field states its own reason rather than inheriting a blanket
    /// "durations must be positive" rule, because the reasons genuinely differ
    /// and `retry_after` is a legitimate zero. Relaxing exactly such a blanket
    /// rule — in `parse_duration` — is what silently removed the only guard on
    /// a field where zero is a hot loop.
    pub fn validate(&self) -> Result<()> {
        if self.lease_for.is_zero() {
            return Err(Error::InvalidRelayConfig {
                field: "lease_for",
                reason: "must be greater than zero; a claim that expires as it is taken lets \
                         another worker reclaim and republish in a tight loop",
            });
        }
        if self.batch_limit <= 0 {
            return Err(Error::InvalidRelayConfig {
                field: "batch_limit",
                reason: "must be greater than zero",
            });
        }
        if self.poll_interval.is_zero() {
            return Err(Error::InvalidRelayConfig {
                field: "poll_interval",
                reason: "must be greater than zero; it is the only thing that yields between \
                         empty polls",
            });
        }
        Ok(())
    }
}

/// What one relay cycle did.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelayStats {
    pub claimed: usize,
    pub published: usize,
    pub failed: usize,
    /// A mark was rejected because the row's claim had been lost to another
    /// worker (lease expired and reclaimed).
    pub stale: usize,
    /// A mark found no row at all (the outbox row was deleted/purged).
    pub missing: usize,
}
