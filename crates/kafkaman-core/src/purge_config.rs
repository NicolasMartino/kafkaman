use std::time::Duration;

use crate::{Error, Result};

/// How much of the outbox to keep, and how fast to reclaim the rest.
///
/// Retention applies to the outbox alone. The received table is the only durable
/// record of what was done and its dedupe window *is* its retention window; cache
/// tables are the state. See the outbox retention decision.
#[derive(Clone, Debug)]
pub struct PurgeConfig {
    /// Age past which a terminal row is reclaimable, measured from `created_at`.
    pub older_than: Duration,
    /// Rows per batch. Bounds how long one delete holds locks and how much
    /// write-ahead log it generates, which is why the purge is batched at all.
    pub batch_size: i64,
    /// Sleep between sweeps once a batch comes back empty.
    pub poll_interval: Duration,
    /// Whether to reclaim `Failed` rows too.
    ///
    /// Off by default: `Failed` outbox rows are the invalid-send audit trail, and
    /// unlike every other terminal row they have no successor that carries the same
    /// information. Deleting them discards the only record that the work was
    /// rejected.
    pub include_failed: bool,
}

impl Default for PurgeConfig {
    fn default() -> Self {
        Self {
            older_than: Duration::from_secs(7 * 24 * 60 * 60),
            batch_size: 1_000,
            poll_interval: Duration::from_secs(60),
            include_failed: false,
        }
    }
}

impl PurgeConfig {
    /// Reject configurations that would delete more than intended or spin the
    /// purger.
    ///
    /// Zero is rejected per field deliberately, rather than by inheriting a blanket
    /// rule. A zero `older_than` deletes a row the instant it goes terminal, which
    /// destroys the operational record while an incident is still being diagnosed;
    /// a zero `poll_interval` turns the idle sweep into a busy loop.
    pub fn validate(&self) -> Result<()> {
        if self.older_than.is_zero() {
            return Err(Error::InvalidPurgeConfig {
                field: "older_than",
                reason: "must be greater than zero; retaining nothing deletes rows as they land",
            });
        }
        if self.batch_size <= 0 {
            return Err(Error::InvalidPurgeConfig {
                field: "batch_size",
                reason: "must be greater than zero",
            });
        }
        if self.poll_interval.is_zero() {
            return Err(Error::InvalidPurgeConfig {
                field: "poll_interval",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

/// What one purge batch reclaimed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PurgeStats {
    pub deleted: u64,
}
