//! Which runtime loops a host wants to start.

use std::fmt;
use std::ops::{BitOr, BitOrAssign};

/// A set of kafkaman runtime subsystems.
///
/// Roles describe what the service is capable of: publishing, caching, or
/// handling a message type. Subsystems describe which loops this process should
/// actually run for those roles. An embedded HTTP service usually runs the
/// default set. A worker-role binary can choose explicitly, for example
/// [`Subsystems::PIPELINE`].
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct Subsystems {
    bits: u8,
}

impl Subsystems {
    /// Start relay loops for published message types.
    pub const RELAY: Self = Self { bits: 1 << 0 };
    /// Start Kafka ingest loops for consumed message types.
    pub const INGEST: Self = Self { bits: 1 << 1 };
    /// Start received-row dispatcher loops for consumed message types.
    pub const DISPATCH: Self = Self { bits: 1 << 2 };
    /// Start outbox purger loops for published message types when `[retention]`
    /// is configured.
    pub const PURGE: Self = Self { bits: 1 << 3 };
    /// Start the queue-depth sampler when the `metrics` feature is enabled.
    pub const QUEUE_METRICS: Self = Self { bits: 1 << 4 };
    /// Relay, ingest, dispatch, and queue metrics. Purging stays opt-in.
    pub const PIPELINE: Self = Self {
        bits: Self::RELAY.bits | Self::INGEST.bits | Self::DISPATCH.bits | Self::QUEUE_METRICS.bits,
    };

    /// No loops. Useful for migration-only checks.
    #[must_use]
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// Every loop the declared roles can imply.
    ///
    /// The builder default, because it is the *same* behavior as
    /// [`PIPELINE`](Self::PIPELINE) for every service that has not configured
    /// retention: `PURGE` starts no purger loop without a `[retention]` section,
    /// so a host that never opts in deletes nothing either way. A host that did
    /// opt in configured retention in order to have it applied, which makes
    /// running the purger the answer that surprises nobody. `PIPELINE` is for
    /// the case the default cannot express — retention configured, but this
    /// particular process not the one that should enforce it.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            bits: Self::RELAY.bits
                | Self::INGEST.bits
                | Self::DISPATCH.bits
                | Self::PURGE.bits
                | Self::QUEUE_METRICS.bits,
        }
    }

    /// Relay, ingest, dispatch, and queue metrics. Purging stays opt-in.
    #[must_use]
    pub const fn pipeline() -> Self {
        Self::PIPELINE
    }

    #[must_use]
    pub const fn contains(self, subsystem: Self) -> bool {
        (self.bits & subsystem.bits) == subsystem.bits
    }

    #[must_use]
    pub const fn intersects(self, subsystem: Self) -> bool {
        (self.bits & subsystem.bits) != 0
    }

    #[must_use]
    pub const fn without(self, subsystem: Self) -> Self {
        Self {
            bits: self.bits & !subsystem.bits,
        }
    }
}

impl Default for Subsystems {
    fn default() -> Self {
        Self::all()
    }
}

impl fmt::Debug for Subsystems {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut set = f.debug_set();
        for (flag, name) in [
            (Self::RELAY, "relay"),
            (Self::INGEST, "ingest"),
            (Self::DISPATCH, "dispatch"),
            (Self::PURGE, "purge"),
            (Self::QUEUE_METRICS, "queue-metrics"),
        ] {
            if self.contains(flag) {
                set.entry(&name);
            }
        }
        set.finish()
    }
}

impl BitOr for Subsystems {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self {
            bits: self.bits | rhs.bits,
        }
    }
}

impl BitOrAssign for Subsystems {
    fn bitor_assign(&mut self, rhs: Self) {
        self.bits |= rhs.bits;
    }
}
