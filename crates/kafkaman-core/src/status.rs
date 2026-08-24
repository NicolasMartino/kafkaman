use crate::enum_macros::sql_enum;
use crate::Error;

sql_enum! {
    /// Where an outbox row is in its journey to the broker.
    ///
    /// `Superseded` is the one that is not a step: it marks a row a newer
    /// snapshot of the same entity overtook before it was ever published.
    pub enum OutboxStatus {
        Pending,
        Publishing,
        Published,
        Superseded,
        Failed,
    }
    invalid = Error::InvalidOutboxStatus;
}

sql_enum! {
    /// Where a received row is in its journey through dispatch.
    ///
    /// `Retryable` and `Failed` are distinct on purpose: a `Retryable` row has
    /// budget left and a scheduled `next_attempt_at`, so it recovers on its own,
    /// while `Failed` is terminal and only a redrive moves it.
    pub enum ReceiveStatus {
        Pending,
        Processing,
        Processed,
        Retryable,
        Failed,
    }
    invalid = Error::InvalidReceiveStatus;
}

impl OutboxStatus {
    /// Whether no worker will ever act on a row in this status again.
    ///
    /// Retention's delete predicate and the queue-age warning both need this
    /// split, and they must agree: a status the purger treats as reclaimable is
    /// by definition not queued work, so its age must never raise a backlog
    /// warning.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Published | Self::Superseded | Self::Failed)
    }
}

impl ReceiveStatus {
    /// Whether no dispatcher will pick this row up again without an operator
    /// redrive. `Failed` counts: a DLQ row waits for a human, not the queue.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Processed | Self::Failed)
    }
}
