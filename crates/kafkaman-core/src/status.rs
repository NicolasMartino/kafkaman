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
