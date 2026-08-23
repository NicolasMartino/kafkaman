use kafkaman_sqlx::ReceivedInsertOutcome;

use crate::Result;

/// Observer invoked after a receive row has committed but before the Kafka
/// offset is acknowledged.
///
/// That window is the one a test needs to interrupt: it is the only point where
/// the row is durable and the record is not yet acknowledged, so returning an
/// error here simulates a crash that must redeliver rather than lose. The
/// consumer propagates the error with `?`, so the offset stays uncommitted.
pub(crate) type PostDurableWriteObserver =
    dyn Fn(IngestCommitEvent) -> Result<()> + Send + Sync + 'static;

/// What the ingest wrote, reported to a [`PostDurableWriteObserver`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct IngestCommitEvent {
    pub partition: i32,
    pub offset: i64,
    pub outcome: ReceivedInsertOutcome,
}
