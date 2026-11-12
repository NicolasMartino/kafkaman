/// Largest number of rows one redrive may move back to `Pending`.
///
/// Redrive hands rows back to the dispatcher, so an unbounded request is a way
/// to give it an entire DLQ in one transaction. Operators who need more issue
/// more requests, which also gives them a natural place to stop.
pub const MAX_REDRIVE_ROWS: i64 = 10_000;
