/// What one [`RdkafkaConsumer::ingest_once`] call did with a single record.
///
/// `partition` and `offset` locate that one record; they are not cumulative,
/// which is why [`IngestLoopStats`]'s cycle accounting sums the counters and drops
/// them.
///
/// [`RdkafkaConsumer::ingest_once`]: crate::RdkafkaConsumer::ingest_once
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngestStats {
    pub consumed: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub skipped: usize,
    pub committed: usize,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngestLoopStats {
    pub cycles: usize,
    pub consumed: usize,
    pub inserted: usize,
    pub duplicates: usize,
    pub skipped: usize,
    pub committed: usize,
    pub transient_errors: usize,
}

impl IngestLoopStats {
    pub(crate) fn record_cycle(&mut self, stats: &IngestStats) {
        self.cycles += 1;
        self.consumed += stats.consumed;
        self.inserted += stats.inserted;
        self.duplicates += stats.duplicates;
        self.skipped += stats.skipped;
        self.committed += stats.committed;
    }
}
