use crate::planner::ExecutionStrategy;
use std::time::Duration;
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionMetrics {
    pub strategy: ExecutionStrategy,
    pub end_to_end: Duration,
    /// Time used to materialize all live windows. Kept separate from the
    /// historical phase so source-selection work is observable.
    pub live_phase: Duration,
    pub historical_source: Duration,
    pub coordinator: Duration,
    pub live_records: u64,
    pub historical_records: u64,
    pub historical_records_scanned: u64,
    pub historical_entities_looked_up: u64,
    pub historical_records_matched: u64,
    pub bytes_sent_to_historical_source: u64,
    pub bytes_received_from_historical_source: u64,
    pub bytes_transferred: u64,
    pub source_requests: u64,
    /// Source-oriented experiment fields. They remain zero for the preserved
    /// entity-selectivity experiment.
    pub total_sources_declared: u64,
    pub live_sources_declared: u64,
    pub historical_sources_declared: u64,
    pub live_sources_with_window: u64,
    pub historical_sources_contacted: u64,
    pub historical_sources_skipped: u64,
    pub raw_records_transferred: u64,
    pub aggregate_rows_transferred: u64,
    pub result_cardinality: u64,
}
impl ExecutionMetrics {
    pub fn csv_header() -> &'static str {
        "live_sensor_count,strategy,end_to_end_ms,historical_source_ms,coordinator_ms,live_records,historical_records,bytes_transferred,source_requests,result_cardinality"
    }
    pub fn csv_row(&self, live_sensors: usize) -> String {
        format!(
            "{live_sensors},{},{:.3},{:.3},{:.3},{},{},{},{},{}",
            self.strategy.as_str(),
            self.end_to_end.as_secs_f64() * 1000.0,
            self.historical_source.as_secs_f64() * 1000.0,
            self.coordinator.as_secs_f64() * 1000.0,
            self.live_records,
            self.historical_records,
            self.bytes_transferred,
            self.source_requests,
            self.result_cardinality
        )
    }
}
