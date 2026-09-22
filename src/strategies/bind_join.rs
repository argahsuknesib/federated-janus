use super::StrategyOutput;
use crate::sources::{HistoricalSource, LiveSource};
use std::collections::HashSet;

pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    live_bounds: (u64, u64),
    historical_bounds: (u64, u64),
) -> StrategyOutput {
    let live_rows = live.materialize_live_window(live_bounds.0, live_bounds.1);
    let bindings: HashSet<_> = live_rows.iter().map(|row| row.sensor.clone()).collect();
    let binding_bytes_sent = bindings.iter().map(|sensor| sensor.len() as u64).sum();
    let averages = history.averages(Some(&bindings), historical_bounds.0, historical_bounds.1);
    let historical_bytes_received = averages
        .keys()
        .map(|sensor| (sensor.len() + std::mem::size_of::<f64>()) as u64)
        .sum();

    StrategyOutput {
        historical_rows_returned: averages.len() as u64,
        live_rows,
        averages,
        historical_bytes_received,
        binding_bytes_sent,
    }
}
