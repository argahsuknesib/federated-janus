use super::StrategyOutput;
use crate::sources::{HistoricalSource, LiveSource};
use std::collections::HashMap;

pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    live_bounds: (u64, u64),
    historical_bounds: (u64, u64),
) -> StrategyOutput {
    let live_rows = live.materialize_live_window(live_bounds.0, live_bounds.1);
    let historical_rows =
        history.materialize_historical_window(historical_bounds.0, historical_bounds.1);
    let historical_rows_returned = historical_rows.len() as u64;
    let historical_bytes_received = historical_rows
        .iter()
        .map(crate::sources::Observation::serialized_bytes)
        .sum();

    let mut sums = HashMap::<String, (f64, u64)>::new();
    for row in historical_rows {
        let entry = sums.entry(row.sensor).or_insert((0.0, 0));
        entry.0 += row.value;
        entry.1 += 1;
    }
    let averages = sums
        .into_iter()
        .map(|(sensor, (sum, count))| (sensor, sum / count as f64))
        .collect();

    StrategyOutput {
        live_rows,
        averages,
        historical_rows_returned,
        historical_bytes_received,
        binding_bytes_sent: 0,
    }
}
