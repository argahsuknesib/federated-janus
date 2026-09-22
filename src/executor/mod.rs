use crate::sources::compact::CompactHistoricalSource;
use crate::{
    metrics::ExecutionMetrics,
    planner::{ExecutionStrategy, LogicalPlan},
    sources::{HistoricalSource, LiveSource, Observation},
    strategies,
};
use std::collections::HashSet;
use std::{collections::HashMap, time::Instant};
#[derive(Debug, Clone, PartialEq)]
pub struct Anomaly {
    pub sensor: String,
    pub current_value: f64,
    pub historical_average: f64,
}
#[derive(Debug, Clone)]
pub struct ExecutionOutcome {
    pub results: Vec<Anomaly>,
    pub metrics: ExecutionMetrics,
}
#[derive(Debug)]
pub struct ExecutionError(pub String);
impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ExecutionError {}
/// Executes the fixed benchmark query over compact numeric sensor IDs. This is
/// intentionally benchmark-only; it avoids treating RDF string allocation as a plan cost.
pub fn execute_compact(
    strategy: ExecutionStrategy,
    plan: &LogicalPlan,
    live: &[(u32, f64)],
    history: &CompactHistoricalSource,
    start: u64,
    end: u64,
) -> ExecutionOutcome {
    let total = Instant::now();
    let source = Instant::now();
    let (averages, stats) = match strategy {
        ExecutionStrategy::BindJoin => {
            let bound: HashSet<u32> = live.iter().map(|(s, _)| *s).collect();
            history.aggregate_bound(&bound, start, end)
        }
        _ => history.aggregate_all(start, end),
    };
    let historical_source = source.elapsed();
    let coordinator = Instant::now();
    let mut results: Vec<_> = live
        .iter()
        .filter_map(|(sensor, current)| {
            averages
                .get(sensor)
                .filter(|avg| *current > plan.anomaly_multiplier * *avg)
                .map(|avg| Anomaly {
                    sensor: format!("https://example.org/sensor{sensor}"),
                    current_value: *current,
                    historical_average: *avg,
                })
        })
        .collect();
    results.sort_by(|a, b| a.sensor.cmp(&b.sensor));
    let returned = match strategy {
        ExecutionStrategy::FetchAll => stats.records_matched,
        _ => stats.records_returned,
    };
    let received = match strategy {
        ExecutionStrategy::FetchAll => stats.records_matched * 24,
        _ => stats.records_returned * 12,
    };
    let sent = if strategy == ExecutionStrategy::BindJoin {
        live.iter().map(|_| 4u64).sum()
    } else {
        0
    };
    ExecutionOutcome {
        metrics: ExecutionMetrics {
            strategy,
            end_to_end: total.elapsed(),
            historical_source,
            coordinator: coordinator.elapsed(),
            live_records: live.len() as u64,
            historical_records: returned,
            historical_records_scanned: stats.records_scanned,
            historical_entities_looked_up: stats.entities_looked_up,
            historical_records_matched: stats.records_matched,
            bytes_sent_to_historical_source: sent,
            bytes_received_from_historical_source: received,
            bytes_transferred: sent + received + live.len() as u64 * 12,
            source_requests: 2,
            result_cardinality: results.len() as u64,
        },
        results,
    }
}
pub fn execute(
    strategy: ExecutionStrategy,
    plan: &LogicalPlan,
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
) -> Result<ExecutionOutcome, ExecutionError> {
    let total = Instant::now();
    let source_start = Instant::now();
    let (live_rows, averages): (Vec<Observation>, HashMap<String, f64>) = match strategy {
        ExecutionStrategy::FetchAll => strategies::fetch_all::run(live, history),
        ExecutionStrategy::AggregatePushdown => strategies::aggregate_pushdown::run(live, history),
        ExecutionStrategy::BindJoin => strategies::bind_join::run(live, history),
    };
    let source_time = source_start.elapsed();
    let coord = Instant::now();
    let mut results: Vec<_> = live_rows
        .iter()
        .filter_map(|o| {
            averages
                .get(&o.sensor)
                .filter(|avg| o.value > plan.anomaly_multiplier * *avg)
                .map(|avg| Anomaly {
                    sensor: o.sensor.clone(),
                    current_value: o.value,
                    historical_average: *avg,
                })
        })
        .collect();
    results.sort_by(|a, b| {
        a.sensor
            .cmp(&b.sensor)
            .then(a.current_value.total_cmp(&b.current_value))
    });
    let historical_records = match strategy {
        ExecutionStrategy::FetchAll => history.materialize_historical_window().len() as u64,
        _ => averages.len() as u64,
    };
    let historical_records_scanned = history.record_count() as u64;
    let historical_bytes: u64 = match strategy {
        ExecutionStrategy::FetchAll => history
            .materialize_historical_window()
            .iter()
            .map(Observation::serialized_bytes)
            .sum(),
        _ => averages.iter().map(|(s, _)| (s.len() + 8) as u64).sum(),
    };
    let live_bytes: u64 = live_rows.iter().map(Observation::serialized_bytes).sum();
    let coordinator = coord.elapsed();
    Ok(ExecutionOutcome {
        metrics: ExecutionMetrics {
            strategy,
            end_to_end: total.elapsed(),
            historical_source: source_time,
            coordinator,
            live_records: live_rows.len() as u64,
            historical_records,
            historical_records_scanned,
            historical_entities_looked_up: 0,
            historical_records_matched: historical_records,
            bytes_sent_to_historical_source: if strategy == ExecutionStrategy::BindJoin {
                live_rows.iter().map(|row| row.sensor.len() as u64).sum()
            } else {
                0
            },
            bytes_received_from_historical_source: historical_bytes,
            bytes_transferred: live_bytes + historical_bytes,
            source_requests: if strategy == ExecutionStrategy::BindJoin {
                2
            } else {
                2
            },
            result_cardinality: results.len() as u64,
        },
        results,
    })
}
