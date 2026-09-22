use crate::sources::compact::CompactHistoricalSource;
use crate::sources::SourceRegistry;
use crate::{
    metrics::ExecutionMetrics,
    planner::{ExecutionStrategy, FederatedLogicalPlan, LogicalPlan},
    sources::{HistoricalSource, LiveSource, Observation},
    strategies,
};
use std::collections::HashSet;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
#[derive(Debug, Clone, PartialEq)]
pub struct Anomaly {
    pub sensor: String,
    pub current_value: f64,
    pub historical_average: f64,
}

/// Executes source fragments lowered from one parsed Janus-QL UNION query.
/// The old multi-query entry point remains below as an archived development
/// stage; this API never receives independently parsed query strings.
pub fn execute_federated(
    strategy: ExecutionStrategy,
    plan: &FederatedLogicalPlan,
    registry: &SourceRegistry,
    evaluation_time: u64,
) -> Result<ExecutionOutcome, ExecutionError> {
    execute_source_oriented(strategy, &plan.branches, registry, evaluation_time)
}
pub fn execute_federated_continuous(
    strategy: ExecutionStrategy,
    plan: &FederatedLogicalPlan,
    registry: &SourceRegistry,
    evaluation_time: u64,
) -> Result<ExecutionOutcome, ExecutionError> {
    execute_source_oriented_with_live_unit(
        strategy,
        &plan.branches,
        registry,
        evaluation_time,
        1_000,
    )
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
    live_events: &[(u32, u64, f64)],
    history: &CompactHistoricalSource,
    evaluation_time: u64,
) -> ExecutionOutcome {
    let (historical_start, historical_end) = plan
        .historical_bounds(evaluation_time)
        .expect("validated Janus historical window");
    let (live_start, live_end) = plan
        .live_bounds(evaluation_time)
        .expect("validated Janus live window");
    let live: Vec<(u32, f64)> = live_events
        .iter()
        .filter(|(_, timestamp, _)| *timestamp >= live_start && *timestamp < live_end)
        .map(|(sensor, _, value)| (*sensor, *value))
        .collect();
    let total = Instant::now();
    let source = Instant::now();
    let (averages, stats) = match strategy {
        ExecutionStrategy::BindJoin => {
            let bound: HashSet<u32> = live.iter().map(|(s, _)| *s).collect();
            history.aggregate_bound(&bound, historical_start, historical_end)
        }
        _ => history.aggregate_all(historical_start, historical_end),
    };
    let historical_source = source.elapsed();
    let coordinator = Instant::now();
    let mut results: Vec<_> = live
        .iter()
        .filter_map(|(sensor, current)| {
            averages
                .get(sensor)
                .filter(|avg| *current > plan.condition.multiplier * *avg)
                .map(|avg| Anomaly {
                    sensor: format!("https://example.org/sensor{sensor}"),
                    current_value: *current,
                    historical_average: *avg,
                })
        })
        .collect();
    results.sort_by(|a, b| a.sensor.cmp(&b.sensor));
    let result_cardinality = results.len() as u64;
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
            live_phase: Duration::ZERO,
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
            total_sources_declared: 0,
            live_sources_declared: 0,
            historical_sources_declared: 0,
            live_sources_with_window: 0,
            historical_sources_contacted: 0,
            historical_sources_skipped: 0,
            raw_records_transferred: 0,
            aggregate_rows_transferred: 0,
            result_cardinality,
        },
        results,
    }
}

/// Execute manually selected source-oriented plans.  Every source is resolved
/// through `SourceRegistry`; no shared multi-sensor historical source exists.
/// `plans` contains one Janus-QL-lowered query for each declared source pair.
pub fn execute_source_oriented(
    strategy: ExecutionStrategy,
    plans: &[LogicalPlan],
    registry: &SourceRegistry,
    evaluation_time: u64,
) -> Result<ExecutionOutcome, ExecutionError> {
    execute_source_oriented_with_live_unit(strategy, plans, registry, evaluation_time, 1)
}
fn execute_source_oriented_with_live_unit(
    strategy: ExecutionStrategy,
    plans: &[LogicalPlan],
    registry: &SourceRegistry,
    evaluation_time: u64,
    live_unit: u64,
) -> Result<ExecutionOutcome, ExecutionError> {
    use std::collections::BTreeMap;
    if !matches!(
        strategy,
        ExecutionStrategy::FetchAllSources
            | ExecutionStrategy::AggregateAllSources
            | ExecutionStrategy::LiveFirstSourceSelection
    ) {
        return Err(ExecutionError(
            "source-oriented execution requires a source-oriented strategy".into(),
        ));
    }
    let (total_sources_declared, live_sources_declared, historical_sources_declared) =
        registry.source_counts();
    if plans.len() as u64 != live_sources_declared {
        return Err(ExecutionError(
            "one lowered Janus-QL query is required per registered sensor source pair".into(),
        ));
    }
    let total = Instant::now();
    let live_phase_start = Instant::now();
    let mut live_by_sensor: BTreeMap<
        u32,
        (
            Vec<Observation>,
            &crate::sources::SensorSourcePair,
            &LogicalPlan,
        ),
    > = BTreeMap::new();
    for plan in plans {
        let pair = registry
            .pair_for_live_iri(&plan.live_window.source_name)
            .ok_or_else(|| {
                ExecutionError(format!(
                    "no registered live source for {}",
                    plan.live_window.source_name
                ))
            })?;
        let historical_pair = registry
            .pair_for_historical_iri(&plan.historical_window.source_name)
            .ok_or_else(|| {
                ExecutionError(format!(
                    "no registered historical source for {}",
                    plan.historical_window.source_name
                ))
            })?;
        if pair.sensor_id != historical_pair.sensor_id {
            return Err(ExecutionError(
                "Janus-QL live and historical IRIs do not identify the same sensor pair".into(),
            ));
        }
        let start = evaluation_time
            .checked_sub(
                plan.live_window
                    .width
                    .checked_mul(live_unit)
                    .ok_or_else(|| ExecutionError("live range overflows timestamp unit".into()))?,
            )
            .ok_or_else(|| ExecutionError("evaluation time precedes live RANGE".into()))?;
        let end = evaluation_time;
        live_by_sensor.insert(
            pair.sensor_id,
            (pair.live.materialize_live_window(start, end), pair, plan),
        );
    }
    let live_phase = live_phase_start.elapsed();
    let live_sources_with_window = live_by_sensor
        .values()
        .filter(|(rows, _, _)| !rows.is_empty())
        .count() as u64;
    let selected: Vec<_> = live_by_sensor
        .values()
        .filter(|(rows, _, _)| {
            strategy != ExecutionStrategy::LiveFirstSourceSelection || !rows.is_empty()
        })
        .collect();
    let historical_phase_start = Instant::now();
    let mut averages = BTreeMap::new();
    let mut historical_records = 0u64;
    let mut historical_scanned = 0u64;
    let mut bytes_received = 0u64;
    for (_, pair, plan) in &selected {
        let (start, end) = plan
            .historical_bounds(evaluation_time)
            .map_err(ExecutionError)?;
        match strategy {
            ExecutionStrategy::FetchAllSources => {
                let rows = pair.historical.materialize_historical_window(start, end);
                historical_scanned += pair.historical.record_count() as u64;
                historical_records += rows.len() as u64;
                bytes_received += rows.iter().map(Observation::serialized_bytes).sum::<u64>();
                let mut sum = 0.0;
                for row in &rows {
                    sum += row.value;
                }
                if !rows.is_empty() {
                    averages.insert(pair.sensor_id, sum / rows.len() as f64);
                }
            }
            _ => {
                let values = pair.historical.averages(None, start, end);
                historical_scanned += pair.historical.record_count() as u64;
                if let Some(avg) = values.values().next() {
                    averages.insert(pair.sensor_id, *avg);
                    historical_records += 1;
                    bytes_received += 12;
                }
            }
        }
    }
    let historical_source = historical_phase_start.elapsed();
    let coordinator_start = Instant::now();
    let mut results = Vec::new();
    let mut live_records = 0u64;
    for (sensor, (rows, _, plan)) in &live_by_sensor {
        live_records += rows.len() as u64;
        if let Some(avg) = averages.get(sensor) {
            for row in rows {
                if row.value > plan.condition.multiplier * avg {
                    results.push(Anomaly {
                        sensor: row.sensor.clone(),
                        current_value: row.value,
                        historical_average: *avg,
                    });
                }
            }
        }
    }
    results.sort_by(|a, b| a.sensor.cmp(&b.sensor));
    let result_cardinality = results.len() as u64;
    let coordinator = coordinator_start.elapsed();
    let historical_sources_contacted = selected.len() as u64;
    Ok(ExecutionOutcome {
        results,
        metrics: ExecutionMetrics {
            strategy,
            end_to_end: total.elapsed(),
            live_phase,
            historical_source,
            coordinator,
            live_records,
            historical_records,
            historical_records_scanned: historical_scanned,
            historical_entities_looked_up: 0,
            historical_records_matched: historical_records,
            bytes_sent_to_historical_source: 0,
            bytes_received_from_historical_source: bytes_received,
            bytes_transferred: bytes_received + live_records * 12,
            source_requests: live_sources_declared + historical_sources_contacted,
            total_sources_declared,
            live_sources_declared,
            historical_sources_declared,
            live_sources_with_window,
            historical_sources_contacted,
            historical_sources_skipped: historical_sources_declared - historical_sources_contacted,
            raw_records_transferred: if strategy == ExecutionStrategy::FetchAllSources {
                historical_records
            } else {
                0
            },
            aggregate_rows_transferred: if strategy == ExecutionStrategy::FetchAllSources {
                0
            } else {
                historical_records
            },
            result_cardinality,
        },
    })
}
pub fn execute(
    strategy: ExecutionStrategy,
    plan: &LogicalPlan,
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    evaluation_time: u64,
) -> Result<ExecutionOutcome, ExecutionError> {
    if matches!(
        strategy,
        ExecutionStrategy::FetchAllSources
            | ExecutionStrategy::AggregateAllSources
            | ExecutionStrategy::LiveFirstSourceSelection
    ) {
        return Err(ExecutionError(
            "source-oriented strategy requires execute_source_oriented".into(),
        ));
    }
    let live_bounds = plan.live_bounds(evaluation_time).map_err(ExecutionError)?;
    let historical_bounds = plan
        .historical_bounds(evaluation_time)
        .map_err(ExecutionError)?;
    let total = Instant::now();
    let source_start = Instant::now();
    let (live_rows, averages): (Vec<Observation>, HashMap<String, f64>) = match strategy {
        ExecutionStrategy::FetchAll => {
            strategies::fetch_all::run(live, history, live_bounds, historical_bounds)
        }
        ExecutionStrategy::AggregatePushdown => {
            strategies::aggregate_pushdown::run(live, history, live_bounds, historical_bounds)
        }
        ExecutionStrategy::BindJoin => {
            strategies::bind_join::run(live, history, live_bounds, historical_bounds)
        }
        ExecutionStrategy::FetchAllSources
        | ExecutionStrategy::AggregateAllSources
        | ExecutionStrategy::LiveFirstSourceSelection => unreachable!("checked above"),
    };
    let source_time = source_start.elapsed();
    let coord = Instant::now();
    let mut results: Vec<_> = live_rows
        .iter()
        .filter_map(|o| {
            averages
                .get(&o.sensor)
                .filter(|avg| o.value > plan.condition.multiplier * *avg)
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
        ExecutionStrategy::FetchAll => history
            .materialize_historical_window(historical_bounds.0, historical_bounds.1)
            .len() as u64,
        _ => averages.len() as u64,
    };
    let historical_records_scanned = history.record_count() as u64;
    let historical_bytes: u64 = match strategy {
        ExecutionStrategy::FetchAll => history
            .materialize_historical_window(historical_bounds.0, historical_bounds.1)
            .iter()
            .map(Observation::serialized_bytes)
            .sum(),
        _ => averages.keys().map(|s| (s.len() + 8) as u64).sum(),
    };
    let live_bytes: u64 = live_rows.iter().map(Observation::serialized_bytes).sum();
    let coordinator = coord.elapsed();
    Ok(ExecutionOutcome {
        metrics: ExecutionMetrics {
            strategy,
            end_to_end: total.elapsed(),
            live_phase: Duration::ZERO,
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
            source_requests: 2,
            total_sources_declared: 0,
            live_sources_declared: 0,
            historical_sources_declared: 0,
            live_sources_with_window: 0,
            historical_sources_contacted: 0,
            historical_sources_skipped: 0,
            raw_records_transferred: 0,
            aggregate_rows_transferred: 0,
            result_cardinality: results.len() as u64,
        },
        results,
    })
}
