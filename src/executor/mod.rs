use crate::planner::LocalExecutionPlan;
use crate::sources::SegmentedHistoricalSource;
use crate::sources::SourceRegistry;
use crate::{
    metrics::ExecutionMetrics,
    planner::{ExecutionStrategy, FederatedLogicalPlan, LogicalPlan},
    sources::{HistoricalSource, LiveSource, Observation},
    strategies,
};
use janus::storage::segmented_storage::AccessMetrics;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
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
/// Storage telemetry is separate from `operator_output_rows`: the latter is
/// the cardinality of the historical aggregate operator, not storage rows.
#[derive(Debug, Clone)]
pub struct LocalPlanOutcome {
    pub results: Vec<Anomaly>,
    pub historical_averages: HashMap<String, f64>,
    pub storage: AccessMetrics,
    pub operator_output_rows: u64,
    pub live_bindings: HashSet<String>,
    pub historical_interval: (u64, u64),
    pub historical_storage: Duration,
    pub historical_operator: Duration,
    pub coordinator: Duration,
    pub total_execution: Duration,
}

/// Execute one explicit local plan against the real segmented Janus archive.
pub fn execute_local_plan(
    plan_kind: LocalExecutionPlan,
    plan: &LogicalPlan,
    live: &dyn LiveSource,
    history: &SegmentedHistoricalSource,
    evaluation_time: u64,
) -> Result<LocalPlanOutcome, ExecutionError> {
    let total_start = Instant::now();
    let live_bounds = plan.live_bounds(evaluation_time).map_err(ExecutionError)?;
    let historical_interval = plan
        .historical_bounds(evaluation_time)
        .map_err(ExecutionError)?;
    let live_rows = live.materialize_live_window(live_bounds.0, live_bounds.1);
    let live_bindings = live_rows
        .iter()
        .map(|row| row.sensor.clone())
        .collect::<HashSet<_>>();
    let storage_start = Instant::now();
    let (historical_rows, storage) = match plan_kind {
        LocalExecutionPlan::AggregatePushdown | LocalExecutionPlan::TimestampOnlyBindJoin => {
            history
                .rows_with_metrics(historical_interval.0, historical_interval.1)
                .map_err(|e| ExecutionError(e.to_string()))?
        }
        LocalExecutionPlan::SubjectAwareBindJoin
        | LocalExecutionPlan::SubjectAwareLinearBindJoin
        | LocalExecutionPlan::SubjectAwareBinaryBindJoin => {
            let mode = if plan_kind == LocalExecutionPlan::SubjectAwareBinaryBindJoin {
                janus::storage::segmented_storage::SubjectAccessMode::Binary
            } else {
                janus::storage::segmented_storage::SubjectAccessMode::Linear
            };
            let (rows, metrics) = history
                .rows_for_subjects_mode(
                    historical_interval.0,
                    historical_interval.1,
                    &live_bindings,
                    mode,
                )
                .map_err(|e| ExecutionError(e.to_string()))?;
            if !metrics.subject_index_used {
                return Err(ExecutionError("subject-aware local plan requires a valid index for every queried persisted segment".into()));
            }
            (rows, metrics)
        }
    };
    let historical_storage = storage_start.elapsed();
    let operator_start = Instant::now();
    let aggregate_input = if plan_kind == LocalExecutionPlan::AggregatePushdown {
        historical_rows
    } else {
        historical_rows
            .into_iter()
            .filter(|row| live_bindings.contains(&row.sensor))
            .collect()
    };
    let mut sums = HashMap::<String, (f64, u64)>::new();
    for row in aggregate_input {
        let sum = sums.entry(row.sensor).or_insert((0.0, 0));
        sum.0 += row.value;
        sum.1 += 1;
    }
    let historical_averages = sums
        .into_iter()
        .map(|(sensor, (sum, count))| (sensor, sum / count as f64))
        .collect::<HashMap<_, _>>();
    let operator_output_rows = historical_averages.len() as u64;
    let historical_operator = operator_start.elapsed();
    let coordinator_start = Instant::now();
    let mut results = live_rows
        .into_iter()
        .filter_map(|row| {
            historical_averages
                .get(&row.sensor)
                .filter(|average| row.value > plan.condition.multiplier * **average)
                .map(|average| Anomaly {
                    sensor: row.sensor,
                    current_value: row.value,
                    historical_average: *average,
                })
        })
        .collect::<Vec<_>>();
    results.sort_by(|left, right| left.sensor.cmp(&right.sensor));
    Ok(LocalPlanOutcome {
        results,
        historical_averages,
        storage,
        operator_output_rows,
        live_bindings,
        historical_interval,
        historical_storage,
        historical_operator,
        coordinator: coordinator_start.elapsed(),
        total_execution: total_start.elapsed(),
    })
}
#[derive(Debug)]
pub struct ExecutionError(pub String);
impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ExecutionError {}
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
        let (start, end) = if live_unit == 1_000 {
            plan.continuous_bounds_ms(evaluation_time)
                .map_err(ExecutionError)?
                .1
        } else {
            plan.live_bounds(evaluation_time).map_err(ExecutionError)?
        };
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
        let (start, end) = if live_unit == 1_000 {
            plan.continuous_bounds_ms(evaluation_time)
                .map_err(ExecutionError)?
                .0
        } else {
            plan.historical_bounds(evaluation_time)
                .map_err(ExecutionError)?
        };
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
    execute_with_bounds(
        strategy,
        plan,
        live,
        history,
        plan.live_bounds(evaluation_time).map_err(ExecutionError)?,
        plan.historical_bounds(evaluation_time)
            .map_err(ExecutionError)?,
    )
}

/// Execute one lowered query against millisecond continuous timestamps.
pub fn execute_continuous(
    strategy: ExecutionStrategy,
    plan: &LogicalPlan,
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    evaluation_time_ms: u64,
) -> Result<ExecutionOutcome, ExecutionError> {
    let (historical_bounds, live_bounds) = plan
        .continuous_bounds_ms(evaluation_time_ms)
        .map_err(ExecutionError)?;
    execute_with_bounds(
        strategy,
        plan,
        live,
        history,
        live_bounds,
        historical_bounds,
    )
}

fn execute_with_bounds(
    strategy: ExecutionStrategy,
    plan: &LogicalPlan,
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    live_bounds: (u64, u64),
    historical_bounds: (u64, u64),
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

    let total = Instant::now();
    let source_start = Instant::now();
    let strategies::StrategyOutput {
        live_rows,
        averages,
        historical_rows_returned,
        historical_bytes_received,
        binding_bytes_sent,
    } = match strategy {
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
    let historical_source = source_start.elapsed();

    let coordinator_start = Instant::now();
    let mut results: Vec<_> = live_rows
        .iter()
        .filter_map(|row| {
            averages
                .get(&row.sensor)
                .filter(|avg| row.value > plan.condition.multiplier * *avg)
                .map(|avg| Anomaly {
                    sensor: row.sensor.clone(),
                    current_value: row.value,
                    historical_average: *avg,
                })
        })
        .collect();
    results.sort_by(|left, right| {
        left.sensor
            .cmp(&right.sensor)
            .then(left.current_value.total_cmp(&right.current_value))
    });
    let coordinator = coordinator_start.elapsed();

    // The current Janus segmented store is indexed by timestamp, not by
    // subject/predicate. The historical-scale benchmark places the full archive
    // inside the selected historical interval, so each strategy scans the full
    // historical quad count. BindJoin still reduces bindings/results transferred,
    // but it does not claim subject-index I/O pruning.
    let historical_records_scanned = history.record_count() as u64;
    let live_bytes: u64 = live_rows.iter().map(Observation::serialized_bytes).sum();

    Ok(ExecutionOutcome {
        metrics: ExecutionMetrics {
            strategy,
            end_to_end: total.elapsed(),
            live_phase: Duration::ZERO,
            historical_source,
            coordinator,
            live_records: live_rows.len() as u64,
            historical_records: historical_rows_returned,
            historical_records_scanned,
            historical_entities_looked_up: 0,
            historical_records_matched: historical_records_scanned,
            bytes_sent_to_historical_source: binding_bytes_sent,
            bytes_received_from_historical_source: historical_bytes_received,
            bytes_transferred: live_bytes + binding_bytes_sent + historical_bytes_received,
            source_requests: 2,
            total_sources_declared: 0,
            live_sources_declared: 0,
            historical_sources_declared: 0,
            live_sources_with_window: 0,
            historical_sources_contacted: 0,
            historical_sources_skipped: 0,
            raw_records_transferred: if strategy == ExecutionStrategy::FetchAll {
                historical_rows_returned
            } else {
                0
            },
            aggregate_rows_transferred: if strategy == ExecutionStrategy::FetchAll {
                0
            } else {
                historical_rows_returned
            },
            result_cardinality: results.len() as u64,
        },
        results,
    })
}
