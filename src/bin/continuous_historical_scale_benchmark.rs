//! Controlled historical-archive scaling experiment with a real 4 Hz live stream.
use clap::Parser;
use federated_janus::{
    execute_continuous, Anomaly, ExecutionStrategy, InMemoryLiveSource, LiveSource, LogicalPlan,
    Observation, SegmentedHistoricalSource,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const BASE_TIME_MS: u64 = 3_000_000_000;
const PUBLISH_INTERVAL_MS: u64 = 250;
const ARCHIVE_WINDOW_GUARD_MS: u64 = 5_000;
type ExpectedResults = (usize, u64, Vec<(String, u64)>);

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "queries/anomaly.janusql")]
    query: PathBuf,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "100,1000,10000,100000,1000000"
    )]
    historical_quads: Vec<usize>,
    #[arg(long, default_value_t = 10)]
    measured_evaluations: usize,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = 100)]
    historical_sensors: usize,
    #[arg(long, default_value_t = 10)]
    live_sensors: usize,
    #[arg(long, default_value_t = 7)]
    random_seed: u64,
    #[arg(long, default_value_t = 100_000)]
    segment_quads: usize,
    #[arg(long, default_value = "results-continuous-historical-scale")]
    output_dir: PathBuf,
}

#[derive(Clone)]
struct Row {
    quads: usize,
    evaluation: usize,
    order: usize,
    strategy: ExecutionStrategy,
    time_ms: u64,
    historical_start_ms: u64,
    historical_end_ms: u64,
    live_start_ms: u64,
    live_end_ms: u64,
    live_records: u64,
    publisher_events_total: u64,
    rate: f64,
    aggregate_rate: f64,
    drift_ms: i64,
    total_ms: f64,
    source_ms: f64,
    coordinator_ms: f64,
    scanned: u64,
    returned: u64,
    sent: u64,
    received: u64,
    transferred: u64,
    disk: u64,
    segments: usize,
    results: u64,
    hash: u64,
}

fn order(index: usize) -> [ExecutionStrategy; 3] {
    use ExecutionStrategy::{AggregatePushdown as A, BindJoin as B, FetchAll as F};
    match index % 3 {
        0 => [F, A, B],
        1 => [A, B, F],
        _ => [B, F, A],
    }
}
fn hash(results: &[Anomaly]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for r in results {
        for b in format!(
            "{}|{:.12}|{:.12}\n",
            r.sensor, r.current_value, r.historical_average
        )
        .bytes()
        {
            h = (h ^ b as u64).wrapping_mul(0x100000001b3);
        }
    }
    h
}
fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}
fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    if v.len().is_multiple_of(2) {
        (v[v.len() / 2 - 1] + v[v.len() / 2]) / 2.0
    } else {
        v[v.len() / 2]
    }
}
fn stddev(v: &[f64]) -> f64 {
    let m = mean(v);
    (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / v.len() as f64).sqrt()
}
fn p95(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) * 95).div_ceil(100)]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.historical_quads.is_empty()
        || args.historical_quads.contains(&0)
        || args.warmups == 0
        || args.measured_evaluations == 0
        || args.historical_sensors != 100
        || args.live_sensors != 10
        || args
            .historical_quads
            .iter()
            .any(|n| *n < args.historical_sensors)
        || args.segment_quads != 100_000
    {
        return Err("use positive archive sizes >= 100, one or more warmups/evaluations, exactly 100 historical sensors, 10 live sensors, and segment target 100000".into());
    }
    fs::create_dir_all(args.output_dir.join("plots"))?;
    let query_text = fs::read_to_string(&args.query)?;
    let parser = JanusQLParser::new()?;
    let parse_start = Instant::now();
    let parsed = parser.parse(&query_text)?;
    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
    let lower_start = Instant::now();
    let plan = LogicalPlan::lower(&parsed)?;
    let lower_ms = lower_start.elapsed().as_secs_f64() * 1000.0;
    let ((history_start, history_end), (live_start, live_end)) =
        plan.continuous_bounds_ms(BASE_TIME_MS)?;
    if (history_start, history_end, live_start, live_end)
        != (407_940_000, 2_999_940_000, 2_999_940_000, BASE_TIME_MS)
    {
        return Err("query windows are not the required adjacent 30-day historical and 60-second live intervals".into());
    }
    let range_ms = live_end - live_start;
    let step_ms = plan.live_window.slide * 1_000;
    let mut rows = Vec::new();
    let mut setup = String::from("historical_quads,query_parse_ms,query_lowering_ms,archive_build_ms,base_time_ms,historical_window,archive_data_interval,live_window,segment_target,seed\n");

    for &quads in &args.historical_quads {
        let archive_path = args.output_dir.join(format!("segmented-archive-{quads}"));
        if archive_path.exists() {
            fs::remove_dir_all(&archive_path)?;
        }
        let build_start = Instant::now();
        // Each archive stays inside every measured sliding historical window.
        // That keeps archive size as the sole independent variable: all N
        // quads are eligible at every measured timestamp.
        let latest_evaluation_offset_ms = range_ms + args.measured_evaluations as u64 * step_ms;
        let archive_start = history_start + latest_evaluation_offset_ms + ARCHIVE_WINDOW_GUARD_MS;
        let history = SegmentedHistoricalSource::deterministic(
            &archive_path,
            quads,
            100,
            args.random_seed,
            archive_start,
            history_end,
            args.segment_quads,
        )?;
        let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
        let disk = history.disk_bytes()?;
        let segments = history.segment_count()?;
        setup.push_str(&format!("{quads},{parse_ms:.6},{lower_ms:.6},{build_ms:.6},{BASE_TIME_MS},\"[{history_start},{history_end})\",\"[{archive_start},{history_end})\",\"[{live_start},{live_end})\",{},{}\n", args.segment_quads, args.random_seed));

        let live = InMemoryLiveSource::new(Vec::new());
        let stop = Arc::new(AtomicBool::new(false));
        let published = Arc::new(AtomicU64::new(0));
        // Capturing T under this gate makes it impossible for an event that
        // arrived after T to be back-dated into that evaluation's window.
        let timestamp_gate = Arc::new(Mutex::new(()));
        let origin = Instant::now();
        let mut workers = Vec::new();
        for sensor_id in 1..=10u32 {
            let live = live.clone();
            let stop = stop.clone();
            let published = published.clone();
            let timestamp_gate = timestamp_gate.clone();
            workers.push(thread::spawn(move || {
                let mut event = 0u64;
                while !stop.load(Ordering::Acquire) {
                    let due = origin + Duration::from_millis(event * PUBLISH_INTERVAL_MS);
                    if due > Instant::now() {
                        thread::sleep(due - Instant::now());
                    }
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let _gate = timestamp_gate.lock().expect("timestamp gate poisoned");
                    live.publish(Observation::new(
                        // Use the actual publication clock, not a synthetic
                        // preconstructed snapshot or a back-dated schedule.
                        BASE_TIME_MS + origin.elapsed().as_millis() as u64,
                        format!("https://example.org/sensor{sensor_id}"),
                        210.0,
                    ));
                    published.fetch_add(1, Ordering::Relaxed);
                    event += 1;
                }
            }));
        }
        let warmup_due = origin + Duration::from_millis(range_ms);
        if warmup_due > Instant::now() {
            thread::sleep(warmup_due - Instant::now());
        }
        let warmup_time = {
            let _gate = timestamp_gate.lock().expect("timestamp gate poisoned");
            BASE_TIME_MS + origin.elapsed().as_millis() as u64
        };
        for _ in 0..args.warmups {
            for strategy in order(0) {
                let _ = execute_continuous(strategy, &plan, &live, &history, warmup_time)?;
            }
        }

        for evaluation in 0..args.measured_evaluations {
            let logical_elapsed = range_ms + (evaluation as u64 + 1) * step_ms;
            let due = origin + Duration::from_millis(logical_elapsed);
            if due > Instant::now() {
                thread::sleep(due - Instant::now());
            }
            let evaluation_time = {
                let _gate = timestamp_gate.lock().expect("timestamp gate poisoned");
                BASE_TIME_MS + origin.elapsed().as_millis() as u64
            };
            let (
                (evaluation_history_start, evaluation_history_end),
                (evaluation_live_start, evaluation_live_end),
            ) = plan.continuous_bounds_ms(evaluation_time)?;
            if evaluation_live_end != evaluation_time
                || evaluation_history_end != evaluation_live_start
                || evaluation_live_start != evaluation_time - range_ms
            {
                return Err(
                    "continuous bounds are not adjacent at the evaluation timestamp".into(),
                );
            }
            let drift_ms = origin.elapsed().as_millis() as i64 - logical_elapsed as i64;
            let total_published = published.load(Ordering::Acquire);
            let rate = total_published as f64 / (logical_elapsed as f64 / 1_000.0) / 10.0;
            let aggregate_rate = total_published as f64 / (logical_elapsed as f64 / 1_000.0);
            let mut expected: Option<ExpectedResults> = None;
            for (execution_order, strategy) in order(evaluation).into_iter().enumerate() {
                let outcome =
                    execute_continuous(strategy, &plan, &live, &history, evaluation_time)?;
                let result_hash = hash(&outcome.results);
                let averages = outcome
                    .results
                    .iter()
                    .map(|r| (r.sensor.clone(), r.historical_average.to_bits()))
                    .collect::<Vec<_>>();
                if let Some(value) = &expected {
                    if *value != (outcome.results.len(), result_hash, averages.clone()) {
                        return Err(format!("semantic mismatch: historical_quads={quads}, evaluation={evaluation}, strategy={}", strategy.as_str()).into());
                    }
                } else {
                    expected = Some((outcome.results.len(), result_hash, averages));
                }
                let m = outcome.metrics;
                if m.live_records
                    != live
                        .materialize_live_window(evaluation_time - range_ms, evaluation_time)
                        .len() as u64
                {
                    return Err("strategy did not use the common half-open live window".into());
                }
                rows.push(Row {
                    quads,
                    evaluation,
                    order: execution_order,
                    strategy,
                    time_ms: evaluation_time,
                    historical_start_ms: evaluation_history_start,
                    historical_end_ms: evaluation_history_end,
                    live_start_ms: evaluation_live_start,
                    live_end_ms: evaluation_live_end,
                    live_records: m.live_records,
                    publisher_events_total: total_published,
                    rate,
                    aggregate_rate,
                    drift_ms,
                    total_ms: m.end_to_end.as_secs_f64() * 1000.0,
                    source_ms: m.historical_source.as_secs_f64() * 1000.0,
                    coordinator_ms: m.coordinator.as_secs_f64() * 1000.0,
                    scanned: m.historical_records_scanned,
                    returned: m.historical_records,
                    sent: m.bytes_sent_to_historical_source,
                    received: m.bytes_received_from_historical_source,
                    transferred: m.bytes_transferred,
                    disk,
                    segments,
                    results: m.result_cardinality,
                    hash: result_hash,
                });
            }
        }
        stop.store(true, Ordering::Release);
        for worker in workers {
            worker.join().map_err(|_| "publisher panicked")?;
        }
        if published.load(Ordering::Acquire) == 0 {
            return Err("publishers emitted no events".into());
        }
    }
    fs::write(args.output_dir.join("setup_metadata.csv"), setup)?;
    let mut raw = String::from("historical_quads,evaluation_index,execution_order,evaluation_time_ms,historical_window_start_ms,historical_window_end_ms,live_window_start_ms,live_window_end_ms,strategy,live_window_records,total_events_published,publisher_rate_hz_per_sensor,publisher_rate_hz_aggregate,evaluation_drift_ms,total_execution_ms,historical_source_ms,coordinator_ms,historical_records_scanned,historical_records_returned,bytes_sent_to_historical_source,bytes_received_from_historical_source,total_bytes_transferred,storage_disk_bytes,segment_count,result_count,result_hash\n");
    for r in &rows {
        raw.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{}\n",
            r.quads,
            r.evaluation,
            r.order,
            r.time_ms,
            r.historical_start_ms,
            r.historical_end_ms,
            r.live_start_ms,
            r.live_end_ms,
            r.strategy.as_str(),
            r.live_records,
            r.publisher_events_total,
            r.rate,
            r.aggregate_rate,
            r.drift_ms,
            r.total_ms,
            r.source_ms,
            r.coordinator_ms,
            r.scanned,
            r.returned,
            r.sent,
            r.received,
            r.transferred,
            r.disk,
            r.segments,
            r.results,
            r.hash
        ));
    }
    fs::write(
        args.output_dir
            .join("continuous_historical_scale_measurements.csv"),
        raw,
    )?;
    let mut summary = String::from("historical_quads,strategy,n,mean_latency_ms,median_latency_ms,stddev_latency_ms,p95_latency_ms,mean_historical_source_ms,mean_coordinator_ms,mean_historical_records_scanned,mean_historical_records_returned,mean_transferred_bytes,aggregate_pushdown_bytes_over_fetch_all,bind_join_bytes_over_fetch_all,aggregate_pushdown_latency_over_fetch_all,bind_join_latency_over_fetch_all\n");
    for &quads in &args.historical_quads {
        let mut baselines = [(ExecutionStrategy::FetchAll, 0.0, 0.0); 3];
        for (slot, strategy) in [
            ExecutionStrategy::FetchAll,
            ExecutionStrategy::AggregatePushdown,
            ExecutionStrategy::BindJoin,
        ]
        .into_iter()
        .enumerate()
        {
            let group: Vec<_> = rows
                .iter()
                .filter(|r| r.quads == quads && r.strategy == strategy)
                .collect();
            let lat: Vec<_> = group.iter().map(|r| r.total_ms).collect();
            baselines[slot] = (
                strategy,
                mean(&lat),
                mean(
                    &group
                        .iter()
                        .map(|r| r.transferred as f64)
                        .collect::<Vec<_>>(),
                ),
            );
        }
        let fetch_latency = baselines[0].1;
        let fetch_bytes = baselines[0].2;
        for (strategy, latency, bytes) in baselines {
            let group: Vec<_> = rows
                .iter()
                .filter(|r| r.quads == quads && r.strategy == strategy)
                .collect();
            let lat: Vec<_> = group.iter().map(|r| r.total_ms).collect();
            summary.push_str(&format!("{quads},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.6},{:.6},{:.6},{:.6}\n",strategy.as_str(),group.len(),mean(&lat),median(lat.clone()),stddev(&lat),p95(lat),mean(&group.iter().map(|r|r.source_ms).collect::<Vec<_>>()),mean(&group.iter().map(|r|r.coordinator_ms).collect::<Vec<_>>()),mean(&group.iter().map(|r|r.scanned as f64).collect::<Vec<_>>()),mean(&group.iter().map(|r|r.returned as f64).collect::<Vec<_>>()),bytes,if strategy==ExecutionStrategy::AggregatePushdown {bytes/fetch_bytes}else{f64::NAN},if strategy==ExecutionStrategy::BindJoin {bytes/fetch_bytes}else{f64::NAN},if strategy==ExecutionStrategy::AggregatePushdown {latency/fetch_latency}else{f64::NAN},if strategy==ExecutionStrategy::BindJoin {latency/fetch_latency}else{f64::NAN}));
        }
    }
    fs::write(
        args.output_dir
            .join("continuous_historical_scale_summary.csv"),
        summary,
    )?;
    let status = std::process::Command::new("python3")
        .args([
            "scripts/plot_results.py",
            "--root",
            args.output_dir.to_str().ok_or("non-UTF-8 output path")?,
        ])
        .status()?;
    if !status.success() {
        return Err("plot generation failed".into());
    }
    Ok(())
}
