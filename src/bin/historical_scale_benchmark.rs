//! Benchmark the three explicit execution strategies while varying only the
//! amount of historical RDF data stored in Janus's segmented storage.
use clap::Parser;
use federated_janus::{
    execute, Anomaly, ExecutionStrategy, InMemoryLiveSource, LogicalPlan, Observation,
    SegmentedHistoricalSource,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{
    fs,
    path::PathBuf,
    process,
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "queries/anomaly.janusql")]
    query: PathBuf,

    /// Total historical RDF quads in the single historical source.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "100,1000,10000,100000,1000000"
    )]
    historical_quads: Vec<usize>,

    /// Fixed number of sensor subjects represented in every historical archive.
    #[arg(long, default_value_t = 100)]
    historical_sensors: usize,

    /// Fixed live-side cardinality. This is deliberately not varied.
    #[arg(long, default_value_t = 10)]
    live_sensors: usize,

    /// Maximum number of quads written before forcing a Janus segment flush.
    #[arg(long, default_value_t = 100_000)]
    segment_quads: usize,

    #[arg(long, default_value_t = 1)]
    warmups: usize,

    #[arg(long, default_value_t = 5)]
    repetitions: usize,

    #[arg(long, default_value_t = 7)]
    random_seed: u64,

    #[arg(long, default_value = "results-historical-scale")]
    output_dir: PathBuf,
}

#[derive(Clone)]
struct Row {
    strategy: ExecutionStrategy,
    historical_quads: usize,
    repetition: usize,
    execution_order: usize,
    latency_ms: f64,
    source_ms: f64,
    coordinator_ms: f64,
    scanned: u64,
    returned: u64,
    sent: u64,
    received: u64,
    transferred: u64,
    requests: u64,
    results: u64,
    hash: u64,
    storage_disk_bytes: u64,
    segment_count: usize,
    build_ms: f64,
}

fn strategies_for(rep: usize) -> [ExecutionStrategy; 3] {
    let all = [
        ExecutionStrategy::FetchAll,
        ExecutionStrategy::AggregatePushdown,
        ExecutionStrategy::BindJoin,
    ];
    match rep % 3 {
        0 => [all[0], all[1], all[2]],
        1 => [all[1], all[2], all[0]],
        _ => [all[2], all[0], all[1]],
    }
}

fn hash(results: &[Anomaly]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for result in results {
        for byte in format!(
            "{}|{:.12}|{:.12}\n",
            result.sensor, result.current_value, result.historical_average
        )
        .bytes()
        {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    if values.len().is_multiple_of(2) {
        (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.0
    } else {
        values[values.len() / 2]
    }
}

fn stddev(values: &[f64]) -> f64 {
    let m = mean(values);
    (values.iter().map(|x| (x - m).powi(2)).sum::<f64>() / values.len() as f64).sqrt()
}

fn p95(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) * 95).div_ceil(100)]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.historical_quads.is_empty()
        || args.historical_quads.contains(&0)
        || args.historical_sensors == 0
        || args.historical_quads.iter().any(|&n| n < args.historical_sensors)
        || args.live_sensors == 0
        || args.live_sensors > args.historical_sensors
        || args.segment_quads == 0
        || args.repetitions == 0
    {
        return Err(
            "historical quads/sensors, live sensors, repetitions and segment size must be positive; every archive must represent all historical sensors, and live_sensors must not exceed historical_sensors"
                .into(),
        );
    }

    let query_text = fs::read_to_string(&args.query)?;
    let parser = JanusQLParser::new()?;

    let parse_start = Instant::now();
    let parsed = parser.parse(&query_text)?;
    let parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

    let lower_start = Instant::now();
    let plan = LogicalPlan::lower(&parsed)?;
    let lowering_ms = lower_start.elapsed().as_secs_f64() * 1000.0;

    let evaluation_time = 3_000_000u64;
    let (historical_start, historical_end) = plan.historical_bounds(evaluation_time)?;
    let (live_start, live_end) = plan.live_bounds(evaluation_time)?;

    let base = 100.0 + (args.random_seed % 5) as f64;
    let live = InMemoryLiveSource::new(
        (1..=args.live_sensors)
            .map(|sensor_id| {
                Observation::new(
                    live_start + 1,
                    format!("https://example.org/sensor{sensor_id}"),
                    base * 2.0,
                )
            })
            .collect(),
    );

    fs::create_dir_all(args.output_dir.join("plots"))?;

    let mut rows = Vec::new();

    for &quad_count in &args.historical_quads {
        let storage_path = std::env::temp_dir().join(format!(
            "federated-janus-segmented-{}-{quad_count}",
            process::id()
        ));

        let build_start = Instant::now();
        let history = SegmentedHistoricalSource::deterministic(
            &storage_path,
            quad_count,
            args.historical_sensors,
            args.random_seed,
            historical_start,
            historical_end,
            args.segment_quads,
        )?;
        let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
        let storage_disk_bytes = history.disk_bytes()?;
        let segment_count = history.segment_count()?;

        let mut expected: Option<(usize, u64)> = None;

        for warmup in 0..args.warmups {
            for strategy in strategies_for(warmup) {
                let outcome = execute(strategy, &plan, &live, &history, evaluation_time)?;
                let result_hash = hash(&outcome.results);
                if let Some(expected_value) = expected {
                    if expected_value != (outcome.results.len(), result_hash) {
                        return Err(format!(
                            "semantic mismatch during warmup: historical_quads={quad_count} strategy={}",
                            strategy.as_str()
                        )
                        .into());
                    }
                } else {
                    expected = Some((outcome.results.len(), result_hash));
                }
            }
        }

        for repetition in 0..args.repetitions {
            for (execution_order, strategy) in
                strategies_for(repetition).into_iter().enumerate()
            {
                let outcome = execute(strategy, &plan, &live, &history, evaluation_time)?;
                let result_hash = hash(&outcome.results);

                if let Some(expected_value) = expected {
                    if expected_value != (outcome.results.len(), result_hash) {
                        return Err(format!(
                            "semantic mismatch: historical_quads={quad_count} strategy={}",
                            strategy.as_str()
                        )
                        .into());
                    }
                } else {
                    expected = Some((outcome.results.len(), result_hash));
                }

                let metrics = outcome.metrics;
                rows.push(Row {
                    strategy,
                    historical_quads: quad_count,
                    repetition,
                    execution_order,
                    latency_ms: metrics.end_to_end.as_secs_f64() * 1000.0,
                    source_ms: metrics.historical_source.as_secs_f64() * 1000.0,
                    coordinator_ms: metrics.coordinator.as_secs_f64() * 1000.0,
                    scanned: metrics.historical_records_scanned,
                    returned: metrics.historical_records,
                    sent: metrics.bytes_sent_to_historical_source,
                    received: metrics.bytes_received_from_historical_source,
                    transferred: metrics.bytes_transferred,
                    requests: metrics.source_requests,
                    results: metrics.result_cardinality,
                    hash: result_hash,
                    storage_disk_bytes,
                    segment_count,
                    build_ms,
                });
            }
        }

        drop(history);
        if storage_path.exists() {
            fs::remove_dir_all(storage_path)?;
        }
    }

    let mut measurements = String::from(
        "strategy,historical_quads,historical_sensors,fixed_live_sensors,repetition,execution_order,evaluation_time,total_latency_ms,historical_source_time_ms,coordinator_time_ms,historical_records_scanned,historical_records_returned,bytes_sent_to_historical_source,bytes_received_from_historical_source,total_bytes_transferred,source_requests,result_count,result_hash,storage_disk_bytes,segment_count,storage_build_ms\n",
    );
    for row in &rows {
        measurements.push_str(&format!(
            "{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{:.6}\n",
            row.strategy.as_str(),
            row.historical_quads,
            args.historical_sensors,
            args.live_sensors,
            row.repetition,
            row.execution_order,
            evaluation_time,
            row.latency_ms,
            row.source_ms,
            row.coordinator_ms,
            row.scanned,
            row.returned,
            row.sent,
            row.received,
            row.transferred,
            row.requests,
            row.results,
            row.hash,
            row.storage_disk_bytes,
            row.segment_count,
            row.build_ms
        ));
    }
    fs::write(
        args.output_dir.join("historical_scale_measurements.csv"),
        measurements,
    )?;

    let mut summary = String::from(
        "strategy,historical_quads,n,mean_latency_ms,median_latency_ms,stddev_latency_ms,p95_latency_ms,mean_historical_source_time_ms,mean_coordinator_time_ms,mean_historical_records_scanned,mean_historical_records_returned,mean_transferred_bytes,storage_disk_bytes,segment_count\n",
    );

    for &quad_count in &args.historical_quads {
        for strategy in [
            ExecutionStrategy::FetchAll,
            ExecutionStrategy::AggregatePushdown,
            ExecutionStrategy::BindJoin,
        ] {
            let group: Vec<_> = rows
                .iter()
                .filter(|row| {
                    row.historical_quads == quad_count && row.strategy == strategy
                })
                .collect();
            let latencies: Vec<_> = group.iter().map(|row| row.latency_ms).collect();
            summary.push_str(&format!(
                "{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{},{}\n",
                strategy.as_str(),
                quad_count,
                group.len(),
                mean(&latencies),
                median(latencies.clone()),
                stddev(&latencies),
                p95(latencies),
                mean(&group.iter().map(|row| row.source_ms).collect::<Vec<_>>()),
                mean(
                    &group
                        .iter()
                        .map(|row| row.coordinator_ms)
                        .collect::<Vec<_>>()
                ),
                mean(&group.iter().map(|row| row.scanned as f64).collect::<Vec<_>>()),
                mean(
                    &group
                        .iter()
                        .map(|row| row.returned as f64)
                        .collect::<Vec<_>>()
                ),
                mean(
                    &group
                        .iter()
                        .map(|row| row.transferred as f64)
                        .collect::<Vec<_>>()
                ),
                group[0].storage_disk_bytes,
                group[0].segment_count
            ));
        }
    }
    fs::write(args.output_dir.join("historical_scale_summary.csv"), summary)?;

    fs::write(
        args.output_dir.join("query_metadata.csv"),
        format!(
            "query,storage_backend,historical_quads,fixed_historical_sensors,fixed_live_sensors,segment_quads,query_parse_ms,query_lowering_ms,evaluation_time,live_window,historical_window,indexing_note\n{},Janus StreamingSegmentedStorage,"{}",{},{},{},{parse_ms:.6},{lowering_ms:.6},{evaluation_time},"[{live_start},{live_end})","[{historical_start},{historical_end})","timestamp sparse/two-level index; no subject/predicate inverted index"\n",
            args.query.display(),
            args.historical_quads
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join("|"),
            args.historical_sensors,
            args.live_sensors,
            args.segment_quads,
        ),
    )?;

    let status = std::process::Command::new("python3")
        .args(["scripts/plot_results.py", "--root", "."])
        .status()?;
    if !status.success() {
        return Err("plot generation failed".into());
    }

    println!(
        "completed {} historical sizes × 3 strategies × {} repetitions",
        args.historical_quads.len(),
        args.repetitions
    );
    Ok(())
}
