//! Wall-clock continuous-stream smoke benchmark.  It deliberately has no
//! networking: each source remains an independently addressable in-process edge.
use clap::Parser;
use federated_janus::{
    execute_federated_continuous, generate_federated_anomaly_query, Anomaly, ExecutionStrategy,
    FederatedLogicalPlan, Observation, SensorSourcePair, SourceRegistry,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

// Janus-QL OFFSET literals are seconds.  Continuous event timestamps are
// milliseconds, so this must safely exceed the 30-day OFFSET in milliseconds.
const BASE_TIME_MS: u64 = 3_000_000_000;
#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 5)]
    source_pairs: usize,
    #[arg(long, default_value = "1,3,5")]
    active_schedule: String,
    #[arg(long, default_value_t = 3)]
    measured_evaluations: usize,
    #[arg(long, default_value = "results-continuous-realtime")]
    output_dir: PathBuf,
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
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    let schedule: Vec<usize> = a
        .active_schedule
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    if a.source_pairs == 0
        || schedule.len() < a.measured_evaluations
        || schedule.iter().any(|n| *n > a.source_pairs)
    {
        return Err("schedule must provide one 0..=source_pairs entry per evaluation".into());
    }
    fs::create_dir_all(a.output_dir.join("plots"))?;
    let query = generate_federated_anomaly_query(a.source_pairs);
    let parser = JanusQLParser::new()?;
    let setup = Instant::now();
    let parsed = parser.parse(&query)?;
    let (plan, lower, decompose) = FederatedLogicalPlan::lower_timed(&parsed)?;
    let setup_ms = setup.elapsed().as_secs_f64() * 1000.;
    let range_ms = plan.branches[0].live_window.width * 1000;
    let step_ms = plan.branches[0].live_window.slide * 1000;
    if range_ms != 60_000 || step_ms != 30_000 || plan.branches.len() != a.source_pairs {
        return Err("parsed query is not the registered RANGE 60 STEP 30 federation".into());
    }
    let mut registry = SourceRegistry::default();
    for id in 1..=a.source_pairs as u32 {
        let sensor = format!("https://example.org/sensor{id}");
        registry.register(SensorSourcePair::new(
            id,
            vec![],
            vec![Observation::new(1, sensor, 100.)],
        ))?;
    }
    let active = Arc::new(AtomicUsize::new(schedule[0]));
    let stop = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicU64::new(0));
    let origin = Instant::now();
    let mut workers = Vec::new();
    for id in 1..=a.source_pairs as u32 {
        let source = registry.live_source(id).unwrap();
        let active = active.clone();
        let stop = stop.clone();
        let received = received.clone();
        workers.push(thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Acquire) {
                let due = origin + Duration::from_millis(n * 250);
                let now = Instant::now();
                if due > now {
                    thread::sleep(due - now);
                }
                if stop.load(Ordering::Acquire) {
                    break;
                }
                if id as usize <= active.load(Ordering::Acquire) {
                    let t = BASE_TIME_MS + origin.elapsed().as_millis() as u64;
                    source.publish(Observation::new(
                        t,
                        format!("https://example.org/sensor{id}"),
                        200.,
                    ));
                    received.fetch_add(1, Ordering::Relaxed);
                }
                n += 1;
            }
        }));
    }
    let mut csv=String::from("evaluation_index,evaluation_time,elapsed_stream_time_s,strategy,live_events_received_total,live_events_received_since_previous_evaluation,live_window_records,live_sources_declared,live_sources_with_results,historical_sources_contacted,historical_sources_skipped,historical_records_scanned,bytes_transferred,source_requests,execution_latency_ms,live_phase_ms,historical_phase_ms,coordinator_ms,result_count,result_hash,publisher_rate_hz_per_source\n");
    let mut previous = 0;
    let mut chart = String::new();
    for index in 0..a.measured_evaluations {
        let due = origin + Duration::from_millis(range_ms + index as u64 * step_ms);
        if due > Instant::now() {
            thread::sleep(due - Instant::now());
        }
        let elapsed = origin.elapsed().as_millis() as u64;
        let evaluation_time = BASE_TIME_MS + elapsed;
        let total = received.load(Ordering::Acquire);
        let mut expected = None;
        for strategy in [
            ExecutionStrategy::FetchAllSources,
            ExecutionStrategy::AggregateAllSources,
            ExecutionStrategy::LiveFirstSourceSelection,
        ] {
            let out = execute_federated_continuous(strategy, &plan, &registry, evaluation_time)?;
            let h = hash(&out.results);
            if let Some(v) = expected {
                if v != (out.results.len(), h) {
                    return Err(format!("result mismatch at evaluation {index}").into());
                }
            } else {
                expected = Some((out.results.len(), h));
            }
            let m = out.metrics;
            csv.push_str(&format!("{index},{evaluation_time},{:.3},{},{total},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{},{},{:.4}\n",elapsed as f64/1000.,strategy.as_str(),total-previous,m.live_records,m.live_sources_declared,m.live_sources_with_window,m.historical_sources_contacted,m.historical_sources_skipped,m.historical_records_scanned,m.bytes_transferred,m.source_requests,m.end_to_end.as_secs_f64()*1000.,m.live_phase.as_secs_f64()*1000.,m.historical_source.as_secs_f64()*1000.,m.coordinator.as_secs_f64()*1000.,m.result_cardinality,h,total as f64/(elapsed as f64/1000.)/(active.load(Ordering::Acquire) as f64).max(1.)));
            if strategy == ExecutionStrategy::LiveFirstSourceSelection {
                chart.push_str(&format!(
                    "{index},{:.3},{},{},{},{}\n",
                    elapsed as f64 / 1000.,
                    m.end_to_end.as_secs_f64() * 1000.,
                    m.historical_sources_contacted,
                    m.live_sources_with_window,
                    m.historical_records_scanned
                ));
            }
        }
        previous = total;
        if index + 1 < a.measured_evaluations {
            active.store(schedule[index + 1], Ordering::Release);
        }
    }
    stop.store(true, Ordering::Release);
    for w in workers {
        w.join().map_err(|_| "publisher panicked")?;
    }
    fs::write(a.output_dir.join("continuous_measurements.csv"), csv)?;
    fs::write(a.output_dir.join("continuous_live_first.csv"),format!("evaluation_index,elapsed_s,execution_latency_ms,historical_sources_contacted,live_sources_with_results,historical_records_scanned\n{chart}"))?;
    let status = Command::new("python3")
        .args(["scripts/plot_results.py", "--root", "."])
        .status()?;
    if !status.success() {
        return Err("Matplotlib plot generation failed".into());
    }
    fs::write(a.output_dir.join("query_metadata.csv"),format!("query_parse_lower_decompose_once,setup_ms,lowering_ms,decomposition_ms,range_s,step_s,branch_count\ntrue,{setup_ms:.6},{:.6},{:.6},60,30,{}\n",lower.as_secs_f64()*1000.,decompose.as_secs_f64()*1000.,plan.branches.len()))?;
    Ok(())
}
