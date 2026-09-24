//! One physical strategy per real-time continuous-query campaign.
//!
//! Transport delay is physically injected by `historical_edge_service`, so the
//! measured wall-clock total already includes the controlled emulation.
use clap::Parser;
use federated_janus::{
    remote::{profile, RemoteClient},
    Anomaly, ExecutionStrategy, InMemoryLiveSource, LiveSource, LogicalPlan, Observation,
    SegmentedHistoricalSource,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

const LOGICAL_ORIGIN_MS: u64 = 4_000_000_000;
const RANGE_MS: u64 = 60_000;
const STEP_MS: u64 = 30_000;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    historical_quads: usize,
    #[arg(long, default_value = "native-localhost")]
    transport_profile: String,
    /// Exactly one explicit physical strategy: fetch-all, aggregate-pushdown, or bind-join.
    #[arg(long)]
    strategy: String,
    #[arg(long, default_value_t = 5)]
    measured_evaluations: usize,
    #[arg(long, default_value_t = 600)]
    max_campaign_seconds: u64,
    #[arg(long, default_value_t = 20)]
    max_scheduled_evaluations: usize,
    /// Run exactly one scheduled evaluation after the 60-second live-window fill.
    #[arg(long)]
    diagnostic: bool,
    #[arg(long, default_value = "results-network-historical-scale")]
    output_dir: PathBuf,
    #[arg(long)]
    service_bin: Option<PathBuf>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Completed,
    MissedDueToOverload,
}
impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::MissedDueToOverload => "missed_due_to_overload",
        }
    }
}
#[derive(Clone, Debug)]
struct SchedulingRecord {
    index: usize,
    scheduled_logical_ms: u64,
    scheduled_wall_offset_ms: u64,
    status: Status,
    actual_start_wall_offset_ms: Option<u64>,
    scheduler_lag_ms: Option<u64>,
}
fn parse_strategy(value: &str) -> Result<ExecutionStrategy, String> {
    match value {
        "fetch-all" => Ok(ExecutionStrategy::FetchAll),
        "aggregate-pushdown" => Ok(ExecutionStrategy::AggregatePushdown),
        "bind-join" => Ok(ExecutionStrategy::BindJoin),
        _ => Err("--strategy must be fetch-all, aggregate-pushdown, or bind-join".into()),
    }
}
fn scheduling_record(index: usize, actual_offset_ms: u64, executor_busy: bool) -> SchedulingRecord {
    let scheduled_wall_offset_ms = RANGE_MS + index as u64 * STEP_MS;
    SchedulingRecord {
        index,
        scheduled_logical_ms: LOGICAL_ORIGIN_MS + scheduled_wall_offset_ms,
        scheduled_wall_offset_ms,
        status: if executor_busy {
            Status::MissedDueToOverload
        } else {
            Status::Completed
        },
        actual_start_wall_offset_ms: (!executor_busy).then_some(actual_offset_ms),
        scheduler_lag_ms: (!executor_busy)
            .then_some(actual_offset_ms.saturating_sub(scheduled_wall_offset_ms)),
    }
}
fn deadline(total_execution_ms: f64) -> (bool, f64, f64) {
    (
        total_execution_ms > STEP_MS as f64,
        (total_execution_ms - STEP_MS as f64).max(0.0),
        total_execution_ms / STEP_MS as f64,
    )
}
fn hash(rows: &[Anomaly]) -> u64 {
    rows.iter().fold(0xcbf29ce484222325u64, |h, r| {
        format!(
            "{}|{:.12}|{:.12}",
            r.sensor, r.current_value, r.historical_average
        )
        .bytes()
        .fold(h, |a, b| (a ^ b as u64).wrapping_mul(0x100000001b3))
    })
}
fn ms(i: Instant) -> f64 {
    i.elapsed().as_secs_f64() * 1000.0
}
#[derive(Debug)]
struct Measurement {
    record: SchedulingRecord,
    warmup: bool,
    live_window_records: usize,
    historical_storage_ms: f64,
    historical_operator_ms: f64,
    serialization_ms: f64,
    deserialization_ms: f64,
    local_http_roundtrip_ms: f64,
    coordinator_ms: f64,
    total_execution_ms: f64,
    historical_records_scanned: u64,
    historical_records_returned: u64,
    request_rows: u64,
    response_rows: u64,
    request_payload_bytes: u64,
    response_payload_bytes: u64,
    emulated_rtt_ms: f64,
    emulated_bandwidth_delay_ms: f64,
    total_emulated_transport_delay_ms: f64,
    result_count: usize,
    result_hash: u64,
}

fn execute_one(
    plan: &LogicalPlan,
    live: &InMemoryLiveSource,
    address: &str,
    strategy: ExecutionStrategy,
    record: SchedulingRecord,
    warmup: bool,
) -> Result<Measurement, String> {
    let total = Instant::now();
    // Scheduled logical time is immutable; physical start time is never used here.
    let (historical, live_bounds) = plan
        .continuous_bounds_ms(record.scheduled_logical_ms)
        .map_err(|e| e.to_string())?;
    let coordinator = Instant::now();
    let live_rows = live.materialize_live_window(live_bounds.0, live_bounds.1);
    let http = Instant::now();
    let client = RemoteClient::new(address.to_string());
    let (averages, metrics, request_rows) = match strategy {
        ExecutionStrategy::FetchAll => {
            let (rows, m) = client.window(historical.0, historical.1)?;
            let mut sums = HashMap::<String, (f64, u64)>::new();
            for row in rows {
                let entry = sums.entry(row.sensor).or_insert((0.0, 0));
                entry.0 += row.value;
                entry.1 += 1;
            }
            (
                sums.into_iter()
                    .map(|(sensor, (sum, count))| (sensor, sum / count as f64))
                    .collect(),
                m,
                0,
            )
        }
        ExecutionStrategy::AggregatePushdown => {
            let (x, m) = client.aggregate(historical.0, historical.1, None)?;
            (x, m, 0)
        }
        ExecutionStrategy::BindJoin => {
            let bindings = live_rows
                .iter()
                .map(|x| x.sensor.clone())
                .collect::<HashSet<_>>();
            let (x, m) = client.aggregate(historical.0, historical.1, Some(&bindings))?;
            (x, m, bindings.len() as u64)
        }
        _ => return Err("network campaign supports only the three explicit strategies".into()),
    };
    let http_elapsed = ms(http);
    let mut results = live_rows
        .iter()
        .filter_map(|x| {
            averages
                .get(&x.sensor)
                .filter(|average| x.value > plan.condition.multiplier * **average)
                .map(|average| Anomaly {
                    sensor: x.sensor.clone(),
                    current_value: x.value,
                    historical_average: *average,
                })
        })
        .collect::<Vec<_>>();
    results.sort_by(|a, b| a.sensor.cmp(&b.sensor));
    let coordinator_ms = ms(coordinator) - http_elapsed;
    Ok(Measurement {
        record,
        warmup,
        live_window_records: live_rows.len(),
        historical_storage_ms: metrics.storage_ms,
        historical_operator_ms: metrics.operator_ms,
        serialization_ms: metrics.serialization_ms,
        deserialization_ms: metrics.deserialization_ms,
        local_http_roundtrip_ms: (http_elapsed - metrics.total_emulated_transport_delay_ms)
            .max(0.0),
        coordinator_ms,
        total_execution_ms: ms(total),
        historical_records_scanned: metrics.scanned,
        historical_records_returned: metrics.rows,
        request_rows,
        response_rows: metrics.rows,
        request_payload_bytes: metrics.request_payload_bytes,
        response_payload_bytes: metrics.response_payload_bytes,
        emulated_rtt_ms: metrics.emulated_rtt_ms,
        emulated_bandwidth_delay_ms: metrics.emulated_bandwidth_delay_ms,
        total_emulated_transport_delay_ms: metrics.total_emulated_transport_delay_ms,
        result_count: results.len(),
        result_hash: hash(&results),
    })
}

fn measurement_header() -> &'static str {
    "historical_quads,transport_profile,strategy,phase,evaluation_index,scheduled_logical_time_ms,scheduled_wall_offset_ms,actual_start_wall_offset_ms,scheduler_lag_ms,step_interval_ms,deadline_missed,deadline_overrun_ms,realtime_factor,live_window_records,publisher_rate_hz_per_sensor,historical_records_scanned,historical_records_returned,request_rows,response_rows,request_payload_bytes,response_payload_bytes,total_application_payload_bytes,local_http_roundtrip_ms,historical_storage_ms,historical_operator_ms,serialization_ms,deserialization_ms,emulated_rtt_ms,emulated_bandwidth_delay_ms,total_emulated_transport_delay_ms,coordinator_ms,total_execution_ms,result_count,result_hash\n"
}
fn measurement_csv(
    n: usize,
    profile: &str,
    strategy: ExecutionStrategy,
    m: &Measurement,
) -> String {
    let (missed, overrun, factor) = deadline(m.total_execution_ms);
    format!("{n},{profile},{},{},{},{},{},{},{},{STEP_MS},{missed},{overrun:.6},{factor:.6},{},4,{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{}\n", strategy.as_str(), if m.warmup { "warmup" } else { "measured" }, m.record.index, m.record.scheduled_logical_ms, m.record.scheduled_wall_offset_ms, m.record.actual_start_wall_offset_ms.unwrap_or(0), m.record.scheduler_lag_ms.unwrap_or(0), m.live_window_records, m.historical_records_scanned, m.historical_records_returned, m.request_rows, m.response_rows, m.request_payload_bytes, m.response_payload_bytes, m.request_payload_bytes + m.response_payload_bytes, m.local_http_roundtrip_ms, m.historical_storage_ms, m.historical_operator_ms, m.serialization_ms, m.deserialization_ms, m.emulated_rtt_ms, m.emulated_bandwidth_delay_ms, m.total_emulated_transport_delay_ms, m.coordinator_ms, m.total_execution_ms, m.result_count, m.result_hash)
}
fn scheduling_header() -> &'static str {
    "historical_quads,transport_profile,strategy,evaluation_index,scheduled_logical_time_ms,scheduled_wall_offset_ms,status,actual_start_wall_offset_ms,scheduler_lag_ms\n"
}
fn scheduling_csv(n: usize, p: &str, s: ExecutionStrategy, r: &SchedulingRecord) -> String {
    format!(
        "{n},{p},{},{},{},{},{},{},{}\n",
        s.as_str(),
        r.index,
        r.scheduled_logical_ms,
        r.scheduled_wall_offset_ms,
        r.status.as_str(),
        r.actual_start_wall_offset_ms
            .map(|x| x.to_string())
            .unwrap_or_default(),
        r.scheduler_lag_ms
            .map(|x| x.to_string())
            .unwrap_or_default()
    )
}
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        0.0
    } else {
        sorted[((sorted.len() - 1) as f64 * p).round() as usize]
    }
}
fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}
fn summary_csv(
    n: usize,
    p: &str,
    s: ExecutionStrategy,
    scheduled: usize,
    missed: usize,
    completed: &[Measurement],
) -> String {
    let measured = completed.iter().filter(|x| !x.warmup).collect::<Vec<_>>();
    let total = measured
        .iter()
        .map(|x| x.total_execution_ms)
        .collect::<Vec<_>>();
    let mut sorted = total.clone();
    sorted.sort_by(f64::total_cmp);
    let factors = measured
        .iter()
        .map(|x| x.total_execution_ms / STEP_MS as f64)
        .collect::<Vec<_>>();
    let mut factor_sorted = factors.clone();
    factor_sorted.sort_by(f64::total_cmp);
    let metric = |f: fn(&Measurement) -> f64| {
        if measured.is_empty() {
            0.0
        } else {
            mean(&measured.iter().map(|x| f(x)).collect::<Vec<_>>())
        }
    };
    let avg = if total.is_empty() { 0.0 } else { mean(&total) };
    let stddev = if total.is_empty() {
        0.0
    } else {
        (total.iter().map(|x| (x - avg).powi(2)).sum::<f64>() / total.len() as f64).sqrt()
    };
    let deadline_miss_count = measured
        .iter()
        .filter(|x| x.total_execution_ms > STEP_MS as f64)
        .count();
    format!("historical_quads,transport_profile,strategy,scheduled_evaluations,executed_evaluations,completed_evaluations,missed_due_to_overload,n,mean_total_ms,median_total_ms,stddev_total_ms,p95_total_ms,mean_historical_storage_ms,mean_local_http_ms,mean_emulated_transport_ms,mean_coordinator_ms,mean_request_payload_bytes,mean_response_payload_bytes,mean_total_application_payload_bytes,mean_records_scanned,mean_records_returned,deadline_miss_count,deadline_miss_rate,overload_miss_count,overload_miss_rate,median_realtime_factor,p95_realtime_factor\n{n},{p},{},{scheduled},{},{},{missed},{},{avg:.6},{:.6},{stddev:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{:.6},{missed},{:.6},{:.6},{:.6}\n", s.as_str(), completed.len(), completed.len(), measured.len(), percentile(&sorted, 0.5), percentile(&sorted, 0.95), metric(|x| x.historical_storage_ms), metric(|x| x.local_http_roundtrip_ms), metric(|x| x.total_emulated_transport_delay_ms), metric(|x| x.coordinator_ms), metric(|x| x.request_payload_bytes as f64), metric(|x| x.response_payload_bytes as f64), metric(|x| (x.request_payload_bytes + x.response_payload_bytes) as f64), metric(|x| x.historical_records_scanned as f64), metric(|x| x.historical_records_returned as f64), deadline_miss_count, if measured.is_empty() { 0.0 } else { deadline_miss_count as f64 / measured.len() as f64 }, if scheduled == 0 { 0.0 } else { missed as f64 / scheduled as f64 }, percentile(&factor_sorted, 0.5), percentile(&factor_sorted, 0.95))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let strategy = parse_strategy(&args.strategy)?;
    let transport = profile(&args.transport_profile).ok_or("unknown transport profile")?;
    if args.diagnostic && (args.measured_evaluations != 5 || args.max_scheduled_evaluations != 20) {
        return Err("--diagnostic cannot be combined with campaign-count controls".into());
    }
    fs::create_dir_all(&args.output_dir)?;
    let plan = LogicalPlan::lower(
        &JanusQLParser::new()?.parse(&fs::read_to_string("queries/anomaly.janusql")?)?,
    )?;
    let archive = args
        .output_dir
        .join(format!("segmented-archive-{}", args.historical_quads));
    let (historical_bounds, _) = plan.continuous_bounds_ms(LOGICAL_ORIGIN_MS + RANGE_MS)?;
    // Keep the fixed historical archive inside every possible scheduled
    // historical window, including those observed before a safety stop.
    let archive_start = historical_bounds.0
        + RANGE_MS
        + (args.max_scheduled_evaluations as u64 + 2) * STEP_MS
        + 5_000;
    if archive_start >= historical_bounds.1 {
        return Err("max_scheduled_evaluations exceeds the fixed historical-window overlap".into());
    }
    SegmentedHistoricalSource::deterministic(
        &archive,
        args.historical_quads,
        100,
        7,
        archive_start,
        historical_bounds.1,
        100_000,
    )?;
    let bin = args.service_bin.unwrap_or(
        std::env::current_exe()?
            .parent()
            .ok_or("no executable dir")?
            .join("historical_edge_service"),
    );
    let address = "127.0.0.1:49000".to_string();
    let mut service = Command::new(bin)
        .args([
            "--archive",
            archive.to_str().ok_or("bad archive")?,
            "--quads",
            &args.historical_quads.to_string(),
            "--profile",
            transport.name,
            "--address",
            &address,
        ])
        .spawn()?;
    thread::sleep(Duration::from_millis(100));
    let live = InMemoryLiveSource::new(Vec::new());
    let stop = Arc::new(AtomicBool::new(false));
    let origin = Instant::now();
    let publishers = (1..=10)
        .map(|id| {
            let source = live.clone();
            let stopped = stop.clone();
            thread::spawn(move || {
                let mut slot = 0_u64;
                while !stopped.load(Ordering::Acquire) {
                    let due = origin + Duration::from_millis(slot * 250);
                    if due > Instant::now() {
                        thread::sleep(due - Instant::now());
                    }
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    source.publish(Observation::new(
                        LOGICAL_ORIGIN_MS + origin.elapsed().as_millis() as u64,
                        format!("https://example.org/sensor{id}"),
                        210.0,
                    ));
                    slot += 1;
                }
            })
        })
        .collect::<Vec<_>>();
    let mut scheduling = Vec::new();
    let mut completed = Vec::new();
    let (tx, rx) = mpsc::channel::<Result<Measurement, String>>();
    let mut busy = false;
    let mut warmup_started = false;
    let mut measured_completed = 0usize;
    let mut index = 0usize;
    let target_measured = if args.diagnostic {
        1
    } else {
        args.measured_evaluations
    };
    let deadline_end = origin + Duration::from_secs(args.max_campaign_seconds);
    loop {
        while let Ok(result) = rx.try_recv() {
            busy = false;
            let m = result?;
            if !m.warmup {
                measured_completed += 1;
            }
            completed.push(m);
        }
        if measured_completed >= target_measured
            || index >= args.max_scheduled_evaluations
            || Instant::now() >= deadline_end
        {
            if !busy {
                break;
            }
            thread::sleep(Duration::from_millis(5));
            continue;
        }
        let due = origin + Duration::from_millis(RANGE_MS + index as u64 * STEP_MS);
        if due > Instant::now() {
            thread::sleep((due - Instant::now()).min(Duration::from_millis(50)));
            continue;
        }
        let actual = origin.elapsed().as_millis() as u64;
        let record = scheduling_record(index, actual, busy);
        index += 1;
        scheduling.push(record.clone());
        if record.status == Status::MissedDueToOverload {
            continue;
        }
        busy = true;
        let tx = tx.clone();
        let p = plan.clone();
        let l = live.clone();
        let a = address.clone();
        let warmup = !args.diagnostic && !warmup_started;
        warmup_started = true;
        thread::spawn(move || {
            let _ = tx.send(execute_one(&p, &l, &a, strategy, record, warmup));
        });
    }
    stop.store(true, Ordering::Release);
    for publisher in publishers {
        publisher.join().map_err(|_| "publisher panicked")?;
    }
    let _ = service.kill();
    let _ = service.wait();
    let mut raw = measurement_header().to_string();
    for row in &completed {
        raw.push_str(&measurement_csv(
            args.historical_quads,
            transport.name,
            strategy,
            row,
        ));
    }
    fs::write(args.output_dir.join("measurements.csv"), raw)?;
    let mut schedule = scheduling_header().to_string();
    for row in &scheduling {
        schedule.push_str(&scheduling_csv(
            args.historical_quads,
            transport.name,
            strategy,
            row,
        ));
    }
    fs::write(args.output_dir.join("scheduling.csv"), schedule)?;
    let missed = scheduling
        .iter()
        .filter(|x| x.status == Status::MissedDueToOverload)
        .count();
    fs::write(
        args.output_dir.join("summary.csv"),
        summary_csv(
            args.historical_quads,
            transport.name,
            strategy,
            scheduling.len(),
            missed,
            &completed,
        ),
    )?;
    fs::write(args.output_dir.join("setup_metadata.csv"), format!("key,value\nlogical_origin_ms,{LOGICAL_ORIGIN_MS}\nrange_ms,{RANGE_MS}\nstep_ms,{STEP_MS}\npublisher_rate_hz_per_sensor,4\nlive_sensors,10\ntransport_model,physical delay injected into HTTP service; total_execution_ms includes it exactly once\nstopping_rule,one completed warmup then {target_measured} completed measured evaluations; safety limits stop a pathological campaign\nmax_campaign_seconds,{}\nmax_scheduled_evaluations,{}\n", args.max_campaign_seconds, args.max_scheduled_evaluations))?;
    if args.diagnostic {
        if let Some(m) = completed.iter().find(|m| !m.warmup) {
            println!(
                "historical_quads={}, profile={}, strategy={}, historical_storage_ms={:.6}, local_http_roundtrip_ms={:.6}, request_payload_bytes={}, response_payload_bytes={}, emulated_rtt_ms={:.6}, emulated_bandwidth_delay_ms={:.6}, total_execution_ms={:.6}",
                args.historical_quads, transport.name, strategy.as_str(), m.historical_storage_ms,
                m.local_http_roundtrip_ms, m.request_payload_bytes, m.response_payload_bytes,
                m.emulated_rtt_ms, m.emulated_bandwidth_delay_ms, m.total_execution_ms,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn logical_schedule_is_exactly_one_step_apart() {
        let a = scheduling_record(0, RANGE_MS, false);
        let b = scheduling_record(1, RANGE_MS + STEP_MS, false);
        assert_eq!(b.scheduled_logical_ms - a.scheduled_logical_ms, STEP_MS);
    }
    #[test]
    fn execution_delay_cannot_change_logical_time_or_window() {
        let x = scheduling_record(2, RANGE_MS + 2 * STEP_MS + 20_000, false);
        assert_eq!(
            x.scheduled_logical_ms,
            LOGICAL_ORIGIN_MS + RANGE_MS + 2 * STEP_MS
        );
        assert_eq!(x.scheduler_lag_ms, Some(20_000));
        assert_eq!(
            (x.scheduled_logical_ms - RANGE_MS, x.scheduled_logical_ms),
            (
                LOGICAL_ORIGIN_MS + 2 * STEP_MS,
                LOGICAL_ORIGIN_MS + RANGE_MS + 2 * STEP_MS
            )
        );
    }
    #[test]
    fn delayed_materialization_excludes_events_after_scheduled_window() {
        let live = InMemoryLiveSource::new(vec![
            Observation::new(LOGICAL_ORIGIN_MS + 1, "sensor-early", 1.0),
            Observation::new(LOGICAL_ORIGIN_MS + RANGE_MS - 1, "sensor-in-window", 1.0),
            Observation::new(LOGICAL_ORIGIN_MS + RANGE_MS + 20_000, "sensor-late", 1.0),
        ]);
        let scheduled = scheduling_record(0, RANGE_MS + 20_000, false);
        let rows = live.materialize_live_window(
            scheduled.scheduled_logical_ms - RANGE_MS,
            scheduled.scheduled_logical_ms,
        );
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.sensor != "sensor-late"));
    }
    #[test]
    fn busy_step_is_skipped_not_queued() {
        let x = scheduling_record(1, RANGE_MS + STEP_MS, true);
        assert_eq!(x.status, Status::MissedDueToOverload);
        assert_eq!(x.actual_start_wall_offset_ms, None);
        assert_eq!(x.scheduler_lag_ms, None);
    }
    #[test]
    fn one_worker_prevents_concurrent_launches() {
        let first = scheduling_record(0, RANGE_MS, false);
        let second = scheduling_record(1, RANGE_MS + STEP_MS, first.status == Status::Completed);
        assert_eq!(second.status, Status::MissedDueToOverload);
    }
    #[test]
    fn deadline_metrics_are_step_based() {
        let (missed, overrun, factor) = deadline(45_000.0);
        assert!(missed);
        assert_eq!(overrun, 15_000.0);
        assert_eq!(factor, 1.5);
        let (missed, overrun, factor) = deadline(29_000.0);
        assert!(!missed);
        assert_eq!(overrun, 0.0);
        assert!((factor - 29.0 / 30.0).abs() < 1e-12);
    }
    #[test]
    fn application_payload_names_are_not_wire_names() {
        assert!(measurement_header().contains("total_application_payload_bytes"));
        assert!(!measurement_header().contains("wire"));
    }
}
