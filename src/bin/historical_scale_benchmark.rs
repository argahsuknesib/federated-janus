//! Fixed-live workload benchmark varying only the historical archive population.
use clap::Parser;
use federated_janus::{
    execute_compact, sources::compact::CompactHistoricalSource, Anomaly, ExecutionStrategy,
    LogicalPlan,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{fs, path::PathBuf, time::Instant};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "queries/anomaly.janusql")]
    query: PathBuf,
    #[arg(long, default_value_t = 100)]
    live_sensors: usize,
    #[arg(long, default_value_t = 100)]
    historical_observations_per_sensor: usize,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "1000,5000,10000,25000,50000,100000"
    )]
    historical_sensor_counts: Vec<usize>,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = 5)]
    repetitions: usize,
    #[arg(long, default_value_t = 7)]
    random_seed: u64,
    #[arg(long, default_value = "results-janusql-historical-scale")]
    output_dir: PathBuf,
}
#[derive(Clone)]
struct Row {
    strategy: ExecutionStrategy,
    sensors: usize,
    rep: usize,
    latency: f64,
    source: f64,
    coordinator: f64,
    entities: u64,
    scanned: u64,
    matched: u64,
    returned: u64,
    sent: u64,
    received: u64,
    bytes: u64,
    requests: u64,
    results: u64,
    hash: u64,
}
#[derive(Clone)]
struct Summary {
    strategy: ExecutionStrategy,
    sensors: usize,
    mean_latency: f64,
    median_latency: f64,
    stddev_latency: f64,
    p95_latency: f64,
    source: f64,
    coordinator: f64,
    scanned: f64,
    returned: f64,
    bytes: f64,
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
fn strategies() -> [ExecutionStrategy; 3] {
    [
        ExecutionStrategy::FetchAll,
        ExecutionStrategy::AggregatePushdown,
        ExecutionStrategy::BindJoin,
    ]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    if a.historical_sensor_counts
        .iter()
        .any(|&n| n < a.live_sensors)
    {
        return Err("every historical population must contain the fixed live population".into());
    }
    let text = fs::read_to_string(&a.query)?;
    let parser = JanusQLParser::new()?;
    let start = Instant::now();
    let parsed = parser.parse(&text)?;
    let parse_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = Instant::now();
    let plan = LogicalPlan::lower(&parsed)?;
    let lowering_ms = start.elapsed().as_secs_f64() * 1000.0;
    let t = 3_000_000u64;
    let (historical_start, historical_end) = plan.historical_bounds(t)?;
    let (live_start, live_end) = plan.live_bounds(t)?;
    println!("Janus-QL query: {}", a.query.display());
    println!(
        "live source={} resolved=[{}, {}) range={} step={}",
        plan.live_window.source_name,
        live_start,
        live_end,
        plan.live_window.width,
        plan.live_window.slide
    );
    println!(
        "historical source={} resolved=[{}, {}) offset={} range={} step={}",
        plan.historical_window.source_name,
        historical_start,
        historical_end,
        plan.historical_window.offset.unwrap_or_default(),
        plan.historical_window.width,
        plan.historical_window.slide
    );
    println!(
        "join={} group_by={} aggregate={}({}) AS {} predicate={} threshold={}",
        plan.join_variable,
        plan.historical_aggregate.group_variable,
        plan.historical_aggregate.function,
        plan.historical_aggregate.input_variable,
        plan.historical_aggregate.output_variable,
        plan.value_predicate,
        plan.condition.multiplier
    );
    println!("query_parse_ms={parse_ms:.6} query_lowering_ms={lowering_ms:.6} (excluded from strategy timings)");
    fs::create_dir_all(a.output_dir.join("plots"))?;
    let base = 100.0 + (a.random_seed % 5) as f64;
    let live: Vec<_> = (0..a.live_sensors)
        .map(|sensor| (sensor as u32, live_start + 1, base * 2.0))
        .collect();
    let mut rows = Vec::new();
    for &sensors in &a.historical_sensor_counts {
        let history = CompactHistoricalSource::deterministic(
            sensors,
            a.historical_observations_per_sensor,
            a.random_seed,
            historical_start,
            historical_end,
        );
        let mut expected = None;
        for strategy in strategies() {
            for _ in 0..a.warmups {
                let _ = execute_compact(strategy, &plan, &live, &history, t);
            }
            for rep in 0..a.repetitions {
                let outcome = execute_compact(strategy, &plan, &live, &history, t);
                let result_hash = hash(&outcome.results);
                if let Some((count, prior_hash)) = expected {
                    if count != outcome.results.len() || prior_hash != result_hash {
                        return Err(format!(
                            "result mismatch historical_sensors={sensors} strategy={}",
                            strategy.as_str()
                        )
                        .into());
                    }
                } else {
                    expected = Some((outcome.results.len(), result_hash));
                }
                let m = outcome.metrics;
                rows.push(Row {
                    strategy,
                    sensors,
                    rep,
                    latency: m.end_to_end.as_secs_f64() * 1000.0,
                    source: m.historical_source.as_secs_f64() * 1000.0,
                    coordinator: m.coordinator.as_secs_f64() * 1000.0,
                    entities: m.historical_entities_looked_up,
                    scanned: m.historical_records_scanned,
                    matched: m.historical_records_matched,
                    returned: m.historical_records,
                    sent: m.bytes_sent_to_historical_source,
                    received: m.bytes_received_from_historical_source,
                    bytes: m.bytes_transferred,
                    requests: m.source_requests,
                    results: m.result_cardinality,
                    hash: result_hash,
                });
            }
        }
    }
    let mut measurements = String::from("strategy,historical_sensors,historical_observations_per_sensor,historical_observations,live_sensors,historical_scale_relative_to_live,repetition,random_seed,evaluation_time,total_latency_ms,historical_source_time_ms,coordinator_time_ms,historical_entities_looked_up,historical_records_scanned,historical_records_matched,historical_records_returned,bytes_sent_to_historical_source,bytes_received_from_historical_source,total_bytes_transferred,source_requests,result_count,result_hash\n");
    for r in &rows {
        measurements.push_str(&format!(
            "{},{},{},{},{},{:.6},{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{}\n",
            r.strategy.as_str(),
            r.sensors,
            a.historical_observations_per_sensor,
            r.sensors * a.historical_observations_per_sensor,
            a.live_sensors,
            r.sensors as f64 / a.live_sensors as f64,
            r.rep,
            a.random_seed,
            t,
            r.latency,
            r.source,
            r.coordinator,
            r.entities,
            r.scanned,
            r.matched,
            r.returned,
            r.sent,
            r.received,
            r.bytes,
            r.requests,
            r.results,
            r.hash
        ));
    }
    fs::write(
        a.output_dir.join("historical_scale_measurements.csv"),
        measurements,
    )?;
    fs::write(a.output_dir.join("query_metadata.csv"), format!("query,query_parse_ms,query_lowering_ms,evaluation_time,live_start,live_end,historical_start,historical_end,join_variable,aggregate,predicate,threshold\n{},{parse_ms:.6},{lowering_ms:.6},{t},{live_start},{live_end},{historical_start},{historical_end},{},{},{},{}\n", a.query.display(), plan.join_variable, plan.historical_aggregate.function, plan.value_predicate, plan.condition.multiplier))?;
    let mut summaries = Vec::new();
    for &sensors in &a.historical_sensor_counts {
        for strategy in strategies() {
            let r: Vec<_> = rows
                .iter()
                .filter(|r| r.sensors == sensors && r.strategy == strategy)
                .collect();
            let latency: Vec<_> = r.iter().map(|r| r.latency).collect();
            summaries.push(Summary {
                strategy,
                sensors,
                mean_latency: mean(&latency),
                median_latency: median(latency.clone()),
                stddev_latency: stddev(&latency),
                p95_latency: p95(latency),
                source: mean(&r.iter().map(|r| r.source).collect::<Vec<_>>()),
                coordinator: mean(&r.iter().map(|r| r.coordinator).collect::<Vec<_>>()),
                scanned: mean(&r.iter().map(|r| r.scanned as f64).collect::<Vec<_>>()),
                returned: mean(&r.iter().map(|r| r.returned as f64).collect::<Vec<_>>()),
                bytes: mean(&r.iter().map(|r| r.bytes as f64).collect::<Vec<_>>()),
            });
        }
    }
    let mut summary_csv = String::from("strategy,historical_sensors,historical_observations,live_sensors,historical_scale_relative_to_live,n,mean_latency_ms,median_latency_ms,stddev_latency_ms,p95_latency_ms,mean_historical_source_time_ms,mean_coordinator_time_ms,mean_historical_records_scanned,mean_historical_records_returned,mean_transferred_bytes\n");
    for s in &summaries {
        summary_csv.push_str(&format!(
            "{},{},{},{},{:.6},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3}\n",
            s.strategy.as_str(),
            s.sensors,
            s.sensors * a.historical_observations_per_sensor,
            a.live_sensors,
            s.sensors as f64 / a.live_sensors as f64,
            a.repetitions,
            s.mean_latency,
            s.median_latency,
            s.stddev_latency,
            s.p95_latency,
            s.source,
            s.coordinator,
            s.scanned,
            s.returned,
            s.bytes
        ));
    }
    fs::write(
        a.output_dir.join("historical_scale_summary.csv"),
        summary_csv,
    )?;
    write_plots(&a, &summaries)?;
    println!("wrote {} measurements", rows.len());
    Ok(())
}

fn label(n: usize) -> String {
    if n >= 1_000_000 {
        if n.is_multiple_of(1_000_000) {
            format!("{}M", n / 1_000_000)
        } else {
            format!("{:.1}M", n as f64 / 1_000_000.0)
        }
    } else {
        format!("{}k", n / 1_000)
    }
}
#[allow(unreachable_code)]
fn write_plots(a: &Args, summaries: &[Summary]) -> Result<(), Box<dyn std::error::Error>> {
    let _ = (a, summaries);
    let status = std::process::Command::new("python3")
        .args(["scripts/plot_results.py", "--root", "."])
        .status()?;
    if !status.success() {
        return Err("Matplotlib plot generation failed".into());
    }
    return Ok(());
    for (file, title, ylabel, value, log_y) in [
        (
            "historical_scale_latency.svg",
            "Median execution latency",
            "median execution latency (ms)",
            0usize,
            false,
        ),
        (
            "historical_scale_work.svg",
            "Historical observations scanned",
            "historical observations scanned (log scale)",
            1usize,
            true,
        ),
        (
            "historical_scale_bytes.svg",
            "Mean transferred data",
            "mean transferred data (MB, log scale)",
            2usize,
            true,
        ),
    ] {
        let v = |s: &Summary| match value {
            0 => s.median_latency,
            1 => s.scanned,
            _ => s.bytes / 1_000_000.0,
        };
        let all: Vec<_> = summaries.iter().map(v).collect();
        let min = all.iter().copied().fold(f64::INFINITY, f64::min);
        let max = all.iter().copied().fold(0.0, f64::max);
        let y = |value: f64| {
            if log_y {
                360.0
                    - ((value.max(0.0001).ln() - min.max(0.0001).ln())
                        / (max.max(0.0001).ln() - min.max(0.0001).ln()).max(0.0001))
                        * 280.0
            } else {
                360.0 - value / max.max(0.0001) * 280.0
            }
        };
        let mut svg = format!("<svg xmlns='http://www.w3.org/2000/svg' width='900' height='440'><rect width='100%' height='100%' fill='white'/><text x='30' y='28' font-size='18'>{title}</text><text x='450' y='428' text-anchor='middle'>historical observations</text><text x='18' y='220' transform='rotate(-90 18 220)' text-anchor='middle'>{ylabel}</text><line x1='70' y1='360' x2='870' y2='360' stroke='#333'/><line x1='70' y1='60' x2='70' y2='360' stroke='#333'/>");
        for (i, &sensors) in a.historical_sensor_counts.iter().enumerate() {
            let x = 80.0
                + i as f64 * 780.0
                    / (a.historical_sensor_counts.len().saturating_sub(1).max(1) as f64);
            svg.push_str(&format!(
                "<text x='{x:.2}' y='380' text-anchor='middle' font-size='11'>{}</text>",
                label(sensors * a.historical_observations_per_sensor)
            ));
        }
        for (index, strategy) in strategies().iter().enumerate() {
            let color = ["#c33", "#36c", "#292"][index];
            let mut points = String::new();
            for (i, &sensors) in a.historical_sensor_counts.iter().enumerate() {
                let x = 80.0
                    + i as f64 * 780.0
                        / (a.historical_sensor_counts.len().saturating_sub(1).max(1) as f64);
                let s = summaries
                    .iter()
                    .find(|s| s.sensors == sensors && s.strategy == *strategy)
                    .unwrap();
                let yy = y(v(s));
                points.push_str(&format!("{x:.2},{yy:.2} "));
                svg.push_str(&format!(
                    "<circle cx='{x:.2}' cy='{yy:.2}' r='3.5' fill='{color}'/>"
                ));
            }
            svg.push_str(&format!("<polyline points='{points}' fill='none' stroke='{color}' stroke-width='2'/><text x='700' y='{}' fill='{color}'>{}</text>", 55+index*18, strategy.as_str()));
        }
        svg.push_str("</svg>");
        fs::write(a.output_dir.join("plots").join(file), svg)?;
    }
    Ok(())
}
