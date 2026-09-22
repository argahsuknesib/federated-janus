use clap::{Parser, ValueEnum};
use federated_janus::{
    execute_compact, sources::compact::CompactHistoricalSource, Anomaly, ExecutionStrategy,
    LogicalPlan,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{fs, path::PathBuf, time::Instant};
#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 100_000)]
    historical_sensors: usize,
    #[arg(long, default_value_t = 100)]
    historical_observations_per_sensor: usize,
    #[arg(long, default_value_t = 5)]
    warmups: usize,
    #[arg(long, default_value_t = 30)]
    repetitions: usize,
    #[arg(long, default_value_t = 7)]
    random_seed: u64,
    #[arg(long, default_value = "results")]
    output_dir: PathBuf,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "10,100,1000,5000,10000,25000,50000,75000,100000"
    )]
    live_sensor_counts: Vec<usize>,
    /// Janus-QL benchmark query; defaults to the repository's canonical query.
    #[arg(long, default_value = "queries/anomaly.janusql")]
    query: PathBuf,
    /// Run one manually selected plan; omit to compare all three plans.
    #[arg(long, value_enum)]
    strategy: Option<StrategyArg>,
}
#[derive(Clone, ValueEnum)]
enum StrategyArg {
    FetchAll,
    AggregatePushdown,
    BindJoin,
}
impl From<StrategyArg> for ExecutionStrategy {
    fn from(value: StrategyArg) -> Self {
        match value {
            StrategyArg::FetchAll => Self::FetchAll,
            StrategyArg::AggregatePushdown => Self::AggregatePushdown,
            StrategyArg::BindJoin => Self::BindJoin,
        }
    }
}
#[derive(Clone)]
struct Row {
    strategy: ExecutionStrategy,
    live: usize,
    rep: usize,
    lat: f64,
    hist: f64,
    coord: f64,
    scan: u64,
    entities: u64,
    matched: u64,
    returned: u64,
    live_records: u64,
    sent: u64,
    received: u64,
    bytes: u64,
    requests: u64,
    results: u64,
    hash: u64,
    evaluation_time: u64,
}
type Summary = (
    ExecutionStrategy,
    usize,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
    f64,
);
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
fn avg(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}
fn med(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    if v.len().is_multiple_of(2) {
        (v[v.len() / 2 - 1] + v[v.len() / 2]) / 2.0
    } else {
        v[v.len() / 2]
    }
}
fn sd(v: &[f64]) -> f64 {
    let m = avg(v);
    (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / v.len() as f64).sqrt()
}
fn p95(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) * 95).div_ceil(100)]
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    let query_text = fs::read_to_string(&a.query)?;
    let parser = JanusQLParser::new()?;
    let parse_start = Instant::now();
    let parsed = parser.parse(&query_text)?;
    let query_parse_ms = parse_start.elapsed().as_secs_f64() * 1000.0;
    let lower_start = Instant::now();
    let plan = LogicalPlan::lower(&parsed)?;
    let query_lowering_ms = lower_start.elapsed().as_secs_f64() * 1000.0;
    let evaluation_time = 3_000_000u64;
    let (historical_start, historical_end) = plan.historical_bounds(evaluation_time)?;
    let (live_start, _) = plan.live_bounds(evaluation_time)?;
    if a.live_sensor_counts
        .iter()
        .any(|&n| n > a.historical_sensors)
    {
        return Err("live sensor count exceeds historical population".into());
    }
    println!("Janus-QL query: {}", a.query.display());
    println!(
        "live source={} range={} step={}",
        plan.live_window.source_name, plan.live_window.width, plan.live_window.slide
    );
    println!(
        "historical source={} offset={} range={} step={} resolved=[{}, {})",
        plan.historical_window.source_name,
        plan.historical_window.offset.unwrap_or_default(),
        plan.historical_window.width,
        plan.historical_window.slide,
        historical_start,
        historical_end
    );
    println!("live resolved=[{}, {}) join={} group_by={} aggregate={}({}) AS {} predicate={} threshold={}", live_start, evaluation_time, plan.join_variable, plan.historical_aggregate.group_variable, plan.historical_aggregate.function, plan.historical_aggregate.input_variable, plan.historical_aggregate.output_variable, plan.value_predicate, plan.condition.multiplier);
    println!("query_parse_ms={query_parse_ms:.6} query_lowering_ms={query_lowering_ms:.6} (excluded from strategy timings)");
    fs::create_dir_all(a.output_dir.join("plots"))?;
    fs::write(
        a.output_dir.join("query_metadata.csv"),
        format!(
            "query,query_parse_ms,query_lowering_ms,evaluation_time,live_source,live_range,live_step,live_start,live_end,historical_source,historical_offset,historical_range,historical_step,historical_start,historical_end,join_variable,group_variable,aggregate,aggregate_input,aggregate_output,predicate,threshold\n{},{query_parse_ms:.6},{query_lowering_ms:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            a.query.display(), evaluation_time, plan.live_window.source_name, plan.live_window.width, plan.live_window.slide, live_start, evaluation_time, plan.historical_window.source_name, plan.historical_window.offset.unwrap_or_default(), plan.historical_window.width, plan.historical_window.slide, historical_start, historical_end, plan.join_variable, plan.historical_aggregate.group_variable, plan.historical_aggregate.function, plan.historical_aggregate.input_variable, plan.historical_aggregate.output_variable, plan.value_predicate, plan.condition.multiplier
        ),
    )?;
    let base = 100.0 + (a.random_seed % 5) as f64;
    let history = CompactHistoricalSource::deterministic(
        a.historical_sensors,
        a.historical_observations_per_sensor,
        a.random_seed,
        historical_start,
        historical_end,
    );
    let mut rows = Vec::new();
    for &live_n in &a.live_sensor_counts {
        let live: Vec<(u32, u64, f64)> = (0..live_n)
            .map(|s| (s as u32, live_start + 1, base * 2.0))
            .collect();
        let mut expected = None;
        let strategies: Vec<_> = a
            .strategy
            .clone()
            .map(ExecutionStrategy::from)
            .map(|s| vec![s])
            .unwrap_or_else(|| {
                vec![
                    ExecutionStrategy::FetchAll,
                    ExecutionStrategy::AggregatePushdown,
                    ExecutionStrategy::BindJoin,
                ]
            });
        for strategy in strategies {
            for _ in 0..a.warmups {
                let _ = execute_compact(strategy, &plan, &live, &history, evaluation_time);
            }
            for rep in 0..a.repetitions {
                let o = execute_compact(strategy, &plan, &live, &history, evaluation_time);
                let h = hash(&o.results);
                if let Some((count, reference)) = expected {
                    if count != o.results.len() || reference != h {
                        return Err(format!(
                            "result mismatch live={live_n} strategy={}",
                            strategy.as_str()
                        )
                        .into());
                    }
                } else {
                    expected = Some((o.results.len(), h));
                }
                let m = o.metrics;
                rows.push(Row {
                    strategy,
                    live: live_n,
                    rep,
                    lat: m.end_to_end.as_secs_f64() * 1000.0,
                    hist: m.historical_source.as_secs_f64() * 1000.0,
                    coord: m.coordinator.as_secs_f64() * 1000.0,
                    scan: m.historical_records_scanned,
                    entities: m.historical_entities_looked_up,
                    matched: m.historical_records_matched,
                    returned: m.historical_records,
                    live_records: m.live_records,
                    sent: m.bytes_sent_to_historical_source,
                    received: m.bytes_received_from_historical_source,
                    bytes: m.bytes_transferred,
                    requests: m.source_requests,
                    results: m.result_cardinality,
                    hash: h,
                    evaluation_time,
                });
            }
        }
    }
    let mut csv=String::from("strategy,historical_sensors,historical_observations,historical_observations_per_sensor,live_sensors,live_fraction,repetition,random_seed,evaluation_time,total_latency_ms,historical_source_time_ms,coordinator_time_ms,historical_entities_looked_up,historical_records_scanned,historical_records_matched,historical_records_returned,live_records_processed,bytes_sent_to_historical_source,bytes_received_from_historical_source,total_bytes_transferred,source_requests,result_count,result_hash\n");
    for r in &rows {
        csv.push_str(&format!(
            "{},{},{},{},{},{:.8},{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{}\n",
            r.strategy.as_str(),
            a.historical_sensors,
            a.historical_sensors * a.historical_observations_per_sensor,
            a.historical_observations_per_sensor,
            r.live,
            r.live as f64 / a.historical_sensors as f64,
            r.rep,
            a.random_seed,
            r.evaluation_time,
            r.lat,
            r.hist,
            r.coord,
            r.entities,
            r.scan,
            r.matched,
            r.returned,
            r.live_records,
            r.sent,
            r.received,
            r.bytes,
            r.requests,
            r.results,
            r.hash
        ));
    }
    fs::write(a.output_dir.join("live_cardinality_measurements.csv"), csv)?;
    if a.strategy.is_some() {
        println!(
            "wrote {} measurements for the selected strategy",
            rows.len()
        );
        return Ok(());
    }
    let mut summary=String::from("strategy,live_sensors,live_fraction,n,mean_latency_ms,median_latency_ms,stddev_latency_ms,p95_latency_ms,mean_historical_source_time_ms,mean_coordinator_time_ms,mean_records_scanned,mean_records_returned,mean_bytes_transferred,mean_request_count,fetch_all_bytes_over_bind_join,aggregate_pushdown_bytes_over_bind_join,fetch_all_latency_over_bind_join,aggregate_pushdown_latency_over_bind_join\n");
    let mut sums: Vec<Summary> = Vec::new();
    for &live in &a.live_sensor_counts {
        for strategy in [
            ExecutionStrategy::FetchAll,
            ExecutionStrategy::AggregatePushdown,
            ExecutionStrategy::BindJoin,
        ] {
            let x: Vec<_> = rows
                .iter()
                .filter(|r| r.live == live && r.strategy == strategy)
                .collect();
            let l: Vec<_> = x.iter().map(|r| r.lat).collect();
            sums.push((
                strategy,
                live,
                avg(&l),
                med(l.clone()),
                sd(&l),
                p95(l),
                avg(&x.iter().map(|r| r.hist).collect::<Vec<_>>()),
                avg(&x.iter().map(|r| r.coord).collect::<Vec<_>>()),
                avg(&x.iter().map(|r| r.scan as f64).collect::<Vec<_>>()),
                avg(&x.iter().map(|r| r.returned as f64).collect::<Vec<_>>()),
                avg(&x.iter().map(|r| r.bytes as f64).collect::<Vec<_>>()),
                avg(&x.iter().map(|r| r.requests as f64).collect::<Vec<_>>()),
            ))
        }
    }
    for q in &sums {
        let b = sums
            .iter()
            .find(|x| x.0 == ExecutionStrategy::BindJoin && x.1 == q.1)
            .unwrap();
        let ratio = |n: f64, d: f64| {
            if d == 0.0 {
                String::new()
            } else {
                format!("{:.6}", n / d)
            }
        };
        summary.push_str(&format!(
            "{},{},{:.8},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.3},{:.3},{:.3},{:.3},{},{},{},{}\n",
            q.0.as_str(),
            q.1,
            q.1 as f64 / a.historical_sensors as f64,
            a.repetitions,
            q.2,
            q.3,
            q.4,
            q.5,
            q.6,
            q.7,
            q.8,
            q.9,
            q.10,
            q.11,
            if q.0 == ExecutionStrategy::FetchAll {
                ratio(q.10, b.10)
            } else {
                String::new()
            },
            if q.0 == ExecutionStrategy::AggregatePushdown {
                ratio(q.10, b.10)
            } else {
                String::new()
            },
            if q.0 == ExecutionStrategy::FetchAll {
                ratio(q.2, b.2)
            } else {
                String::new()
            },
            if q.0 == ExecutionStrategy::AggregatePushdown {
                ratio(q.2, b.2)
            } else {
                String::new()
            }
        ));
    }
    fs::write(a.output_dir.join("live_cardinality_summary.csv"), summary)?;
    for (name, title, field) in [
        ("latency.svg", "Median execution latency", 0),
        ("bytes.svg", "Mean transferred data", 1),
        ("historical_work.svg", "Historical observations scanned", 2),
    ] {
        let value = |q: &Summary| match field {
            0 => q.3,
            1 => q.10 / 1_000_000.0,
            _ => q.8 / (a.historical_sensors * a.historical_observations_per_sensor) as f64 * 100.0,
        };
        let values: Vec<_> = sums.iter().map(value).collect();
        let min_log = values
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min)
            .max(0.0001)
            .ln();
        let max_log = values.iter().copied().fold(0.0, f64::max).max(0.0001).ln();
        let y = |v: f64| {
            360.0 - ((v.max(0.0001).ln() - min_log) / (max_log - min_log).max(0.0001)) * 280.0
        };
        let ylabel = match field {
            0 => "median execution latency (ms, log scale)",
            1 => "transferred data (MB, log scale)",
            _ => "historical observations scanned (%, log scale)",
        };
        let mut svg=format!("<svg xmlns='http://www.w3.org/2000/svg' width='900' height='440'><rect width='100%' height='100%' fill='white'/><text x='20' y='25' font-size='18'>{title}</text><text x='450' y='430' text-anchor='middle'>live-side coverage (%)</text><text x='16' y='220' transform='rotate(-90 16 220)' text-anchor='middle'>{ylabel}</text><line x1='70' y1='360' x2='870' y2='360' stroke='#333'/><line x1='70' y1='60' x2='70' y2='360' stroke='#333'/>");
        for (i, &live) in a.live_sensor_counts.iter().enumerate() {
            let x = 80.0
                + i as f64 * 780.0 / (a.live_sensor_counts.len().saturating_sub(1).max(1) as f64);
            svg.push_str(&format!(
                "<text x='{x}' y='380' text-anchor='middle' font-size='10'>{:.3}</text>",
                live as f64 / a.historical_sensors as f64 * 100.0
            ));
        }
        for (j, st) in [
            ExecutionStrategy::FetchAll,
            ExecutionStrategy::AggregatePushdown,
            ExecutionStrategy::BindJoin,
        ]
        .iter()
        .enumerate()
        {
            let color = ["#c33", "#36c", "#292"][j];
            let mut points = String::new();
            for (i, &live) in a.live_sensor_counts.iter().enumerate() {
                let x = 80.0
                    + i as f64 * 780.0
                        / (a.live_sensor_counts.len().saturating_sub(1).max(1) as f64);
                let q = sums.iter().find(|q| q.0 == *st && q.1 == live).unwrap();
                let yy = y(value(q));
                points.push_str(&format!("{x:.2},{yy:.2} "));
                svg.push_str(&format!(
                    "<circle cx='{x:.2}' cy='{yy:.2}' r='3.5' fill='{color}'/>"
                ));
            }
            svg.push_str(&format!(
                "<polyline points='{points}' fill='none' stroke='{color}' stroke-width='2'/>"
            ));
            svg.push_str(&format!(
                "<text x='700' y='{}' fill='{color}'>{}</text>",
                55 + j * 18,
                st.as_str()
            ));
        }
        if field == 0 {
            for pair in a.live_sensor_counts.windows(2) {
                let diff = |live| {
                    let b = sums
                        .iter()
                        .find(|q| q.0 == ExecutionStrategy::BindJoin && q.1 == live)
                        .unwrap()
                        .3;
                    let ap = sums
                        .iter()
                        .find(|q| q.0 == ExecutionStrategy::AggregatePushdown && q.1 == live)
                        .unwrap()
                        .3;
                    b - ap
                };
                if diff(pair[0]).is_sign_positive() != diff(pair[1]).is_sign_positive() {
                    let x0 = 80.0
                        + a.live_sensor_counts
                            .iter()
                            .position(|x| *x == pair[0])
                            .unwrap() as f64
                            * 780.0
                            / (a.live_sensor_counts.len() - 1) as f64;
                    let x1 = 80.0
                        + a.live_sensor_counts
                            .iter()
                            .position(|x| *x == pair[1])
                            .unwrap() as f64
                            * 780.0
                            / (a.live_sensor_counts.len() - 1) as f64;
                    svg.push_str(&format!("<rect x='{x0:.2}' y='60' width='{:.2}' height='300' fill='#fc3' opacity='.16'/><text x='{:.2}' y='75' font-size='10'>crossover</text>", x1-x0, (x0+x1)/2.0));
                    break;
                }
            }
        }
        svg.push_str("</svg>");
        fs::write(a.output_dir.join("plots").join(name), svg)?;
    }
    let mut svg=String::from("<svg xmlns='http://www.w3.org/2000/svg' width='900' height='420'><text x='20' y='25'>BindJoin median latency / AggregatePushdown median latency</text><line x1='60' y1='215' x2='870' y2='215' stroke='#888'/><text x='20' y='210'>1.0</text>");
    for (i, &live) in a.live_sensor_counts.iter().enumerate() {
        let x =
            80.0 + i as f64 * 780.0 / (a.live_sensor_counts.len().saturating_sub(1).max(1) as f64);
        let bj = sums
            .iter()
            .find(|q| q.0 == ExecutionStrategy::BindJoin && q.1 == live)
            .unwrap()
            .3;
        let ap = sums
            .iter()
            .find(|q| q.0 == ExecutionStrategy::AggregatePushdown && q.1 == live)
            .unwrap()
            .3;
        let ratio = bj / ap;
        let y = 215.0 - (ratio - 1.0).clamp(-1.0, 1.0) * 150.0;
        svg.push_str(&format!("<circle cx='{x}' cy='{y}' r='4' fill='#292'/><text x='{x}' y='405' font-size='10'>{live}</text>"));
    }
    svg.push_str("</svg>");
    fs::write(
        a.output_dir.join("plots").join("bind_over_aggregate.svg"),
        svg,
    )?;
    println!("wrote {} measurements", rows.len());
    Ok(())
}
