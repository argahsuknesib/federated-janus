use clap::Parser;
use federated_janus::{
    execute_compact, sources::compact::CompactHistoricalSource, Anomaly, ExecutionStrategy,
    LogicalPlan,
};
use std::{fs, path::PathBuf};
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
fn avg(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}
fn med(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    if v.len() % 2 == 0 {
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
    v[((v.len() - 1) * 95 + 99) / 100]
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    if a.live_sensor_counts
        .iter()
        .any(|&n| n > a.historical_sensors)
    {
        return Err("live sensor count exceeds historical population".into());
    }
    fs::create_dir_all(a.output_dir.join("plots"))?;
    let base = 100.0 + (a.random_seed % 5) as f64;
    let history = CompactHistoricalSource::deterministic(
        a.historical_sensors,
        a.historical_observations_per_sensor,
        a.random_seed,
    );
    let mut rows = Vec::new();
    for &live_n in &a.live_sensor_counts {
        let live: Vec<(u32, f64)> = (0..live_n).map(|s| (s as u32, base * 2.0)).collect();
        let mut expected = None;
        for strategy in [
            ExecutionStrategy::FetchAll,
            ExecutionStrategy::AggregatePushdown,
            ExecutionStrategy::BindJoin,
        ] {
            for _ in 0..a.warmups {
                let _ = execute_compact(
                    strategy,
                    &LogicalPlan::default(),
                    &live,
                    &history,
                    0,
                    a.historical_observations_per_sensor as u64,
                );
            }
            for rep in 0..a.repetitions {
                let o = execute_compact(
                    strategy,
                    &LogicalPlan::default(),
                    &live,
                    &history,
                    0,
                    a.historical_observations_per_sensor as u64,
                );
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
                });
            }
        }
    }
    let mut csv=String::from("strategy,historical_sensors,historical_observations,historical_observations_per_sensor,live_sensors,repetition,random_seed,total_latency_ms,historical_source_time_ms,coordinator_time_ms,historical_entities_looked_up,historical_records_scanned,historical_records_matched,historical_records_returned,live_records_processed,bytes_sent_to_historical_source,bytes_received_from_historical_source,total_bytes_transferred,source_requests,result_count,result_hash\n");
    for r in &rows {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{},{},{},{},{},{},{}\n",
            r.strategy.as_str(),
            a.historical_sensors,
            a.historical_sensors * a.historical_observations_per_sensor,
            a.historical_observations_per_sensor,
            r.live,
            r.rep,
            a.random_seed,
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
    let mut summary=String::from("strategy,live_sensors,live_fraction,n,mean_latency_ms,median_latency_ms,stddev_latency_ms,p95_latency_ms,mean_historical_source_time_ms,mean_coordinator_time_ms,mean_records_scanned,mean_records_returned,mean_bytes_transferred,mean_request_count,fetch_all_bytes_over_bind_join,aggregate_pushdown_bytes_over_bind_join,fetch_all_latency_over_bind_join,aggregate_pushdown_latency_over_bind_join\n");
    let mut sums = Vec::new();
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
        ("latency.svg", "Median total latency (ms)", 0),
        ("bytes.svg", "Mean bytes transferred", 1),
        ("historical_work.svg", "Mean historical records scanned", 2),
    ] {
        let max = sums
            .iter()
            .map(|q| match field {
                0 => q.3,
                1 => q.10,
                _ => q.8,
            })
            .fold(1.0, f64::max);
        let mut svg=format!("<svg xmlns='http://www.w3.org/2000/svg' width='900' height='420'><text x='20' y='25'>{title}</text>");
        for (i, &live) in a.live_sensor_counts.iter().enumerate() {
            let x = 80.0
                + i as f64 * 780.0 / (a.live_sensor_counts.len().saturating_sub(1).max(1) as f64);
            svg.push_str(&format!(
                "<text x='{x}' y='405' font-size='10'>{live}</text>"
            ));
            for (j, st) in [
                ExecutionStrategy::FetchAll,
                ExecutionStrategy::AggregatePushdown,
                ExecutionStrategy::BindJoin,
            ]
            .iter()
            .enumerate()
            {
                let q = sums.iter().find(|q| q.0 == *st && q.1 == live).unwrap();
                let v = match field {
                    0 => q.3,
                    1 => q.10,
                    _ => q.8,
                };
                let y = 380.0 - v / max * 330.0;
                svg.push_str(&format!(
                    "<circle cx='{x}' cy='{y}' r='4' fill='{}'/>",
                    ["#c33", "#36c", "#292"][j]
                ));
            }
        }
        svg.push_str("<text x='20' y='50' fill='#c33'>FetchAll</text><text x='20' y='70' fill='#36c'>AggregatePushdown</text><text x='20' y='90' fill='#292'>BindJoin</text></svg>");
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
