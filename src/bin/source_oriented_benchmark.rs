//! Experiment 1: one parsed Janus-QL UNION query over an increasing federation width.
use clap::Parser;
use federated_janus::{
    execute_federated, generate_federated_anomaly_query, Anomaly, ExecutionStrategy,
    FederatedLogicalPlan, Observation, SensorSourcePair, SourceRegistry,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{fs, path::PathBuf, time::Instant};

#[derive(Parser)]
struct Args {
    #[arg(long, value_delimiter = ',', default_value = "1,5,10,25,50,100")]
    source_pairs: Vec<usize>,
    #[arg(long, default_value_t = 10_000)]
    historical_observations_per_source: usize,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = 5)]
    repetitions: usize,
    #[arg(long, default_value = "results-single-query-federation-width")]
    output_dir: PathBuf,
}
#[derive(Clone)]
struct Row {
    strategy: ExecutionStrategy,
    pairs: usize,
    active: usize,
    rep: usize,
    latency: f64,
    live: f64,
    historical: f64,
    coordinator: f64,
    contacted: u64,
    skipped: u64,
    scanned: u64,
    raw: u64,
    aggregates: u64,
    bytes: u64,
    requests: u64,
    results: u64,
    hash: u64,
}
fn active_count(pairs: usize) -> usize {
    ((pairs + 5) / 10).max(1)
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
fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}
fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}
fn build(
    n: usize,
    observations: usize,
    active: usize,
    hs: u64,
    he: u64,
    ls: u64,
) -> Result<SourceRegistry, String> {
    let mut registry = SourceRegistry::default();
    for id in 1..=n as u32 {
        let sensor = format!("https://example.org/sensor{id}");
        let history = (0..observations)
            .map(|i| {
                Observation::new(
                    hs + ((i as u64 + 1) * (he - hs) / (observations as u64 + 1)),
                    sensor.clone(),
                    100.0 + (i % 5) as f64,
                )
            })
            .collect();
        let live = if id as usize <= active {
            vec![Observation::new(ls + 1, sensor, 200.)]
        } else {
            vec![]
        };
        registry.register(SensorSourcePair::new(id, live, history))?;
    }
    Ok(registry)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    if a.source_pairs.is_empty() || a.source_pairs.contains(&0) {
        return Err("source_pairs must be positive".into());
    };
    fs::create_dir_all(a.output_dir.join("plots"))?;
    fs::create_dir_all("docs/figures")?;
    let parser = JanusQLParser::new()?;
    let mut rows = Vec::new();
    let mut metadata=String::from("source_pairs,logical_query_count,logical_branch_count,active_live_fraction,active_live_sources,active_rounding_rule,query_parse_ms,query_lowering_ms,query_decomposition_ms,live_window,historical_window\n");
    for &pairs in &a.source_pairs {
        let text = generate_federated_anomaly_query(pairs);
        let start = Instant::now();
        let parsed = parser.parse(&text)?;
        let parse_ms = start.elapsed().as_secs_f64() * 1000.;
        let start = Instant::now();
        let plan = FederatedLogicalPlan::lower(&parsed)?;
        let lowering_ms = start.elapsed().as_secs_f64() * 1000.;
        let t = 3_000_000;
        let (hs, he) = plan.branches[0].historical_bounds(t)?;
        let (ls, le) = plan.branches[0].live_bounds(t)?;
        let active = active_count(pairs);
        let registry = build(
            pairs,
            a.historical_observations_per_source,
            active,
            hs,
            he,
            ls,
        )?;
        metadata.push_str(&format!("{pairs},1,{},0.1,{active},ceil(source_pairs/10) with minimum 1,{parse_ms:.6},{lowering_ms:.6},{lowering_ms:.6},[{},{}),[{}, {})\n",plan.branches.len(),ls,le,hs,he));
        let mut expected = None;
        for strategy in [
            ExecutionStrategy::FetchAllSources,
            ExecutionStrategy::AggregateAllSources,
            ExecutionStrategy::LiveFirstSourceSelection,
        ] {
            for _ in 0..a.warmups {
                execute_federated(strategy, &plan, &registry, t)?;
            }
            for rep in 0..a.repetitions {
                let outcome = execute_federated(strategy, &plan, &registry, t)?;
                let result_hash = hash(&outcome.results);
                if let Some(v) = expected {
                    if v != (outcome.results.len(), result_hash) {
                        return Err(format!(
                            "semantic mismatch source_pairs={pairs} strategy={}",
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
                    pairs,
                    active,
                    rep,
                    latency: m.end_to_end.as_secs_f64() * 1000.,
                    live: m.live_phase.as_secs_f64() * 1000.,
                    historical: m.historical_source.as_secs_f64() * 1000.,
                    coordinator: m.coordinator.as_secs_f64() * 1000.,
                    contacted: m.historical_sources_contacted,
                    skipped: m.historical_sources_skipped,
                    scanned: m.historical_records_scanned,
                    raw: m.raw_records_transferred,
                    aggregates: m.aggregate_rows_transferred,
                    bytes: m.bytes_transferred,
                    requests: m.source_requests,
                    results: m.result_cardinality,
                    hash: result_hash,
                });
            }
        }
    }
    let mut csv=String::from("strategy,source_pairs,historical_observations_per_source,active_live_fraction,active_live_sources,repetition,logical_query_count,logical_branch_count,live_sources_declared,historical_sources_declared,live_sources_contacted,live_sources_with_results,historical_sources_contacted,historical_sources_skipped,source_requests,historical_records_scanned,historical_records_returned,raw_records_transferred,aggregate_rows_transferred,total_bytes_transferred,total_execution_ms,live_phase_ms,historical_phase_ms,coordinator_ms,query_parse_ms,query_lowering_ms,query_decomposition_ms,result_count,result_hash\n");
    for r in &rows {
        csv.push_str(&format!("{},{},{},0.1,{},{},1,{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},excluded,excluded,excluded,{},{}\n",r.strategy.as_str(),r.pairs,a.historical_observations_per_source,r.active,r.rep,r.pairs,r.pairs,r.pairs,r.pairs,r.active,r.contacted,r.skipped,r.requests,r.scanned,r.raw+r.aggregates,r.raw,r.aggregates,r.bytes,r.latency,r.live,r.historical,r.coordinator,r.results,r.hash));
    }
    fs::write(a.output_dir.join("federation_width_measurements.csv"), csv)?;
    fs::write(a.output_dir.join("query_metadata.csv"), metadata)?;
    let mut summary=String::from("strategy,source_pairs,n,median_execution_ms,mean_historical_sources_contacted,mean_historical_records_scanned,mean_bytes_transferred\n");
    for &pairs in &a.source_pairs {
        for strategy in [
            ExecutionStrategy::FetchAllSources,
            ExecutionStrategy::AggregateAllSources,
            ExecutionStrategy::LiveFirstSourceSelection,
        ] {
            let g: Vec<_> = rows
                .iter()
                .filter(|r| r.pairs == pairs && r.strategy == strategy)
                .collect();
            summary.push_str(&format!(
                "{},{},{},{:.6},{:.3},{:.3},{:.3}\n",
                strategy.as_str(),
                pairs,
                g.len(),
                median(g.iter().map(|r| r.latency).collect()),
                mean(&g.iter().map(|r| r.contacted as f64).collect::<Vec<_>>()),
                mean(&g.iter().map(|r| r.scanned as f64).collect::<Vec<_>>()),
                mean(&g.iter().map(|r| r.bytes as f64).collect::<Vec<_>>())
            ));
        }
    }
    fs::write(a.output_dir.join("federation_width_summary.csv"), summary)?;
    write_plots(&a, &rows)?;
    Ok(())
}
#[allow(unreachable_code)]
fn write_plots(a: &Args, rows: &[Row]) -> Result<(), Box<dyn std::error::Error>> {
    let _ = (a, rows);
    let status = std::process::Command::new("python3")
        .args(["scripts/plot_results.py", "--root", "."])
        .status()?;
    if !status.success() {
        return Err("Matplotlib plot generation failed".into());
    }
    return Ok(());
    for (file, title, kind) in [
        (
            "federation_width_latency.svg",
            "Median execution latency (ms)",
            0,
        ),
        (
            "federation_width_sources_contacted.svg",
            "Historical sources contacted",
            1,
        ),
        (
            "federation_width_records_scanned.svg",
            "Historical records scanned",
            2,
        ),
        ("federation_width_bytes.svg", "Total bytes transferred", 3),
    ] {
        let val = |r: &Row| match kind {
            0 => r.latency,
            1 => r.contacted as f64,
            2 => r.scanned as f64,
            _ => r.bytes as f64,
        };
        let max = rows.iter().map(val).fold(1., f64::max);
        let mut s=format!("<svg xmlns='http://www.w3.org/2000/svg' width='900' height='460'><style>text{{font:14px sans-serif}}.a{{stroke:#333}}.l{{fill:none;stroke-width:2.5}}</style><text x='60' y='28'>{title}</text><line class='a' x1='70' y1='390' x2='870' y2='390'/><line class='a' x1='70' y1='55' x2='70' y2='390'/><text x='420' y='440'>Source pairs</text><text x='8' y='50'>max {max:.3}</text>");
        for (i, &n) in a.source_pairs.iter().enumerate() {
            let x = 70. + i as f64 * 800. / (a.source_pairs.len() - 1).max(1) as f64;
            s.push_str(&format!("<text x='{:.0}' y='410'>{n}</text>", x - 8.));
        }
        for (stroke, label, strategy) in [
            (
                "#d62728",
                "FetchAllSources",
                ExecutionStrategy::FetchAllSources,
            ),
            (
                "#1f77b4",
                "AggregateAllSources",
                ExecutionStrategy::AggregateAllSources,
            ),
            (
                "#2ca02c",
                "LiveFirstSourceSelection",
                ExecutionStrategy::LiveFirstSourceSelection,
            ),
        ] {
            let mut points = String::new();
            for (i, &n) in a.source_pairs.iter().enumerate() {
                let g: Vec<_> = rows
                    .iter()
                    .filter(|r| r.pairs == n && r.strategy == strategy)
                    .collect();
                let yv = if kind == 0 {
                    median(g.iter().map(|r| r.latency).collect())
                } else {
                    mean(&g.iter().map(|r| val(r)).collect::<Vec<_>>())
                };
                let x = 70. + i as f64 * 800. / (a.source_pairs.len() - 1).max(1) as f64;
                points.push_str(&format!("{x:.1},{:.1} ", 390. - yv / max * 300.));
            }
            s.push_str(&format!("<polyline class='l' stroke='{stroke}' points='{points}'/><text x='670' y='{}' fill='{stroke}'>{label}</text>",65+match strategy{ExecutionStrategy::FetchAllSources=>0,ExecutionStrategy::AggregateAllSources=>20,_=>40}));
        }
        s.push_str("</svg>");
        fs::write(a.output_dir.join("plots").join(file), &s)?;
        fs::write(PathBuf::from("docs/figures").join(file), s)?;
    }
    Ok(())
}
