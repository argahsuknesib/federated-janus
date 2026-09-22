//! Experiment 2: fixed 100-source federation, varied runtime live activity.
use clap::Parser;
use federated_janus::{
    execute_federated, generate_federated_anomaly_query, Anomaly, ExecutionStrategy,
    FederatedLogicalPlan, Observation, SensorSourcePair, SourceRegistry,
};
use janus::parsing::janusql_parser::JanusQLParser;
use std::{fs, path::PathBuf, time::Instant};

const SOURCE_PAIRS: usize = 100;
const EVALUATION_TIME: u64 = 3_000_000;
#[derive(Parser)]
struct Args {
    #[arg(long, value_delimiter = ',', default_value = "1,5,10,25,50,75,100")]
    active_live_sources: Vec<usize>,
    #[arg(long, default_value_t = 10_000)]
    historical_observations_per_source: usize,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = 5)]
    repetitions: usize,
    #[arg(long, default_value = "results-single-query-active-selectivity")]
    output_dir: PathBuf,
}
#[derive(Clone)]
struct Row {
    strategy: ExecutionStrategy,
    active: usize,
    rep: usize,
    latency: f64,
    live: f64,
    historical: f64,
    coordinator: f64,
    contacted: u64,
    skipped: u64,
    scanned: u64,
    returned: u64,
    raw: u64,
    aggregates: u64,
    bytes: u64,
    requests: u64,
    results: u64,
    hash: u64,
    parse: f64,
    lower: f64,
    decomposition: f64,
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
fn build_history(observations: usize, hs: u64, he: u64) -> Result<SourceRegistry, String> {
    let mut r = SourceRegistry::default();
    for id in 1..=SOURCE_PAIRS as u32 {
        let sensor = format!("https://example.org/sensor{id}");
        let history = (0..observations)
            .map(|i| {
                Observation::new(
                    hs + ((i as u64 + 1) * (he - hs) / (observations as u64 + 1)),
                    sensor.clone(),
                    100. + (i % 5) as f64,
                )
            })
            .collect();
        r.register(SensorSourcePair::new(id, vec![], history))?;
    }
    Ok(r)
}
fn set_active_prefix(
    registry: &mut SourceRegistry,
    active: usize,
    live_start: u64,
) -> Result<(), String> {
    for id in 1..=SOURCE_PAIRS as u32 {
        let rows = if id as usize <= active {
            vec![Observation::new(
                live_start + 1,
                format!("https://example.org/sensor{id}"),
                200.,
            )]
        } else {
            vec![]
        };
        registry.set_live_observations(id, rows)?;
    }
    Ok(())
}
fn value(r: &Row, kind: usize) -> f64 {
    match kind {
        0 => r.contacted as f64,
        1 => r.latency,
        2 => r.scanned as f64,
        _ => r.bytes as f64,
    }
}
fn format_bytes(bytes: f64) -> String {
    if bytes >= 1_000_000. {
        format!("{:.1} MB", bytes / 1_000_000.)
    } else {
        format!("{:.1} KB", bytes / 1_000.)
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    if a.active_live_sources.is_empty()
        || a.active_live_sources.contains(&0)
        || a.active_live_sources.iter().any(|n| *n > SOURCE_PAIRS)
    {
        return Err("active_live_sources must be 1..=100".into());
    }
    fs::create_dir_all(a.output_dir.join("plots"))?;
    fs::create_dir_all("docs/figures")?;
    let query = generate_federated_anomaly_query(SOURCE_PAIRS);
    let parser = JanusQLParser::new()?;
    let initial = parser.parse(&query)?;
    let initial_plan = FederatedLogicalPlan::lower(&initial)?;
    if initial_plan.branches.len() != SOURCE_PAIRS {
        return Err("expected exactly 100 logical branches".into());
    }
    let (hs, he) = initial_plan.branches[0].historical_bounds(EVALUATION_TIME)?;
    let (ls, le) = initial_plan.branches[0].live_bounds(EVALUATION_TIME)?;
    let mut registry = build_history(a.historical_observations_per_source, hs, he)?;
    let mut rows = Vec::new();
    let mut meta=String::from("total_source_pairs,active_live_sources,active_live_fraction,logical_query_count,logical_branch_count,query_parse_ms,query_lowering_ms,query_decomposition_ms,live_window,historical_window\n");
    for &active in &a.active_live_sources {
        let start = Instant::now();
        let parsed = parser.parse(&query)?;
        let parse = start.elapsed().as_secs_f64() * 1000.;
        let (plan, lowering, decomposition) = FederatedLogicalPlan::lower_timed(&parsed)?;
        let lower = lowering.as_secs_f64() * 1000.;
        let decomposition = decomposition.as_secs_f64() * 1000.;
        if plan.branches.len() != SOURCE_PAIRS {
            return Err("logical branch count changed".into());
        }
        set_active_prefix(&mut registry, active, ls)?;
        meta.push_str(&format!(
            "100,{active},{:.2},1,100,{parse:.6},{lower:.6},{decomposition:.6},[{},{}),[{}, {})\n",
            active as f64 / 100.,
            ls,
            le,
            hs,
            he
        ));
        let mut expected = None;
        for strategy in [
            ExecutionStrategy::FetchAllSources,
            ExecutionStrategy::AggregateAllSources,
            ExecutionStrategy::LiveFirstSourceSelection,
        ] {
            for _ in 0..a.warmups {
                execute_federated(strategy, &plan, &registry, EVALUATION_TIME)?;
            }
            for rep in 0..a.repetitions {
                let out = execute_federated(strategy, &plan, &registry, EVALUATION_TIME)?;
                let h = hash(&out.results);
                if let Some(e) = expected {
                    if e != (out.results.len(), h) {
                        return Err(format!(
                            "semantic mismatch active={active} strategy={}",
                            strategy.as_str()
                        )
                        .into());
                    }
                } else {
                    expected = Some((out.results.len(), h));
                }
                let m = out.metrics;
                rows.push(Row {
                    strategy,
                    active,
                    rep,
                    latency: m.end_to_end.as_secs_f64() * 1000.,
                    live: m.live_phase.as_secs_f64() * 1000.,
                    historical: m.historical_source.as_secs_f64() * 1000.,
                    coordinator: m.coordinator.as_secs_f64() * 1000.,
                    contacted: m.historical_sources_contacted,
                    skipped: m.historical_sources_skipped,
                    scanned: m.historical_records_scanned,
                    returned: m.historical_records,
                    raw: m.raw_records_transferred,
                    aggregates: m.aggregate_rows_transferred,
                    bytes: m.bytes_transferred,
                    requests: m.source_requests,
                    results: m.result_cardinality,
                    hash: h,
                    parse,
                    lower,
                    decomposition,
                });
            }
        }
    }
    let mut raw=String::from("strategy,total_source_pairs,active_live_sources,active_live_fraction,repetition,logical_query_count,logical_branch_count,live_sources_declared,historical_sources_declared,live_sources_contacted,live_sources_with_results,historical_sources_contacted,historical_sources_skipped,source_requests,historical_records_scanned,historical_records_returned,raw_records_transferred,aggregate_rows_transferred,total_bytes_transferred,total_execution_ms,live_phase_ms,historical_phase_ms,coordinator_ms,query_parse_ms,query_lowering_ms,query_decomposition_ms,result_count,result_hash\n");
    for r in &rows {
        raw.push_str(&format!("{},100,{},{:.2},{},1,100,100,100,100,{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{}\n",r.strategy.as_str(),r.active,r.active as f64/100.,r.rep,r.active,r.contacted,r.skipped,r.requests,r.scanned,r.returned,r.raw,r.aggregates,r.bytes,r.latency,r.live,r.historical,r.coordinator,r.parse,r.lower,r.decomposition,r.results,r.hash));
    }
    fs::write(
        a.output_dir.join("active_selectivity_measurements.csv"),
        raw,
    )?;
    fs::write(a.output_dir.join("query_metadata.csv"), meta)?;
    let mut summary=String::from("strategy,active_live_sources,active_live_fraction,n,median_execution_ms,mean_historical_sources_contacted,mean_historical_sources_skipped,mean_historical_records_scanned,mean_transferred_bytes\n");
    for &active in &a.active_live_sources {
        for strategy in [
            ExecutionStrategy::FetchAllSources,
            ExecutionStrategy::AggregateAllSources,
            ExecutionStrategy::LiveFirstSourceSelection,
        ] {
            let g: Vec<_> = rows
                .iter()
                .filter(|r| r.active == active && r.strategy == strategy)
                .collect();
            summary.push_str(&format!(
                "{},{},{:.2},{},{:.6},{:.3},{:.3},{:.3},{:.3}\n",
                strategy.as_str(),
                active,
                active as f64 / 100.,
                g.len(),
                median(g.iter().map(|r| r.latency).collect()),
                mean(&g.iter().map(|r| r.contacted as f64).collect::<Vec<_>>()),
                mean(&g.iter().map(|r| r.skipped as f64).collect::<Vec<_>>()),
                mean(&g.iter().map(|r| r.scanned as f64).collect::<Vec<_>>()),
                mean(&g.iter().map(|r| r.bytes as f64).collect::<Vec<_>>())
            ));
        }
    }
    fs::write(a.output_dir.join("active_selectivity_summary.csv"), summary)?;
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
            "active_selectivity_sources_contacted.svg",
            "Historical sources contacted",
            0,
        ),
        (
            "active_selectivity_latency.svg",
            "Median execution latency (ms)",
            1,
        ),
        (
            "active_selectivity_records_scanned.svg",
            "Historical observations scanned",
            2,
        ),
        ("active_selectivity_bytes.svg", "Mean transferred data", 3),
    ] {
        let max = rows.iter().map(|r| value(r, kind)).fold(1., f64::max);
        let mut s=format!("<svg xmlns='http://www.w3.org/2000/svg' width='920' height='460'><style>text{{font:14px sans-serif}}.a{{stroke:#333}}.l{{fill:none;stroke-width:2.5}}</style><text x='60' y='28'>{title}</text><line class='a' x1='70' y1='390' x2='870' y2='390'/><line class='a' x1='70' y1='55' x2='70' y2='390'/><text x='390' y='440'>Active live sources (%)</text><text x='8' y='50'>max {}</text>",if kind==3{format_bytes(max)}else{format!("{max:.3}")});
        for (i, &active) in a.active_live_sources.iter().enumerate() {
            let x = 70. + i as f64 * 800. / (a.active_live_sources.len() - 1).max(1) as f64;
            s.push_str(&format!(
                "<text x='{:.0}' y='410'>{}%</text>",
                x - 10.0,
                active
            ));
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
            let mut p = String::new();
            for (i, &active) in a.active_live_sources.iter().enumerate() {
                let g: Vec<_> = rows
                    .iter()
                    .filter(|r| r.active == active && r.strategy == strategy)
                    .collect();
                let y = if kind == 1 {
                    median(g.iter().map(|r| r.latency).collect())
                } else {
                    mean(&g.iter().map(|r| value(r, kind)).collect::<Vec<_>>())
                };
                let x = 70. + i as f64 * 800. / (a.active_live_sources.len() - 1).max(1) as f64;
                p.push_str(&format!("{x:.1},{:.1} ", 390. - y / max * 300.));
            }
            s.push_str(&format!("<polyline class='l' stroke='{stroke}' points='{p}'/><text x='650' y='{}' fill='{stroke}'>{label}</text>",65+match strategy{ExecutionStrategy::FetchAllSources=>0,ExecutionStrategy::AggregateAllSources=>20,_=>40}));
        }
        s.push_str("</svg>");
        fs::write(a.output_dir.join("plots").join(file), &s)?;
        fs::write(PathBuf::from("docs/figures").join(file), s)?;
    }
    Ok(())
}
