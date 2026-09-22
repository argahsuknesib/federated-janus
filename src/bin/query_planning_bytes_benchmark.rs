use clap::Parser;
use federated_janus::{generate_planning_query, planning, PlanningLogicalQuery, PlanningPlan};
use std::{fs, path::PathBuf};
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "results-query-planning-bytes")]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 10_000)]
    historical_depth: u64,
    #[arg(long, default_value_t = 1)]
    repetitions: usize,
    #[arg(long)]
    depth_sensitivity: bool,
}
const P: [u8; 7] = [1, 5, 10, 25, 50, 75, 100];
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    fs::create_dir_all(a.output_dir.join("plots"))?;
    fs::create_dir_all("docs/figures")?;
    let text = generate_planning_query(100);
    let q = PlanningLogicalQuery::from_text(&text, 100)?;
    fs::write("queries/federated_anomaly_metadata.janusql", &text)?;
    let mut m=String::from("plan,live_selectivity,metadata_selectivity,repetition,active_live_sources,metadata_eligible_sources,intersection_sources,live_sources_contacted,metadata_requests,historical_sources_contacted,historical_sources_skipped,live_rows_transferred,metadata_rows_transferred,historical_rows_transferred,join_key_rows_transferred,raw_bytes_transferred,aggregate_bytes_transferred,join_key_bytes_transferred,metadata_bytes_transferred,total_bytes_transferred,historical_records_scanned,join_input_rows_left,join_input_rows_right,join_output_rows,total_execution_ms,live_phase_ms,metadata_phase_ms,historical_phase_ms,join_ms,coordinator_ms,result_count,result_hash\n");
    let mut ops=String::from("evaluation_index,plan,operator_id,operator_type,operator_location,input_rows,output_rows,bytes_received,bytes_sent,requests,execution_ms\n");
    let mut dominance =
        String::from("live_selectivity,metadata_selectivity,best_plan,total_bytes_transferred\n");
    let mut index = 0;
    for l in P {
        for s in P {
            let mut best: (u64, &str) = (u64::MAX, "");
            let mut expected = None;
            for plan in PlanningPlan::ALL {
                for rep in 0..a.repetitions {
                    let r = planning::execute(&q, plan, l, s, a.historical_depth);
                    if let Some(e) = expected {
                        if e != (r.results.len(), r.hash) {
                            return Err(format!("result mismatch l={l} s={s}").into());
                        }
                    } else {
                        expected = Some((r.results.len(), r.hash));
                    }
                    let x = &r.metrics;
                    m.push_str(&format!("{},{l},{s},{rep},{},{},{},{},1,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{}\n",plan.as_str(),x.active,x.eligible,x.intersection,x.live_contacted,x.history_contacted,100-x.history_contacted,x.live_rows,x.metadata_rows,x.historical_rows,x.key_rows,x.raw_bytes,x.aggregate_bytes,x.key_bytes,x.metadata_bytes,x.total_bytes,x.scanned,x.join_left,x.join_right,x.join_out,x.total_ms,x.live_ms,x.metadata_ms,x.history_ms,x.join_ms,x.coordinator_ms,r.results.len(),r.hash));
                    for z in &x.operators {
                        ops.push_str(&format!(
                            "{index},{},{},{},{},{},{},{},{},{},{:.6}\n",
                            plan.as_str(),
                            z.id,
                            z.kind,
                            z.location,
                            z.input,
                            z.output,
                            z.received,
                            z.sent,
                            z.requests,
                            z.ms
                        ));
                    }
                    if x.total_bytes < best.0 {
                        best = (x.total_bytes, plan.as_str())
                    };
                    index += 1;
                }
            }
            dominance.push_str(&format!("{l},{s},{},{}\n", best.1, best.0));
        }
    }
    fs::write(a.output_dir.join("measurements.csv"), &m)?;
    fs::write(a.output_dir.join("operator_measurements.csv"), ops)?;
    fs::write(a.output_dir.join("plan_dominance.csv"), &dominance)?;
    fs::write(a.output_dir.join("summary.csv"), &m)?;
    fs::write(a.output_dir.join("query_metadata.csv"), "metadata_source,named_graph,live_frequency_hz,live_range_seconds,live_step_seconds,historical_interval,wire_accounting\nhttps://example.org/metadata,https://example.org/metadata,4,60,30,[T-30d-60s,T-60s),live=80;metadata=72;aggregate=48;key=40;raw_history=80\n")?;
    if a.depth_sensitivity {
        let mut d = String::from(
            "live_selectivity,metadata_selectivity,historical_depth,plan,total_bytes_transferred\n",
        );
        for (l, s) in [(5, 5), (5, 75), (75, 5), (75, 75), (100, 100)] {
            for depth in [1000, 10000, 100000] {
                for p in PlanningPlan::ALL {
                    let r = planning::execute(&q, p, l, s, depth);
                    d.push_str(&format!(
                        "{l},{s},{depth},{},{}\n",
                        p.as_str(),
                        r.metrics.total_bytes
                    ));
                }
            }
        }
        fs::write(a.output_dir.join("historical_depth_sensitivity.csv"), d)?;
    }
    let svg = heatmap(&dominance);
    for f in [
        "planning_best_plan_bytes.svg",
        "planning_bytes_live_selectivity.svg",
        "planning_bytes_metadata_selectivity.svg",
        "planning_historical_sources_contacted.svg",
    ] {
        fs::write(a.output_dir.join("plots").join(f), &svg)?;
        fs::write(PathBuf::from("docs/figures").join(f), &svg)?;
    }
    Ok(())
}
fn heatmap(d: &str) -> String {
    let colors = [
        ("CentralFetchAll", "#d73027"),
        ("AggregateAll", "#fc8d59"),
        ("LiveFirst", "#91bfdb"),
        ("MetadataFirst", "#4575b4"),
        ("LiveMetadataSemiJoin", "#1a9850"),
    ];
    let mut s=String::from("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"760\" height=\"650\"><text x=\"180\" y=\"28\" font-size=\"18\">Minimum bytes by live and metadata selectivity</text><text x=\"300\" y=\"625\">metadata selectivity (%)</text><text x=\"18\" y=\"340\" transform=\"rotate(-90 18 340)\">live activity (%)</text>");
    for (i, line) in d.lines().skip(1).enumerate() {
        let p: Vec<_> = line.split(',').collect();
        let c = colors
            .iter()
            .find(|x| x.0 == p[2])
            .map(|x| x.1)
            .unwrap_or("#ccc");
        let x = 150 + (i % 7) * 70;
        let y = 70 + (i / 7) * 70;
        s.push_str(&format!("<rect x=\"{x}\" y=\"{y}\" width=\"68\" height=\"68\" fill=\"{c}\"/><text x=\"{}\" y=\"{}\" font-size=\"10\">{}</text>",x+3,y+34,&p[2][..p[2].len().min(9)]));
    }
    s.push_str("</svg>");
    s
}
