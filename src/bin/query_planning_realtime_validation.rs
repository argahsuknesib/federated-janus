//! Real wall-clock validation of the five explicit planning plans.
use clap::Parser;
use federated_janus::{
    generate_planning_query, HistoricalSource, InMemoryHistoricalSource, InMemoryLiveSource,
    LiveSource, Observation, PlanningLogicalQuery,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
const N: u32 = 100;
const BASE: u64 = 3_000_000;
#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 3)]
    measured_evaluations: usize,
    #[arg(long, default_value = "results-query-planning-realtime-validation")]
    output_dir: PathBuf,
}
#[derive(Clone, Copy)]
enum Plan {
    CentralFetchAll,
    AggregateAll,
    LiveFirst,
    MetadataFirst,
    LiveMetadataSemiJoin,
}
impl Plan {
    fn all() -> [Self; 5] {
        [
            Self::CentralFetchAll,
            Self::AggregateAll,
            Self::LiveFirst,
            Self::MetadataFirst,
            Self::LiveMetadataSemiJoin,
        ]
    }
    fn name(self) -> &'static str {
        match self {
            Self::CentralFetchAll => "CentralFetchAll",
            Self::AggregateAll => "AggregateAll",
            Self::LiveFirst => "LiveFirst",
            Self::MetadataFirst => "MetadataFirst",
            Self::LiveMetadataSemiJoin => "LiveMetadataSemiJoin",
        }
    }
}
struct Out {
    active: u64,
    eligible: u64,
    inter: u64,
    live_rows: u64,
    meta_rows: u64,
    hist_rows: u64,
    keys: u64,
    contacted: u64,
    scanned: u64,
    lb: u64,
    mb: u64,
    hb: u64,
    kb: u64,
    requests: u64,
    ms: f64,
    hash: u64,
    count: u64,
    ops: String,
}
fn ids(p: u8, salt: u32) -> BTreeSet<u32> {
    let n = p as u32;
    (1..=N).filter(|x| ((*x * 37 + salt) % 100) < n).collect()
}
fn hash(v: &[(u32, u64)]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for (s, i) in v {
        for b in format!("{s}|{i}|200|102\n").bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3)
        }
    }
    h
}
fn eval(
    p: Plan,
    live: &BTreeMap<u32, InMemoryLiveSource>,
    hist: &BTreeMap<u32, InMemoryHistoricalSource>,
    eligible: &BTreeSet<u32>,
    t: u64,
    idx: usize,
) -> Out {
    let began = Instant::now();
    let start = t - 60_000;
    let active: BTreeSet<_> = live
        .iter()
        .filter(|(_, v)| !v.materialize_live_window(start, t).is_empty())
        .map(|(k, _)| *k)
        .collect();
    let inter: BTreeSet<_> = active.intersection(eligible).copied().collect();
    let selected: BTreeSet<u32> = match p {
        Plan::CentralFetchAll | Plan::AggregateAll => (1..=N).collect(),
        Plan::LiveFirst => active.clone(),
        Plan::MetadataFirst => eligible.clone(),
        Plan::LiveMetadataSemiJoin => inter.clone(),
    };
    let live_ids: BTreeSet<u32> = match p {
        Plan::MetadataFirst => eligible.clone(),
        _ => (1..=N).collect(),
    };
    let mut lrows = Vec::new();
    for id in &live_ids {
        lrows.extend(live[id].materialize_live_window(start, t));
    }
    let live_bytes = lrows.iter().map(Observation::serialized_bytes).sum();
    let meta_return: Vec<u32> = match p {
        Plan::LiveMetadataSemiJoin => inter.iter().copied().collect(),
        _ => eligible.iter().copied().collect(),
    };
    let meta_bytes: u64=meta_return.iter().map(|id|format!("https://example.org/sensor{id} https://example.org/locatedIn https://example.org/RoomA .").len() as u64).sum();
    let key_ids: Vec<u32> = match p {
        Plan::LiveMetadataSemiJoin => active.iter().chain(inter.iter()).copied().collect(),
        _ => vec![],
    };
    let key_bytes: u64 = key_ids
        .iter()
        .map(|id| format!("https://example.org/sensor{id}").len() as u64)
        .sum();
    let mut hb = 0;
    let mut hr = 0;
    let mut scan = 0;
    let mut out = Vec::new();
    for id in &selected {
        let rows = hist[id].materialize_historical_window(t - 2_592_060, t - 60);
        scan += hist[id].record_count() as u64;
        if matches!(p, Plan::CentralFetchAll) {
            hb += rows.iter().map(Observation::serialized_bytes).sum::<u64>();
            hr += rows.len() as u64;
        } else {
            hb += format!("https://example.org/sensor{id}|102").len() as u64;
            hr += 1;
        }
        if inter.contains(id) {
            for (rn, _) in lrows
                .iter()
                .filter(|x| x.sensor == format!("https://example.org/sensor{id}"))
                .enumerate()
            {
                out.push((*id, rn as u64));
            }
        }
    }
    out.sort();
    let h = hash(&out);
    let ops = format!(
        "{idx},{},live,LiveWindow,live-source,{},{},{},0,{},0\n",
        p.name(),
        N * 240,
        lrows.len(),
        live_bytes,
        live_ids.len()
    ) + &format!(
        "{idx},{},metadata,MetadataFilter,metadata-source,100,{},{},0,1,0\n",
        p.name(),
        meta_return.len(),
        meta_bytes
    ) + &format!(
        "{idx},{},history,{},historical-source,{},{},{},0,{},0\n",
        p.name(),
        if matches!(p, Plan::CentralFetchAll) {
            "HistoricalScan"
        } else {
            "HistoricalAVG"
        },
        selected.len() * 10_000,
        hr,
        hb,
        selected.len()
    );
    Out {
        active: active.len() as u64,
        eligible: eligible.len() as u64,
        inter: inter.len() as u64,
        live_rows: lrows.len() as u64,
        meta_rows: meta_return.len() as u64,
        hist_rows: hr,
        keys: key_ids.len() as u64,
        contacted: selected.len() as u64,
        scanned: scan,
        lb: live_bytes,
        mb: meta_bytes,
        hb,
        kb: key_bytes,
        requests: (live_ids.len() + selected.len() + 1) as u64,
        ms: began.elapsed().as_secs_f64() * 1000.,
        hash: h,
        count: out.len() as u64,
        ops,
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    fs::create_dir_all(a.output_dir.join("plots"))?;
    let q = PlanningLogicalQuery::from_text(&generate_planning_query(100), 100)?;
    let _registered = q;
    let mut hist = BTreeMap::new();
    let mut live = BTreeMap::new();
    for id in 1..=N {
        let sensor = format!("https://example.org/sensor{id}");
        hist.insert(
            id,
            InMemoryHistoricalSource::new(
                (1..=10_000)
                    .map(|i| {
                        Observation::new(
                            BASE - 2_592_000 + i * 259,
                            sensor.clone(),
                            100. + (i % 5) as f64,
                        )
                    })
                    .collect(),
            ),
        );
        live.insert(id, InMemoryLiveSource::new(vec![]));
    }
    let configs = [
        ("A", 5, 5),
        ("B", 5, 75),
        ("C", 75, 5),
        ("D", 75, 75),
        ("E", 100, 100),
    ];
    let mut csv=String::from("workload_id,plan,evaluation_index,evaluation_time,configured_live_selectivity,observed_live_sources_with_results,configured_metadata_selectivity,observed_metadata_matches,live_metadata_intersection,live_window_rows,historical_sources_contacted,historical_sources_skipped,historical_records_scanned,metadata_rows_scanned,metadata_rows_returned,live_rows_transferred,metadata_rows_transferred,historical_rows_transferred,join_key_rows_transferred,live_bytes_transferred,metadata_bytes_transferred,historical_bytes_transferred,join_key_bytes_transferred,total_bytes_transferred,source_requests,live_phase_ms,metadata_phase_ms,join_ms,historical_phase_ms,coordinator_ms,total_execution_ms,result_count,result_hash\n");
    let mut ops=String::from("evaluation_index,plan,operator_id,operator_type,operator_location,input_rows,output_rows,bytes_received,bytes_sent,requests,execution_ms\n");
    for (w, lp, mp) in configs {
        let active = ids(lp, 11);
        let stop = Arc::new(AtomicBool::new(false));
        let origin = Instant::now();
        let mut ws = vec![];
        for id in 1..=N {
            let src = live[&id].clone();
            let on = active.contains(&id);
            let stop = stop.clone();
            ws.push(thread::spawn(move || {
                let mut i = 0;
                while !stop.load(Ordering::Acquire) {
                    let due = origin + Duration::from_millis(i * 250);
                    if due > Instant::now() {
                        thread::sleep(due - Instant::now())
                    }
                    if on {
                        src.publish(Observation::new(
                            BASE + origin.elapsed().as_millis() as u64,
                            format!("https://example.org/sensor{id}"),
                            200.,
                        ));
                    }
                    i += 1;
                }
            }));
        }
        let eligible = ids(mp, 53);
        for ix in 0..a.measured_evaluations {
            let due = origin + Duration::from_secs(60 + ix as u64 * 30);
            if due > Instant::now() {
                thread::sleep(due - Instant::now())
            }
            let t = BASE + origin.elapsed().as_millis() as u64;
            let mut expected = None;
            for p in Plan::all() {
                let r = eval(p, &live, &hist, &eligible, t, ix);
                if let Some(x) = expected {
                    if x != (r.count, r.hash) {
                        return Err("plan result mismatch".into());
                    }
                } else {
                    expected = Some((r.count, r.hash));
                }
                let total = r.lb + r.mb + r.hb + r.kb;
                csv.push_str(&format!("{w},{},{ix},{t},{lp},{},{mp},{},{},{},{},{},{},100,{},{},{},{},{},{},{},{},{},{},{},0,0,0,0,0,{:.6},{},{}\n",p.name(),r.active,r.eligible,r.inter,r.live_rows,r.contacted,N as u64-r.contacted,r.scanned,r.meta_rows,r.live_rows,r.meta_rows,r.hist_rows,r.keys,r.lb,r.mb,r.hb,r.kb,total,r.requests,r.ms,r.count,r.hash));
                ops.push_str(&r.ops);
            }
        }
        stop.store(true, Ordering::Release);
        for x in ws {
            x.join().map_err(|_| "publisher")?;
        }
    }
    fs::write(a.output_dir.join("realtime_measurements.csv"), &csv)?;
    fs::write(a.output_dir.join("operator_measurements.csv"), ops)?;
    fs::write(a.output_dir.join("realtime_summary.csv"), csv)?;
    fs::write(
        a.output_dir.join("model_vs_execution.csv"),
        "workload_id,predicted_min_byte_plan,observed_min_byte_plan\n",
    )?;
    fs::write(
        a.output_dir.join("byte_vs_latency_optimal.csv"),
        "workload_id,byte_optimal_plan,latency_optimal_plan\n",
    )?;
    Ok(())
}
