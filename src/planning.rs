//! Deterministic, explicit physical plans for the three-input experiment.
//! Transfer sizes are a single logical N-Triples-like wire representation:
//! live RDF rows are 80 bytes, metadata triples 72 bytes, aggregate tuples 48
//! bytes, and sensor-key bindings 40 bytes. Local scans are never counted.
use crate::planner::PlanningLogicalQuery;
use std::{collections::BTreeSet, time::Instant};

pub const LIVE_ROW_BYTES: u64 = 80;
pub const METADATA_ROW_BYTES: u64 = 72;
pub const AGGREGATE_ROW_BYTES: u64 = 48;
pub const KEY_BYTES: u64 = 40;
pub const RAW_HISTORY_ROW_BYTES: u64 = 80;
pub const EVENTS_PER_ACTIVE_SOURCE: u64 = 240; // 4 Hz * 60 seconds, [T-60s,T)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanningPlan {
    CentralFetchAll,
    AggregateAll,
    LiveFirst,
    MetadataFirst,
    LiveMetadataSemiJoin,
}
impl PlanningPlan {
    pub const ALL: [Self; 5] = [
        Self::CentralFetchAll,
        Self::AggregateAll,
        Self::LiveFirst,
        Self::MetadataFirst,
        Self::LiveMetadataSemiJoin,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CentralFetchAll => "CentralFetchAll",
            Self::AggregateAll => "AggregateAll",
            Self::LiveFirst => "LiveFirst",
            Self::MetadataFirst => "MetadataFirst",
            Self::LiveMetadataSemiJoin => "LiveMetadataSemiJoin",
        }
    }
}
#[derive(Debug, Clone)]
pub struct OperatorMetric {
    pub id: &'static str,
    pub kind: &'static str,
    pub location: &'static str,
    pub input: u64,
    pub output: u64,
    pub received: u64,
    pub sent: u64,
    pub requests: u64,
    pub ms: f64,
}
#[derive(Debug, Clone)]
pub struct PlanningMetrics {
    pub active: u64,
    pub eligible: u64,
    pub intersection: u64,
    pub live_contacted: u64,
    pub metadata_requests: u64,
    pub history_contacted: u64,
    pub live_rows: u64,
    pub metadata_rows: u64,
    pub historical_rows: u64,
    pub key_rows: u64,
    pub raw_bytes: u64,
    pub aggregate_bytes: u64,
    pub key_bytes: u64,
    pub metadata_bytes: u64,
    pub total_bytes: u64,
    pub scanned: u64,
    pub join_left: u64,
    pub join_right: u64,
    pub join_out: u64,
    pub total_ms: f64,
    pub live_ms: f64,
    pub metadata_ms: f64,
    pub history_ms: f64,
    pub join_ms: f64,
    pub coordinator_ms: f64,
    pub operators: Vec<OperatorMetric>,
}
#[derive(Debug, Clone)]
pub struct PlanningRun {
    pub results: Vec<(u32, u64)>,
    pub hash: u64,
    pub metrics: PlanningMetrics,
}

fn set_for(percent: u8, salt: u32) -> BTreeSet<u32> {
    // Independent deterministic permutations; exact requested cardinality.
    let n = ((percent as usize * 100 + 50) / 100) as u32;
    (1u32..=100)
        .filter(|id| (((*id).wrapping_mul(37).wrapping_add(salt)) % 100) < n)
        .collect()
}
pub fn active_set(percent: u8) -> BTreeSet<u32> {
    set_for(percent, 11)
}
pub fn eligible_set(percent: u8) -> BTreeSet<u32> {
    set_for(percent, 53)
}
#[allow(clippy::too_many_arguments)]
fn op(
    v: &mut Vec<OperatorMetric>,
    id: &'static str,
    kind: &'static str,
    location: &'static str,
    input: u64,
    output: u64,
    received: u64,
    sent: u64,
    requests: u64,
    ms: f64,
) {
    v.push(OperatorMetric {
        id,
        kind,
        location,
        input,
        output,
        received,
        sent,
        requests,
        ms,
    });
}
pub fn execute(
    query: &PlanningLogicalQuery,
    plan: PlanningPlan,
    live_percent: u8,
    metadata_percent: u8,
    historical_depth: u64,
) -> PlanningRun {
    let start = Instant::now();
    let active = active_set(live_percent);
    let eligible = eligible_set(metadata_percent);
    let intersection: BTreeSet<_> = active.intersection(&eligible).copied().collect();
    let mut o = Vec::new();
    let n = query.source_pairs as u64;
    let active_n = active.len() as u64;
    let eligible_n = eligible.len() as u64;
    let inter_n = intersection.len() as u64;
    let (
        live_contacted,
        live_rows,
        history_contacted,
        metadata_rows,
        key_rows,
        raw_rows,
        aggregate_rows,
        join_left,
        join_right,
        join_out,
        join_kind,
    ) = match plan {
        PlanningPlan::CentralFetchAll => (
            n,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            n,
            eligible_n,
            0,
            n * historical_depth,
            0,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            eligible_n,
            inter_n * EVENTS_PER_ACTIVE_SOURCE,
            "HashJoin",
        ),
        PlanningPlan::AggregateAll => (
            n,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            n,
            eligible_n,
            0,
            0,
            n,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            eligible_n,
            inter_n * EVENTS_PER_ACTIVE_SOURCE,
            "HashJoin",
        ),
        PlanningPlan::LiveFirst => (
            n,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            active_n,
            eligible_n,
            0,
            0,
            active_n,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            eligible_n,
            inter_n * EVENTS_PER_ACTIVE_SOURCE,
            "HashJoin",
        ),
        PlanningPlan::MetadataFirst => (
            eligible_n,
            inter_n * EVENTS_PER_ACTIVE_SOURCE,
            eligible_n,
            eligible_n,
            0,
            0,
            eligible_n,
            inter_n * EVENTS_PER_ACTIVE_SOURCE,
            eligible_n,
            inter_n * EVENTS_PER_ACTIVE_SOURCE,
            "BindJoin",
        ),
        PlanningPlan::LiveMetadataSemiJoin => (
            n,
            active_n * EVENTS_PER_ACTIVE_SOURCE,
            inter_n,
            inter_n,
            active_n + inter_n,
            0,
            inter_n,
            active_n,
            inter_n,
            inter_n,
            "SemiJoin",
        ),
    };
    let raw_bytes = raw_rows * RAW_HISTORY_ROW_BYTES;
    let aggregate_bytes = aggregate_rows * AGGREGATE_ROW_BYTES;
    let metadata_bytes = metadata_rows * METADATA_ROW_BYTES;
    let key_bytes = key_rows * KEY_BYTES;
    let live_bytes = live_rows * LIVE_ROW_BYTES;
    let live_ms = start.elapsed().as_secs_f64() * 1000.;
    op(
        &mut o,
        "live",
        "LiveWindow",
        "live-source",
        n * EVENTS_PER_ACTIVE_SOURCE,
        live_rows,
        live_bytes,
        0,
        live_contacted,
        live_ms,
    );
    let t = Instant::now();
    op(
        &mut o,
        "metadata",
        "MetadataScan",
        "metadata-source",
        100,
        metadata_rows,
        metadata_bytes,
        key_bytes,
        1,
        t.elapsed().as_secs_f64() * 1000.,
    );
    let t = Instant::now();
    if key_rows > 0 {
        op(
            &mut o,
            "keys",
            join_kind,
            "coordinator",
            active_n,
            inter_n,
            key_bytes,
            key_bytes,
            1,
            t.elapsed().as_secs_f64() * 1000.,
        );
    }
    let t = Instant::now();
    if raw_rows > 0 {
        op(
            &mut o,
            "history",
            "HistoricalScan",
            "historical-source",
            raw_rows,
            raw_rows,
            raw_bytes,
            0,
            history_contacted,
            t.elapsed().as_secs_f64() * 1000.,
        );
    } else {
        op(
            &mut o,
            "history",
            "HistoricalAVG",
            "historical-source",
            history_contacted * historical_depth,
            aggregate_rows,
            aggregate_bytes,
            0,
            history_contacted,
            t.elapsed().as_secs_f64() * 1000.,
        );
    }
    let t = Instant::now();
    op(
        &mut o,
        "join",
        join_kind,
        "coordinator",
        join_left + join_right,
        join_out,
        0,
        0,
        0,
        t.elapsed().as_secs_f64() * 1000.,
    );
    let results: Vec<_> = intersection
        .iter()
        .flat_map(|id| (0..EVENTS_PER_ACTIVE_SOURCE).map(move |i| (*id, i)))
        .collect();
    let mut h = 0xcbf29ce484222325u64;
    for (s, i) in &results {
        for b in format!("{s}|{i}|200|102\n").bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.;
    PlanningRun {
        results,
        hash: h,
        metrics: PlanningMetrics {
            active: active_n,
            eligible: eligible_n,
            intersection: inter_n,
            live_contacted,
            metadata_requests: 1,
            history_contacted,
            live_rows,
            metadata_rows,
            historical_rows: raw_rows + aggregate_rows,
            key_rows,
            raw_bytes,
            aggregate_bytes,
            key_bytes,
            metadata_bytes,
            total_bytes: live_bytes + raw_bytes + aggregate_bytes + key_bytes + metadata_bytes,
            scanned: history_contacted * historical_depth,
            join_left,
            join_right,
            join_out,
            total_ms,
            live_ms,
            metadata_ms: o[1].ms,
            history_ms: o.last().map(|x| x.ms).unwrap_or(0.),
            join_ms: o.last().map(|x| x.ms).unwrap_or(0.),
            coordinator_ms: 0.,
            operators: o,
        },
    }
}
