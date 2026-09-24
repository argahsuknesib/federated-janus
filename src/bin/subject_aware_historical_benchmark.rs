use federated_janus::{
    execute_local_plan, InMemoryLiveSource, LocalExecutionPlan, LogicalPlan, Observation,
    SegmentedHistoricalSource,
};
use std::{fs, io::Write, path::PathBuf, time::Instant};
const SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];
const E: u64 = 3_000_000;
const HEADER:&str="historical_quads,plan,repetition,execution_order,storage_records_examined,storage_records_matched,storage_records_returned,operator_output_rows,index_entries_examined,index_seek_count,log_seek_count,segments_touched,subject_index_used,index_lookup_ms,log_read_decode_ms,historical_storage_ms,historical_operator_ms,coordinator_ms,total_execution_ms,result_count,result_hash,base_storage_bytes,subject_index_bytes,total_storage_bytes,index_entry_count,storage_build_ms";
fn plans(r: usize) -> [LocalExecutionPlan; 4] {
    let p = [
        LocalExecutionPlan::AggregatePushdown,
        LocalExecutionPlan::TimestampOnlyBindJoin,
        LocalExecutionPlan::SubjectAwareLinearBindJoin,
        LocalExecutionPlan::SubjectAwareBinaryBindJoin,
    ];
    [p[r % 4], p[(r + 1) % 4], p[(r + 2) % 4], p[(r + 3) % 4]]
}
fn pn(p: LocalExecutionPlan) -> &'static str {
    match p {
        LocalExecutionPlan::AggregatePushdown => "AggregatePushdown",
        LocalExecutionPlan::TimestampOnlyBindJoin => "TimestampOnlyBindJoin",
        LocalExecutionPlan::SubjectAwareBindJoin => "SubjectAwareBindJoin",
        LocalExecutionPlan::SubjectAwareLinearBindJoin => "SubjectAwareLinearBindJoin",
        LocalExecutionPlan::SubjectAwareBinaryBindJoin => "SubjectAwareBinaryBindJoin",
    }
}
fn h(o: &federated_janus::LocalPlanOutcome) -> String {
    o.results
        .iter()
        .map(|x| {
            format!(
                "{}:{:.8}:{:.8}",
                x.sensor, x.current_value, x.historical_average
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}
fn ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1000.
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from("results-subject-aware-binary-historical");
    fs::create_dir_all(&root)?;
    let mut f = fs::File::create(root.join("subject_aware_measurements.csv"))?;
    writeln!(f, "{HEADER}")?;
    let plan = LogicalPlan::from_text(include_str!("../../queries/anomaly.janusql"))?;
    for n in SIZES {
        let archive = root.join(format!("archive-{n}"));
        let b = Instant::now();
        let hist = SegmentedHistoricalSource::deterministic(
            &archive, n, 100, 7, 300_000, 2_950_000, 10_000,
        )?;
        let build = ms(b.elapsed());
        let live = InMemoryLiveSource::new(
            (1..=10)
                .map(|x| Observation::new(E - 1, format!("https://example.org/sensor{x}"), 1000.))
                .collect(),
        );
        let a = execute_local_plan(
            LocalExecutionPlan::AggregatePushdown,
            &plan,
            &live,
            &hist,
            E,
        )?;
        let t = execute_local_plan(
            LocalExecutionPlan::TimestampOnlyBindJoin,
            &plan,
            &live,
            &hist,
            E,
        )?;
        let i = execute_local_plan(
            LocalExecutionPlan::SubjectAwareLinearBindJoin,
            &plan,
            &live,
            &hist,
            E,
        )?;
        let binary = execute_local_plan(
            LocalExecutionPlan::SubjectAwareBinaryBindJoin,
            &plan,
            &live,
            &hist,
            E,
        )?;
        if h(&a) != h(&t)
            || h(&t) != h(&i)
            || h(&i) != h(&binary)
            || a.historical_averages
                .iter()
                .filter(|(s, _)| t.live_bindings.contains(*s))
                .count()
                != 10
            || t.historical_averages != i.historical_averages
            || a.operator_output_rows != 100
            || t.operator_output_rows != 10
            || i.operator_output_rows != 10
            || binary.operator_output_rows != 10
            || !i.storage.subject_index_used
            || !binary.storage.subject_index_used
            || i.storage.records_examined != binary.storage.records_examined
            || i.storage.records_matched != binary.storage.records_matched
            || i.storage.records_returned != binary.storage.records_returned
            || a.historical_interval != t.historical_interval
            || t.historical_interval != i.historical_interval
            || a.live_bindings != t.live_bindings
            || t.live_bindings != i.live_bindings
        {
            return Err("correctness gate failed before timing".into());
        };
        let z = hist.storage_size_accounting()?;
        for r in 0..11 {
            for p in plans(r) {
                let o = execute_local_plan(p, &plan, &live, &hist, E)?;
                if r > 0 {
                    writeln!(f,"{n},{},{},{}/{}/{}/{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{},{},{},{},{:.6},{:.6}",pn(p),r-1,pn(plans(r)[0]),pn(plans(r)[1]),pn(plans(r)[2]),pn(plans(r)[3]),o.storage.records_examined,o.storage.records_matched,o.storage.records_returned,o.operator_output_rows,o.storage.index_entries_examined,o.storage.index_seek_count,o.storage.log_seek_count,o.storage.segments_touched,o.storage.subject_index_used,ms(o.storage.index_lookup),ms(o.storage.log_read_decode),ms(o.historical_storage),ms(o.historical_operator),ms(o.coordinator),ms(o.total_execution),o.results.len(),h(&o),z.base_storage_bytes,z.subject_index_bytes,z.total_storage_bytes,z.index_entry_count,build)?;
                }
            }
        }
    }
    f.flush()?;
    Ok(())
}
