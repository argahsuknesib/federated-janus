use federated_janus::sources::compact::CompactHistoricalSource;
use federated_janus::{execute_compact, ExecutionStrategy, LogicalPlan};
use std::collections::HashSet;
const T: u64 = 3_000_000;
fn plan() -> LogicalPlan {
    LogicalPlan::from_text(include_str!("../queries/anomaly.janusql")).unwrap()
}
fn ids(values: &[u32]) -> HashSet<u32> {
    values.iter().copied().collect()
}
#[test]
fn one_sensor_touches_only_its_records() {
    let h = CompactHistoricalSource::deterministic(10, 100, 1, 0, 100);
    let (_, s) = h.aggregate_bound(&ids(&[3]), 0, 100);
    assert_eq!(
        (
            s.entities_looked_up,
            s.records_scanned,
            s.records_matched,
            s.records_returned
        ),
        (1, 100, 100, 1)
    );
}
#[test]
fn multiple_sensors_touch_only_requested_records() {
    let h = CompactHistoricalSource::deterministic(10, 100, 1, 0, 100);
    let (_, s) = h.aggregate_bound(&ids(&[1, 4, 9]), 0, 100);
    assert_eq!(
        (
            s.entities_looked_up,
            s.records_scanned,
            s.records_matched,
            s.records_returned
        ),
        (3, 300, 300, 3)
    );
}
#[test]
fn unknown_sensor_has_no_result() {
    let h = CompactHistoricalSource::deterministic(2, 10, 1, 0, 10);
    let (a, s) = h.aggregate_bound(&ids(&[99]), 0, 10);
    assert!(a.is_empty());
    assert_eq!(
        (s.entities_looked_up, s.records_scanned, s.records_returned),
        (1, 0, 0)
    );
}
#[test]
fn range_and_indexed_aggregation_match_full_scan() {
    let h = CompactHistoricalSource::deterministic(4, 10, 1, 0, 10);
    let (full, _) = h.aggregate_all(2, 7);
    let (bound, s) = h.aggregate_bound(&ids(&[0, 1, 2, 3]), 2, 7);
    assert_eq!(full, bound);
    assert_eq!(s.records_scanned, 40);
}
#[test]
fn duplicate_live_bindings_do_not_repeat_historical_work() {
    let p = plan();
    let (start, end) = p.historical_bounds(T).unwrap();
    let h = CompactHistoricalSource::deterministic(3, 100, 1, start, end);
    let o = execute_compact(
        ExecutionStrategy::BindJoin,
        &p,
        &[(1, T - 1, 200.), (1, T - 1, 201.)],
        &h,
        T,
    );
    assert_eq!(o.metrics.historical_records_scanned, 100);
    assert_eq!(o.metrics.historical_entities_looked_up, 1);
}
