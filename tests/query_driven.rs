use federated_janus::*;
const T: u64 = 3_000_000;
fn query() -> LogicalPlan {
    LogicalPlan::from_text(include_str!("../queries/anomaly.janusql")).unwrap()
}
fn sources() -> (InMemoryLiveSource, InMemoryHistoricalSource) {
    let live = InMemoryLiveSource::new(vec![
        Observation::new(T - 1, "a", 140.),
        Observation::new(T - 60, "leaves-on-next-step", 100.),
        Observation::new(T - 61, "outside", 999.),
    ]);
    let history = InMemoryHistoricalSource::new(vec![
        Observation::new(T - 2, "a", 100.),
        Observation::new(T - 2_592_001, "old", 1.),
    ]);
    (live, history)
}
#[test]
fn fixture_parses_and_lowers() {
    let p = query();
    assert_eq!(p.live_window.width, 60);
    assert_eq!(p.live_window.slide, 5);
    assert_eq!(p.historical_window.width, 2_592_000);
    assert_eq!(p.historical_window.offset, Some(2_592_000));
    assert_eq!(p.join_variable, "?sensor");
    assert_eq!(p.historical_aggregate.function, "AVG");
    assert_eq!(p.condition.multiplier, 1.3);
    assert_eq!(
        p.live_window.source_name,
        "https://example.org/live-sensors"
    );
    assert_eq!(
        p.historical_window.source_name,
        "https://example.org/historical-sensors"
    );
}
#[test]
fn historical_bounds_are_janus_bounds() {
    assert_eq!(query().historical_bounds(T).unwrap(), (T - 2_592_000, T));
}
#[test]
fn temporal_membership_uses_same_evaluation_time() {
    let p = query();
    let (l, h) = sources();
    assert_eq!(
        l.materialize_live_window(p.live_bounds(T).unwrap().0, T)
            .len(),
        2
    );
    assert_eq!(
        l.materialize_live_window(
            p.live_bounds(T + p.live_window.slide).unwrap().0,
            T + p.live_window.slide
        )
        .len(),
        1
    );
    assert_eq!(
        h.materialize_historical_window(p.historical_bounds(T).unwrap().0, T)
            .len(),
        1
    );
}
#[test]
fn plans_and_expected_anomaly_are_equivalent() {
    let p = query();
    let (l, h) = sources();
    let results: Vec<_> = [
        ExecutionStrategy::FetchAll,
        ExecutionStrategy::AggregatePushdown,
        ExecutionStrategy::BindJoin,
    ]
    .into_iter()
    .map(|s| execute(s, &p, &l, &h, T).unwrap().results)
    .collect();
    assert_eq!(results[0], results[1]);
    assert_eq!(results[1], results[2]);
    assert_eq!(results[0].len(), 1);
}
#[test]
fn threshold_is_query_driven() {
    let p = query();
    let stricter =
        LogicalPlan::from_text(&include_str!("../queries/anomaly.janusql").replace("1.3", "1.5"))
            .unwrap();
    let (l, h) = sources();
    assert_eq!(
        execute(ExecutionStrategy::FetchAll, &p, &l, &h, T)
            .unwrap()
            .results
            .len(),
        1
    );
    assert!(execute(ExecutionStrategy::FetchAll, &stricter, &l, &h, T)
        .unwrap()
        .results
        .is_empty());
}
