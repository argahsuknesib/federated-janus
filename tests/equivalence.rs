use federated_janus::*;
fn sources(
    live: Vec<(&str, f64)>,
    historical: Vec<(&str, f64)>,
) -> (InMemoryLiveSource, InMemoryHistoricalSource) {
    (
        InMemoryLiveSource::new(
            live.into_iter()
                .map(|(s, v)| Observation::new(1, s, v))
                .collect(),
        ),
        InMemoryHistoricalSource::new(
            historical
                .into_iter()
                .map(|(s, v)| Observation::new(0, s, v))
                .collect(),
        ),
    )
}
fn all(l: &InMemoryLiveSource, h: &InMemoryHistoricalSource) -> Vec<Vec<Anomaly>> {
    [
        ExecutionStrategy::FetchAll,
        ExecutionStrategy::AggregatePushdown,
        ExecutionStrategy::BindJoin,
    ]
    .into_iter()
    .map(|s| execute(s, &LogicalPlan::default(), l, h).unwrap().results)
    .collect()
}
#[test]
fn strategies_are_equivalent() {
    let (l, h) = sources(
        vec![("a", 150.), ("b", 100.)],
        vec![("a", 100.), ("a", 100.), ("b", 100.)],
    );
    let r = all(&l, &h);
    assert_eq!(r[0], r[1]);
    assert_eq!(r[1], r[2]);
    assert_eq!(r[0].len(), 1)
}
#[test]
fn no_matching_sensors() {
    let (l, h) = sources(vec![("live", 200.)], vec![("history", 100.)]);
    assert!(all(&l, &h).into_iter().all(|x| x.is_empty()))
}
#[test]
fn all_live_sensors_match() {
    let (l, h) = sources(
        vec![("a", 140.), ("b", 140.)],
        vec![("a", 100.), ("b", 100.)],
    );
    assert_eq!(all(&l, &h)[0].len(), 2)
}
#[test]
fn one_live_sensor() {
    let (l, h) = sources(vec![("a", 140.)], vec![("a", 100.), ("b", 200.)]);
    let r = all(&l, &h);
    assert_eq!(r[2].len(), 1);
    assert_eq!(r[0], r[2])
}
#[test]
fn duplicate_history_aggregates() {
    let (l, h) = sources(vec![("a", 200.)], vec![("a", 100.), ("a", 200.)]);
    let r = all(&l, &h);
    assert_eq!(r[0][0].historical_average, 150.)
}
#[test]
fn bind_restriction_is_correct() {
    let (l, h) = sources(vec![("a", 140.)], vec![("a", 100.), ("other", 1.)]);
    let r = execute(ExecutionStrategy::BindJoin, &LogicalPlan::default(), &l, &h).unwrap();
    assert_eq!(r.metrics.historical_records, 1);
    assert_eq!(r.results.len(), 1)
}
#[test]
fn anomaly_filter_boundary() {
    let (l, h) = sources(
        vec![("a", 130.), ("b", 130.01)],
        vec![("a", 100.), ("b", 100.)],
    );
    let r = all(&l, &h);
    assert_eq!(r[0].len(), 1);
    assert_eq!(r[0], r[1]);
}
#[test]
fn generator_is_deterministic() {
    let (l, h) = sources(vec![("a", 200.)], vec![("a", 100.)]);
    assert_eq!(all(&l, &h), all(&l, &h));
}
