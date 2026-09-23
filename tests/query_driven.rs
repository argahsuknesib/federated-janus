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
        Observation::new(T - 61, "a", 100.),
        Observation::new(T - 60, "overlaps-live-boundary", 999.),
        Observation::new(T - 2_592_061, "old", 1.),
    ]);
    (live, history)
}

#[test]
fn fixture_parses_and_lowers_without_deprecated_baseline_syntax() {
    let text = include_str!("../queries/anomaly.janusql");
    assert!(!text.contains("DEFINE BASELINE"));
    assert!(!text.contains("USING BASELINE"));

    let p = query();
    assert_eq!(p.live_window.width, 60);
    assert_eq!(p.live_window.slide, 30);
    assert_eq!(p.historical_window.width, 2_592_000);
    assert_eq!(p.historical_window.offset, Some(2_592_060));
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
fn historical_and_live_windows_are_non_overlapping() {
    let p = query();
    let historical = p.historical_bounds(T).unwrap();
    let live = p.live_bounds(T).unwrap();
    assert_eq!(historical, (T - 2_592_060, T - 60));
    assert_eq!(live, (T - 60, T));
    assert_eq!(historical.1, live.0);
}

#[test]
fn temporal_membership_uses_same_evaluation_time() {
    let p = query();
    let (l, h) = sources();
    let live = p.live_bounds(T).unwrap();
    let history = p.historical_bounds(T).unwrap();

    assert_eq!(l.materialize_live_window(live.0, live.1).len(), 2);
    assert_eq!(
        l.materialize_live_window(
            p.live_bounds(T + p.live_window.slide).unwrap().0,
            T + p.live_window.slide
        )
        .len(),
        1
    );
    assert_eq!(
        h.materialize_historical_window(history.0, history.1).len(),
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
fn threshold_is_query_driven_from_having() {
    let p = query();
    let stricter = LogicalPlan::from_text(
        &include_str!("../queries/anomaly.janusql").replace("1.3 * AVG", "1.5 * AVG"),
    )
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

#[test]
fn deprecated_baseline_form_is_rejected_by_federated_lowering() {
    let old = r#"
PREFIX ex: <https://example.org/>
FROM NAMED WINDOW ex:live ON STREAM ex:live-sensors [RANGE 60 STEP 30]
FROM NAMED WINDOW ex:history ON LOG ex:historical-sensors [OFFSET 2592060 RANGE 2592000 STEP 30]
DEFINE BASELINE ex:historicalAverage ON WINDOW ex:history AS
SELECT ?sensor (AVG(?historical) AS ?historicalAverage)
WHERE { ?sensor ex:value ?historical . }
GROUP BY ?sensor
REGISTER RStream ex:anomalies AS
USING BASELINE ex:historicalAverage
SELECT ?sensor ?current ?historicalAverage
WHERE {
  WINDOW ex:live { ?sensor ex:value ?current . }
}
"#;
    let error = LogicalPlan::from_text(old).unwrap_err();
    assert!(error.contains("deprecated"));
}
