use federated_janus::{
    generate_federated_anomaly_query, ExecutionStrategy, FederatedLogicalPlan, LiveSource,
    Observation, RegisteredContinuousQuery, SensorSourcePair, SourceRegistry,
};

const T0: u64 = 3_000_000;

fn registered(n: usize) -> (RegisteredContinuousQuery, SourceRegistry) {
    let plan = FederatedLogicalPlan::from_text(&generate_federated_anomaly_query(n)).unwrap();
    let q = RegisteredContinuousQuery::new(plan, n).unwrap();
    let mut registry = SourceRegistry::default();
    for id in 1..=n as u32 {
        let sensor = format!("https://example.org/sensor{id}");
        registry
            .register(SensorSourcePair::new(
                id,
                vec![],
                vec![Observation::new(1, sensor, 100.)],
            ))
            .unwrap();
    }
    (q, registry)
}

#[test]
fn range_step_and_half_open_overlap_are_clock_driven() {
    let (q, registry) = registered(1);
    assert_eq!((q.range_ms(), q.step_ms()), (60_000, 30_000));
    let live = registry.live_source(1).unwrap();
    // 240 events at four per second in [T-60s,T), with the exact end excluded.
    for i in 0..240 {
        live.publish(Observation::new(
            T0 - 60_000 + i * 250,
            "https://example.org/sensor1",
            200.,
        ));
    }
    live.publish(Observation::new(T0, "https://example.org/sensor1", 200.));
    assert_eq!(
        q.evaluate(ExecutionStrategy::AggregateAllSources, &registry, T0)
            .unwrap()
            .metrics
            .live_records,
        240
    );
    // Add precisely the next 30 seconds.  The old [T-60,T-30) expires and the
    // retained [T-30,T) portion overlaps exactly 120 events.
    for i in 1..=120 {
        live.publish(Observation::new(
            T0 + i * 250,
            "https://example.org/sensor1",
            200.,
        ));
    }
    let next = q
        .evaluate(
            ExecutionStrategy::AggregateAllSources,
            &registry,
            T0 + q.step_ms(),
        )
        .unwrap();
    assert_eq!(next.metrics.live_records, 240); // boundary event at T is one of 120 new arrivals.
    assert!(!live
        .materialize_live_window(T0 - 60_000, T0 - 30_000)
        .is_empty());
    assert_eq!(
        live.materialize_live_window(T0 - 60_000, T0 - 30_000).len(),
        120
    );
    assert_eq!(live.materialize_live_window(T0 - 30_000, T0).len(), 120);
}

#[test]
fn live_first_discovers_dynamic_branches_without_schedule_input() {
    let (q, registry) = registered(5);
    for id in 1..=2 {
        registry.live_source(id).unwrap().publish(Observation::new(
            T0 - 1,
            format!("https://example.org/sensor{id}"),
            200.,
        ));
    }
    let first = q
        .evaluate(ExecutionStrategy::LiveFirstSourceSelection, &registry, T0)
        .unwrap();
    assert_eq!(
        (
            first.metrics.live_sources_with_window,
            first.metrics.historical_sources_contacted
        ),
        (2, 2)
    );
    for id in 3..=5 {
        registry.live_source(id).unwrap().publish(Observation::new(
            T0 + 1,
            format!("https://example.org/sensor{id}"),
            200.,
        ));
    }
    let second = q
        .evaluate(
            ExecutionStrategy::LiveFirstSourceSelection,
            &registry,
            T0 + q.step_ms(),
        )
        .unwrap();
    assert_eq!(
        (
            second.metrics.live_sources_with_window,
            second.metrics.historical_sources_contacted
        ),
        (5, 5)
    );
    for strategy in [
        ExecutionStrategy::FetchAllSources,
        ExecutionStrategy::AggregateAllSources,
        ExecutionStrategy::LiveFirstSourceSelection,
    ] {
        let out = q.evaluate(strategy, &registry, T0 + q.step_ms()).unwrap();
        assert_eq!(out.results, second.results);
    }
    assert_eq!(q.source_pairs(), 5);
}
