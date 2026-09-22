use federated_janus::{
    execute_federated, execute_source_oriented, generate_federated_anomaly_query,
    sensor_pair_query, ExecutionStrategy, FederatedLogicalPlan, LogicalPlan, Observation,
    SensorSourcePair, SourceRegistry,
};

const T: u64 = 3_000_000;

fn registry_and_plans() -> (SourceRegistry, Vec<LogicalPlan>) {
    let mut registry = SourceRegistry::default();
    let mut plans = Vec::new();
    for id in 1..=3u32 {
        let plan = LogicalPlan::from_text(&sensor_pair_query(id)).unwrap();
        let (hs, he) = plan.historical_bounds(T).unwrap();
        let (ls, _) = plan.live_bounds(T).unwrap();
        let sensor = format!("https://example.org/sensor{id}");
        let live = if id <= 2 {
            vec![Observation::new(ls + 1, sensor.clone(), 200.)]
        } else {
            vec![]
        };
        let history = vec![
            Observation::new(hs + 1, sensor, 100.),
            Observation::new(he - 1, format!("https://example.org/sensor{id}"), 100.),
        ];
        registry
            .register(SensorSourcePair::new(id, live, history))
            .unwrap();
        plans.push(plan);
    }
    (registry, plans)
}

#[test]
fn one_generated_query_parses_once_and_lowers_independent_source_branches() {
    let query = generate_federated_anomaly_query(3);
    assert_eq!(
        query,
        include_str!("../queries/federated_anomaly_3.janusql")
    );
    let plan = FederatedLogicalPlan::from_text(&query).unwrap();
    assert_eq!(plan.branches.len(), 3);
    assert_eq!(
        plan.branches[0].live_window.source_name,
        "https://example.org/sensors/1/live"
    );
    assert_eq!(
        plan.branches[2].historical_window.source_name,
        "https://example.org/sensors/3/history"
    );
    assert!(query.contains("\n  UNION\n"));
}

#[test]
fn parsed_union_branches_combine_without_cross_source_conjunction() {
    let (registry, _) = registry_and_plans();
    let plan = FederatedLogicalPlan::from_text(&generate_federated_anomaly_query(3)).unwrap();
    let outcome =
        execute_federated(ExecutionStrategy::AggregateAllSources, &plan, &registry, T).unwrap();
    assert_eq!(outcome.results.len(), 2);
    assert_eq!(outcome.metrics.historical_sources_contacted, 3);
}

#[test]
fn every_generated_query_declares_its_own_source_iris() {
    let p1 = LogicalPlan::from_text(&sensor_pair_query(1)).unwrap();
    let p2 = LogicalPlan::from_text(&sensor_pair_query(2)).unwrap();
    assert_eq!(
        p1.live_window.source_name,
        "https://example.org/sensors/1/live"
    );
    assert_eq!(
        p2.historical_window.source_name,
        "https://example.org/sensors/2/history"
    );
    assert_ne!(p1.live_window.source_name, p2.live_window.source_name);
}

#[test]
fn live_first_contacts_only_active_historical_sources_and_preserves_results() {
    let (registry, plans) = registry_and_plans();
    let all = execute_source_oriented(ExecutionStrategy::AggregateAllSources, &plans, &registry, T)
        .unwrap();
    let selective = execute_source_oriented(
        ExecutionStrategy::LiveFirstSourceSelection,
        &plans,
        &registry,
        T,
    )
    .unwrap();
    assert_eq!(all.results, selective.results);
    assert_eq!(all.metrics.total_sources_declared, 6);
    assert_eq!(all.metrics.live_sources_with_window, 2);
    assert_eq!(all.metrics.historical_sources_contacted, 3);
    assert_eq!(selective.metrics.historical_sources_contacted, 2);
    assert_eq!(selective.metrics.historical_records_scanned, 4);
}
