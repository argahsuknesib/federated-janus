use federated_janus::*;

const EVALUATION_TIME: u64 = 3_000_000;

fn result_hash(rows: &[Anomaly]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "{}:{:.6}:{:.6}",
                row.sensor, row.current_value, row.historical_average
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

#[test]
fn explicit_local_plans_are_semantically_equivalent_and_keep_cardinalities_separate() {
    let directory =
        std::env::temp_dir().join(format!("federated-janus-local-plan-{}", std::process::id()));
    let history = SegmentedHistoricalSource::deterministic(
        &directory, 1_000, 100, 7, 300_000, 2_950_000, 100,
    )
    .unwrap();
    let bound = (1..=10)
        .map(|id| format!("https://example.org/sensor{id}"))
        .collect::<Vec<_>>();
    let live = InMemoryLiveSource::new(
        bound
            .iter()
            .map(|sensor| Observation::new(EVALUATION_TIME - 1, sensor, 1_000.0))
            .collect(),
    );
    let plan = LogicalPlan::from_text(include_str!("../queries/anomaly.janusql")).unwrap();
    let aggregate = execute_local_plan(
        LocalExecutionPlan::AggregatePushdown,
        &plan,
        &live,
        &history,
        EVALUATION_TIME,
    )
    .unwrap();
    let timestamp = execute_local_plan(
        LocalExecutionPlan::TimestampOnlyBindJoin,
        &plan,
        &live,
        &history,
        EVALUATION_TIME,
    )
    .unwrap();
    let indexed = execute_local_plan(
        LocalExecutionPlan::SubjectAwareBindJoin,
        &plan,
        &live,
        &history,
        EVALUATION_TIME,
    )
    .unwrap();
    let binary = execute_local_plan(
        LocalExecutionPlan::SubjectAwareBinaryBindJoin,
        &plan,
        &live,
        &history,
        EVALUATION_TIME,
    )
    .unwrap();
    assert_eq!(aggregate.results.len(), timestamp.results.len());
    assert_eq!(timestamp.results.len(), indexed.results.len());
    assert_eq!(indexed.results, binary.results);
    assert_eq!(
        result_hash(&aggregate.results),
        result_hash(&timestamp.results)
    );
    assert_eq!(
        result_hash(&timestamp.results),
        result_hash(&indexed.results)
    );
    for sensor in &bound {
        assert_eq!(
            aggregate.historical_averages.get(sensor),
            timestamp.historical_averages.get(sensor)
        );
    }
    assert_eq!(timestamp.historical_averages, indexed.historical_averages);
    assert_eq!(aggregate.operator_output_rows, 100);
    assert_eq!(timestamp.operator_output_rows, 10);
    assert_eq!(indexed.operator_output_rows, 10);
    assert!(indexed.storage.subject_index_used);
    assert!(binary.storage.subject_index_used);
    assert_eq!(
        binary.storage.records_examined,
        indexed.storage.records_examined
    );
    assert_eq!(aggregate.historical_interval, timestamp.historical_interval);
    assert_eq!(timestamp.historical_interval, indexed.historical_interval);
    assert_eq!(aggregate.live_bindings, timestamp.live_bindings);
    assert_eq!(timestamp.live_bindings, indexed.live_bindings);
    assert!(timestamp.storage.records_returned > timestamp.operator_output_rows);
    assert!(indexed.storage.records_returned >= indexed.operator_output_rows);
    std::fs::remove_dir_all(directory).unwrap();
}
