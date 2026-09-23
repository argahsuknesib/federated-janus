use federated_janus::LogicalPlan;

#[test]
fn continuous_millisecond_windows_are_adjacent_and_non_overlapping() {
    let plan = LogicalPlan::from_file("queries/anomaly.janusql").unwrap();
    let evaluation_time_ms = 3_000_000_000;
    let (historical, live) = plan.continuous_bounds_ms(evaluation_time_ms).unwrap();

    assert_eq!(historical, (407_940_000, 2_999_940_000));
    assert_eq!(live, (2_999_940_000, 3_000_000_000));
    assert_eq!(historical.1, live.0);
    assert!(historical.0 < historical.1 && live.0 < live.1);
}
