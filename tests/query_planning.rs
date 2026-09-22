use federated_janus::{generate_planning_query, planning, PlanningLogicalQuery, PlanningPlan};
#[test]
fn static_named_graph_query_parses_and_all_explicit_plans_agree() {
    let q = PlanningLogicalQuery::from_text(&generate_planning_query(100), 100).unwrap();
    let mut expected = None;
    for p in PlanningPlan::ALL {
        let r = planning::execute(&q, p, 5, 75, 10_000);
        if let Some(x) = expected {
            assert_eq!(x, (r.results.len(), r.hash));
        } else {
            expected = Some((r.results.len(), r.hash));
        }
    }
}
#[test]
fn independent_assignments_are_not_identical() {
    assert_ne!(planning::active_set(25), planning::eligible_set(25));
}
