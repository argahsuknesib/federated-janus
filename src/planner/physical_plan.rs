/// Manually selected Phase-1 physical strategies. No automatic selection exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    FetchAll,
    AggregatePushdown,
    BindJoin,
}
impl ExecutionStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FetchAll => "fetch-all",
            Self::AggregatePushdown => "aggregate-pushdown",
            Self::BindJoin => "bind-join",
        }
    }
}
/// Deliberately small representation reserved for a later planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    SourceScan,
    Window,
    Filter,
    Projection,
    Aggregate,
    Join,
}
