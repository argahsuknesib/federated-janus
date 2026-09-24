/// Manually selected Phase-1 physical strategies. No automatic selection exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    FetchAll,
    AggregatePushdown,
    BindJoin,
    FetchAllSources,
    AggregateAllSources,
    LiveFirstSourceSelection,
}

/// Explicit local historical access plans for the subject-index correctness
/// gate.  These are manual alternatives, never an optimizer choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalExecutionPlan {
    AggregatePushdown,
    TimestampOnlyBindJoin,
    SubjectAwareBindJoin,
    SubjectAwareLinearBindJoin,
    SubjectAwareBinaryBindJoin,
}
impl ExecutionStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FetchAll => "fetch-all",
            Self::AggregatePushdown => "aggregate-pushdown",
            Self::BindJoin => "bind-join",
            Self::FetchAllSources => "fetch-all-sources",
            Self::AggregateAllSources => "aggregate-all-sources",
            Self::LiveFirstSourceSelection => "live-first-source-selection",
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
