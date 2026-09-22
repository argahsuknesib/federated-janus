//! A small, explicit physical-plan experiment built on Janus data types.
pub mod continuous;
pub mod executor;
pub mod metrics;
pub mod planner;
pub mod planning;
pub mod sources;
pub mod strategies;

pub use continuous::RegisteredContinuousQuery;
pub use executor::{
    execute, execute_compact, execute_federated, execute_federated_continuous,
    execute_source_oriented, Anomaly, ExecutionError, ExecutionOutcome,
};
pub use planner::{
    generate_federated_anomaly_query, sensor_pair_query, ExecutionStrategy, FederatedLogicalPlan,
    LogicalPlan, PhysicalPlan,
};
pub use planner::{generate_planning_query, PlanningLogicalQuery};
pub use planning::{PlanningMetrics, PlanningPlan, PlanningRun};
pub use sources::{
    HistoricalSource, InMemoryHistoricalSource, InMemoryLiveSource, LiveSource, Observation,
    SensorId, SensorSourcePair, SourceRegistry,
};
