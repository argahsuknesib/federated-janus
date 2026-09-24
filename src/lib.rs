//! Explicit physical-plan experiments built on Janus data types and storage.
pub mod continuous;
pub mod executor;
pub mod metrics;
pub mod planner;
pub mod remote;
pub mod sources;
pub mod strategies;

pub use continuous::RegisteredContinuousQuery;
pub use executor::{
    execute, execute_continuous, execute_federated, execute_federated_continuous,
    execute_local_plan, execute_source_oriented, Anomaly, ExecutionError, ExecutionOutcome,
    LocalPlanOutcome,
};
pub use planner::{
    generate_federated_anomaly_query, sensor_pair_query, ExecutionStrategy, FederatedLogicalPlan,
    LocalExecutionPlan, LogicalPlan, PhysicalPlan,
};
pub use sources::{
    HistoricalSource, InMemoryHistoricalSource, InMemoryLiveSource, LiveSource, Observation,
    SegmentedHistoricalSource, SensorId, SensorSourcePair, SourceRegistry,
};
