//! A small, explicit physical-plan experiment built on Janus data types.
pub mod executor;
pub mod metrics;
pub mod planner;
pub mod sources;
pub mod strategies;

pub use executor::{execute, execute_compact, Anomaly, ExecutionError, ExecutionOutcome};
pub use planner::{ExecutionStrategy, LogicalPlan, PhysicalPlan};
pub use sources::{
    HistoricalSource, InMemoryHistoricalSource, InMemoryLiveSource, LiveSource, Observation,
};
