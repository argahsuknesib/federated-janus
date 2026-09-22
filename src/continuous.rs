//! Registered, clock-driven execution for the continuous federation experiment.
//! The registered query owns one parsed/lowered plan; evaluation never reparses it.
use crate::{
    execute_federated_continuous, ExecutionError, ExecutionOutcome, ExecutionStrategy,
    FederatedLogicalPlan, SourceRegistry,
};

#[derive(Debug, Clone)]
pub struct RegisteredContinuousQuery {
    plan: FederatedLogicalPlan,
    source_pairs: usize,
}

impl RegisteredContinuousQuery {
    pub fn new(plan: FederatedLogicalPlan, source_pairs: usize) -> Result<Self, String> {
        if plan.branches.len() != source_pairs {
            return Err("registered branch count differs from source pairs".into());
        }
        let first = plan
            .branches
            .first()
            .ok_or("continuous query has no branches")?;
        if first.live_window.width != 60 || first.live_window.slide != 30 {
            return Err("continuous experiment requires parsed RANGE 60 STEP 30".into());
        }
        Ok(Self { plan, source_pairs })
    }
    pub fn plan(&self) -> &FederatedLogicalPlan {
        &self.plan
    }
    pub fn source_pairs(&self) -> usize {
        self.source_pairs
    }
    pub fn range_ms(&self) -> u64 {
        self.plan.branches[0].live_window.width * 1_000
    }
    pub fn step_ms(&self) -> u64 {
        self.plan.branches[0].live_window.slide * 1_000
    }
    pub fn evaluate(
        &self,
        strategy: ExecutionStrategy,
        registry: &SourceRegistry,
        instant_ms: u64,
    ) -> Result<ExecutionOutcome, ExecutionError> {
        execute_federated_continuous(strategy, &self.plan, registry, instant_ms)
    }
}
