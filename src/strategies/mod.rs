use crate::sources::Observation;
use std::collections::HashMap;

pub mod aggregate_pushdown;
pub mod bind_join;
pub mod fetch_all;

pub struct StrategyOutput {
    pub live_rows: Vec<Observation>,
    pub averages: HashMap<String, f64>,
    pub historical_rows_returned: u64,
    pub historical_bytes_received: u64,
    pub binding_bytes_sent: u64,
}
