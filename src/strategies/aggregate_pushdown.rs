use crate::sources::{HistoricalSource, LiveSource, Observation};
use std::collections::HashMap;
pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    live_bounds: (u64, u64),
    historical_bounds: (u64, u64),
) -> (Vec<Observation>, HashMap<String, f64>) {
    (
        live.materialize_live_window(live_bounds.0, live_bounds.1),
        history.averages(None, historical_bounds.0, historical_bounds.1),
    )
}
