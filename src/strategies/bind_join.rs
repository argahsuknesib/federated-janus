use crate::sources::{HistoricalSource, LiveSource, Observation};
use std::collections::{HashMap, HashSet};
pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
    live_bounds: (u64, u64),
    historical_bounds: (u64, u64),
) -> (Vec<Observation>, HashMap<String, f64>) {
    let l = live.materialize_live_window(live_bounds.0, live_bounds.1);
    let bindings: HashSet<_> = l.iter().map(|o| o.sensor.clone()).collect();
    let a = history.averages(Some(&bindings), historical_bounds.0, historical_bounds.1);
    (l, a)
}
