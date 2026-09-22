use crate::sources::{HistoricalSource, LiveSource, Observation};
use std::collections::{HashMap, HashSet};
pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
) -> (Vec<Observation>, HashMap<String, f64>) {
    let l = live.materialize_live_window();
    let bindings: HashSet<_> = l.iter().map(|o| o.sensor.clone()).collect();
    let a = history.averages(Some(&bindings));
    (l, a)
}
