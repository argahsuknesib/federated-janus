use crate::sources::{HistoricalSource, LiveSource, Observation};
use std::collections::HashMap;
pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
) -> (Vec<Observation>, HashMap<String, f64>) {
    (live.materialize_live_window(), history.averages(None))
}
