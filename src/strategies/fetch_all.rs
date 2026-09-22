use crate::sources::{HistoricalSource, LiveSource, Observation};
use std::collections::HashMap;
pub fn run(
    live: &dyn LiveSource,
    history: &dyn HistoricalSource,
) -> (Vec<Observation>, HashMap<String, f64>) {
    let l = live.materialize_live_window();
    let h = history.materialize_historical_window();
    let mut sums = HashMap::<String, (f64, u64)>::new();
    for o in h {
        let x = sums.entry(o.sensor).or_insert((0.0, 0));
        x.0 += o.value;
        x.1 += 1;
    }
    (
        l,
        sums.into_iter()
            .map(|(s, (v, n))| (s, v / n as f64))
            .collect(),
    )
}
