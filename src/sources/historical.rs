use super::Observation;
use std::collections::{HashMap, HashSet};
pub trait HistoricalSource {
    fn materialize_historical_window(&self, start: u64, end: u64) -> Vec<Observation>;
    fn averages(
        &self,
        bindings: Option<&HashSet<String>>,
        start: u64,
        end: u64,
    ) -> HashMap<String, f64>;
    fn record_count(&self) -> usize;
}
#[derive(Debug, Clone)]
pub struct InMemoryHistoricalSource {
    observations: Vec<Observation>,
}
impl InMemoryHistoricalSource {
    pub fn new(observations: Vec<Observation>) -> Self {
        Self { observations }
    }
}
impl HistoricalSource for InMemoryHistoricalSource {
    fn materialize_historical_window(&self, start: u64, end: u64) -> Vec<Observation> {
        self.observations
            .iter()
            .filter(|o| o.rdf.timestamp >= start && o.rdf.timestamp < end)
            .cloned()
            .collect()
    }
    fn averages(
        &self,
        bindings: Option<&HashSet<String>>,
        start: u64,
        end: u64,
    ) -> HashMap<String, f64> {
        let mut sums: HashMap<String, (f64, u64)> = HashMap::new();
        for o in &self.observations {
            if o.rdf.timestamp >= start
                && o.rdf.timestamp < end
                && bindings.is_none_or(|b| b.contains(&o.sensor))
            {
                let x = sums.entry(o.sensor.clone()).or_insert((0.0, 0));
                x.0 += o.value;
                x.1 += 1;
            }
        }
        sums.into_iter()
            .map(|(s, (sum, n))| (s, sum / n as f64))
            .collect()
    }
    fn record_count(&self) -> usize {
        self.observations.len()
    }
}
