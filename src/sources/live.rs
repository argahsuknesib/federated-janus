use super::Observation;
pub trait LiveSource {
    fn materialize_live_window(&self, start: u64, end: u64) -> Vec<Observation>;
}
#[derive(Debug, Clone)]
pub struct InMemoryLiveSource {
    observations: Vec<Observation>,
}
impl InMemoryLiveSource {
    pub fn new(observations: Vec<Observation>) -> Self {
        Self { observations }
    }
}
impl LiveSource for InMemoryLiveSource {
    fn materialize_live_window(&self, start: u64, end: u64) -> Vec<Observation> {
        self.observations
            .iter()
            .filter(|o| o.rdf.timestamp >= start && o.rdf.timestamp < end)
            .cloned()
            .collect()
    }
}
