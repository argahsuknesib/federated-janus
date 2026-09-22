use super::Observation;
use std::sync::{Arc, Mutex};
pub trait LiveSource {
    fn materialize_live_window(&self, start: u64, end: u64) -> Vec<Observation>;
}
#[derive(Debug, Clone)]
pub struct InMemoryLiveSource {
    observations: Arc<Mutex<Vec<Observation>>>,
}
impl InMemoryLiveSource {
    pub fn new(observations: Vec<Observation>) -> Self {
        Self {
            observations: Arc::new(Mutex::new(observations)),
        }
    }
    pub fn replace_observations(&mut self, observations: Vec<Observation>) {
        *self.observations.lock().expect("live source lock poisoned") = observations;
    }
    /// Ingest one event at publication time.  Events are retained until a
    /// caller explicitly clears/replaces them; window materialization enforces
    /// the canonical half-open range, so no future window is pre-built.
    pub fn publish(&self, observation: Observation) {
        self.observations
            .lock()
            .expect("live source lock poisoned")
            .push(observation);
    }
    pub fn received_count(&self) -> u64 {
        self.observations
            .lock()
            .expect("live source lock poisoned")
            .len() as u64
    }
}
impl LiveSource for InMemoryLiveSource {
    fn materialize_live_window(&self, start: u64, end: u64) -> Vec<Observation> {
        self.observations
            .lock()
            .expect("live source lock poisoned")
            .iter()
            .filter(|o| o.rdf.timestamp >= start && o.rdf.timestamp < end)
            .cloned()
            .collect()
    }
}
