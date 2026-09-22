//! Explicit independently addressable source pairs used only by the
//! source-oriented experiments.  A registry maps each declared IRI to one
//! concrete source instance; it is intentionally not a shared all-sensor map.
use super::{
    HistoricalSource, InMemoryHistoricalSource, InMemoryLiveSource, LiveSource, Observation,
};
use std::collections::BTreeMap;

pub type SensorId = u32;

#[derive(Debug, Clone)]
pub struct SensorSourcePair {
    pub sensor_id: SensorId,
    pub live_source_iri: String,
    pub historical_source_iri: String,
    pub live: InMemoryLiveSource,
    pub historical: InMemoryHistoricalSource,
}

impl SensorSourcePair {
    pub fn new(sensor_id: SensorId, live: Vec<Observation>, historical: Vec<Observation>) -> Self {
        Self {
            sensor_id,
            live_source_iri: format!("https://example.org/sensors/{sensor_id}/live"),
            historical_source_iri: format!("https://example.org/sensors/{sensor_id}/history"),
            live: InMemoryLiveSource::new(live),
            historical: InMemoryHistoricalSource::new(historical),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SourceRegistry {
    pairs: BTreeMap<SensorId, SensorSourcePair>,
    live_by_iri: BTreeMap<String, SensorId>,
    historical_by_iri: BTreeMap<String, SensorId>,
}

impl SourceRegistry {
    pub fn register(&mut self, pair: SensorSourcePair) -> Result<(), String> {
        if self.pairs.contains_key(&pair.sensor_id)
            || self.live_by_iri.contains_key(&pair.live_source_iri)
            || self
                .historical_by_iri
                .contains_key(&pair.historical_source_iri)
        {
            return Err("sensor ID or source IRI is already registered".into());
        }
        self.live_by_iri
            .insert(pair.live_source_iri.clone(), pair.sensor_id);
        self.historical_by_iri
            .insert(pair.historical_source_iri.clone(), pair.sensor_id);
        self.pairs.insert(pair.sensor_id, pair);
        Ok(())
    }
    pub fn pairs(&self) -> impl Iterator<Item = &SensorSourcePair> {
        self.pairs.values()
    }
    pub fn pair_for_live_iri(&self, iri: &str) -> Option<&SensorSourcePair> {
        self.live_by_iri.get(iri).and_then(|id| self.pairs.get(id))
    }
    pub fn pair_for_historical_iri(&self, iri: &str) -> Option<&SensorSourcePair> {
        self.historical_by_iri
            .get(iri)
            .and_then(|id| self.pairs.get(id))
    }
    pub fn source_counts(&self) -> (u64, u64, u64) {
        let n = self.pairs.len() as u64;
        (n * 2, n, n)
    }
    /// Benchmark setup may change synthetic live events, but execution only
    /// observes each live source through `materialize_live_window`.
    pub fn set_live_observations(
        &mut self,
        sensor_id: SensorId,
        observations: Vec<Observation>,
    ) -> Result<(), String> {
        let pair = self
            .pairs
            .get_mut(&sensor_id)
            .ok_or_else(|| format!("unknown sensor source {sensor_id}"))?;
        pair.live.replace_observations(observations);
        Ok(())
    }
    pub fn live_source(&self, sensor_id: SensorId) -> Option<InMemoryLiveSource> {
        self.pairs.get(&sensor_id).map(|pair| pair.live.clone())
    }
}

// Keep trait imports meaningful in this module's public source model.
const _: fn(&InMemoryLiveSource, u64, u64) -> Vec<Observation> =
    LiveSource::materialize_live_window;
const _: fn(&InMemoryHistoricalSource, u64, u64) -> Vec<Observation> =
    HistoricalSource::materialize_historical_window;
