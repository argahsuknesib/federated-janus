pub mod compact;
pub mod federated;
mod historical;
mod live;
pub use federated::{SensorId, SensorSourcePair, SourceRegistry};
pub use historical::{HistoricalSource, InMemoryHistoricalSource};
use janus::core::RDFEvent;
pub use live::{InMemoryLiveSource, LiveSource};

/// One numeric `ex:value` RDF observation. The embedded Janus event preserves the RDF shape.
#[derive(Debug, Clone)]
pub struct Observation {
    pub rdf: RDFEvent,
    pub sensor: String,
    pub value: f64,
}
impl Observation {
    pub fn new(timestamp: u64, sensor: impl Into<String>, value: f64) -> Self {
        let sensor = sensor.into();
        let rdf = RDFEvent::new_typed_literal_object(
            timestamp,
            &sensor,
            "https://example.org/value",
            &value.to_string(),
            "https://example.org/graph",
            "http://www.w3.org/2001/XMLSchema#double",
        );
        Self { rdf, sensor, value }
    }
    pub fn serialized_bytes(&self) -> u64 {
        (self.rdf.subject.len()
            + self.rdf.predicate.len()
            + self.rdf.object.len()
            + self.rdf.graph.len()
            + 24) as u64
    }
}
