//! Benchmark-only compact historical representation. It models one numeric RDF
//! predicate (`ex:value`) with stable numeric sensor IDs; Janus remains the RDF
//! storage/execution implementation for production queries.
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
pub struct HistoricalObservation {
    pub timestamp: u64,
    pub value: f64,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct AccessStats {
    pub entities_looked_up: u64,
    pub records_scanned: u64,
    pub records_matched: u64,
    pub records_returned: u64,
}
#[derive(Debug, Clone)]
pub struct CompactHistoricalSource {
    by_sensor: Vec<Vec<HistoricalObservation>>,
}
impl CompactHistoricalSource {
    pub fn deterministic(sensors: usize, observations_per_sensor: usize, seed: u64) -> Self {
        let base = 100.0 + (seed % 5) as f64;
        Self {
            by_sensor: (0..sensors)
                .map(|_| {
                    (0..observations_per_sensor)
                        .map(|i| HistoricalObservation {
                            timestamp: i as u64,
                            value: base + (i % 5) as f64,
                        })
                        .collect()
                })
                .collect(),
        }
    }
    pub fn sensor_count(&self) -> usize {
        self.by_sensor.len()
    }
    pub fn aggregate_all(&self, start: u64, end: u64) -> (HashMap<u32, f64>, AccessStats) {
        let mut out = HashMap::with_capacity(self.by_sensor.len());
        let mut st = AccessStats::default();
        for (id, rows) in self.by_sensor.iter().enumerate() {
            st.entities_looked_up += 1;
            let mut sum = 0.;
            let mut n = 0;
            for r in rows {
                st.records_scanned += 1;
                if r.timestamp >= start && r.timestamp < end {
                    sum += r.value;
                    n += 1;
                    st.records_matched += 1;
                }
            }
            if n > 0 {
                out.insert(id as u32, sum / n as f64);
                st.records_returned += 1;
            }
        }
        (out, st)
    }
    pub fn aggregate_bound(
        &self,
        bound: &HashSet<u32>,
        start: u64,
        end: u64,
    ) -> (HashMap<u32, f64>, AccessStats) {
        let mut out = HashMap::with_capacity(bound.len());
        let mut st = AccessStats::default();
        for &id in bound {
            st.entities_looked_up += 1;
            let Some(rows) = self.by_sensor.get(id as usize) else {
                continue;
            };
            let mut sum = 0.;
            let mut n = 0;
            for r in rows {
                st.records_scanned += 1;
                if r.timestamp >= start && r.timestamp < end {
                    sum += r.value;
                    n += 1;
                    st.records_matched += 1;
                }
            }
            if n > 0 {
                out.insert(id, sum / n as f64);
                st.records_returned += 1;
            }
        }
        (out, st)
    }
}
