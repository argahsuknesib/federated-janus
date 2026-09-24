//! Adapter over Janus's original on-disk segmented RDF stream storage.
//!
//! The storage is timestamp-indexed. It does not provide a subject/predicate
//! inverted index, so bound-subject aggregation still scans the selected time
//! range and applies the binding filter at the source. That distinction is
//! intentional: BindJoin can reduce returned rows/bytes without pretending the
//! underlying segmented store can skip unrelated subjects.
use super::{HistoricalSource, Observation};
use janus::storage::{
    segmented_storage::{
        AccessMetrics, StorageSizeAccounting, StreamingSegmentedStorage, SubjectAccessMode,
    },
    util::StreamingConfig,
};
use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::{Path, PathBuf},
};

pub struct SegmentedHistoricalSource {
    storage: StreamingSegmentedStorage,
    record_count: usize,
    base_path: PathBuf,
}

impl SegmentedHistoricalSource {
    /// Reopen an archive created by `deterministic`.  This is used by the
    /// remote edge process; the archive remains Janus's on-disk storage.
    pub fn open_existing(
        base_path: impl AsRef<Path>,
        record_count: usize,
        segment_quads: usize,
    ) -> io::Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let storage = StreamingSegmentedStorage::new(StreamingConfig {
            max_batch_events: segment_quads as u64,
            max_batch_age_seconds: 60,
            max_batch_bytes: 64 * 1024 * 1024,
            sparse_interval: 1_000,
            entries_per_index_block: 1_024,
            segment_base_path: base_path.to_string_lossy().into_owned(),
        })?;
        Ok(Self {
            storage,
            record_count,
            base_path,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn deterministic(
        base_path: impl AsRef<Path>,
        total_quads: usize,
        sensor_count: usize,
        seed: u64,
        start: u64,
        end: u64,
        segment_quads: usize,
    ) -> io::Result<Self> {
        if total_quads == 0 || sensor_count == 0 || segment_quads == 0 || start >= end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "total_quads, sensor_count, segment_quads and historical interval must be positive",
            ));
        }

        let base_path = base_path.as_ref().to_path_buf();
        if base_path.exists() {
            fs::remove_dir_all(&base_path)?;
        }
        fs::create_dir_all(&base_path)?;

        let config = StreamingConfig {
            max_batch_events: segment_quads as u64,
            max_batch_age_seconds: 60,
            max_batch_bytes: 64 * 1024 * 1024,
            sparse_interval: 1_000,
            entries_per_index_block: 1_024,
            segment_base_path: base_path.to_string_lossy().into_owned(),
        };
        let storage = StreamingSegmentedStorage::new(config)?;
        let span = end - start;
        let base_value = 100.0 + (seed % 5) as f64;

        for i in 0..total_quads {
            let sensor_id = (i % sensor_count) + 1;
            let sensor = format!("https://example.org/sensor{sensor_id}");
            let timestamp_offset =
                (((i as u128 + 1) * span as u128) / (total_quads as u128 + 1)) as u64;
            let timestamp = start + timestamp_offset.min(span - 1);
            let value = base_value + ((i / sensor_count) % 5) as f64;

            storage.write_rdf(
                timestamp,
                &sensor,
                "https://example.org/value",
                &value.to_string(),
                "https://example.org/history",
            )?;

            if (i + 1).is_multiple_of(segment_quads) {
                storage.flush()?;
            }
        }
        storage.flush()?;

        Ok(Self {
            storage,
            record_count: total_quads,
            base_path,
        })
    }

    pub fn disk_bytes(&self) -> io::Result<u64> {
        let mut bytes = 0u64;
        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                bytes += entry.metadata()?.len();
            }
        }
        Ok(bytes)
    }

    pub fn segment_count(&self) -> io::Result<usize> {
        let mut count = 0usize;
        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".log"))
            {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn storage_path(&self) -> &Path {
        &self.base_path
    }

    pub fn storage_size_accounting(&self) -> io::Result<StorageSizeAccounting> {
        self.storage.storage_size_accounting()
    }

    pub fn rows_with_metrics(
        &self,
        start: u64,
        end: u64,
    ) -> io::Result<(Vec<Observation>, AccessMetrics)> {
        let (rows, metrics) = self.storage.query_half_open_with_metrics(start, end)?;
        let dict = self.storage.get_dictionary().read().unwrap();
        Ok((
            rows.into_iter()
                .map(|event| event.decode(&dict))
                .map(|rdf| {
                    let sensor = rdf.subject.clone();
                    let value = rdf
                        .object
                        .parse::<f64>()
                        .unwrap_or_else(|e| panic!("historical object is not numeric: {e}"));
                    Observation { rdf, sensor, value }
                })
                .collect(),
            metrics,
        ))
    }

    fn rows(&self, start: u64, end: u64) -> Vec<Observation> {
        self.rows_with_metrics(start, end)
            .unwrap_or_else(|e| panic!("Janus segmented-storage query failed: {e}"))
            .0
    }

    pub fn rows_for_subjects(
        &self,
        start: u64,
        end: u64,
        subjects: &HashSet<String>,
    ) -> io::Result<(Vec<Observation>, AccessMetrics)> {
        self.rows_for_subjects_mode(start, end, subjects, SubjectAccessMode::Linear)
    }
    pub fn rows_for_subjects_mode(
        &self,
        start: u64,
        end: u64,
        subjects: &HashSet<String>,
        mode: SubjectAccessMode,
    ) -> io::Result<(Vec<Observation>, AccessMetrics)> {
        let (rows, metrics) = self
            .storage
            .query_rdf_half_open_for_subjects_with_metrics_mode(start, end, subjects, mode)?;
        Ok((
            rows.into_iter()
                .map(|rdf| {
                    let sensor = rdf.subject.clone();
                    let value = rdf
                        .object
                        .parse::<f64>()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    Ok(Observation { rdf, sensor, value })
                })
                .collect::<io::Result<Vec<_>>>()?,
            metrics,
        ))
    }
}

impl HistoricalSource for SegmentedHistoricalSource {
    fn materialize_historical_window(&self, start: u64, end: u64) -> Vec<Observation> {
        self.rows(start, end)
    }

    fn averages(
        &self,
        bindings: Option<&HashSet<String>>,
        start: u64,
        end: u64,
    ) -> HashMap<String, f64> {
        let mut sums: HashMap<String, (f64, u64)> = HashMap::new();
        for row in self.rows(start, end) {
            if bindings.is_some_and(|set| !set.contains(&row.sensor)) {
                continue;
            }
            let entry = sums.entry(row.sensor).or_insert((0.0, 0));
            entry.0 += row.value;
            entry.1 += 1;
        }
        sums.into_iter()
            .map(|(sensor, (sum, count))| (sensor, sum / count as f64))
            .collect()
    }

    fn record_count(&self) -> usize {
        self.record_count
    }
}
