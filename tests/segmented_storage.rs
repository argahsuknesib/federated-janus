use federated_janus::{HistoricalSource, SegmentedHistoricalSource};
use std::{
    collections::HashSet,
    fs, process,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn temp_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "federated-janus-segmented-test-{}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn janus_segmented_storage_materializes_and_aggregates_quads() {
    let path = temp_path();
    let source =
        SegmentedHistoricalSource::deterministic(&path, 1_000, 100, 7, 1_000, 11_000, 100).unwrap();

    assert_eq!(source.record_count(), 1_000);
    assert_eq!(source.segment_count().unwrap(), 10);
    assert!(source.disk_bytes().unwrap() > 0);

    let rows = source.materialize_historical_window(1_000, 11_000);
    assert_eq!(rows.len(), 1_000);

    let all = source.averages(None, 1_000, 11_000);
    assert_eq!(all.len(), 100);

    let bindings = HashSet::from([
        "https://example.org/sensor1".to_string(),
        "https://example.org/sensor2".to_string(),
    ]);
    let bound = source.averages(Some(&bindings), 1_000, 11_000);
    assert_eq!(bound.len(), 2);

    drop(source);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn segmented_storage_uses_half_open_historical_bounds() {
    let path = temp_path();
    let source =
        SegmentedHistoricalSource::deterministic(&path, 100, 100, 7, 1_000, 2_000, 100).unwrap();

    assert_eq!(
        source.materialize_historical_window(1_000, 2_000).len(),
        100
    );
    assert!(source
        .materialize_historical_window(2_000, 3_000)
        .is_empty());

    drop(source);
    fs::remove_dir_all(path).unwrap();
}
