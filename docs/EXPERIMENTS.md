# Benchmark methodology

## Research question

> How do FetchAll, AggregatePushdown, and BindJoin behave as the amount of historical RDF data grows when all three use Janus's segmented storage implementation?

The benchmark varies only the historical archive size.

## Independent variable

Default historical quad counts:

```text
100
1,000
10,000
100,000
1,000,000
```

## Fixed workload

The following stay constant:

- historical sensor subjects: 100
- live sensor bindings: 10
- query and anomaly threshold
- evaluation time
- live window
- historical window
- random seed
- storage configuration
- physical source topology

The live workload is fixed for every run.

## Storage construction

For each historical size, the benchmark creates a fresh temporary Janus `StreamingSegmentedStorage`.

Quads are distributed deterministically across 100 sensor subjects and across the historical time interval. The default segment target is 100,000 quads, so a one-million-quad archive produces approximately ten data segments.

Storage construction and flush time are measured separately and excluded from execution latency.

## Repetitions

Default:

- one warmup
- five measured repetitions
- three physical strategies

Strategy execution order rotates between repetitions to reduce a fixed ordering bias after the shared filesystem cache has warmed.

## Metrics

Each measurement records:

- total latency
- historical-source time
- coordinator time
- historical records scanned
- historical rows returned
- binding bytes sent
- historical bytes received
- total logical transfer bytes
- source requests
- result count
- result hash
- segmented-storage disk bytes
- segment count
- storage build time

## Correctness

For every archive size, all three strategies must produce the same result count and stable hash. The benchmark aborts on a semantic mismatch.

## Interpreting BindJoin

The original Janus segmented store has timestamp indexing but no subject/predicate inverted index in the API used here.

As a result, BindJoin receives the live sensor bindings and applies them at the historical source, but the source still scans the selected historical time range. The expected difference is primarily in returned aggregate rows and transferred bytes, not in historical disk records scanned.

This is preferable to using a benchmark-only subject index because it measures the behavior of the actual Janus storage implementation.

## Run

```sh
cargo run --release --bin historical_scale_benchmark
```

Generated CSVs and plots stay under the ignored `results-historical-scale/` directory.
