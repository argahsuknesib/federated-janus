# Federated-Janus

Federated-Janus is an experimental execution layer for **Janus-QL**. It compares equivalent physical execution strategies for queries that combine live RDF streams with historical RDF data.

The current benchmark is deliberately simple: **only the amount of historical data changes**.

## Current benchmark

The benchmark holds the live workload fixed and varies one historical RDF source from:

```text
100
1,000
10,000
100,000
1,000,000 quads
```

The fixed workload uses 100 historical sensor subjects and 10 live sensor bindings. Those values do not vary across the experiment.

Three manually selected strategies execute the same Janus-QL query:

- **FetchAll** — return the raw historical window and aggregate at the coordinator.
- **AggregatePushdown** — scan the historical window and compute per-sensor averages at the historical source.
- **BindJoin** — send the fixed live sensor bindings to the historical source and return averages only for those sensors.

All strategies must produce the same result count and stable result hash.

## Historical storage

Historical quads are stored with Janus's original **`StreamingSegmentedStorage`** implementation, not the previous compact benchmark store.

Janus stores dictionary-encoded RDF events as fixed-size records in segmented `.log` files, with a persisted dictionary and sparse/two-level timestamp indexes in `.idx` files.

An important consequence is that the current segmented store is **timestamp-indexed, not subject/predicate-indexed**. For this benchmark the historical window covers the complete archive, so all three strategies scan the same historical quad count. `BindJoin` can reduce the aggregate rows and bytes returned to the coordinator, but it does not pretend to avoid disk scanning of unrelated subjects.

That distinction is intentional and is part of what this benchmark should reveal.

## Query

The canonical query is [`queries/anomaly.janusql`](queries/anomaly.janusql). It uses the current Janus-QL window form directly:

```text
live WINDOW
     │
     ├───────────────┐
     │               │
     ▼               ▼
?current       historical WINDOW
                     │
                     AVG
                     │
                     ▼
             ?historicalAverage
                     │
                     ▼
                  HAVING
```

## Run

Federated-Janus expects Janus as a sibling repository:

```text
parent/
├── janus/
└── federated-janus/
```

Run the tests:

```sh
cargo test
```

Run the historical-scale benchmark:

```sh
cargo run --release --bin historical_scale_benchmark
```

The default run uses:

```text
historical quads:   100, 1k, 10k, 100k, 1M
historical sensors: 100 (fixed)
live sensors:       10 (fixed)
segment size:       100k quads
warmups:            1
repetitions:        5
```

Results and plots are generated locally under `results-historical-scale/` and are ignored by Git.

## What is measured

For every archive size and strategy the benchmark records:

- end-to-end execution latency
- historical-source execution time
- coordinator time
- historical records scanned
- historical rows returned
- bytes sent to and received from the historical source
- total logical bytes transferred
- on-disk segmented-storage size
- segment count
- result count and stable result hash

Storage construction time is recorded separately and excluded from strategy execution latency.

## Documentation

- [Architecture and storage](docs/ARCHITECTURE.md)
- [Benchmark methodology](docs/EXPERIMENTS.md)
- [Research roadmap](docs/ROADMAP.md)

## Scope

The current benchmark is not an automatic optimizer and is not a network deployment. The three strategies are selected explicitly. The primary question is how their work and data movement change as the historical RDF archive grows when all three use the same Janus segmented storage backend.
