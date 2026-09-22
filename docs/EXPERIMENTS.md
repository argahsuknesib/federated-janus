# Experiments and benchmark methodology

The top-level README describes only the current system. This document records the experimental progression and the purpose of each benchmark. Current query fixtures express historical aggregation directly with `WINDOW`, `AVG`, `GROUP BY`, and `HAVING`; the old baseline compatibility syntax is not used.

Generated CSVs and benchmark-specific plots are local artifacts and are not versioned. Stable documentation figures live in `docs/figures/`.

## 1. Shared-source entity selectivity

The earliest experiments used one live source and one historical source containing many sensor entities.

The compared plans were:

- `FetchAll`
- `AggregatePushdown`
- `BindJoin`

These experiments answer entity-selectivity questions inside a shared source. They should not be described as federation-width experiments.

### Historical population

The live side is fixed while the historical archive grows. The experiment studies whether a bound/indexed lookup can keep historical work proportional to the relevant live entities rather than the full archive.

Documentation figures:

- [historical-scale latency](figures/historical_scale_latency.png)
- [historical work](figures/historical_scale_work.png)
- [historical-scale transfer](figures/historical_scale_bytes.png)

### Live-side coverage

A fixed historical archive is queried with increasing live-side coverage. This shows the workload-dependent crossover between bound historical access and full historical aggregation.

Documentation figures:

- [latency](figures/latency.svg)
- [transferred bytes](figures/bytes.svg)
- [historical work](figures/historical_work.svg)

The key interpretation is qualitative: bind-style access is strongest when the live side is selective; its work grows with bound coverage, while full aggregation has a relatively stable full-population cost.

## 2. Source-oriented federation width

The source-oriented model makes each sensor independently addressable:

```text
N source pairs =
N live sources +
N historical sources
```

The benchmark generates one Janus-QL query containing all source-pair anomaly branches connected by `UNION`, parses it once, lowers/decomposes it once, and then reuses the logical plan across repetitions.

Compared strategies:

- `FetchAllSources`
- `AggregateAllSources`
- `LiveFirstSourceSelection`

The federation-width experiment varies the number of independent source pairs while keeping per-source history fixed.

## 3. Active-source selectivity

This experiment fixes the federation at 100 source pairs and varies how many live branches contain observations.

The important mechanism is source pruning:

```text
100 histories declared

1 active live branch   -> LiveFirst contacts 1 history
5 active live branches -> LiveFirst contacts 5 histories
...
100 active branches    -> LiveFirst contacts all 100 histories
```

`FetchAllSources` and `AggregateAllSources` continue to contact all historical sources.

![Historical sources contacted by active live sources](figures/active_selectivity_sources_contacted.svg)

This experiment established that the value of live-first source selection depends directly on runtime branch activity.

## 4. Continuous real-time federation

The real-time benchmark replaces pre-materialized live state with continuously arriving RDF observations.

Configuration:

- 4 Hz per active source
- `RANGE 60`
- `STEP 30`
- one registered query reused across evaluations

A deterministic publisher schedule can change which sources are producing observations over time. The coordinator still discovers active branches by evaluating live windows.

The main purpose is to validate that source-pruning behavior survives continuous window evolution rather than only isolated snapshots.

Run:

```sh
cargo run --release --bin continuous_realtime_benchmark
```

## 5. Query planning by transferred bytes

The planning workload adds one independent metadata source to the live/history anomaly query.

Metadata condition:

```sparql
GRAPH <https://example.org/metadata> {
  ?sensor ex:locatedIn ex:RoomA .
}
```

It varies two independent selectivities:

- fraction of live sources with window results
- fraction of sensors satisfying the metadata predicate

The controlled study evaluates five explicit plans:

- `CentralFetchAll`
- `AggregateAll`
- `LiveFirst`
- `MetadataFirst`
- `LiveMetadataSemiJoin`

The main research question is:

> Under which source cardinalities and selectivities does each operator ordering or placement reduce transferred bytes?

The resulting dominance map shows that no single manual plan is byte-minimal over the entire workload space.

![Byte-optimal physical plan](figures/planning_best_plan_bytes.svg)

Observed regions include:

- metadata-first execution when metadata is strongly selective
- live/metadata semijoin behavior when metadata is broad but live activity is sparse
- live-first behavior when live activity is sufficiently selective
- full aggregation becoming competitive when neither predicate prunes useful work

These are empirical workload regions, not an automatic optimizer.

Run:

```sh
cargo run --release --bin query_planning_bytes_benchmark -- --depth-sensitivity
```

## Correctness discipline

All benchmark families compare alternative physical plans for the same logical query.

The test harness checks:

- equal result counts
- stable result hashes
- source/branch associations
- no cross-source leakage
- half-open temporal window behavior
- consistent evaluation instants
- source pruning derived from query execution rather than benchmark hints

## Methodological boundary

Current source abstractions execute in-process. Consequently:

- query and operator execution are real within the prototype
- live publishing is real-time in the continuous benchmark
- logical transfer accounting represents bytes crossing conceptual source boundaries
- network latency, serialization stacks, remote failures, and deployment overhead are not yet modeled as a real distributed system

These limitations should be kept explicit when interpreting absolute latency values.
