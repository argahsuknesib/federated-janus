# Research roadmap

## Current milestone

The current controlled experiment varies historical RDF size from 100 to 1,000,000 quads while keeping the live workload fixed.

It compares:

- FetchAll
- AggregatePushdown
- BindJoin

over the same Janus `StreamingSegmentedStorage` backend.

## Immediate analysis

After running the benchmark, inspect:

1. how latency scales with historical quads;
2. how many historical rows each strategy returns;
3. how transfer volume scales;
4. whether aggregation pushdown saves transfer as expected;
5. whether BindJoin saves additional transfer for the fixed live bindings;
6. whether all strategies scan the same number of historical records.

The sixth point is particularly important because the current segmented store is timestamp-indexed rather than subject-indexed.

## Possible next storage experiment

If BindJoin is limited by full historical scans, a later experiment can add a subject-aware index to Janus itself and compare:

```text
timestamp-only segmented storage
vs
timestamp + subject-aware historical access
```

That should be treated as a separate storage contribution rather than hidden inside the Federated-Janus benchmark.

## Later directions

Once the basic historical-scaling behavior is understood:

- multiple independently addressable historical sources
- actual remote/network source execution
- operator placement across edge nodes
- simple cost estimation
- policy-constrained plans with ODRL/UMA

Automatic plan selection should come only after the measured costs of the explicit plans are understood.
