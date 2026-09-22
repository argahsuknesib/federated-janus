# Research roadmap

Federated-Janus is intentionally being developed in stages so that each optimization mechanism can be evaluated before automatic plan selection is introduced.

## Completed

### Hybrid-query execution

- Janus-QL parsing and lowering
- historical + live anomaly workload
- explicit equivalent physical plans
- shared-source aggregation pushdown and bind-style access

### Source-oriented federation

- independently addressable live/history sensor pairs
- one Janus-QL query spanning many source pairs
- structural `UNION` decomposition
- manual live-first source selection
- federation-width and active-source-selectivity studies

### Continuous execution

- real-time 4 Hz publishers
- 60-second live windows
- 30-second evaluation step
- registered-once query plan
- changing active source sets across evaluations

### Multi-input query planning

- static RDF metadata source
- metadata filtering
- live-first and metadata-first ordering
- central hash/bind/semijoin-style comparisons
- operator-level transfer accounting
- empirical byte-optimal plan regions

## Next

### Validate planning regions under execution

The controlled selectivity study provides a plan-dominance hypothesis. The next goal is to compare those predictions with representative continuous executions and determine whether byte-optimal and latency-optimal plans coincide.

### Simple analytical cost model

Only after the manual plan regions are validated should Federated-Janus estimate plan cost automatically.

A first model can remain small:

```text
estimated_bytes(plan)
    =
live transfer
+ metadata transfer
+ join-key transfer
+ historical transfer
```

Inputs can include:

- number of sources
- observed/estimated live activity
- metadata selectivity
- expected live/metadata intersection
- historical rows per source
- tuple/key sizes

The first optimizer should enumerate the known plan set and choose the minimum estimated transfer cost. There is no need for a learned optimizer at this stage.

## Later research directions

### Networked edge deployment

Replace in-process source abstractions with actual remote endpoints while preserving the logical/physical plan model. This would allow measurement of serialization, network latency, concurrency, and failure behavior.

### Policy-constrained planning

Introduce ODRL/UMA-style source constraints such as:

- raw observations may not leave the source
- only aggregates may be returned
- intermediate bindings may or may not be transferred
- results may or may not be cached
- requester-specific source access

Planning then becomes optimization over only the physically and policy-valid plans.

### Additional source topologies

Possible future workloads include:

- one live + multiple historical sources
- multiple live + one historical source
- multiple live + multiple historical sources
- multiple metadata sources
- reusable/cached historical intermediate results

### Adaptive planning

Runtime replanning is intentionally later. It becomes meaningful only after a static cost model can explain the measured plan regions.

## Explicit non-goals for the current prototype

The current milestone does not attempt:

- arbitrary SPARQL federation
- automatic source discovery
- general-purpose distributed joins
- production network deployment
- learned query optimization
- adaptive replanning
- policy enforcement
