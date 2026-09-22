# Federated-Janus

Federated query planning, join optimization, and distributed execution for Janus-QL over historical RDF data and live RDF streams.

Janus provides unified continuous querying over historical RDF data and live RDF streams. Federated-Janus is a narrow experimental framework for measuring how equivalent hybrid queries behave under different physical plans when their logical sources are distinct. The present goal is not automatic optimization: plans are explicit and manually selected.

The project studies operator placement, join strategies, aggregation pushdown, transferred-data reduction, multi-source execution, and query-plan trade-offs. Policy-constrained execution is a future direction, not current functionality.

## Current use case

The benchmark parses an actual Janus-QL query (`queries/anomaly.janusql`) and lowers the relevant historical/live query structure into alternative Federated-Janus physical execution plans. It identifies current readings above a per-sensor historical baseline:

```text
live observations             historical observations
?sensor ?current              ?sensor ?historical
       \                        /
        \--- join ?sensor -----/
                 |
       AVG(?historical) per sensor
                 |
  FILTER(?current > 1.3 * average)
```

All three physical plans preserve this logical result.

```text
                 Janus-QL
                    │
                    ▼
             JanusQLParser
                    │
                    ▼
                Janus AST
                    │
                    ▼
         Federated-Janus lowering
                    │
                    ▼
                 Log
```

The canonical query declares a 60-second live window with a 5-second step and
a historical sliding window with `OFFSET = RANGE = 30 days`. Janus resolves
that historical interval as `[T - OFFSET, T - OFFSET + RANGE)`, therefore
`[T - 30d, T)`. The benchmark uses one deterministic evaluation time `T` for
both sides.

The executed query is real Janus-QL (the fixture is authoritative):

```sparql
PREFIX ex: <https://example.org/>
FROM NAMED WINDOW ex:live ON STREAM ex:live-sensors [RANGE 60 STEP 5]
FROM NAMED WINDOW ex:history ON LOG ex:historical-sensors [OFFSET 2592000 RANGE 2592000 STEP 5]

DEFINE BASELINE ex:historicalAverage ON WINDOW ex:history AS
SELECT ?sensor (AVG(?historical) AS ?historicalAverage)
WHERE { ?sensor ex:value ?historical . }
GROUP BY ?sensor

REGISTER RStream ex:anomalies AS
USING BASELINE ex:historicalAverage
SELECT ?sensor ?current ?historicalAverage
WHERE {
  WINDOW ex:live { ?sensor ex:value ?current . }
  GRAPH ex:historicalAverage { ?sensor ex:historicalAverage ?historicalAverage . }
  FILTER(?current > 1.3 * ?historicalAverage)
}
```

The execution pipeline is `anomaly.janusql → JanusQLParser → Janus AST →
Federated-Janus lowering → logical hybrid query → FetchAll /
AggregatePushdown / BindJoin`.

### FetchAll

```text
Live source ---------> coordinator
Historical source ---> coordinator -> aggregate + join + filter
```

The coordinator receives the complete relevant historical window and aggregates it.

### AggregatePushdown

```text
Historical source: full scan -> AVG per sensor --\
                                              coordinator -> join + filter
Live source -----------------------------------/
```

The historical source aggregates all sensors and transfers only baselines.

### BindJoin

```text
Live source -> distinct sensor IDs -> historical source index
                                      |
                               AVG selected sensors
                                      |
                                coordinator -> join + filter
```

The historical source accesses only sensor partitions referenced by the live window.

## Preliminary Janus-QL-driven results

These are **preliminary**, not publication-final results. The 10M-row campaign used the actual parsed [anomaly.janusql](queries/anomaly.janusql) path, one shared deterministic historical dataset, one warmup, and three measured repetitions per strategy/cardinality. Parser and lowering work were performed once per invocation (0.199 ms and 0.004 ms in this run) and excluded from execution latency.

```text
Historical sensors:       100,000
Observations per sensor:  100
Historical observations: 10,000,000
Live cardinalities:       10 to 100,000 (including 30,000 and 35,000)
```

| Live sensors | Live fraction | FetchAll median ms | AggregatePushdown median ms | BindJoin median ms |
|---:|---:|---:|---:|---:|
| 10 | 0.01% | 7.935 | 7.359 | 0.001 |
| 100 | 0.1% | 7.409 | 6.801 | 0.011 |
| 1,000 | 1% | 6.691 | 6.769 | 0.115 |
| 5,000 | 5% | 6.940 | 6.904 | 0.834 |
| 10,000 | 10% | 7.016 | 7.169 | 2.456 |
| 25,000 | 25% | 7.971 | 8.208 | 6.841 |
| 30,000 | 30% | 8.336 | 8.175 | 8.744 |
| 35,000 | 35% | 10.995 | 9.433 | 12.416 |
| 50,000 | 50% | 9.862 | 10.048 | 14.149 |
| 75,000 | 75% | 10.437 | 10.451 | 30.445 |
| 100,000 | 100% | 12.688 | 12.633 | 31.757 |

All three strategies had equal result count and stable result hash at every cardinality. FetchAll transferred about 240 MB per execution; AggregatePushdown transferred about 1.2–2.4 MB; BindJoin ranged from 280 bytes to 2.8 MB as its bindings grew. FetchAll and AggregatePushdown each scanned 10M historical observations (100%); BindJoin scanned 0.01% at 10 live sensors and scales to 100% at full coverage.

The median crossover is between 25% and 30% coverage: BindJoin is lower at 25%, while AggregatePushdown is lower at 30% and 35%. This is an observed workload-specific region rather than a universal threshold. It moved below the earlier prototype's reported 30–35% interval. Peak RSS was not recorded.

## Diagnostic figures

![Median execution latency by live-side coverage](docs/figures/latency.svg)

![Mean transferred data by live-side coverage](docs/figures/bytes.svg)

![Historical observations scanned by live-side coverage](docs/figures/historical_work.svg)

README-facing copies live under `docs/figures/`; the query-driven raw artifacts are in `results-janusql-full/`. Earlier `results*` directories remain preserved prototype results from the fixed Rust logical-query stage and are not the headline measurements.

## Preliminary observations

The following observations are limited to the measured two-source workload:

- FetchAll results in substantially greater data movement than the pushed-down alternatives.
- BindJoin reduces historical processing when the live side is selective.
- AggregatePushdown has a relatively stable historical-processing cost because it processes the full historical population.
- BindJoin cost increases with the number of live bindings.
- Different physical plans become preferable under different workload characteristics.

These findings reproduce the prototype's qualitative behavior: FetchAll transfers far more data, BindJoin is strongest for selective live inputs and grows with bound historical work, and AggregatePushdown scans the full historical population. They do not establish a general threshold or claim an automatic optimizer; automatic planning is optional future work.

## Next use case: three-source anomaly detection

The next experimental design adds a logically independent metadata source to the current query:

```text
Source A: live observations       ?sensor ex:value ?currentValue
Source B: historical observations ?sensor ex:value ?historicalValue
Source C: sensor metadata         ?sensor ex:locatedIn ?location
                                  ?sensor ex:sensorType ?type
```

The query detects live measurements above their historical average only when a metadata condition holds, for example `?sensor ex:locatedIn ex:RoomA`.

```text
Live ?sensor ?current ----\
                           join ---- filter current > historical baseline
Metadata RoomA ----------/  |
                              AVG historical values per sensor
                                      |
                               Historical ?sensor ?historical
```

This is a design only: no metadata source, new query lowering, or additional runtime plan is implemented yet.

### Candidate three-source physical plans

| Plan | Manual execution idea |
|---|---|
| A — Fetch everything | Materialize live, historical, and metadata sources centrally; aggregate and join at the coordinator. |
| B — Historical aggregate pushdown | Compute historical `AVG` remotely, then join live, metadata, and historical baselines centrally. |
| C — Live to historical bind join | Use live sensor IDs to restrict historical aggregation, then join metadata at the coordinator. |
| D — Metadata-first restriction | Evaluate the metadata predicate first, restrict live and historical work to matching sensors, then aggregate and join. |
| E — Live plus metadata semijoin | Intersect live sensor IDs with filtered metadata before the bound historical lookup, then perform the final join. |

The next experiment should ask whether metadata selectivity changes plan preference; whether metadata-first or live-first restriction is better; how much historical work the intersection avoids; and how join order changes latency, transfer, and intermediate-result size. No automatic join-order optimizer is planned at this stage.

## Future physical-plan families

The following are research candidates, not implemented strategies: FilterPushdown, ProjectionPushdown, SemiJoin, Multi-stage BindJoin, Metadata-first restriction, Live-first restriction, Historical-first execution, remote join execution, partial aggregation, hierarchical aggregation, cached historical aggregates, and reuse of historical intermediate results across continuous evaluations. FetchAll, AggregatePushdown, and BindJoin are the only currently implemented plans.

## Multi-source direction

Later experiments should grow beyond one live and one historical source toward `1 live + 2 historical`, `2 live + 1 historical`, `2 live + 2 historical`, `1 live + 1 historical + 1 metadata`, and multiple historical archives. Additional sources increase possible join orders, intermediate-result sizes, communication cost, and operator-placement choices. These scenarios are future experiments, not current implementation commitments.

## Policy direction

Future work may introduce source-specific policy constraints using technologies such as ODRL and UMA. Policies could constrain whether raw observations leave a source, whether aggregation executes remotely, whether intermediate bindings transfer, whether results are cached, and which requester may access a source. This would frame the problem as finding useful physical execution plans subject to source-specific policy constraints. Policies are intentionally separate from the current resource-optimization experiments and are not implemented.

## Research roadmap

```text
Phase 1 — Two-source physical plans
✓ FetchAll
✓ AggregatePushdown
✓ BindJoin
✓ preliminary scale experiment

Phase 2 — Three-source joins
○ metadata source
○ join ordering
○ semijoins
○ multi-stage bind joins

Phase 3 — Larger federation
○ multiple historical sources
○ multiple live streams
○ operator placement

Phase 4 — Governance
○ ODRL
○ UMA
○ policy-constrained plans

Phase 5 — Optional future optimization
○ automatic strategy selection
○ cost-based planning
```

## Running

Run one explicit plan using the canonical query:

```sh
cargo run --release --bin federated_benchmark -- --query queries/anomaly.janusql --strategy bind-join --live-sensor-counts 100 --repetitions 1 --warmups 0
```

Run the full diagnostic defaults:

```sh
cargo run --release --bin federated_benchmark -- --output-dir results
```

The runner records measured repetitions only, verifies result hashes, and writes CSVs and SVG diagnostics. It has no `--strategy auto` and no planner.

## Janus integration boundary

Janus exports its parser/AST, historical storage, historical executor, Oxigraph adapter, live processor, and RDF event model. This milestone supports only the parsed anomaly-query subset: one `ON STREAM` live window, one `ON LOG` historical window, a baseline `AVG` grouped by the shared subject variable, and `FILTER(?current > multiplier * ?average)`. It does not implement arbitrary Janus-QL federation or automatic plan selection.

For this first integration the live side is a lightweight deterministic temporal evaluator, not `rsp-rs`: it applies the parsed live `RANGE` and `STEP` to timestamped synthetic events using `[T - RANGE, T)`. The historical side remains the compact indexed benchmark source, but receives the exact interval resolved by Janus's `WindowDefinition`; it is not Janus production storage.
