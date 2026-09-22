# Architecture and execution model

This document contains the architectural detail intentionally kept out of the top-level README.

## Relationship with Janus

Janus provides the query language and hybrid historical/live semantics. Federated-Janus adds a coordinator that reasons about where a parsed query should execute.

The core pipeline is:

```text
Janus-QL text
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
logical federated query
    │
    ▼
manual physical plan
    │
    ▼
source-specific operators
```

The coordinator does not construct benchmark semantics directly in Rust. Query structure, source IRIs, windows, aggregates, grouping, and HAVING conditions are derived from parsed Janus-QL. Federated-Janus uses the current public Janus-QL window form directly and does not generate the deprecated `DEFINE BASELINE` / `USING BASELINE` compatibility syntax.

## Single-query federation

Federation is represented as **one logical Janus-QL query**. For multi-source anomaly detection, top-level `UNION` branches keep source-pair computations independent.

Conceptually:

```text
branch(sensor 1)
UNION
branch(sensor 2)
UNION
...
branch(sensor N)
```

Federated-Janus lowers this structure into source-specific branch fragments. The logical branch structure is discovered from the parsed AST, not benchmark configuration.

## Source model

A sensor source pair contains one live and one historical source:

```text
Sensor N
├── https://example.org/sensors/N/live
└── https://example.org/sensors/N/history
```

Each IRI resolves through the `SourceRegistry` to an independent source instance. Historical instances hold only their own sensor's observations.

This is distinct from the earlier entity-selectivity prototype, where many sensor entities lived inside one shared live source and one shared historical source.

## Logical query versus physical plan

The logical query defines what result is required. A physical plan defines how it is produced.

Implemented source-oriented strategies include:

- `FetchAllSources` — fetch complete historical windows and aggregate centrally
- `AggregateAllSources` — compute historical averages at every historical source
- `LiveFirstSourceSelection` — evaluate live branches first and skip histories for empty live branches

The metadata planning workload adds the following explicit plans:

- `CentralFetchAll`
- `AggregateAll`
- `LiveFirst`
- `MetadataFirst`
- `LiveMetadataSemiJoin`

These are manually selected experimental strategies. Federated-Janus does not automatically choose among them yet.

## Source-local execution

When a physical operator is pushed down, it executes at the source abstraction before an intermediate result is returned to the coordinator.

Examples:

```text
HistoricalAVG  -> historical source
MetadataFilter -> metadata source
LiveWindow     -> live source
```

The current deployment is in-process, so transfer metrics model logical wire traffic rather than measured network traffic.

## Continuous execution

The continuous benchmark registers the parsed/lowered plan once and then evaluates it repeatedly.

Current live-stream configuration:

- 4 Hz per publishing source
- one observation every 250 ms
- `RANGE 60`
- `STEP 30`
- half-open live windows `[T - 60s, T)`

After the initial fill, a continuously publishing source has approximately 240 observations in its current window and approximately 120 new observations between evaluations.

The coordinator uses one shared evaluation instant for all branches. `LiveFirstSourceSelection` discovers which branches are active by evaluating live windows; active source IDs are not passed directly from benchmark configuration.

## Metadata input

Static metadata is expressed with standard SPARQL named-graph syntax:

```sparql
GRAPH <https://example.org/metadata> {
  ?sensor ex:locatedIn ex:RoomA .
}
```

This did not require a Janus grammar extension.

The metadata predicate creates a second selectivity dimension independent of live activity. Plans can therefore compare live-first, metadata-first, central joins, and semijoin-style execution before accessing history.

## Byte accounting

Transfer accounting excludes purely local processing and counts intermediate data crossing the conceptual source/coordinator boundary.

The controlled planning study uses one consistent logical wire model:

- live RDF row: 80 B
- metadata RDF triple: 72 B
- aggregate tuple: 48 B
- sensor-key binding: 40 B
- raw historical RDF row: 80 B

Operator metrics include input/output rows, bytes sent/received, request counts, execution time, and operator location.

## Janus integration boundary

The repository intentionally supports a narrow subset of Janus-QL relevant to the research workloads. It is not an arbitrary federated SPARQL engine.

Current work depends on Janus parser/AST support for:

- multiple named windows
- `ON STREAM`
- `ON LOG`
- `WINDOW` graph patterns
- aggregates such as `AVG`
- `GROUP BY`
- filters
- nested subqueries where supported
- top-level `UNION` branch structure

The `UNION` structure was added upstream to Janus so branch-local `WINDOW` blocks are preserved structurally rather than flattened into raw `WHERE` text.
