# Architecture and storage

## Query path

Federated-Janus uses the current public Janus-QL query form:

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
Federated-Janus logical plan
   │
   ▼
explicit physical strategy
```

Query structure, windows, aggregates, grouping, and HAVING conditions are derived from parsed Janus-QL. Deprecated `DEFINE BASELINE` / `USING BASELINE` compatibility syntax is not generated.

## Physical strategies

The historical-scale experiment compares three strategies for the same logical query.

### FetchAll

```text
historical source
      │
      │ raw historical quads
      ▼
 coordinator
      │
      AVG + join + HAVING
```

### AggregatePushdown

```text
historical source
      │
      AVG per sensor
      │
      ▼
 coordinator
      │
      join + HAVING
```

### BindJoin

```text
live bindings
      │
      ▼
historical source
      │
filter to bound sensors
      │
AVG selected sensors
      │
      ▼
 coordinator
```

The plan names describe data-flow strategies. They do not imply that Janus currently has every possible physical index needed to make each strategy I/O-selective.

## Janus segmented storage

Historical quads are persisted using `janus::storage::segmented_storage::StreamingSegmentedStorage`.

The benchmark adapter writes RDF quads through Janus's dictionary encoder and forces periodic segment flushes. The default benchmark uses at most 100,000 quads per segment.

The Janus storage layout contains:

- `dictionary.bin` — persisted RDF-term dictionary
- `segment-*.log` — fixed-size dictionary-encoded event records
- `segment-*.idx` — sparse timestamp index entries

The underlying Janus `Event` record contains:

```text
timestamp : u64
subject   : u32
predicate : u32
object    : u32
graph     : u32
```

which is a 24-byte encoded record before filesystem/index overhead.

Historical reads use Janus's half-open `query_rdf_half_open(start, end)` adapter and therefore preserve Janus-QL's half-open historical window semantics.

## Remote historical edge experiment

`historical_edge_service` can place that same `StreamingSegmentedStorage` behind
a separate localhost HTTP process.  It deliberately exposes only `/window`,
`/aggregate`, and `/aggregate-bound`; it is not a SPARQL endpoint.  The manual
FetchAll, AggregatePushdown, and BindJoin plans retain their respective raw-row,
all-sensor-aggregate, and live-bound-aggregate data flows.  No optimizer is
introduced and the Janus storage/index implementation is unchanged.

## Indexing limitation

The current segmented storage performs sparse/two-level **timestamp** indexing. It does not expose a subject/predicate inverted index for this benchmark.

Therefore, when the query's historical time window contains the whole generated archive:

- FetchAll scans the full historical archive.
- AggregatePushdown scans the full historical archive and returns aggregates.
- BindJoin also scans the full historical archive, filters bound subjects at the source, and returns fewer aggregates.

This means a BindJoin result with fewer transferred bytes must not be described as fewer disk records scanned. A future subject-aware index could change that behavior and would be a separate experiment.

## Live side

The historical-scale benchmark intentionally keeps the live side fixed. The default contains 10 live sensor bindings inside the parsed 60-second live window.

The live workload is fixed for every historical archive size.
