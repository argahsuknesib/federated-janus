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

## Network historical-scale experiment

`network_historical_scale_benchmark` is a continuous, real-time experiment,
not a three-strategy loop.  Each process invocation is one campaign for exactly
one `--historical-quads`, `--transport-profile`, and `--strategy` value:

```sh
cargo run --release --bin network_historical_scale_benchmark -- \
  --historical-quads 1000000 --transport-profile constrained-edge \
  --strategy fetch-all --output-dir results/1m-constrained-fetch-all
```

The profiles are `native-localhost`, `lan`, `moderate-edge`, and
`constrained-edge`. `scripts/run_network_historical_campaigns.sh` launches the
3 × 4 × 3 matrix as independent sequential processes, then derives comparison
and sustainability files from actual campaign summaries. It does not fabricate
rows or plots when no campaign result exists.

The clock has a wall-clock origin and a fixed logical origin of 4,000,000,000
ms. The first scheduled evaluation is at offset 60,000 ms, then exactly every
30,000 ms; the scheduled logical timestamp is passed to Janus even if physical
execution starts late. The publisher remains real: ten sensors publish once per
250 ms with timestamps based on logical origin plus wall-clock elapsed time.
Live reads are half-open `[scheduled_T - 60,000, scheduled_T)` windows.

There is one worker and no queue. At every STEP, a busy worker produces a
`missed_due_to_overload` row in `scheduling.csv`; it is never run later. A
normal campaign requires one completed warmup and five completed measurements.
The deterministic `--max-campaign-seconds` and `--max-scheduled-evaluations`
safety limits prevent pathological cases from running indefinitely. `--diagnostic`
runs one scheduled completed measurement after fill, for targeted transport
inspection.

`measurements.csv` contains completed events; `scheduling.csv` contains every
scheduled STEP including overload misses; and `summary.csv` reports their
separate counts. A deadline miss is `total_execution_ms > 30000`; overload
misses are scheduler skips and are not deadline misses. `realtime_factor` is
`total_execution_ms / 30000`.

The HTTP service serializes UTF-8 TSV *application bodies*: FetchAll responses
contain `(timestamp, subject, predicate, object, graph)` rows;
AggregatePushdown responses contain `(subject, average)` rows; BindJoin requests
contain sensor bindings and its responses contain `(subject, average)` rows.
CSV fields consequently use `request_payload_bytes`, `response_payload_bytes`,
and `total_application_payload_bytes`, never wire/network bytes.

For non-local profiles the HTTP service physically injects the controlled delay
`fixed_rtt_ms + 1000 * 8 * total_application_payload_bytes / bandwidth_bits_per_second`.
`total_execution_ms` already includes that sleep exactly once. The CSV exposes
the known injected RTT and bandwidth terms separately; `local_http_roundtrip_ms`
is the client elapsed HTTP time minus the injected term, so it is a localhost
residual rather than a measured WAN latency.
