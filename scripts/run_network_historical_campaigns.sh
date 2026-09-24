#!/usr/bin/env bash
# Runs independent processes.  It intentionally never combines strategies in
# one wall-clock campaign.
set -euo pipefail

root=${1:-results-network-historical-scale}
for quads in 10000 100000 1000000; do
  for profile in native-localhost lan moderate-edge constrained-edge; do
    for strategy in fetch-all aggregate-pushdown bind-join; do
      out="$root/${quads}-${profile}-${strategy}"
      cargo run --release --bin network_historical_scale_benchmark -- \
        --historical-quads "$quads" --transport-profile "$profile" \
        --strategy "$strategy" --output-dir "$out"
    done
  done
done
python3 scripts/summarize_network_historical_campaigns.py --root "$root"
