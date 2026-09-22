#!/usr/bin/env python3
"""Generate local plots for the historical-scale benchmark."""
import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def read(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def grouped(rows, y, title, ylabel, output, log_y=False):
    series = defaultdict(list)
    for row in rows:
        series[row["strategy"]].append(
            (float(row["historical_quads"]), float(row[y]))
        )

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    for name, points in sorted(series.items()):
        points.sort()
        x, values = zip(*points)
        ax.plot(x, values, marker="o", linewidth=2.2, label=name.replace("-", " "))

    ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    ax.set(
        title=title,
        xlabel="Historical RDF quads",
        ylabel=ylabel,
    )
    ax.legend(frameon=True)
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()
    root = Path(args.root)

    for summary in root.rglob("historical_scale_summary.csv"):
        rows = read(summary)
        out = summary.parent / "plots"

        grouped(
            rows,
            "median_latency_ms",
            "Execution latency vs historical archive size",
            "Median execution latency (ms)",
            out / "historical_scale_latency.png",
            log_y=True,
        )
        grouped(
            rows,
            "mean_transferred_bytes",
            "Transferred data vs historical archive size",
            "Transferred bytes",
            out / "historical_scale_bytes.png",
            log_y=True,
        )
        grouped(
            rows,
            "mean_historical_records_scanned",
            "Historical work vs archive size",
            "Historical records scanned",
            out / "historical_scale_scanned.png",
            log_y=True,
        )
        grouped(
            rows,
            "mean_historical_records_returned",
            "Historical rows returned vs archive size",
            "Historical rows returned",
            out / "historical_scale_returned.png",
            log_y=True,
        )


if __name__ == "__main__":
    main()
