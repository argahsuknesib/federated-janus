#!/usr/bin/env python3
"""Derive comparison, sustainability, and plots from actual campaign CSVs only."""
import argparse
import csv
from pathlib import Path


def rows(root):
    """Load only direct matrix campaigns, never preserved failed attempts."""
    summaries = []
    for path in sorted(root.glob("*/summary.csv")):
        if ".failed-attempt-" in path.parent.name:
            continue
        summaries.append(next(csv.DictReader(path.open(newline=""))))
    return summaries


def number(row, key):
    value = row.get(key, "")
    return float(value) if value not in ("", None) else None


def write(path, fields, data):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)


def reduction(baseline, candidate):
    if baseline is None or candidate is None or baseline <= 0:
        return ""
    return 100.0 * (baseline - candidate) / baseline


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    root = Path(args.root)
    summaries = rows(root)
    print(f"loaded_campaigns={len(summaries)}")
    by_key = {(r["historical_quads"], r["transport_profile"], r["strategy"]): r for r in summaries}

    sustainability = []
    for r in summaries:
        median_factor = number(r, "median_realtime_factor")
        deadline_rate = number(r, "deadline_miss_rate")
        overload_rate = number(r, "overload_miss_rate")
        sustainability.append({
            "historical_quads": r["historical_quads"], "transport_profile": r["transport_profile"],
            "strategy": r["strategy"], "step_ms": 30000,
            "scheduled_evaluations": r["scheduled_evaluations"],
            "completed_evaluations": r["completed_evaluations"],
            "missed_due_to_overload": r["missed_due_to_overload"],
            "deadline_miss_count": r["deadline_miss_count"],
            "overload_miss_count": r["overload_miss_count"],
            "median_execution_ms": r["median_total_ms"], "p95_execution_ms": r["p95_total_ms"],
            "median_realtime_factor": r["median_realtime_factor"],
            "deadline_miss_rate": r["deadline_miss_rate"], "overload_miss_rate": r["overload_miss_rate"],
            "sustainable": str(median_factor is not None and median_factor < 1 and deadline_rate == 0 and overload_rate == 0).lower(),
        })
    write(root / "sustainability.csv", list(sustainability[0]) if sustainability else ["historical_quads"], sustainability)

    comparisons = []
    pairs = {(r["historical_quads"], r["transport_profile"]) for r in summaries}
    for quads, profile in sorted(pairs):
        baseline = by_key.get((quads, profile, "fetch-all"))
        if baseline is None:
            continue
        for strategy in ("aggregate-pushdown", "bind-join"):
            candidate = by_key.get((quads, profile, strategy))
            if candidate is None:
                continue
            fetch_payload = number(baseline, "mean_total_application_payload_bytes")
            candidate_payload = number(candidate, "mean_total_application_payload_bytes")
            fetch_latency = number(baseline, "median_total_ms")
            candidate_latency = number(candidate, "median_total_ms")
            comparisons.append({"historical_quads": quads, "transport_profile": profile, "strategy": strategy,
                "fetchall_payload_bytes": "" if fetch_payload is None else fetch_payload,
                "strategy_payload_bytes": "" if candidate_payload is None else candidate_payload,
                "payload_reduction_pct": reduction(fetch_payload, candidate_payload),
                "fetchall_median_latency_ms": "" if fetch_latency is None else fetch_latency,
                "strategy_median_latency_ms": "" if candidate_latency is None else candidate_latency,
                "latency_reduction_pct": reduction(fetch_latency, candidate_latency)})
    fields = ["historical_quads", "transport_profile", "strategy", "fetchall_payload_bytes", "strategy_payload_bytes", "payload_reduction_pct", "fetchall_median_latency_ms", "strategy_median_latency_ms", "latency_reduction_pct"]
    write(root / "transfer_vs_latency.csv", fields, comparisons)

    # No data is invented: plots are emitted only from completed campaign summaries.
    if not summaries:
        return
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return
    plots = root / "plots"; plots.mkdir(exist_ok=True)
    for column, name, ylabel, reference in [
        ("median_total_ms", "latency_vs_historical_quads.png", "Median execution (ms)", None),
        ("mean_total_application_payload_bytes", "application_payload_vs_historical_quads.png", "Application payload (bytes)", None),
        ("median_realtime_factor", "realtime_factor_vs_historical_quads.png", "Median realtime factor", 1.0),
        ("deadline_miss_rate", "deadline_miss_rate_vs_historical_quads.png", "Deadline miss rate", None),
        ("overload_miss_rate", "overload_miss_rate_vs_historical_quads.png", "Overload miss rate", None),
    ]:
        profiles = sorted({r["transport_profile"] for r in summaries})
        fig, axes = plt.subplots(2, 2, figsize=(10, 7), sharex=True)
        for axis, profile in zip(axes.flat, profiles):
            series = {}
            for r in summaries:
                if r["transport_profile"] == profile:
                    series.setdefault(r["strategy"], []).append((float(r["historical_quads"]), number(r, column)))
            for strategy, points in sorted(series.items()):
                points = sorted((x, y) for x, y in points if y is not None)
                if points:
                    axis.plot(*zip(*points), marker="o", label=strategy)
            axis.set_xscale("log"); axis.set(title=profile, xlabel="Historical quads", ylabel=ylabel)
            if reference is not None:
                axis.axhline(reference, color="black", linestyle="--", linewidth=1)
            axis.legend(fontsize="small")
        fig.tight_layout(); fig.savefig(plots / name, dpi=180); plt.close(fig)

    # Decomposition is intentionally limited to actual 1M-quad rows.
    million = [r for r in summaries if r["historical_quads"] == "1000000"]
    if million:
        labels = [f'{r["transport_profile"]}\n{r["strategy"]}' for r in million]
        fig, axis = plt.subplots(figsize=(10, 4.8))
        bottom = [0.0] * len(million)
        for column, label in [("mean_historical_storage_ms", "historical storage"),
                              ("mean_local_http_ms", "localhost HTTP residual"),
                              ("mean_emulated_transport_ms", "injected transport"),
                              ("mean_coordinator_ms", "coordinator")]:
            values = [number(r, column) or 0.0 for r in million]
            axis.bar(labels, values, bottom=bottom, label=label)
            bottom = [a + b for a, b in zip(bottom, values)]
        axis.set(ylabel="Mean time (ms)", title="1M-quad latency decomposition")
        axis.legend(); fig.tight_layout(); fig.savefig(plots / "1m_latency_decomposition.png", dpi=180); plt.close(fig)
    if comparisons:
        fig, axis = plt.subplots()
        for strategy in sorted({r["strategy"] for r in comparisons}):
            points = [(r["payload_reduction_pct"], r["latency_reduction_pct"]) for r in comparisons if r["strategy"] == strategy and r["payload_reduction_pct"] != "" and r["latency_reduction_pct"] != ""]
            if points: axis.scatter(*zip(*points), label=strategy)
        axis.set(xlabel="Payload reduction (%)", ylabel="Latency reduction (%)", title="Payload reduction vs latency reduction")
        axis.legend(); fig.tight_layout(); fig.savefig(plots / "payload_reduction_vs_latency_reduction.png", dpi=180); plt.close(fig)

    aggregate_bind = [r for r in summaries if r["strategy"] in ("aggregate-pushdown", "bind-join")]
    if aggregate_bind:
        profiles = sorted({r["transport_profile"] for r in aggregate_bind})
        fig, axes = plt.subplots(2, 2, figsize=(10, 7), sharex=True)
        for axis, profile in zip(axes.flat, profiles):
            for strategy in ("aggregate-pushdown", "bind-join"):
                points = sorted(
                    (float(r["historical_quads"]), number(r, "median_total_ms"))
                    for r in aggregate_bind
                    if r["transport_profile"] == profile and r["strategy"] == strategy
                )
                axis.plot(*zip(*points), marker="o", label=strategy)
            axis.set_xscale("log")
            axis.set(title=profile, xlabel="Historical quads", ylabel="Median execution (ms)")
            axis.legend(fontsize="small")
        fig.tight_layout(); fig.savefig(plots / "aggregate_pushdown_vs_bind_join_latency.png", dpi=180); plt.close(fig)


if __name__ == "__main__":
    main()
