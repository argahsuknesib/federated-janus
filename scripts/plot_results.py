#!/usr/bin/env python3
"""Regenerate Federated-Janus benchmark figures as publication-ready PNGs."""
import argparse, csv
from collections import defaultdict
from pathlib import Path
import shutil
import matplotlib.pyplot as plt

plt.style.use("seaborn-v0_8-whitegrid")
COLORS = {"fetch-all": "#b2182b", "aggregate-pushdown": "#2166ac", "bind-join": "#1b7837",
          "fetch-all-sources": "#b2182b", "aggregate-all-sources": "#2166ac", "live-first-source-selection": "#1b7837"}

def read(path):
    with path.open(newline="") as f: return list(csv.DictReader(f))
def num(row, *names):
    for name in names:
        if name in row and row[name] != "": return float(row[name])
    raise KeyError(names)
def save(fig, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(); fig.savefig(path, dpi=180, bbox_inches="tight"); plt.close(fig)
def grouped(rows, x, y, title, xlabel, ylabel, output, log=False):
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    series = defaultdict(list)
    for r in rows:
        try: series[r["strategy"]].append((num(r, x), num(r, y)))
        except (KeyError, ValueError): continue
    for name, points in sorted(series.items()):
        points.sort(); ax.plot(*zip(*points), marker="o", linewidth=2.2, label=name.replace("-", " "), color=COLORS.get(name))
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    if log: ax.set_yscale("log")
    ax.legend(frameon=True); save(fig, output)
def continuous(root):
    rows = read(root / "continuous_live_first.csv")
    specs = [("execution_latency_ms", "Execution latency", "Execution latency (ms)", "latency.png"),
             ("historical_sources_contacted", "Historical sources contacted", "Sources", "historical_sources_contacted.png"),
             ("live_sources_with_results", "Live sources with results", "Sources", "live_sources_with_results.png"),
             ("historical_records_scanned", "Historical records scanned", "Records", "historical_records_scanned.png")]
    for key, title, ylabel, name in specs:
        fig, ax = plt.subplots(figsize=(8.5, 4.8)); x=[num(r,"elapsed_s") for r in rows]; y=[num(r,key) for r in rows]
        ax.plot(x,y,marker="o",linewidth=2.5,color="#2166ac"); ax.set(title=title,xlabel="Evaluation time since stream start (s)",ylabel=ylabel); save(fig,root/"plots"/name)
def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--root",default="."); args=parser.parse_args(); root=Path(args.root)
    for csv_path in root.rglob("*summary.csv"):
        rows=read(csv_path); out=csv_path.parent/"plots"; stem=csv_path.stem
        if rows and "strategy" in rows[0]:
            x="live_sensor_count" if "live_sensor_count" in rows[0] else ("source_pairs" if "source_pairs" in rows[0] else "active_live_sources")
            for y,title,label,log in [("median_execution_ms","Execution latency","Latency (ms)",True),("median_latency_ms","Execution latency","Latency (ms)",True),("mean_transferred_bytes","Transferred data","Bytes",True),("mean_bytes_transferred","Transferred data","Bytes",True),("mean_historical_records_scanned","Historical work","Records scanned",False),("mean_records_scanned","Historical work","Records scanned",False)]:
                if y in rows[0]: grouped(rows,x,y,title,x.replace("_"," ").title(),label,out/f"{stem}_{y}.png",log)
    c=root/"results-continuous-realtime"
    if (c/"continuous_live_first.csv").exists(): continuous(c)
    docs=root/"docs/figures"; docs.mkdir(parents=True,exist_ok=True)
    copies = {
      "active_selectivity_sources_contacted.png": root/"results-single-query-active-selectivity/plots/active_selectivity_summary_mean_historical_records_scanned.png",
      "active_selectivity_latency.png": root/"results-single-query-active-selectivity/plots/active_selectivity_summary_median_execution_ms.png",
      "active_selectivity_records_scanned.png": root/"results-single-query-active-selectivity/plots/active_selectivity_summary_mean_historical_records_scanned.png",
      "active_selectivity_bytes.png": root/"results-single-query-active-selectivity/plots/active_selectivity_summary_mean_transferred_bytes.png",
      "historical_scale_latency.png": root/"results-janusql-historical-scale/plots/historical_scale_summary_median_latency_ms.png",
      "historical_scale_work.png": root/"results-janusql-historical-scale/plots/historical_scale_summary_mean_historical_records_scanned.png",
      "historical_scale_bytes.png": root/"results-janusql-historical-scale/plots/historical_scale_summary_mean_transferred_bytes.png",
      "latency.png": root/"results-full-diagnostic/plots/live_cardinality_summary_median_latency_ms.png",
      "bytes.png": root/"results-full-diagnostic/plots/live_cardinality_summary_mean_bytes_transferred.png",
      "historical_work.png": root/"results-full-diagnostic/plots/live_cardinality_summary_mean_records_scanned.png",
    }
    for target, source in copies.items():
        if source.exists(): shutil.copyfile(source, docs/target)
if __name__ == "__main__": main()
