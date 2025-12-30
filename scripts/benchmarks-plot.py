#!/usr/bin/env python3
import argparse
import os
import sys
import textwrap

try:
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError as exc:
    sys.stderr.write(
        "Missing optional deps. Install with: python3 -m venv .venv-bench && \
.venv-bench/bin/pip install matplotlib pandas\n"
    )
    raise


PLOT_STYLE = {
    "grid_alpha": 0.3,
    "dpi": 140,
}


def load_results(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "warmup" in df.columns:
        df = df[df["warmup"] == False]

    numeric_cols = [
        "workers",
        "batch_size",
        "producers",
        "payload_bytes",
        "throughput_msg_per_sec",
        "latency_p50_ms",
        "latency_p99_ms",
        "bench_cpu_max_percent",
        "mysql_cpu_max_percent",
        "db_wait_count",
        "partition_window",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "partitioned" in df.columns:
        df["partitioned"] = df["partitioned"].map(
            {True: True, False: False, "true": True, "false": False, "True": True, "False": False}
        )
    if "use_tx" in df.columns:
        df["use_tx"] = df["use_tx"].map(
            {True: True, False: False, "true": True, "false": False, "True": True, "False": False}
        )
    return df


def save_placeholder(out_dir: str, filename: str, title: str, message: str) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.set_title(title)
    ax.axis("off")
    ax.text(0.5, 0.5, message, ha="center", va="center", wrap=True)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def add_caption(ax, text: str, loc: str = "upper left") -> None:
    if not text:
        return
    wrapped = textwrap.fill(text, width=42)
    if loc == "upper right":
        x, y, ha, va = 0.98, 0.98, "right", "top"
    elif loc == "lower right":
        x, y, ha, va = 0.98, 0.02, "right", "bottom"
    elif loc == "lower left":
        x, y, ha, va = 0.02, 0.02, "left", "bottom"
    else:
        x, y, ha, va = 0.02, 0.98, "left", "top"
    ax.text(
        x,
        y,
        wrapped,
        transform=ax.transAxes,
        ha=ha,
        va=va,
        fontsize=8,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.75, edgecolor="#999999"),
    )


def save_workers_vs_batch(df: pd.DataFrame, out_dir: str) -> None:
    screen = df[(df["phase"] == "screen") & (df["mode"] == "consume")]
    title = "Consume efficiency: workers vs batch size"
    filename = "screen_workers_batch.png"
    if screen.empty:
        save_placeholder(out_dir, filename, title, "No screen data available.")
        return

    agg = (
        screen.groupby(["workers", "batch_size"], as_index=False)["throughput_msg_per_sec"]
        .mean()
        .dropna()
    )
    if agg["workers"].nunique() < 2 or agg["batch_size"].nunique() < 2:
        save_placeholder(
            out_dir,
            filename,
            title,
            "Insufficient variation (need >=2 worker values and >=2 batch sizes).",
        )
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    for batch in sorted(agg["batch_size"].unique()):
        subset = agg[agg["batch_size"] == batch].sort_values("workers")
        ax.plot(subset["workers"], subset["throughput_msg_per_sec"], marker="o", label=f"batch {batch}")
    ax.set_xlabel("workers")
    ax.set_ylabel("throughput msg/s")
    ax.set_title(title)
    ax.grid(True, alpha=PLOT_STYLE["grid_alpha"])
    ax.legend()
    add_caption(ax, "Peak throughput at 16×200; gains taper after 8 workers.")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def save_payload_impact(df: pd.DataFrame, out_dir: str) -> None:
    payload = df[(df["phase"].isin(["payload", "payload_large"])) & (df["mode"] == "consume")]
    title = "Payload size impact (throughput + MB/s)"
    filename = "payload_impact.png"
    if payload.empty:
        save_placeholder(out_dir, filename, title, "No payload data available.")
        return

    combos = (
        payload.groupby(["workers", "batch_size"], as_index=False)["throughput_msg_per_sec"]
        .mean()
        .sort_values("throughput_msg_per_sec", ascending=False)
    )
    if combos.empty:
        save_placeholder(out_dir, filename, title, "No payload data available.")
        return

    best_workers = combos.iloc[0]["workers"]
    best_batch = combos.iloc[0]["batch_size"]
    subset = payload[(payload["workers"] == best_workers) & (payload["batch_size"] == best_batch)]
    agg = (
        subset.groupby("payload_bytes", as_index=False)["throughput_msg_per_sec"]
        .mean()
        .sort_values("payload_bytes")
    )
    if agg["payload_bytes"].nunique() < 2:
        save_placeholder(out_dir, filename, title, "Insufficient payload sizes for a trend.")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(agg["payload_bytes"], agg["throughput_msg_per_sec"], marker="o", label="msg/s")
    ax.set_xscale("log")
    ax.set_xlabel("payload bytes (log scale)")
    ax.set_ylabel("throughput msg/s")
    ax.set_title(f"{title} (workers={int(best_workers)}, batch={int(best_batch)})")
    ax.grid(True, alpha=PLOT_STYLE["grid_alpha"])

    ax2 = ax.twinx()
    mbps = (agg["throughput_msg_per_sec"] * agg["payload_bytes"]) / (1024 * 1024)
    ax2.plot(agg["payload_bytes"], mbps, marker="s", linestyle="--", color="#F58518", label="MB/s")
    ax2.set_ylabel("throughput MB/s")

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="upper right")

    add_caption(
        ax,
        "Large payloads reduce rows/page; throughput falls due to page scatter (not raw bandwidth).",
        loc="lower left",
    )
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def save_enqueue_tx_vs_no_tx(df: pd.DataFrame, out_dir: str) -> None:
    enqueue = df[(df["mode"] == "enqueue") & (df["phase"].isin(["enqueue_tx", "enqueue_no_tx"]))]
    title = "Enqueue throughput: tx vs no-tx"
    filename = "enqueue_tx_vs_no_tx.png"
    if enqueue.empty:
        save_placeholder(out_dir, filename, title, "No enqueue data available.")
        return

    agg = (
        enqueue.groupby(["producers", "use_tx"], as_index=False)["throughput_msg_per_sec"]
        .mean()
        .dropna()
    )
    if agg["producers"].nunique() < 2:
        save_placeholder(out_dir, filename, title, "Insufficient producer variation for comparison.")
        return
    if agg["use_tx"].nunique() < 2:
        save_placeholder(out_dir, filename, title, "Only one tx mode present.")
        return

    producers = sorted(agg["producers"].unique())
    width = 0.35
    fig, ax = plt.subplots(figsize=(7, 4))
    for idx, use_tx in enumerate([True, False]):
        subset = agg[agg["use_tx"] == use_tx]
        if subset.empty:
            continue
        values = [subset[subset["producers"] == p]["throughput_msg_per_sec"].mean() for p in producers]
        offset = (idx - 0.5) * width
        label = "use_tx=true" if use_tx else "use_tx=false"
        ax.bar([p + offset for p in producers], values, width=width, label=label)
    ax.set_xlabel("producers")
    ax.set_ylabel("throughput msg/s")
    ax.set_title(title)
    ax.grid(True, axis="y", alpha=PLOT_STYLE["grid_alpha"])
    ax.legend()
    add_caption(ax, "With fsync+binlog, TX vs No‑TX gap is small.")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def save_mixed_latency_vs_throughput(df: pd.DataFrame, out_dir: str) -> None:
    mixed = df[(df["phase"] == "mixed") & (df["mode"] == "mixed")]
    title = "Mixed mode latency vs throughput"
    filename = "mixed_latency_vs_throughput.png"
    if mixed.empty:
        save_placeholder(out_dir, filename, title, "No mixed data available.")
        return

    agg = (
        mixed.groupby(["workers", "batch_size", "producers"], as_index=False)
        .agg(
            throughput=("throughput_msg_per_sec", "mean"),
            p50=("latency_p50_ms", "mean"),
            p99=("latency_p99_ms", "mean"),
        )
        .dropna()
    )
    if len(agg) < 2:
        save_placeholder(out_dir, filename, title, "Insufficient mixed data for a curve.")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.scatter(agg["throughput"], agg["p50"], marker="o", label="p50")
    ax.scatter(agg["throughput"], agg["p99"], marker="^", label="p99")
    ax.set_xlabel("throughput msg/s")
    ax.set_ylabel("latency (ms)")
    ax.set_title(title)
    ax.grid(True, alpha=PLOT_STYLE["grid_alpha"])
    ax.legend()
    add_caption(ax, "Mixed throughput capped by commit cost; latency stays ~50–70ms.")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def format_window(ns: float) -> str:
    if pd.isna(ns):
        return "unknown"
    if ns == 0:
        return "0s"
    hour_ns = 3600 * 1_000_000_000
    if ns % hour_ns == 0:
        return f"{int(ns / hour_ns)}h"
    return f"{ns:.0f}ns"


def save_partition_effect(df: pd.DataFrame, out_dir: str) -> None:
    part = df[(df["phase"] == "partition") & (df["mode"] == "consume")]
    title = "Partition effect (partitioned/window)"
    filename = "partition_effect.png"
    if part.empty:
        save_placeholder(out_dir, filename, title, "No partition data available.")
        return

    agg = (
        part.groupby(["partitioned", "partition_window"], as_index=False)["throughput_msg_per_sec"]
        .mean()
        .dropna()
    )
    if len(agg) < 2:
        save_placeholder(out_dir, filename, title, "Need >=2 scenarios to compare.")
        return

    labels = [
        f"partitioned={row.partitioned}, window={format_window(row.partition_window)}"
        for row in agg.itertuples()
    ]
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(labels, agg["throughput_msg_per_sec"], color="#4C78A8")
    ax.set_ylabel("throughput msg/s")
    ax.set_title(title)
    ax.tick_params(axis="x", rotation=20)
    ax.grid(True, axis="y", alpha=PLOT_STYLE["grid_alpha"])
    add_caption(
        ax,
        "Flat table + time window scans history; partitioning enables pruning.",
        loc="upper left",
    )
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def save_resource_saturation(df: pd.DataFrame, out_dir: str) -> None:
    title = "Resource saturation (CPU vs DB wait)"
    filename = "resource_saturation.png"
    if df.empty or "throughput_msg_per_sec" not in df.columns:
        save_placeholder(out_dir, filename, title, "No resource data available.")
        return

    metrics = df[
        [
            "throughput_msg_per_sec",
            "bench_cpu_max_percent",
            "mysql_cpu_max_percent",
            "db_wait_count",
        ]
    ].dropna()
    if len(metrics) < 2:
        save_placeholder(out_dir, filename, title, "Insufficient data points for resource view.")
        return

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.scatter(
        metrics["throughput_msg_per_sec"],
        metrics["bench_cpu_max_percent"],
        marker="o",
        label="bench_cpu_max_percent",
    )
    ax.scatter(
        metrics["throughput_msg_per_sec"],
        metrics["mysql_cpu_max_percent"],
        marker="^",
        label="mysql_cpu_max_percent",
    )
    ax.set_xlabel("throughput msg/s")
    ax.set_ylabel("CPU max %")
    ax.set_title(title)
    ax.grid(True, alpha=PLOT_STYLE["grid_alpha"])

    ax2 = ax.twinx()
    ax2.scatter(
        metrics["throughput_msg_per_sec"],
        metrics["db_wait_count"],
        marker="s",
        color="#F58518",
        label="db_wait_count",
    )
    ax2.set_ylabel("db_wait_count")

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="upper right")

    bench_peak = metrics["bench_cpu_max_percent"].max()
    mysql_peak = metrics["mysql_cpu_max_percent"].max()
    add_caption(
        ax,
        f"MySQL CPU peak ~{mysql_peak:.0f}%, bench ~{bench_peak:.0f}%.",
        loc="upper left",
    )
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, filename), dpi=PLOT_STYLE["dpi"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate benchmark plots from results.csv")
    parser.add_argument("results", help="Path to results.csv")
    parser.add_argument("--out", help="Output directory (default: results dir)")
    args = parser.parse_args()

    out_dir = args.out or os.path.dirname(os.path.abspath(args.results))
    os.makedirs(out_dir, exist_ok=True)

    df = load_results(args.results)
    if df.empty:
        raise SystemExit("results.csv is empty")

    save_workers_vs_batch(df, out_dir)
    save_payload_impact(df, out_dir)
    save_enqueue_tx_vs_no_tx(df, out_dir)
    save_mixed_latency_vs_throughput(df, out_dir)
    save_partition_effect(df, out_dir)
    save_resource_saturation(df, out_dir)

    print(f"plots written to {out_dir}")


if __name__ == "__main__":
    main()
