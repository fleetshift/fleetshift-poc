#!/usr/bin/env python3
"""Plot mutation-to-dispatch latency over time from a stress-test JSON dump.

Usage:
    python3 scripts/plot_stress.py /tmp/stress-results-1785243169.json
    python3 scripts/plot_stress.py /tmp/stress-results-1785243169.json --show
    python3 scripts/plot_stress.py /tmp/stress-results-1785243169.json --linear
    python3 scripts/plot_stress.py /tmp/stress-results-1785243169.json --last-gen
    python3 scripts/plot_stress.py /tmp/stress-results-1785243169.json --pool-stats
"""

import argparse
import json
import math
import statistics
import sys
from pathlib import Path

import matplotlib.pyplot as plt


def load_data(path: Path) -> dict:
    """Load the full JSON results file."""
    with open(path) as f:
        return json.load(f)


def usable_deliveries(data: dict) -> list[dict]:
    """Filter deliveries to those with both mutation timestamp and dispatch latency fields present."""
    return [
        d
        for d in data.get("deliveries", [])
        if "mutation_at_unix_ms" in d and "mutation_to_dispatch_ms" in d
    ]


def filter_last_generation(deliveries: list[dict]) -> list[dict]:
    """Keep only deliveries whose generation matches the max for their fulfillment.

    Uses the "fulfillment_id" field (added to JSON output) to group
    deliveries by fulfillment, then keeps only those at the highest
    recorded generation per fulfillment.
    """
    if not deliveries or "fulfillment_id" not in deliveries[0]:
        print(
            "Warning: deliveries lack 'fulfillment_id' field. "
            "Re-run the stress test to generate JSON with this field.",
            file=sys.stderr,
        )
        return deliveries

    # Build max generation per fulfillment.
    max_gen: dict[str, int] = {}
    for d in deliveries:
        fid = d["fulfillment_id"]
        gen = d["generation"]
        if gen > max_gen.get(fid, -1):
            max_gen[fid] = gen

    # Filter.
    return [
        d
        for d in deliveries
        if d["generation"] == max_gen[d["fulfillment_id"]]
    ]


def rolling_median(xs: list[float], ys: list[float], window: int) -> tuple[list[float], list[float]]:
    """Compute a rolling median of ys over a sliding window, sorted by xs."""
    if len(xs) < window:
        return xs, ys
    pairs = sorted(zip(xs, ys, strict=True))
    rx, ry = [], []
    half = window // 2
    for i in range(half, len(pairs) - half):
        chunk = [p[1] for p in pairs[i - half : i + half + 1]]
        rx.append(pairs[i][0])
        ry.append(statistics.median(chunk))
    return rx, ry


def percentile(values: list[float], p: float) -> float:
    """Return the p-th percentile (0-100) using ceiling-rank, matching
    computeDistribution in stress_report_test.go."""
    sorted_v = sorted(values)
    n = len(sorted_v)
    idx = int(math.ceil(p / 100.0 * n)) - 1
    if idx < 0:
        idx = 0
    if idx >= n:
        idx = n - 1
    return sorted_v[idx]


def plot_pool_stats(ax: plt.Axes, pool_stats: dict, t0: float) -> None:
    """Plot connection pool utilization on the given axes.

    Shows in_use / max_open as a percentage for both app and workflow pools.
    Also plots cumulative wait count on a secondary y-axis.
    """
    for pool_name, color, style in [("app", "#4C72B0", "-"), ("workflow", "#DD8452", "-")]:
        samples = pool_stats.get(pool_name, [])
        if not samples:
            continue
        xs = [(s["ts_unix_ms"] - t0) / 1000.0 for s in samples]
        max_open = [s["max_open"] for s in samples]
        in_use = [s["in_use"] for s in samples]
        utilization = [
            (u / m * 100.0) if m > 0 else 0.0
            for u, m in zip(in_use, max_open, strict=True)
        ]
        ax.plot(xs, utilization, color=color, linestyle=style, linewidth=1.5,
                label=f"{pool_name} in_use% (max={max_open[0]})", alpha=0.8)

    ax.set_ylabel("Pool Utilization (%)")
    ax.set_ylim(-5, 105)
    ax.axhline(100, color="#C44E52", linestyle=":", linewidth=1, alpha=0.5)
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)

    # Secondary y-axis: cumulative wait count delta (new waits per sample).
    ax2 = ax.twinx()
    for pool_name, color in [("app", "#55A868"), ("workflow", "#CCB974")]:
        samples = pool_stats.get(pool_name, [])
        if not samples:
            continue
        xs = [(s["ts_unix_ms"] - t0) / 1000.0 for s in samples]
        waits = [s["wait_count"] for s in samples]
        # Convert cumulative to per-interval delta.
        deltas = [0] + [waits[i] - waits[i - 1] for i in range(1, len(waits))]
        ax2.bar(xs, deltas, width=0.8, alpha=0.3, color=color,
                label=f"{pool_name} new waits/s")

    ax2.set_ylabel("New Connection Waits / sample")
    ax2.legend(loc="upper right", fontsize=8)


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot stress-test mutation-to-dispatch latency over time.")
    parser.add_argument("json_file", type=Path, help="Path to the stress-results JSON file.")
    parser.add_argument("--show", action="store_true", help="Show the plot interactively instead of only saving.")
    parser.add_argument("--linear", action="store_true", help="Use linear y-axis instead of log scale.")
    parser.add_argument("-o", "--output", type=Path, default=None, help="Output PNG path (default: same dir as JSON, .png extension).")
    parser.add_argument("--window", type=int, default=30, help="Rolling median window size (default: 30).")
    parser.add_argument(
        "--last-gen",
        action="store_true",
        help="Highlight last-generation deliveries over superseded ones in gray.",
    )
    parser.add_argument(
        "--pool-stats",
        action="store_true",
        help="Add a subplot showing connection pool utilization over time.",
    )
    args = parser.parse_args()

    if not args.json_file.exists():
        print(f"File not found: {args.json_file}", file=sys.stderr)
        sys.exit(1)

    data = load_data(args.json_file)
    all_deliveries = usable_deliveries(data)
    if not all_deliveries:
        print("No deliveries with mutation_at_unix_ms and mutation_to_dispatch_ms found.", file=sys.stderr)
        sys.exit(1)

    pool_stats = data.get("pool_stats")
    show_pool = args.pool_stats and pool_stats

    # Shared time origin across all plots.
    t0 = min(d["mutation_at_unix_ms"] for d in all_deliveries)

    def to_xy(deliveries: list[dict]) -> tuple[list[float], list[float]]:
        xs = [(d["mutation_at_unix_ms"] - t0) / 1000.0 for d in deliveries]
        ys = [d["mutation_to_dispatch_ms"] for d in deliveries]
        return xs, ys

    # -- Layout --
    if show_pool:
        fig, (ax_latency, ax_pool) = plt.subplots(
            2, 1, figsize=(14, 9), sharex=True,
            gridspec_kw={"height_ratios": [3, 1]},
        )
    else:
        fig, ax_latency = plt.subplots(figsize=(12, 6))

    # -- Latency subplot --
    if args.last_gen:
        last_gen_deliveries = filter_last_generation(all_deliveries)
        last_gen_ids = {d["delivery_id"] for d in last_gen_deliveries}
        superseded_deliveries = [d for d in all_deliveries if d["delivery_id"] not in last_gen_ids]

        sup_xs, sup_ys = to_xy(superseded_deliveries)
        if sup_xs:
            ax_latency.scatter(
                sup_xs, sup_ys, s=8, alpha=0.15, color="#999999",
                label=f"Superseded ({len(superseded_deliveries)})", zorder=1,
            )

        xs, ys = to_xy(last_gen_deliveries)
        ax_latency.scatter(
            xs, ys, s=14, alpha=0.5, color="#4C72B0",
            label=f"Last generation ({len(last_gen_deliveries)})", zorder=2,
        )
        title_suffix = " (Last Generation)"
    else:
        xs, ys = to_xy(all_deliveries)
        ax_latency.scatter(xs, ys, s=10, alpha=0.4, color="#4C72B0", label="Deliveries", zorder=2)
        title_suffix = ""

    # Percentile reference lines.
    p50 = percentile(ys, 50)
    p95 = percentile(ys, 95)
    p99 = percentile(ys, 99)

    ax_latency.axhline(p50, color="#55A868", linestyle="--", linewidth=1, label=f"P50 = {p50:.1f} ms")
    ax_latency.axhline(p95, color="#CCB974", linestyle="--", linewidth=1, label=f"P95 = {p95:.1f} ms")
    ax_latency.axhline(p99, color="#DD8452", linestyle="--", linewidth=1, label=f"P99 = {p99:.1f} ms")

    # Rolling median trend line.
    med_xs, med_ys = rolling_median(xs, ys, args.window)
    if med_xs:
        ax_latency.plot(med_xs, med_ys, color="#C44E52", linewidth=2,
                        label=f"Rolling median (w={args.window})", zorder=3)

    if not args.linear:
        ax_latency.set_yscale("log")

    if not show_pool:
        ax_latency.set_xlabel("Time since start (s)")
    ax_latency.set_ylabel("Mutation-to-Dispatch (ms)")
    ax_latency.set_title(f"Stress Test: Mutation-to-Dispatch Latency Over Time{title_suffix}")
    ax_latency.legend(loc="upper left", fontsize=9)
    ax_latency.grid(True, alpha=0.3)

    # -- Pool stats subplot --
    if show_pool:
        plot_pool_stats(ax_pool, pool_stats, t0)
        ax_pool.set_xlabel("Time since start (s)")
        ax_pool.set_title("Connection Pool Utilization")

    fig.tight_layout()

    # Save.
    if args.output:
        out_path = args.output
    else:
        suffix_parts = []
        if args.last_gen:
            suffix_parts.append("lastgen")
        if show_pool:
            suffix_parts.append("pool")
        stem = args.json_file.stem
        if suffix_parts:
            stem += "-" + "-".join(suffix_parts)
        out_path = args.json_file.with_stem(stem).with_suffix(".png")
    fig.savefig(out_path, dpi=150)
    print(f"Saved: {out_path}")

    if args.show:
        plt.show()


if __name__ == "__main__":
    main()
