#!/usr/bin/env python3
"""Report figures from the benchmark run folders under results/.

Draws the two figures used in the report: the dataset distribution and
end-to-end latency vs. input rate. Reads each run's summary.txt, so the figures
always match the numbers in bench/RESULTS.md. Every point is the median of the
repeats; bands show their range (min to max).

Usage: python3 bench/plots.py [-results results] [-out docs/figures]
Needs matplotlib (pip install matplotlib).
"""

import argparse
import collections
import json
import re
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Categorical slots in fixed order (blue, orange, aqua); text and grid in neutral ink.
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK_2, INK_3, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e4e3df"

E3_RATES = [5000, 50000, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000]
STAGES = ["broker-in", "processing", "broker-out", "end-to-end"]

# Figures are drawn at their printed size so LaTeX doesn't scale the text:
# ACM sigconf columns are 3.33 in wide.
# Include them with \includegraphics{...} without a width, or at \columnwidth.
COLUMN = 3.33
HEIGHT = 2.2

plt.rcParams.update({
    "font.size": 8,
    "axes.labelsize": 8,
    "axes.titlesize": 8,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
    "axes.edgecolor": INK_3,
    "axes.labelcolor": INK,
    "axes.titlecolor": INK,
    "axes.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "axes.grid.axis": "y",
    "axes.axisbelow": True,
    "grid.color": GRID,
    "grid.linewidth": 0.6,
    "xtick.color": INK_2,
    "ytick.color": INK_2,
    "xtick.major.width": 0.6,
    "ytick.major.width": 0.6,
    "xtick.major.size": 2.5,
    "ytick.major.size": 2.5,
    "legend.frameon": False,
    "legend.handlelength": 1.6,
    "legend.columnspacing": 1.2,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.02,
    "pdf.fonttype": 42,
})


def parse_summary(path):
    """Returns the numbers the figures need from one summary.txt."""
    text = path.read_text()
    run = {"latency": {}}
    for stage in STAGES:
        m = re.search(rf"^\s+{stage}\s+\d+\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", text, re.M)
        if m:
            p50, p95, p99, mx = map(float, m.groups())
            run["latency"][stage] = {"p50": p50, "p95": p95, "p99": p99, "max": mx}
    m = re.search(r"^\s+producer\s+(\d+) ticks/s", text, re.M)
    run["producer_rate"] = int(m.group(1)) if m else None
    m = re.search(r"^\s+analyzer\s+(\d+) ticks/s", text, re.M)
    run["analyzer_rate"] = int(m.group(1)) if m else None
    run["valid"] = "Run is VALID" in text
    m = re.search(r"Result: (.+)", text)
    run["sustainable"] = (m.group(1).strip() == "sustainable") if m else None
    return run


def load_runs(folder):
    runs = []
    for summary in sorted(folder.glob("*/summary.txt")):
        run = parse_summary(summary)
        if not run["valid"]:
            print(f"skipping INVALID run {summary.parent}")
            continue
        run["dir"] = summary.parent
        runs.append(run)
    if not runs:
        raise SystemExit(f"no valid runs in {folder}")
    return runs


def spread(values):
    """Median and range of the repeats."""
    return statistics.median(values), min(values), max(values)


def save(fig, out, name):
    for ext in ("pdf", "png"):
        fig.savefig(out / f"{name}.{ext}", dpi=200)
    plt.close(fig)
    print(f"wrote {out / name}.pdf and .png")


def thousands(ax, axis="x"):
    fmt = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}")
    (ax.xaxis if axis == "x" else ax.yaxis).set_major_formatter(fmt)


def plot_e3_latency(results, out):
    """Only sustainable rates are drawn: above them the producer can't reach the
    target, so those runs repeat the highest rate's load (explained in the text)."""
    rates, rows = [], {"p50": [], "p99": []}
    for rate in E3_RATES:
        runs = load_runs(results / f"e3-rate-{rate}")
        if not all(r["sustainable"] for r in runs):
            continue
        rates.append(rate)
        for q in rows:
            rows[q].append(spread([r["latency"]["end-to-end"][q] for r in runs]))

    fig, ax = plt.subplots(figsize=(COLUMN, HEIGHT))
    for q, color, label in (("p99", ORANGE, "p99"), ("p50", BLUE, "p50")):
        med = [v[0] for v in rows[q]]
        ax.fill_between(rates, [v[1] for v in rows[q]], [v[2] for v in rows[q]], color=color, alpha=0.2, lw=0)
        ax.plot(rates, med, color=color, lw=1.5, marker="o", ms=3.5, mec="white", mew=0.6, label=label, zorder=3)

    ax.set_ylim(0, 55)
    ax.set_xlim(-20000, max(rates) + 20000)
    ax.set_ylabel("End-to-end latency (ms)")
    ax.set_xlabel("Input rate (thousand ticks/s)")
    thousands(ax)
    ax.legend(loc="upper center", ncols=2)
    save(fig, out, "e3-latency-vs-rate")


def dataset_stats(path, cache):
    """Trades per symbol, and the busiest second of the input.

    Seconds use the arrival time (Date + Time), the order in which the
    producer replays the file. (Trading time would put the 5,506 trades that
    arrive about 18 hours late into one second.) Reading the week takes under
    a minute; the counts are cached next to the results.
    """
    stamp = f"{path.resolve()}:{path.stat().st_size}:{path.stat().st_mtime_ns}"
    if cache.exists():
        data = json.loads(cache.read_text())
        if data.get("source") == stamp and "per_symbol" in data:
            return data
    per_symbol = collections.Counter()
    per_second = collections.Counter()
    with open(path) as f:
        next(f)
        for line in f:
            cols = line.split(",", 4)
            per_symbol[cols[0]] += 1
            per_second[f"{cols[2]} {cols[3][0:8]}"] += 1
    peak_key, peak = per_second.most_common(1)[0]
    data = {"source": stamp, "per_symbol": per_symbol, "peak_second": peak_key, "peak_second_trades": peak}
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(data))
    return data


QUIET = 100  # symbols with fewer trades than this in the week form the quiet tail


def symbol_groups(per_symbol):
    """Splits the symbols into four groups, each as (label, symbols, trades).

    Two groups publish on a fixed schedule, so their trade counts are (nearly)
    identical: the busiest ETR symbols (about one per second during Xetra
    hours), and the FR/NL symbols at the most common count.
    """
    etr = [c for s, c in per_symbol.items() if s.endswith(".ETR")]
    etr_top = 0.9 * max(etr)
    mode, _ = collections.Counter(per_symbol.values()).most_common(1)[0]
    groups = {"etr": [], "flat": [], "active": [], "quiet": []}
    for s, c in per_symbol.items():
        if s.endswith(".ETR") and c >= etr_top:
            groups["etr"].append(c)
        elif abs(c - mode) <= 1:
            groups["flat"].append(c)
        elif c >= QUIET:
            groups["active"].append(c)
        else:
            groups["quiet"].append(c)
    etr_each = statistics.median(groups["etr"])
    labels = {
        "etr": f"ETR, ~{etr_each / 1000:.0f}k trades per ID",
        "flat": f"FR/NL, ~{mode / 1000:.1f}k trades per ID",
        "active": f"Other, ≥{QUIET} trades",
        "quiet": f"Quiet tail, <{QUIET} trades",
    }
    return [(labels[k], len(v), sum(v)) for k, v in groups.items()]


def plot_dataset(input_file, results, out):
    """Share of symbols vs share of trades for each group of symbols."""
    data = dataset_stats(input_file, results / "dataset-stats.json")
    groups = symbol_groups(data["per_symbol"])
    n = sum(g[1] for g in groups)
    total = sum(g[2] for g in groups)

    fig, ax = plt.subplots(figsize=(COLUMN, HEIGHT))
    h = 0.38
    ys = range(len(groups))
    sym = [100 * g[1] / n for g in groups]
    trd = [100 * g[2] / total for g in groups]
    ax.barh([y - h / 2 for y in ys], sym, height=h, color=BLUE, label="Share of IDs")
    ax.barh([y + h / 2 for y in ys], trd, height=h, color=ORANGE, label="Share of trades")
    for y, a_, b_ in zip(ys, sym, trd):
        for off, v in ((-h / 2, a_), (h / 2, b_)):
            ax.annotate(f"{v:.0f}%" if v >= 1 else f"{v:.1f}%", (v, y + off), xytext=(3, 0),
                        textcoords="offset points", va="center", fontsize=7, color=INK_2)
    ax.set_yticks(list(ys), [f"{g[0]}\n({g[1]:,} IDs)" for g in groups])
    ax.invert_yaxis()
    ax.tick_params(axis="y", length=0)
    ax.grid(True, axis="x")
    ax.grid(False, axis="y")
    ax.set_xlim(0, 50)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncols=2, borderaxespad=0.2)

    for label, k, t in groups:
        print(f"{label}: {k} symbols ({100 * k / n:.1f}%), {t} trades ({100 * t / total:.1f}%)")
    print(f"busiest second {data['peak_second']}: {data['peak_second_trades']} trades")
    save(fig, out, "dataset")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-results", default="results", type=Path)
    parser.add_argument("-out", default="docs/figures", type=Path)
    parser.add_argument("-input", default="data/trades_only_week.csv", type=Path,
                        help="trades of the week, for the dataset figure")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    plot_dataset(args.input, args.results, args.out)
    plot_e3_latency(args.results, args.out)


if __name__ == "__main__":
    main()
