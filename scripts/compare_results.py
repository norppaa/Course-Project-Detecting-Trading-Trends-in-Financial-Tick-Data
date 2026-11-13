#!/usr/bin/env python3
"""Compares the system's results with the ground truth.

The system output can be the offline tool's CSV or a benchmark run's
results.csv; only the first nine columns are used. Rows are matched by
(symbol, window_start).

Reported:
  * coverage    (symbol, window) pairs found in both, missing, extra
  * values      close and has_tick mismatches, largest relative EMA error
  * signals     BUY and SELL precision and recall, also without each
                symbol's first window (the spec's formulas always give a BUY
                there, so it says little about real crossovers)

Exits with 0 if the system matches the ground truth within the tolerance,
and with 1 otherwise.

Example:
    python3 scripts/compare_results.py truth_week.csv results/latency-50k/<run>/results.csv
"""

import argparse
import json
import sys

FIELDS = 9


def window_key(text: str) -> tuple:
    """Sortable form of DD-MM-YYYY HH:MM:SS."""
    return (text[6:10], text[3:5], text[0:2], text[11:])


def load(path):
    """Returns {(symbol, window_start): (close, has_tick, ema38, ema100, signal)} and the duplicate count."""
    rows = {}
    duplicates = 0
    with open(path, "r", encoding="utf-8", buffering=1 << 20) as f:
        header = f.readline()
        if not header.startswith("symbol,"):
            sys.exit(f"{path}: unexpected header {header.strip()!r}")
        for line in f:
            fields = line.rstrip("\r\n").split(",", FIELDS)
            if len(fields) < FIELDS:
                continue
            symbol, _, start, _, close, has_tick, ema38, ema100, signal = fields[:FIELDS]
            key = (symbol, start)
            if key in rows:
                duplicates += 1
            rows[key] = (float(close), has_tick == "true", float(ema38), float(ema100), signal)
    return rows, duplicates


def ratio(a, b):
    return a / b if b else float("nan")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("truth", help="output of scripts/ground_truth.py")
    ap.add_argument("system", help="offline.csv or results.csv")
    ap.add_argument("--tol", type=float, default=1e-9, help="largest allowed relative EMA error")
    ap.add_argument("--json", help="also write the numbers to this JSON file")
    ap.add_argument("--examples", type=int, default=5, help="differences to print")
    args = ap.parse_args()

    truth, _ = load(args.truth)
    system, duplicates = load(args.system)

    first_window = {}
    for symbol, start in truth:
        k = window_key(start)
        if symbol not in first_window or k < first_window[symbol]:
            first_window[symbol] = k

    def is_first(key):
        return first_window.get(key[0]) == window_key(key[1])

    matched = missing = extra = 0
    close_diff = tick_diff = ema_over_tol = 0
    max_rel = 0.0
    max_rel_key = None
    examples = []
    # signal -> [tp, fp, fn], for all windows and without first windows
    sig_all = {"BUY": [0, 0, 0], "SELL": [0, 0, 0]}
    sig_cross = {"BUY": [0, 0, 0], "SELL": [0, 0, 0]}

    def count_signals(key, want, got):
        for table, include in ((sig_all, True), (sig_cross, not is_first(key))):
            if not include:
                continue
            for s in ("BUY", "SELL"):
                if want == s and got == s:
                    table[s][0] += 1
                elif got == s:
                    table[s][1] += 1
                elif want == s:
                    table[s][2] += 1

    def example(kind, key, want, got):
        if len(examples) < args.examples:
            examples.append(f"  {kind:<10} {key[0]} {key[1]}  truth={want}  system={got}")

    for key, want in truth.items():
        got = system.get(key)
        if got is None:
            missing += 1
            count_signals(key, want[4], "NONE")
            example("missing", key, want, None)
            continue
        matched += 1
        if got[0] != want[0]:
            close_diff += 1
            example("close", key, want, got)
        if got[1] != want[1]:
            tick_diff += 1
        worst = 0.0
        for i in (2, 3):
            denom = max(abs(want[i]), 1e-300)
            worst = max(worst, abs(got[i] - want[i]) / denom)
        if worst > max_rel:
            max_rel, max_rel_key = worst, key
        if worst > args.tol:
            ema_over_tol += 1
            if got[0] == want[0]:
                example("ema", key, want, got)
        if got[4] != want[4]:
            example("signal", key, want, got)
        count_signals(key, want[4], got[4])

    for key, got in system.items():
        if key not in truth:
            extra += 1
            count_signals(key, "NONE", got[4])
            example("extra", key, None, got)

    def pr(table):
        out = {}
        for s, (tp, fp, fn) in table.items():
            out[s] = {"tp": tp, "fp": fp, "fn": fn,
                      "precision": ratio(tp, tp + fp), "recall": ratio(tp, tp + fn)}
        return out

    report = {
        "truth_windows": len(truth),
        "system_windows": len(system),
        "matched": matched,
        "missing_in_system": missing,
        "extra_in_system": extra,
        "duplicates_in_system": duplicates,
        "coverage": ratio(matched, len(truth)),
        "close_mismatches": close_diff,
        "has_tick_mismatches": tick_diff,
        "max_relative_ema_error": max_rel,
        "ema_errors_over_tolerance": ema_over_tol,
        "tolerance": args.tol,
        "signals": pr(sig_all),
        "signals_without_first_window": pr(sig_cross),
    }
    ok = (missing == 0 and extra == 0 and duplicates == 0 and close_diff == 0
          and tick_diff == 0 and ema_over_tol == 0
          and all(v["fp"] == 0 and v["fn"] == 0 for v in report["signals"].values()))
    report["match"] = ok

    print(f"Windows      truth {len(truth)}, system {len(system)}")
    print(f"Coverage     {matched} matched ({100 * report['coverage']:.4f}%), "
          f"{missing} missing, {extra} extra, {duplicates} duplicated")
    print(f"Values       {close_diff} close and {tick_diff} has_tick mismatches; "
          f"max relative EMA error {max_rel:.3g}" + (f" at {max_rel_key[0]} {max_rel_key[1]}" if max_rel_key else ""))
    print(f"             {ema_over_tol} windows over the tolerance {args.tol:g}")
    for title, table in (("Signals", report["signals"]), ("Without first windows", report["signals_without_first_window"])):
        print(title)
        for s, v in table.items():
            print(f"  {s:<5} truth {v['tp'] + v['fn']:>8}, system {v['tp'] + v['fp']:>8}, "
                  f"precision {v['precision']:.6f}, recall {v['recall']:.6f}")
    if examples:
        print("Examples of differences")
        print("\n".join(examples))
    print("\nResult: " + ("MATCH" if ok else "DIFFERENT"))

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
