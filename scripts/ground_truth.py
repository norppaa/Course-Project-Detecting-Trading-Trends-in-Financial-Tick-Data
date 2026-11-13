#!/usr/bin/env python3
"""Independent ground truth for Query 1 and Query 2.

Computes the expected results directly from the course spec, as a batch job
over the whole input. It shares no code with analyzer/engine: there are no
watermarks, no grace period and no partitions. Every price event counts, no
matter how late it is in the file, so the output is what a perfect streaming
system would produce.

Definitions (course spec, Query 1 and Query 2):
  * Windows are 5-minute tumbling windows in event time, starting at 00:00.
    Event time is the trading date and time, read as a naive wall clock.
  * The close of a window is the price of the last price event in it, by
    event time. Events with the same time: the one later in the input wins.
  * EMA_j(w) = close(w) * 2/(1+j) + EMA_j(w-1) * (1 - 2/(1+j)), j = 38, 100,
    with EMA_j = 0 before a symbol's first window.
  * BUY  if EMA38(w) > EMA100(w) and EMA38(w-1) <= EMA100(w-1)
    SELL if EMA38(w) < EMA100(w) and EMA38(w-1) >= EMA100(w-1)

Choices the spec leaves open; these must match the system's configuration:
  --policy skip   a symbol is evaluated only in windows where it traded, and
                  "previous window" means its previous evaluated window
  --policy carry  every window from the symbol's first trade to the last
                  window of the input is evaluated, reusing the last close
  --suppress-first  no advisory in a symbol's first window (both EMAs start
                  at 0, so the spec's formulas always give a BUY there)

The output has the same columns as analyzer/cmd/offline, so it can be checked
with scripts/compare_results.py, or with `bench analyze -compare`.

Only the Python standard library is used. Input is the 7-column cleaned CSV
from scripts/clean_data.go; several files are read in the order given.

Example:
    python3 scripts/ground_truth.py data/trades_only_week.csv -o truth_week.csv
"""

import argparse
import datetime
import sys
import time
from collections import defaultdict

ALPHA38 = 2.0 / (1.0 + 38.0)
ALPHA100 = 2.0 / (1.0 + 100.0)

US_PER_SECOND = 1_000_000
WINDOW_US = 5 * 60 * US_PER_SECOND
DAY_US = 24 * 60 * 60 * US_PER_SECOND
WINDOWS_PER_DAY = DAY_US // WINDOW_US

EPOCH = datetime.date(1970, 1, 1)


class BadTimestamp(ValueError):
    pass


_day_cache: dict[str, int] = {}


def day_start_us(date_text: str) -> int:
    """Microseconds from the epoch to 00:00 of a DD-MM-YYYY date."""
    us = _day_cache.get(date_text)
    if us is None:
        try:
            dd, mm, yyyy = date_text.split("-")
            day = datetime.date(int(yyyy), int(mm), int(dd))
        except ValueError as e:
            raise BadTimestamp(date_text) from e
        us = (day - EPOCH).days * DAY_US
        _day_cache[date_text] = us
    return us


def time_of_day_us(clock: str) -> int:
    """Microseconds since midnight of HH:MM:SS[.f to .ffffff]."""
    try:
        hh, mm, rest = clock.split(":")
        sec, _, frac = rest.partition(".")
        h, m, s = int(hh), int(mm), int(sec)
    except ValueError as e:
        raise BadTimestamp(clock) from e
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 60) or len(frac) > 6:
        raise BadTimestamp(clock)
    if frac and not frac.isdigit():
        raise BadTimestamp(clock)
    micros = int(frac.ljust(6, "0")) if frac else 0
    return ((h * 60 + m) * 60 + s) * US_PER_SECOND + micros


def format_window(idx: int) -> str:
    """Window start as DD-MM-YYYY HH:MM:SS, the offline tool's format."""
    day, slot = divmod(idx, WINDOWS_PER_DAY)
    d = EPOCH + datetime.timedelta(days=day)
    minutes = slot * 5
    return f"{d.day:02d}-{d.month:02d}-{d.year:04d} {minutes // 60:02d}:{minutes % 60:02d}:00"


def format_float(x: float) -> str:
    """Shortest round-trip form, written like Go's strconv 'g' with -1."""
    s = repr(x)
    return s[:-2] if s.endswith(".0") else s


def read_closes(paths, fallback_system_time, counts):
    """Returns {(symbol, window index): (event time, price)} and sec types."""
    closes: dict[tuple[str, int], tuple[int, float]] = {}
    sec_types: dict[str, str] = {}
    started = time.monotonic()
    for path in paths:
        with open(path, "r", encoding="utf-8", newline="", buffering=1 << 20) as f:
            for line in f:
                if not line or line[0] == "#" or line.startswith("ID,"):
                    continue
                counts["rows"] += 1
                if counts["rows"] % 10_000_000 == 0:
                    rate = counts["rows"] / (time.monotonic() - started)
                    print(f"  {counts['rows'] // 1_000_000} M rows ({rate / 1e6:.2f} M rows/s)", file=sys.stderr)
                fields = line.rstrip("\r\n").split(",")
                if len(fields) != 7:
                    counts["bad columns"] += 1
                    continue
                symbol, sec_type, sys_date, sys_time, last, trading_time, trading_date = fields
                if not last:
                    counts["no price"] += 1
                    continue
                try:
                    price = float(last)
                except ValueError:
                    counts["bad price"] += 1
                    continue
                if not price > 0:
                    counts["bad price"] += 1
                    continue
                if not trading_time:
                    counts["missing trading time"] += 1
                    if not fallback_system_time:
                        continue
                    trading_date, trading_time = sys_date, sys_time
                elif not trading_date:
                    trading_date = sys_date
                try:
                    ts = day_start_us(trading_date) + time_of_day_us(trading_time)
                except BadTimestamp:
                    counts["bad timestamp"] += 1
                    continue

                counts["price events"] += 1
                if symbol not in sec_types:
                    sec_types[symbol] = sec_type[:1]
                key = (symbol, ts // WINDOW_US)
                current = closes.get(key)
                if current is None or ts >= current[0]:
                    closes[key] = (ts, price)
    return closes, sec_types


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("inputs", nargs="+", help="cleaned CSV files, read in this order")
    ap.add_argument("-o", "--output", required=True, help="output CSV")
    ap.add_argument("--policy", choices=["skip", "carry"], default="skip", help="empty-window policy")
    ap.add_argument("--suppress-first", action="store_true", help="no advisory in a symbol's first window")
    ap.add_argument("--fallback-system-time", action="store_true",
                    help="use system date and time when trading time is missing (default: drop the row)")
    args = ap.parse_args()

    started = time.monotonic()
    counts: dict[str, int] = defaultdict(int)
    closes, sec_types = read_closes(args.inputs, args.fallback_system_time, counts)

    windows_by_symbol: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for (symbol, idx), (_, price) in closes.items():
        windows_by_symbol[symbol].append((idx, price))
    del closes
    last_window = max((idx for wins in windows_by_symbol.values() for idx, _ in wins), default=0)

    rows = buys = sells = 0
    with open(args.output, "w", encoding="utf-8", newline="", buffering=1 << 20) as out:
        out.write("symbol,sec_type,window_start,window_end,close,has_tick,ema38,ema100,signal\n")
        for symbol in sorted(windows_by_symbol):
            traded = sorted(windows_by_symbol[symbol])
            if args.policy == "skip":
                evaluated = [(idx, price, True) for idx, price in traded]
            else:
                evaluated = []
                by_idx = dict(traded)
                close = None
                for idx in range(traded[0][0], last_window + 1):
                    if idx in by_idx:
                        close = by_idx[idx]
                        evaluated.append((idx, close, True))
                    else:
                        evaluated.append((idx, close, False))

            sec_type = sec_types[symbol]
            ema38 = ema100 = 0.0
            first = True
            for idx, close, has_tick in evaluated:
                prev38, prev100 = ema38, ema100
                ema38 = close * ALPHA38 + prev38 * (1 - ALPHA38)
                ema100 = close * ALPHA100 + prev100 * (1 - ALPHA100)
                if ema38 > ema100 and prev38 <= prev100:
                    signal = "BUY"
                elif ema38 < ema100 and prev38 >= prev100:
                    signal = "SELL"
                else:
                    signal = "NONE"
                if first and args.suppress_first:
                    signal = "NONE"
                first = False
                buys += signal == "BUY"
                sells += signal == "SELL"
                rows += 1
                out.write(f"{symbol},{sec_type},{format_window(idx)},{format_window(idx + 1)},"
                          f"{format_float(close)},{'true' if has_tick else 'false'},"
                          f"{format_float(ema38)},{format_float(ema100)},{signal}\n")

    print("\n=== ground truth ===", file=sys.stderr)
    for k in ["rows", "no price", "bad price", "bad columns", "missing trading time", "bad timestamp", "price events"]:
        print(f"{k:<28} {counts[k]:>12}", file=sys.stderr)
    print(f"{'distinct symbols':<28} {len(windows_by_symbol):>12}", file=sys.stderr)
    print(f"{'windows':<28} {rows:>12}", file=sys.stderr)
    print(f"{'BUY / SELL':<28} {buys:>12} / {sells}", file=sys.stderr)
    print(f"{'policy':<28} {args.policy:>12}  (suppress first: {args.suppress_first})", file=sys.stderr)
    print(f"{'elapsed':<28} {time.monotonic() - started:>11.1f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
