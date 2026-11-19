#!/usr/bin/env bash
# E9: the cost of the non-trade events that the main benchmarks filter out
# before the pipeline. Uses one raw day file (about 54M events, 4.6 per trade).
#
#   1. rawstats    events, trades and the busiest second of each (no broker)
#   2. raw-filter  producer filters at the edge (-only-trades=true), max speed:
#                  how fast raw input is read, parsed and filtered
#   3. raw-drain   all events forwarded, topic pre-filled: the analyzer's own
#                  rate when it has to receive and discard the non-trades
#   4. raw-rate    all events forwarded at fixed rates: latency and
#                  sustainability when the non-trades travel through the pipeline
#
# raw-filter and raw-drain both cover the whole day, so their windows, signals
# and late counts must be identical; the script prints them side by side.
#
# Usage: [RAW=file] [REPEATS=3] [RATES="..."] [DURATION=150] bench/raw-events.sh
# About 1 h with the defaults. Results go to results/e9-*/ and results/e9-raw.log.
set -uo pipefail

cd "$(dirname "$0")/.."

RAW=${RAW:-data/debs2022-gc-trading-day-08-11-21.csv}
REPEATS=${REPEATS:-3}
RATES=${RATES:-50000 200000 400000 600000}
DURATION=${DURATION:-150}
LOG=results/e9-raw.log

[ -f "$RAW" ] || { echo "raw file not found: $RAW" >&2; exit 1; }
mkdir -p results

if [ "$(cat /sys/class/power_supply/AC*/online 2>/dev/null | head -1)" != 1 ]; then
    echo "WARNING: not on AC power; the earlier benchmarks were run on AC." >&2
fi
if [ "$(cat /sys/firmware/acpi/platform_profile 2>/dev/null)" != performance ]; then
    echo "WARNING: power profile is not 'performance' (run: powerprofilesctl set performance)." >&2
fi

RUNLOG=$(mktemp)
trap 'rm -f "$RUNLOG"' EXIT

# Runs one benchmark and keeps the summary lines that matter here.
run() {
    local name=$1
    shift
    echo "=== $(date +%T) $name $*" | tee -a "$LOG"
    if env "$@" INPUT="$RAW" bench/run.sh "$name" > "$RUNLOG" 2>&1; then
        grep -E "^  (producer  |analyzer  |end-to-end|max lag)|^  (analyzer|producer) +[0-9.]+ +[0-9.]+|^  analyzer  ticks|FAIL|Result:|Run is|Results saved" "$RUNLOG" | tee -a "$LOG"
    else
        echo "run failed:" | tee -a "$LOG"
        tail -5 "$RUNLOG" | tee -a "$LOG"
    fi
}

echo "=== $(date +%T) start, $RAW, $REPEATS repeats" | tee -a "$LOG"

echo "=== $(date +%T) rawstats" | tee -a "$LOG"
go run ./bench rawstats "$RAW" | tee -a "$LOG"

# Read the file once so the first run does not pay for the disk.
cat "$RAW" > /dev/null

for rep in $(seq 1 "$REPEATS"); do
    run e9-raw-filter ONLY_TRADES=1 MODE=max
    run e9-raw-drain ONLY_TRADES=0 PREFILL=1
done

for rep in $(seq 1 "$REPEATS"); do
    for rate in $RATES; do
        run "e9-raw-rate-$rate" ONLY_TRADES=0 MODE=rate RATE="$rate" MAX_TICKS=$((rate * DURATION))
    done
done

echo "=== $(date +%T) done. Correctness: these lines must match between raw-filter and raw-drain" | tee -a "$LOG"
for d in results/e9-raw-filter/*/ results/e9-raw-drain/*/; do
    printf '%s  ' "$d"
    grep -E "^  analyzer  ticks" "$d/summary.txt" | sed -E 's/ticks [0-9]+, non-trades [0-9]+, heartbeats [0-9]+, eos [0-9]+, //'
done | tee -a "$LOG"
