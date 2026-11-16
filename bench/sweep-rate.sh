#!/usr/bin/env bash
# E3: latency and sustainability across input rates.
# Each run replays the start of the input at a fixed rate for about DURATION
# seconds (warmup included). Repetitions are interleaved, so slow drift of the
# machine is spread over all rates. Results go to results/e3-rate-<rate>/.
# Usage: [RATES="..."] [REPEATS=3] [DURATION=150] [SETTING=value ...] bench/sweep-rate.sh
set -uo pipefail

cd "$(dirname "$0")/.."

RATES=${RATES:-5000 50000 100000 200000 300000 400000 500000 600000 700000 800000 900000}
REPEATS=${REPEATS:-3}
DURATION=${DURATION:-150}
export INPUT=${INPUT:-data/trades_only_week.csv}

LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT
TOTAL=$(($(wc -l < "$INPUT") - 1))

for rep in $(seq 1 "$REPEATS"); do
    for rate in $RATES; do
        ticks=$((rate * DURATION))
        [ "$ticks" -gt "$TOTAL" ] && ticks=$TOTAL
        echo "=== $(date +%T) repeat $rep/$REPEATS, rate $rate, $ticks ticks"
        if MODE=rate RATE=$rate MAX_TICKS=$ticks bench/run.sh "e3-rate-$rate" > "$LOG" 2>&1; then
            grep -E "^  (end-to-end|producer  |analyzer  )|Result:|Run is" "$LOG"
        else
            echo "run failed:"
            tail -5 "$LOG"
        fi
    done
done
echo "=== $(date +%T) done"
