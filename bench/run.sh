#!/usr/bin/env bash
# Runs one benchmark and saves everything to results/<experiment>/<timestamp>/.
# Usage: [SETTING=value ...] bench/run.sh <experiment-name>
# Settings are listed below and explained in bench/README.md.
set -euo pipefail

cd "$(dirname "$0")/.."

NAME=${1:?usage: [SETTING=value ...] bench/run.sh <experiment-name>}

INPUT=${INPUT:-data/trades_cleaned.csv}
MODE=${MODE:-rate}
RATE=${RATE:-10000}
SPEED=${SPEED:-10}
PARTITIONS=${PARTITIONS:-4}
GRACE=${GRACE:-5s}
POLICY=${POLICY:-skip}
HEARTBEAT=${HEARTBEAT:-1s}
MAX_TICKS=${MAX_TICKS:-0}
WARMUP=${WARMUP:-30s}
P99_LIMIT=${P99_LIMIT:-1s}
COMPARE=${COMPARE:-0}
SINK_IDLE=${SINK_IDLE:-10s}
PRODUCER_CPUS=${PRODUCER_CPUS:-}
ANALYZER_CPUS=${ANALYZER_CPUS:-}
SINK_CPUS=${SINK_CPUS:-}

BROKERS=localhost:19092
TICKS_TOPIC=bench-ticks
RESULTS_TOPIC=bench-results

[ -f "$INPUT" ] || { echo "input file not found: $INPUT" >&2; exit 1; }
if [ "$COMPARE" = 1 ] && [ "$MAX_TICKS" != 0 ]; then
    echo "COMPARE=1 needs MAX_TICKS=0, because the offline tool reads the whole input" >&2
    exit 1
fi

OUT=results/$NAME/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
BIN=$(mktemp -d)

rpk() { docker exec redpanda rpk "$@" --brokers redpanda:9092; }

# Runs a command, pinned to CPUs if the first argument is not empty.
pinned() {
    local cpus=$1
    shift
    if [ -n "$cpus" ]; then
        exec taskset -c "$cpus" "$@"
    fi
    exec "$@"
}

PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rpk topic delete "$TICKS_TOPIC" "$RESULTS_TOPIC" >/dev/null 2>&1 || true
    rm -rf "$BIN"
}
trap cleanup EXIT

echo "== Building"
go build -o "$BIN/producer" ./producer
go build -o "$BIN/analyzer" ./analyzer
go build -o "$BIN/offline" ./analyzer/cmd/offline
go build -o "$BIN/bench" ./bench

{
    echo "INPUT=$INPUT"
    echo "MODE=$MODE"
    echo "RATE=$RATE"
    echo "SPEED=$SPEED"
    echo "PARTITIONS=$PARTITIONS"
    echo "GRACE=$GRACE"
    echo "POLICY=$POLICY"
    echo "HEARTBEAT=$HEARTBEAT"
    echo "MAX_TICKS=$MAX_TICKS"
    echo "PRODUCER_CPUS=$PRODUCER_CPUS"
    echo "ANALYZER_CPUS=$ANALYZER_CPUS"
    echo "SINK_CPUS=$SINK_CPUS"
    echo "COMMIT=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- producer analyzer pkg bench || echo -dirty)"
    echo "CPU_MODEL=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
    echo "CPU_COUNT=$(nproc)"
    echo "REDPANDA_ARGS=$(docker inspect -f '{{join .Args " "}}' redpanda)"
} > "$OUT/config.env"

echo "== Creating topics ($PARTITIONS partitions)"
rpk topic delete "$TICKS_TOPIC" "$RESULTS_TOPIC" >/dev/null 2>&1 || true
rpk topic create "$TICKS_TOPIC" "$RESULTS_TOPIC" -p "$PARTITIONS" -c retention.ms=-1 >/dev/null

FINISH_FILE=$OUT/.producer-finished

echo "== Starting sink and analyzer"
( pinned "$SINK_CPUS" "$BIN/bench" sink -brokers "$BROKERS" -topic "$RESULTS_TOPIC" -out "$OUT" \
    -finish-file "$FINISH_FILE" -idle "$SINK_IDLE" ) > "$OUT/sink.log" 2>&1 &
SINK=$!
PIDS+=("$SINK")

( pinned "$ANALYZER_CPUS" "$BIN/analyzer" -brokers "$BROKERS" -input-topic "$TICKS_TOPIC" \
    -signal-topic "$RESULTS_TOPIC" -indicator-topic "$RESULTS_TOPIC" -partitions all \
    -grace "$GRACE" -policy "$POLICY" -idle-flush 0 -metrics-interval 1s ) > "$OUT/analyzer.log" 2>&1 &
ANALYZER=$!
PIDS+=("$ANALYZER")
sleep 2

echo "== Replaying $INPUT (mode $MODE)"
( pinned "$PRODUCER_CPUS" "$BIN/producer" -brokers "$BROKERS" -topic "$TICKS_TOPIC" -input "$INPUT" \
    -mode "$MODE" -rate "$RATE" -speed "$SPEED" -heartbeat "$HEARTBEAT" -max-ticks "$MAX_TICKS" ) > "$OUT/producer.log" 2>&1 &
PRODUCER=$!
PIDS+=("$PRODUCER")

REDPANDA_PID=$(docker inspect -f '{{.State.Pid}}' redpanda)
"$BIN/bench" sample -out "$OUT/resources.csv" \
    "producer=$PRODUCER" "analyzer=$ANALYZER" "sink=$SINK" "redpanda=$REDPANDA_PID" &
SAMPLER=$!
PIDS+=("$SAMPLER")

wait "$PRODUCER" || { echo "producer failed, see $OUT/producer.log" >&2; exit 1; }
touch "$FINISH_FILE"

echo "== Waiting for the last results"
wait "$SINK" || { echo "sink failed, see $OUT/sink.log" >&2; exit 1; }
rm -f "$FINISH_FILE"

kill -TERM "$ANALYZER" "$SAMPLER"
wait "$ANALYZER" || true
wait "$SAMPLER" || true

COMPARE_ARGS=()
if [ "$COMPARE" = 1 ]; then
    echo "== Running the offline tool for comparison"
    "$BIN/offline" -input "$INPUT" -output "$OUT/offline.csv" -grace "$GRACE" -policy "$POLICY" 2> "$OUT/offline.log"
    COMPARE_ARGS=(-compare "$OUT/offline.csv")
fi

echo "== Analyzing"
echo
"$BIN/bench" analyze -dir "$OUT" -warmup "$WARMUP" -p99-limit "$P99_LIMIT" "${COMPARE_ARGS[@]}"
echo
echo "Results saved to $OUT"
