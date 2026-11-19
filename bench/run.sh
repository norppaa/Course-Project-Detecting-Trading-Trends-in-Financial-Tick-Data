#!/usr/bin/env bash
# Runs one benchmark and saves everything to results/<experiment>/<timestamp>/.
# Usage: [SETTING=value ...] bench/run.sh <experiment-name>
# Settings are listed below and explained in bench/README.md.
set -euo pipefail

cd "$(dirname "$0")/.."

NAME=${1:?usage: [SETTING=value ...] bench/run.sh <experiment-name>}

INPUT=${INPUT:-data/trades_cleaned.csv}
# PREFILL=1: the producer fills the topic first and the analyzer starts only
# after it has finished, so the analyzer's read rate is not limited by the
# producer (E2). Latency then mostly measures time spent waiting in the topic.
PREFILL=${PREFILL:-0}
if [ "$PREFILL" = 1 ]; then MODE=${MODE:-max}; fi
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
# ONLY_TRADES=0: the producer forwards every event (quotes, housekeeping) and
# the analyzer discards the non-trades itself. Use with a raw day file.
ONLY_TRADES=${ONLY_TRADES:-1}
# Default core layout for the development laptop (Intel Core Ultra 7 258V):
# CPUs 0-3 are P-cores, 4-7 are low-power E-cores. The system under test
# (analyzer, Redpanda) gets the P-cores, the load generator and sink the
# E-cores. Set a variable to an empty string to leave that component unpinned.
PRODUCER_CPUS=${PRODUCER_CPUS-4-6}
ANALYZER_CPUS=${ANALYZER_CPUS-0-2}
SINK_CPUS=${SINK_CPUS-7}
REDPANDA_CPUS=${REDPANDA_CPUS-3}

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
    echo "PREFILL=$PREFILL"
    echo "ONLY_TRADES=$ONLY_TRADES"
    echo "PRODUCER_CPUS=$PRODUCER_CPUS"
    echo "ANALYZER_CPUS=$ANALYZER_CPUS"
    echo "SINK_CPUS=$SINK_CPUS"
    echo "REDPANDA_CPUS=$REDPANDA_CPUS"
    echo "COMMIT=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- producer analyzer pkg bench || echo -dirty)"
    echo "CPU_MODEL=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
    echo "CPU_COUNT=$(nproc)"
    echo "CPU_MAX_MHZ=$(cat /sys/devices/system/cpu/cpu*/cpufreq/cpuinfo_max_freq 2>/dev/null | awk '{printf "%s%d", s, $1/1000; s=","}')"
    echo "MEMORY_GB=$(awk '/MemTotal/ {printf "%.0f", $2/1048576}' /proc/meminfo)"
    echo "POWER_PROFILE=$(cat /sys/firmware/acpi/platform_profile 2>/dev/null || echo unknown)"
    echo "ON_AC_POWER=$(cat /sys/class/power_supply/AC*/online 2>/dev/null | head -1 || echo unknown)"
    echo "OS=$(. /etc/os-release && echo "$PRETTY_NAME")"
    echo "KERNEL=$(uname -r)"
    echo "GO_VERSION=$(go env GOVERSION)"
    echo "KAFKA_GO_VERSION=$(go list -m -f '{{.Version}}' github.com/segmentio/kafka-go)"
    echo "DOCKER_VERSION=$(docker version --format '{{.Server.Version}}')"
    echo "REDPANDA_VERSION=$(docker exec redpanda rpk version 2>/dev/null | awk '/rpk version/ {print $3}')"
    echo "REDPANDA_IMAGE=$(docker inspect -f '{{.Config.Image}}' redpanda)"
    echo "REDPANDA_ARGS=$(docker inspect -f '{{join .Args " "}}' redpanda)"
} > "$OUT/config.env"

# Pin Redpanda (or give it every CPU again when REDPANDA_CPUS is empty).
docker update --cpuset-cpus "${REDPANDA_CPUS:-0-$(($(nproc) - 1))}" redpanda >/dev/null

echo "== Creating topics ($PARTITIONS partitions)"
rpk topic delete "$TICKS_TOPIC" "$RESULTS_TOPIC" >/dev/null 2>&1 || true
rpk topic create "$TICKS_TOPIC" "$RESULTS_TOPIC" -p "$PARTITIONS" -c retention.ms=-1 >/dev/null

FINISH_FILE=$OUT/.producer-finished
REDPANDA_PID=$(docker inspect -f '{{.State.Pid}}' redpanda)

start_producer() {
    echo "== Replaying $INPUT (mode $MODE)"
    ( pinned "$PRODUCER_CPUS" "$BIN/producer" -brokers "$BROKERS" -topic "$TICKS_TOPIC" -input "$INPUT" \
        -mode "$MODE" -rate "$RATE" -speed "$SPEED" -heartbeat "$HEARTBEAT" -max-ticks "$MAX_TICKS" \
        -only-trades="$( [ "$ONLY_TRADES" = 1 ] && echo true || echo false )" ) > "$OUT/producer.log" 2>&1 &
    PRODUCER=$!
    PIDS+=("$PRODUCER")
}

start_consumers() {
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
}

if [ "$PREFILL" = 1 ]; then
    start_producer
    wait "$PRODUCER" || { echo "producer failed, see $OUT/producer.log" >&2; exit 1; }
    touch "$FINISH_FILE"
    start_consumers
    # The producer is not running any more, so only the readers are sampled.
    "$BIN/bench" sample -out "$OUT/resources.csv" \
        "analyzer=$ANALYZER" "sink=$SINK" "redpanda=$REDPANDA_PID" &
    SAMPLER=$!
    PIDS+=("$SAMPLER")
else
    start_consumers
    sleep 2
    start_producer
    "$BIN/bench" sample -out "$OUT/resources.csv" \
        "producer=$PRODUCER" "analyzer=$ANALYZER" "sink=$SINK" "redpanda=$REDPANDA_PID" &
    SAMPLER=$!
    PIDS+=("$SAMPLER")
    wait "$PRODUCER" || { echo "producer failed, see $OUT/producer.log" >&2; exit 1; }
    touch "$FINISH_FILE"
fi

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
