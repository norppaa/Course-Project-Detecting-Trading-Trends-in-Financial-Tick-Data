# Data Producer Service

The **Data Producer** is responsible for reading raw or preprocessed tick data (`.csv` or `.csv.gz`) and streaming events to the Redpanda message broker (`market-ticks` topic).

---

## 1. Key Functionality

- **Transparent Gzip Streaming:** Reads directly from `.gz` compressed files via streaming decompression without buffering full files into memory.
- **Symbol-Preserving Partitioning:** Publishes ticks using the financial symbol (e.g., `RDSA.NL`) as the Kafka message key, ensuring strict FIFO event ordering per symbol across broker partitions.
- **Multiple Replay Modes:**
  - `max`: Max-speed unthrottled streaming for throughput benchmarking.
  - `rate`: Fixed message rate (e.g., `10,000` msg/s) for steady-state workload testing.
  - `realtime`: Historical event-time delta replay scaled by a speedup factor (e.g., `10x`, `50x`) for live dashboard visualization.
- **High-Throughput Batching:** Employs async batching (up to 5,000 messages) with Snappy compression using `kafka-go`.
- **Heartbeats & End-of-Stream:** Sends control messages to every partition so windows close on quiet partitions, and all remaining windows close when a replay finishes. Heartbeats follow event time, and only trades (`Last > 0`) advance the heartbeat clock: with `-only-trades=false`, non-trade events such as Xetra pre-open quotes can carry misleading timestamps that would otherwise push heartbeats ahead of later trades. No end-of-stream is sent if the replay is interrupted, and neither message is sent after a tick has failed. The topic must exist: at startup the producer waits up to about 30 s for it and then exits.
- **Latency Stamping:** Every message carries a wall-clock produced-at header, which the analyzer copies into its output for end-to-end latency measurement.
- **Graceful Shutdown & Observability:** Live throughput stats every 2 seconds, clean buffer flushing on `SIGINT` / `SIGTERM`.

---

## 2. File Overview

| File | Purpose |
|---|---|
| [`main.go`](main.go) | Service entry point; orchestrates ingestion, replay pacing, Kafka publishing, signal handling, and metrics. |
| [`config.go`](config.go) | CLI flag parsing and environment variable bindings with automatic `.csv` / `.gz` fallback resolution. |
| [`reader.go`](reader.go) | High-performance CSV & gzip reader with 1MB buffered I/O, format auto-detection, and fast timestamp parsing. |
| [`replayer.go`](replayer.go) | Pacing controllers for `max`, `rate`, and `realtime` streaming modes. |
| [`writer.go`](writer.go) | Batched async Kafka publisher with Snappy compression; ticks are keyed by symbol (`MarketTick.ID`) and stamped with a produced-at header. |
| [`control.go`](control.go) | Heartbeat and end-of-stream control messages, the balancer that routes them to every partition (ticks by symbol hash), the event-time heartbeat clock, and the startup wait for the topic's partition count. |
| [`config_test.go`](config_test.go) | Unit tests for flag vs. environment variable precedence and rejection of invalid values. |
| [`writer_test.go`](writer_test.go) | Unit tests for produced-at stamping, control message routing and headers, the heartbeat clock, and partition lookup retries. |
| [`reader_test.go`](reader_test.go) | Unit and benchmark tests for CSV parsing and gzip decompression. |
| [`replayer_test.go`](replayer_test.go) | Unit tests verifying pacing accuracy across all three replay modes. |
| [`Dockerfile`](Dockerfile) | Multi-stage lightweight Alpine container image build. |

---

## 3. Configuration Flags & Environment Variables

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `-input` | `INPUT_FILE` | `data/trades_cleaned.csv.gz` | Path to CSV or GZ tick data |
| `-brokers` | `KAFKA_BROKERS` | `localhost:19092` | Comma-separated Kafka broker addresses |
| `-topic` | `KAFKA_TOPIC` | `market-ticks` | Destination Kafka topic |
| `-mode` | `REPLAY_MODE` | `max` | Replay mode: `max`, `rate`, or `realtime` |
| `-rate` | `RATE_LIMIT` | `10000` | Target events/second (when `mode=rate`) |
| `-speed` | `SPEED_FACTOR` | `10.0` | Speedup factor (when `mode=realtime`) |
| `-batch-size`| `BATCH_SIZE` | `5000` | Kafka async message batch size |
| `-only-trades` | `ONLY_TRADES` | `true` | Filter events to keep only valid trade prices (`Last > 0`) |
| `-max-ticks` | `MAX_TICKS` | `0` | Stop after streaming $N$ ticks (`0` = no limit) |
| `-heartbeat` | `HEARTBEAT_INTERVAL` | `1s` | Event time between heartbeats (`0` = no heartbeats) |

---

## 4. Usage Commands

### Local Development (Host Mode)
Make sure Redpanda is running (`docker compose up -d`):

```bash
# 1. Run at maximum benchmark throughput (default)
go run ./producer -input data/trades_cleaned.csv.gz -brokers localhost:19092

# 2. Run at fixed rate (e.g. 5,000 ticks/sec)
go run ./producer -input data/trades_cleaned.csv.gz -mode rate -rate 5000

# 3. Run simulated real-time replay (e.g. 20x historical speed)
go run ./producer -input data/trades_cleaned.csv.gz -mode realtime -speed 20.0

# 4. Stream only a test slice (e.g. first 50,000 ticks)
go run ./producer -input data/trades_cleaned.csv.gz -max-ticks 50000
```

### Docker Compose Mode
Run containerized along with the broker:

```bash
# Run standalone producer service
docker compose run --rm producer

# Override settings inside container (e.g. rate mode)
docker compose run --rm producer ./producer -mode rate -rate 2000
```

---

## 5. Running Tests & Benchmarks

```bash
# Run unit and race tests
go test -v -race ./producer

# Run decompression and parsing benchmark
go test -bench=. ./producer
```
