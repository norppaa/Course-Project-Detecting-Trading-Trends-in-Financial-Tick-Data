# Detecting Trading Trends in Financial Tick Data

> **CS-E4780 Scalable Systems and Data Management**, course project, Aalto University
> Eero Huhtala, Marika Laine, Noora Puhakka
> Dataset: [DEBS 2022 Grand Challenge (Zenodo)](https://zenodo.org/records/6382482)

A streaming system that replays one week of real financial tick data (8–12 November 2021, about 59 M trades for 4,970 symbols on three exchanges) and, for every symbol and 5-minute window, computes EMA38 and EMA100 (Query 1) and issues BUY/SELL advisories when they cross (Query 2). The results are shown live in a web dashboard.

**Where to find what:**

| | |
|---|---|
| Evaluation results (throughput, latency, scaling, correctness) | [`bench/RESULTS.md`](bench/RESULTS.md) |
| How to run the benchmarks | [`bench/README.md`](bench/README.md) |
| Independent ground truth and correctness check | [`scripts/README.md`](scripts/README.md) |
| Data producer | [`producer/README.md`](producer/README.md) |
| Stream analyzer | [`analyzer/README.md`](analyzer/README.md) |

---

## 1. Quick start

Requirements: Docker with Docker Compose, and Go 1.25+ for the data preparation.

1. **Download data.** Put the raw day files from [Zenodo](https://zenodo.org/records/6382482) in `data/` (ignored by git), e.g. `data/debs2022-gc-trading-day-08-11-21.csv`.
2. **Clean the data.** The containerized producer reads `data/trades_cleaned.csv.gz`. For one day:
   ```bash
   go run scripts/clean_data.go --only-trades \
     --input=data/debs2022-gc-trading-day-08-11-21.csv \
     --output=data/trades_cleaned.csv.gz
   ```
   To build the whole week used in the evaluation, see [`scripts/README.md`](scripts/README.md).
3. **Start everything:**
   ```bash
   docker compose --profile app up -d --build
   ```
   This starts Redpanda, the producer, the analyzer, the gateway and the frontend. The producer replays at full speed by default; to watch the dashboard update live, set `REPLAY_MODE=realtime` for the producer in `docker-compose.yml`.
4. **Open the dashboard** at [http://localhost:3000](http://localhost:3000). The gateway also serves it at [http://localhost:8081](http://localhost:8081), and the Redpanda Console is at [http://localhost:8080](http://localhost:8080).

---

## 2. System pipeline

```
┌─────────────────────┐
│ 1. Data Producer    │ ──► Reads the CSV and replays ticks to the broker
└──────────┬──────────┘
           │
           ▼ (Topic: "market-ticks", partition key: symbol)
┌─────────────────────┐
│ 2. Message Broker   │ ──► Redpanda (Kafka API, ordered per symbol)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 3. Stream Analyzer  │ ──► 5-min tumbling windows, EMA38/100, crossover detection
└──────────┬──────────┘
           │
           ▼ (Topic: "trading-signals")
┌─────────────────────┐
│ 4. Web Gateway      │ ──► WebSocket server for indicators and advisories
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 5. Web Frontend     │ ──► Live dashboard with charts and buy/sell alerts
└─────────────────────┘
```

Both topics have 12 partitions, so 1, 2, 3, 4 or 6 analyzer instances share the work evenly. Docker Compose creates them on startup; topics that already exist keep their partition count, so after changing it start from a clean broker with `docker compose down -v`.

To scale out the analyzer, start several instances in one consumer group (a fresh group id for each run), wait until all of them have joined, and only then start the producer:

```bash
export GROUP_ID=run-1
docker compose --profile app up -d --build --scale analyzer=3 analyzer gateway frontend
docker exec redpanda rpk group describe $GROUP_ID --brokers redpanda:9092   # wait for MEMBERS 3, STATE Stable
docker compose --profile app up -d producer
```

Do not start the producer together with the analyzers, and do not add or remove instances during a run. State is in memory only, so a partition that moves to another instance restarts its EMAs from zero, and the windows it already processed are emitted twice. With the two-step start above, 1 and 3 instances gave identical totals on 8 Nov 2021 (11,794,520 ticks, 373,126 windows, 4,840 BUY).

| Service | Address |
|---|---|
| Kafka API for programs on the host | `localhost:19092` |
| Kafka API inside Docker | `redpanda:9092` |
| Redpanda Console | [http://localhost:8080](http://localhost:8080) |
| Gateway (WebSocket and REST) | [http://localhost:8081](http://localhost:8081) |
| Frontend | [http://localhost:3000](http://localhost:3000) |

---

## 3. Message schemas

All message types are defined in [`pkg/models/tick.go`](pkg/models/tick.go).

### 3.1 Topic `market-ticks` (producer → analyzer)

Ticks are keyed by symbol (`ID`, e.g. `RDSA.NL`), so all ticks of a symbol go to the same partition in order:

```json
{
  "id": "RDSA.NL",
  "sec_type": "E",
  "timestamp": "2021-11-08T09:00:00.123Z",
  "price": 18.45,
  "trading_date": "08-11-2021",
  "trading_time": "09:00:00.123"
}
```

The producer also sends heartbeats and an end-of-stream message to every partition. The `kind` header (`tick`, `heartbeat` or `eos`) tells them apart, and the `produced_at` header carries the send time for latency measurement. See [`producer/README.md`](producer/README.md) and [`analyzer/README.md`](analyzer/README.md).

### 3.2 Topic `trading-signals` (analyzer → gateway)

The analyzer publishes two kinds of messages, told apart by the `kind` field.

**Window indicator** (Query 1), one per symbol per closed window. `signal` is present only if the window produced an advisory:

```json
{
  "kind": "indicator",
  "symbol": "RDSA.NL",
  "sec_type": "E",
  "window_start": "2021-11-08T09:00:00Z",
  "window_end": "2021-11-08T09:05:00Z",
  "last_price": 19.852,
  "has_tick": true,
  "ema_38": 19.824,
  "ema_100": 19.780,
  "signal": "BUY",
  "produced_at": "2026-11-13T20:00:01.123Z",
  "ingested_at": "2026-11-13T20:00:01.131Z",
  "emitted_at": "2026-11-13T20:00:01.132Z"
}
```

**Advisory** (Query 2), one per BUY or SELL crossover:

```json
{
  "kind": "signal",
  "symbol": "RDSA.NL",
  "sec_type": "E",
  "type": "BUY",
  "window_start": "2021-11-08T09:00:00Z",
  "timestamp": "2021-11-08T09:05:00Z",
  "price": 19.852,
  "ema_38": 19.824,
  "ema_100": 19.780,
  "prev_ema_38": 19.771,
  "prev_ema_100": 19.779,
  "produced_at": "2026-11-13T20:00:01.123Z",
  "ingested_at": "2026-11-13T20:00:01.131Z",
  "emitted_at": "2026-11-13T20:00:01.132Z"
}
```

`produced_at`, `ingested_at` and `emitted_at` are wall-clock times of the tick that closed the window, used for latency measurement.

### 3.3 Gateway API (gateway → frontend)

**WebSocket** at `ws://localhost:8081/ws`. Client commands:

| Command | Effect |
|---|---|
| `{"action": "subscribe", "symbols": ["RDSA.NL"]}` | Receive window updates for these symbols, starting with their history |
| `{"action": "unsubscribe", "symbols": ["RDSA.NL"]}` | Stop receiving them |
| `{"action": "subscribe_all"}` / `{"action": "unsubscribe_all"}` | Receive updates for every symbol / none |
| `{"action": "get_history", "symbol": "RDSA.NL"}` | Window history for the chart |
| `{"action": "get_symbols"}` | List of known symbols |
| `{"action": "get_markets"}` | Market overview, sent again as an `info` message |
| `{"action": "ping"}` | Answered with `pong` |

Server messages have a `type`:
- `info`: sent on connect, with symbols, statistics and recent alerts
- `signal`: a window update for a subscribed symbol
- `alert`: a BUY/SELL advisory, sent to every client
- `history`: window history for one symbol
- `symbols`: the symbol list
- `pong`: the answer to `ping`

**REST endpoints:**

| Endpoint | Returns |
|---|---|
| `GET /health` | Health check and number of connected clients |
| `GET /api/symbols` | All known symbols |
| `GET /api/markets?limit=50` | One summary per symbol, most active first |
| `GET /api/history?symbol=RDSA.NL&limit=200` | Window history (close price and EMAs) of one symbol |
| `GET /api/alerts?limit=50` | Recent BUY and SELL advisories |
| `GET /api/snapshot` | The latest event of every symbol |
| `GET /api/stats` | Store and throughput statistics |

---

## 4. Development

**Run services locally** against Redpanda in Docker. This starts quickly and is easy to debug:

```bash
docker compose up -d                                          # Redpanda and the Console only
go run ./analyzer
go run ./producer -input data/trades_cleaned.csv.gz -mode realtime -speed 10
go run ./gateway                                              # dashboard at http://localhost:8081
```

**Run everything in Docker,** as in the quick start:

```bash
docker compose --profile app up -d --build
```

The producer's replay modes (`max`, `rate`, `realtime`) and options are described in [`producer/README.md`](producer/README.md).

**Tests:**

```bash
go test -race ./...
```
