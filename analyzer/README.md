# Stream Analyzer

Computes the two course project queries over 5-minute tumbling windows:

- **Query 1:** EMA38 and EMA100 for every symbol, per window
- **Query 2:** BUY / SELL advisories when the two EMAs cross

## Folders

```
analyzer/
├── main.go          # online service: Kafka/Redpanda in and out, metrics
├── config.go        # flags + environment variables
├── pipeline.go      # one worker (and one engine) per input partition
├── pipeline_test.go
├── engine/          # the query logic, with tests (shared by online and offline)
└── cmd/offline/     # runs the engine on a CSV file, no broker
```

## Online service

```bash
docker compose up -d                       # Redpanda
go run ./analyzer                          # analyzer, joins group "stream-analyzer"
go run ./producer -input data/trades_08-11-21.csv
```

or everything in Docker: `docker compose --profile app up -d --build`.

The analyzer reads `market-ticks`, runs one engine per partition, and writes
JSON results to `trading-signals`. Every 5 s it logs input rate, consumer lag,
late ticks, windows, BUY/SELL counts, the lowest watermark and processing
latency; a summary is printed on shutdown (Ctrl-C).

### Scaling

- **Consumer group (default).** Start more analyzer instances with the same
  `GROUP_ID`; Kafka splits the partitions between them. The number of input
  partitions is the upper limit on parallelism.
- **Fixed partitions.** `-partitions all` (or `0,1`) reads partitions directly
  without a group. Deterministic and good for benchmarks. Reading always
  starts from the beginning of the topic (`-start first`), so a run can be
  repeated on a pre-filled topic to measure pure analyzer throughput.

State (open windows, EMAs) is kept in memory only. If a partition moves to
another instance (rebalance, restart), its EMA history starts again from zero.
For repeatable group-mode runs use a new `GROUP_ID` each time, otherwise the
group continues from its committed offsets.

### Options

Each flag can also be set by the environment variable in brackets.

| Flag | Default | Meaning |
|---|---|---|
| `-brokers` (`KAFKA_BROKERS`) | `localhost:19092` | Broker addresses |
| `-input-topic` (`INPUT_TOPIC`) | `market-ticks` | Tick topic |
| `-signal-topic` (`SIGNAL_TOPIC`) | `trading-signals` | Topic for BUY/SELL advisories |
| `-indicator-topic` (`INDICATOR_TOPIC`) | `trading-signals` | Topic for per-window EMAs, `none` = don't publish |
| `-group` (`GROUP_ID`) | `stream-analyzer` | Consumer group |
| `-partitions` (`PARTITIONS`) | | `all` or `0,1,…`: read without a group |
| `-start` (`START_OFFSET`) | `first` | `first` or `last`, when there is no committed offset |
| `-grace` (`GRACE`) | `5s` | Allowed out-of-orderness of event time |
| `-policy` (`EMPTY_WINDOW_POLICY`) | `skip` | `skip` or `carry` (see below) |
| `-suppress-first` (`SUPPRESS_FIRST_SIGNAL`) | `false` | Hide the BUY every symbol gets in its first window |
| `-idle-flush` (`IDLE_FLUSH`) | `5s` | Close all windows after this long with no input, `0` = never |
| `-metrics-interval` (`METRICS_INTERVAL`) | `5s` | Stats log interval, `0` = off |

### Message contract

Defined in [`pkg/models/tick.go`](../pkg/models/tick.go).

**Input** (`market-ticks`, key = symbol). Header `kind` tells what the
message is; no header means a tick.

| `kind` | Value | Effect |
|---|---|---|
| missing / `tick` | `MarketTick` JSON | Processed by the engine |
| `heartbeat` | `{"event_time": "…"}` | Watermark := event_time − grace. Closes windows on quiet partitions |
| `eos` | empty | End of replay: closes all open windows |

`heartbeat` and `eos` must be written to **every partition** by the producer.
They are optional: without them, windows close when a later tick arrives on
the same partition, and the last windows close after `-idle-flush`.
An optional `produced_at` header (RFC 3339, wall clock) is copied to the output
so end-to-end latency can be measured.

**Output** (key = symbol, header `kind` = `indicator` or `signal`; the JSON
also has a `kind` field):

```json
{"kind":"indicator","symbol":"IEBBB.FR","sec_type":"I",
 "window_start":"2021-11-08T07:00:00Z","window_end":"2021-11-08T07:05:00Z",
 "last_price":52.69,"has_tick":true,"ema_38":2.70,"ema_100":1.04,"signal":"BUY",
 "produced_at":"…","ingested_at":"…","emitted_at":"…"}

{"kind":"signal","symbol":"IEBBB.FR","sec_type":"I","type":"BUY",
 "window_start":"2021-11-08T07:00:00Z","timestamp":"2021-11-08T07:05:00Z",
 "price":52.69,"ema_38":2.70,"ema_100":1.04,"prev_ema_38":0,"prev_ema_100":0,
 "produced_at":"…","ingested_at":"…","emitted_at":"…"}
```

Timestamps are event time with no time-zone conversion (the dataset's local
wall clock written as UTC), the same as the producer's `timestamp`. The
`*_at` fields are wall clock and refer to the input message that closed the
window: `produced_at` (producer, only if it sets the header), `ingested_at`
(analyzer read it), `emitted_at` (analyzer handed the result to the broker).

The analyzer publishes results for **all** symbols; filtering to subscribed
symbols happens in the gateway.

## Offline tool

```bash
# Clean a raw day file (only trade events)
go run ./scripts/clean_data.go \
    -input data/debs2022-gc-trading-day-08-11-21.csv \
    -output data/trades_08-11-21.csv -only-trades

# Run the analyzer on it
go run ./analyzer/cmd/offline \
    -input data/trades_08-11-21.csv \
    -output data/results_08-11-21.csv
```

A summary of the run is printed to the terminal, including how out of order
the event times are (useful for choosing `-grace`).

Tip: run the whole week as one file. A single day is too short for the EMAs
to warm up, so it gives no real crossovers.

| Flag | Default | Meaning |
|---|---|---|
| `-grace` | `5s` | How long to wait for out-of-order events before closing a window |
| `-policy` | `skip` | `skip`: ignore windows with no trades. `carry`: reuse the last price |
| `-suppress-first` | off | Hide the BUY every symbol gets in its first window |
| `-signals-only` | off | Only write windows with a BUY or SELL |
| `-output none` | | Discard results (for speed tests) |

Output:

```
symbol,sec_type,window_start,window_end,close,has_tick,ema38,ema100,signal
```

One row per symbol per window, sorted by window and then symbol. `signal` is
`BUY`, `SELL` or `NONE`.

## How it works

- Event time is `TradingDate` + `TradingTime`. If the trading date is empty, the system `Date` is used.
- Windows are 5 minutes long and start at midnight.
- A window's close price is the last price by event time.
- A window closes when the watermark passes its end. The watermark is the
  latest event time seen on the partition (or the latest heartbeat) minus the
  grace period. Events for an already closed window are dropped and counted.
- EMA: `EMA = close · 2/(1+j) + previous EMA · (1 − 2/(1+j))`, with j = 38 or 100, starting from 0.
- BUY: EMA38 goes from ≤ EMA100 to > EMA100. SELL: EMA38 goes from ≥ EMA100 to < EMA100.
- Empty windows: `skip` evaluates a symbol only in windows where it traded
  ("previous window" = its previous traded window). `carry` evaluates every
  window after the first trade, reusing the last close.

## Verified

On the first 2 M trades of 08-11-2021, the online pipeline (producer →
broker → analyzer, 4 partitions, both with one and with two analyzer
instances) gives a result set byte-identical to the offline tool
(70 683 windows, 3 971 BUY).
