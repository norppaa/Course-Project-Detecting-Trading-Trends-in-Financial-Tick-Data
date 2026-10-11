# Stream Analyzer

Computes the two course project queries over 5-minute tumbling windows:

- **Query 1:** EMA38 and EMA100 for every symbol, per window
- **Query 2:** BUY / SELL advisories when the two EMAs cross

## Folders

```
analyzer/
├── main.go          # online service (Redpanda), not implemented yet
├── engine/          # the query logic, with tests
└── cmd/offline/     # runs the engine on a CSV file
```

## How to run

From the repo root:

```bash
# Test
go test ./analyzer/engine/

# Clean a raw day file (only trade events)
go run ./scripts/clean_data.go \
    -input data/debs2022-gc-trading-day-08-11-21.csv \
    -output data/trades_08-11-21.csv -only-trades

# Run the analyzer on it
go run ./analyzer/cmd/offline \
    -input data/trades_08-11-21.csv \
    -output data/results_08-11-21.csv
```

A summary of the run is printed to the terminal.

Tip: run the whole week as one file. A single day is too short for the EMAs to warm up, so it gives no real crossovers.

## Options

| Flag | Default | Meaning |
|---|---|---|
| `-grace` | `5s` | How long to wait for out-of-order events before closing a window |
| `-policy` | `skip` | `skip`: ignore windows with no trades. `carry`: reuse the last price |
| `-suppress-first` | off | Hide the BUY every symbol gets in its first window |
| `-signals-only` | off | Only write windows with a BUY or SELL |
| `-output none` | | Discard results (for speed tests) |

## Output

```
symbol,sec_type,window_start,window_end,close,has_tick,ema38,ema100,signal
```

One row per symbol per window, sorted by window and then symbol. `signal` is `BUY`, `SELL` or `NONE`.

## How it works

- Event time is `TradingDate` + `TradingTime`. If the trading date is empty, the system `Date` is used.
- Windows are 5 minutes long and start at midnight.
- A window's close price is the last price by event time.
- A window closes when the latest event time seen, minus the grace period, passes its end. Events for an already closed window are dropped and counted.
- EMA: `EMA = close · 2/(1+j) + previous EMA · (1 − 2/(1+j))`, with j = 38 or 100, starting from 0.
- BUY: EMA38 goes from ≤ EMA100 to > EMA100. SELL: EMA38 goes from ≥ EMA100 to < EMA100.