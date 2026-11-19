# Benchmarking guide

`bench/run.sh` runs one complete benchmark with a single command. It:

1. Builds the producer, analyzer and benchmark tools.
2. Recreates its own topics with the chosen number of partitions.
3. Replays the input through producer → Redpanda → analyzer → result sink.
4. Records CPU and memory of each component once per second.
5. Prints a summary and saves everything to `results/<experiment>/<timestamp>/`.

It uses the topics `bench-ticks` and `bench-results` and deletes them afterwards, so `market-ticks` and `trading-signals` are not touched.

## Before you start

You need Docker, Go 1.25+ and a cleaned input file.

```bash
# Start Redpanda
docker compose up -d

# Create a cleaned input file, if you don't have one yet
go run ./scripts/clean_data.go \
    -input data/debs2022-gc-trading-day-08-11-21.csv \
    -output data/trades_only_08-11-21.csv -only-trades
```

Run every command from the repository root. Use an uncompressed `.csv` as input, because reading gzip can limit the producer.

## Quick start

Check that everything works, which takes under a minute:

```bash
MAX_TICKS=300000 RATE=40000 bench/run.sh smoke
```

The end of the output should say `Run is VALID`.

Settings go before the command, and the argument is the experiment name, which becomes the folder name under `results/`.

## Common benchmarks

**Latency at a fixed rate, also checking the results against the offline tool:**

```bash
INPUT=data/trades_only_08-11-21.csv RATE=50000 COMPARE=1 bench/run.sh latency-50k
```

**Maximum throughput:**

```bash
INPUT=data/trades_only_08-11-21.csv MODE=max bench/run.sh max-throughput
```

Use `MODE=max` for throughput only. Its latency numbers mostly measure queueing.

**The analyzer's own maximum throughput (pre-filled topic):** the producer fills the topic first, and the analyzer starts only after it has finished. The analyzer rate in the summary is then not limited by the producer:

```bash
INPUT=data/trades_only_week.csv PREFILL=1 bench/run.sh e2-drain
```

Latency in these runs mostly measures how long ticks waited in the topic, so use only the throughput.

**Replay at 10× historical speed:**

```bash
INPUT=data/trades_only_08-11-21.csv MODE=realtime SPEED=10 bench/run.sh realtime-10x
```

**Finding the maximum sustainable throughput.** Raise the rate until the summary says `NOT sustainable`. The highest rate that passes is your result. `bench/sweep-rate.sh` runs the whole sweep (5k to 900k ticks/s, 3 repeats each) and writes one summary line per run:

```bash
bench/sweep-rate.sh | tee results/e3-sweep.log
```

If the producer reaches less than 98% of the target, the run is marked not sustainable even when the analyzer kept up. Check the lag and the analyzer CPU before blaming the analyzer.

```bash
for rate in 25000 50000 100000 200000; do
    INPUT=data/trades_only_08-11-21.csv RATE=$rate bench/run.sh rate-sweep
done
```

**Scaling with CPU cores.** Use at least as many partitions as analyzer cores. The default layout uses all four P-cores (three for the analyzer, one for Redpanda), so compare 1, 2 and 3 analyzer P-cores and keep the rest unchanged:

```bash
RATE=100000 PARTITIONS=8 ANALYZER_CPUS=0 bench/run.sh scale-1-core
RATE=100000 PARTITIONS=8 ANALYZER_CPUS=0-1 bench/run.sh scale-2-cores
RATE=100000 PARTITIONS=8 ANALYZER_CPUS=0-2 bench/run.sh scale-3-cores
```

Going beyond that means using E-cores, which are slower, so say so in the report or use a bigger machine.

**A single exchange.** Filter the input by symbol suffix (`.FR`, `.NL` or `.ETR`):

```bash
{ head -n 1 data/trades_only_08-11-21.csv; grep -E '^[^,]*\.FR,' data/trades_only_08-11-21.csv; } > data/trades_FR.csv
INPUT=data/trades_FR.csv RATE=50000 bench/run.sh exchange-FR
```

**Raw events (E9).** The other benchmarks replay trades only, so the work of receiving and discarding quotes and housekeeping events (about 4.6 events per trade) is not measured. `bench/raw-events.sh` measures it on one raw day file: the producer filtering at the edge, the analyzer draining all events, and all events through the pipeline at fixed rates. About 1 h; a summary goes to `results/e9-raw.log`:

```bash
bench/raw-events.sh                 # REPEATS=1 for a quick run
go run ./bench rawstats data/debs2022-gc-trading-day-*.csv   # events, trades and busiest second per day
```

**Repeating a run.** Run each configuration about 3 times and report the median and range:

```bash
for i in 1 2 3; do RATE=50000 bench/run.sh latency-50k; done
```

## Settings

The default core layout fits the development laptop (Intel Core Ultra 7 258V): CPUs 0-3 are P-cores and 4-7 are low-power E-cores. The system under test gets the P-cores. Set a CPU setting to an empty string, e.g. `ANALYZER_CPUS=`, to leave that component unpinned. On another machine, check the layout with `lscpu -e` and set all four.

| Setting | Default | Meaning |
|---|---|---|
| `INPUT` | `data/trades_cleaned.csv` | Input file |
| `MODE` | `rate` | `rate`, `max` or `realtime` |
| `RATE` | `10000` | Ticks per second in `rate` mode |
| `SPEED` | `10` | Speedup in `realtime` mode |
| `PARTITIONS` | `4` | Partitions per topic, which is also the analyzer's maximum parallelism |
| `GRACE` | `5s` | Analyzer grace period |
| `POLICY` | `skip` | Empty-window policy: `skip` or `carry` |
| `HEARTBEAT` | `1s` | Event time between heartbeats, `0` = none |
| `MAX_TICKS` | `0` | Stop after this many ticks, `0` = whole file |
| `PREFILL` | `0` | `1` = fill the topic first, then start the analyzer; `MODE` defaults to `max` |
| `WARMUP` | `30s` | Ignore results from this first part of the run |
| `P99_LIMIT` | `1s` | End-to-end p99 limit for the "sustainable" verdict |
| `COMPARE` | `0` | `1` = also run the offline tool and compare results; needs `MAX_TICKS=0` |
| `SINK_IDLE` | `10s` | After the producer ends, stop once there are no results for this long |
| `ONLY_TRADES` | `1` | `0` = the producer forwards every event of a raw file (quotes, housekeeping) and the analyzer discards the non-trades, counted as `non-trades` in the summary |
| `ANALYZER_CPUS` | `0-2` | CPU cores for the analyzer (P-cores) |
| `REDPANDA_CPUS` | `3` | CPU cores for Redpanda (P-core), set with `docker update` |
| `PRODUCER_CPUS` | `4-6` | CPU cores for the producer (E-cores) |
| `SINK_CPUS` | `7` | CPU cores for the result sink (E-core) |

## Reading the summary

The summary is printed at the end of a run and saved as `summary.txt`.

**Throughput.** The rate achieved by the producer and analyzer, and the highest consumer lag. Lag is how many messages the analyzer was behind.

**Latency.** In milliseconds, measured from the message that closed each window:

| Stage | What it covers |
|---|---|
| `broker-in` | Producer to analyzer |
| `processing` | Inside the analyzer |
| `broker-out` | Analyzer to result sink |
| `end-to-end` | The whole path |

If a run is too short for `WARMUP`, no warmup is applied and the summary says so.

**Resources.** Average and highest CPU, and highest memory, per component. 100% CPU means one fully used core, so 200% is two cores. The GPU isn't used.

**Checks.** `Run is VALID` means every component finished and no ticks or results were lost. Don't use the numbers from a run marked `INVALID`.

**Sustainable at this rate?** Shown in `rate` mode. A rate is sustainable if all of these hold:
- The producer reached at least 98% of the target rate.
- Consumer lag stayed bounded instead of growing during the run.
- End-to-end p99 stayed within `P99_LIMIT`.

## Output files

| File | Content |
|---|---|
| `summary.txt` | The summary described above |
| `latency_series.csv` | End-to-end latency per second, for plotting stability |
| `throughput_series.csv` | Ticks, ticks per second and lag per second |
| `resources.csv` | CPU and memory per component, per second |
| `results.csv` | Every window result with its timestamps |
| `config.env` | Settings, commit, core layout, hardware, power profile, and Go, kafka-go, Docker, Redpanda, kernel and OS versions |
| `offline.csv` | Offline tool results, with `COMPARE=1` |
| `*.log` | Output of each component |

To analyze a finished run again, for example with a different p99 limit:

```bash
go run ./bench analyze -dir results/<experiment>/<timestamp> -p99-limit 500ms
```

## For results you put in the report

- Benchmark on a machine that is otherwise idle.
- Check that the producer isn't the limit: if it is using close to all its cores, a higher `RATE` measures the producer and not the analyzer.
- Use the same settings when comparing runs, changing only the one you are testing.
- Keep the Redpanda configuration in mind: `docker-compose.yml` runs it with one core and 1 GB in `dev-container` mode, which may be the bottleneck.

## Troubleshooting

| Problem | What to do |
|---|---|
| `input file not found` | Set `INPUT` to an existing file |
| `docker exec` errors, or the producer can't connect | Start Redpanda with `docker compose up -d` |
| `COMPARE=1 needs MAX_TICKS=0` | The offline tool reads the whole file. Make a smaller input with `head -n` instead of using `MAX_TICKS` |
| `producer failed` or `sink failed` | Look at `producer.log` or `sink.log` in the run folder |
| `Run is INVALID` | The failed check in the summary shows what went wrong; the component logs are in the run folder |
| Disk fills up | Delete old folders under `results/`; each run's `results.csv` grows with the number of windows |
