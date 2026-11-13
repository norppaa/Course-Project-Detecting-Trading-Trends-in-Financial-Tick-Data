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

**Replay at 10× historical speed:**

```bash
INPUT=data/trades_only_08-11-21.csv MODE=realtime SPEED=10 bench/run.sh realtime-10x
```

**Finding the maximum sustainable throughput.** Raise the rate until the summary says `NOT sustainable`. The highest rate that passes is your result.

```bash
for rate in 25000 50000 100000 200000; do
    INPUT=data/trades_only_08-11-21.csv RATE=$rate bench/run.sh rate-sweep
done
```

**Scaling with CPU cores.** Keep the other components on separate cores, and use at least as many partitions as analyzer cores.

```bash
RATE=100000 PARTITIONS=8 ANALYZER_CPUS=0-1 PRODUCER_CPUS=2-3 SINK_CPUS=4 bench/run.sh scale-2-cores
RATE=100000 PARTITIONS=8 ANALYZER_CPUS=0-3 PRODUCER_CPUS=4-5 SINK_CPUS=6 bench/run.sh scale-4-cores
```

Redpanda is not pinned by these settings, so it can still use any core allowed to its container.

**A single exchange.** Filter the input by symbol suffix (`.FR`, `.NL` or `.ETR`):

```bash
{ head -n 1 data/trades_only_08-11-21.csv; grep -E '^[^,]*\.FR,' data/trades_only_08-11-21.csv; } > data/trades_FR.csv
INPUT=data/trades_FR.csv RATE=50000 bench/run.sh exchange-FR
```

**Repeating a run.** Run each configuration about 3 times and report the median and range:

```bash
for i in 1 2 3; do RATE=50000 bench/run.sh latency-50k; done
```

## Settings

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
| `WARMUP` | `30s` | Ignore results from this first part of the run |
| `P99_LIMIT` | `1s` | End-to-end p99 limit for the "sustainable" verdict |
| `COMPARE` | `0` | `1` = also run the offline tool and compare results; needs `MAX_TICKS=0` |
| `SINK_IDLE` | `10s` | After the producer ends, stop once there are no results for this long |
| `PRODUCER_CPUS`, `ANALYZER_CPUS`, `SINK_CPUS` | | CPU cores to pin each process to, e.g. `0-3` |

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
| `config.env` | Settings, commit, CPU model and Redpanda arguments |
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
