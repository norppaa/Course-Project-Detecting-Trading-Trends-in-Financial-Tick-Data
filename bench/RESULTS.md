# Benchmark results

Measured on 13 and 14 November 2026 with `bench/run.sh`. Every number here comes from the run folders under `results/`; the per-run summaries are in `results/<experiment>/<timestamp>/summary.txt`, `results/e3-sweep.log` and `results/e4e5-sweep.log` have one summary per run.

## Summary

| Question | Answer |
|---|---|
| Highest tested sustainable input rate (E3) | **700,000 ticks/s**, all 3 repeats, end-to-end p99 31 ms |
| What limits the live pipeline | The **producer** (load generator), at about 695,000 ticks/s. The analyzer kept up at every rate |
| Analyzer's own maximum throughput (E2, pre-filled topic) | **1.38 million ticks/s** (3 runs: 1,375,127 to 1,379,371), using its 3 cores almost fully |
| End-to-end latency at sustainable rates | p50 17–20 ms, p99 29–31 ms from 50k to 700k ticks/s |
| Where the latency comes from | The two broker hops (about 8 + 10 ms at p50). Processing in the analyzer is 0.1–1.3 ms at p50 |
| Realistic load (data's busiest second ≈ 4,600 trades/s) | About 150× below the tested sustainable rate; latency at 5k ticks/s is p50 18 ms, p99 46 ms |
| Scaling with analyzer cores (E4) | 505k, 992k and 1.37M ticks/s on 1, 2 and 3 cores: 1.96× and 2.72× |
| Each exchange alone (E5) | 1.28M to 1.38M ticks/s on a pre-filled topic; sustainable at 50k ticks/s with p99 19–24 ms |
| Long-tail data (E6) | 10% of symbols make 55% of trades, but the busiest partition holds only 1.07× the average and latency is the same for busy and quiet symbols |
| Correctness under load | All 63 runs VALID: no lost or failed messages, every window and signal reached the sink, identical counts in every repeat |

## Setup

| | |
|---|---|
| Machine | Laptop, Intel Core Ultra 7 258V (4 P-cores up to 4.7–4.8 GHz, 4 LP E-cores up to 3.7 GHz), 30 GB RAM, on AC power, power profile `performance` |
| Software | Ubuntu 26.04.1 LTS, kernel 7.0.0-42, Go 1.26.8, kafka-go v0.4.51, Docker 29.6.2, Redpanda v26.2.4 (`--smp 1 --memory 1G --mode dev-container`) |
| Core layout | Analyzer P-cores 0-2, Redpanda P-core 3, producer E-cores 4-6, result sink E-core 7 |
| Pipeline settings | 4 partitions, grace 5 s, empty-window policy `skip`, first-window BUY kept, heartbeat every 1 s of event time |
| Input | `data/trades_only_week.csv`: trades of 8–12 Nov 2021, 59,391,670 ticks, replayed from the start |
| Code | Commit fa70422 plus uncommitted changes to the producer (chunked sending in paced modes) and the benchmark scripts; the analyzer is unchanged |
| Repeats | 3 per configuration, interleaved (all rates once, then again), so slow drift of the machine is spread over all rates |

Each rate run replays the start of the week for about 150 s: the first 30 s are warmup and are left out of the latency numbers. At 400k ticks/s and above the whole week fits in that time, so those runs are shorter. Runs that reach Wednesday (300k ticks/s and above) include the batch of 5,506 trades that arrive about 18 hours late in the data; the analyzer drops them as late at any grace period, as expected.

## E3: latency and sustainability across input rates

Median of 3 repeats, with the range in brackets. Latency is end-to-end, from the producer sending the tick that closed a window to the sink receiving that window's result.

| Target rate (ticks/s) | Producer reached | p50 ms | p95 ms | p99 ms | Max ms | Max lag (msgs) | Analyzer CPU | Producer CPU | Sustainable |
|---|---|---|---|---|---|---|---|---|---|
| 5,000 | 5,000 | 17.9 (17.2–18.0) | 34.7 | 46.5 (42.1–47.5) | 47.9 | 16 | 3% | 18% | 3/3 |
| 50,000 | 49,999 | 19.1 (18.9–19.7) | 25.5 | 29.1 (26.7–30.2) | 34.1 | 329 | 13% | 34% | 3/3 |
| 100,000 | 99,999 | 17.4 (16.4–17.7) | 25.5 | 29.6 (27.7–30.6) | 36.9 | 864 | 24% | 49% | 3/3 |
| 200,000 | 199,998 | 17.9 (17.9–18.0) | 25.6 | 29.3 (28.8–30.8) | 35.3 | 872 | 45% | 74% | 3/3 |
| 300,000 | 299,996 | 18.2 (18.0–18.3) | 25.5 | 29.1 (28.6–30.9) | 39.2 | 1,418 | 66% | 94% | 3/3 |
| 400,000 | 400,003 | 19.1 (19.1–19.1) | 26.3 | 30.1 (29.9–30.2) | 36.9 | 1,477 | 87% | 113% | 3/3 |
| 500,000 | 499,996 | 19.2 (18.9–19.3) | 26.3 | 29.6 (28.9–30.4) | 43.4 | 2,055 | 107% | 134% | 3/3 |
| 600,000 | 599,991 | 19.5 (19.3–19.7) | 26.9 | 30.3 (30.1–30.5) | 39.5 | 1,901 | 124% | 153% | 3/3 |
| **700,000** | **698,690** | 19.7 (19.7–20.1) | 27.6 | 30.7 (30.4–30.8) | 38.4 | 2,910 | 143% | 172% | **3/3** |
| 800,000 | 695,112 | 19.9 (19.9–20.1) | 27.7 | 30.9 (30.9–31.2) | 41.8 | 3,550 | 142% | 172% | 0/3 |
| 900,000 | 695,817 | 19.9 (19.8–19.9) | 27.7 | 30.4 (30.2–30.9) | 36.3 | 3,505 | 143% | 172% | 0/3 |

CPU is the average over the run, where 100% is one fully used core. The analyzer had 3 cores (300%), the producer 3 (300%).

A rate counts as sustainable when the producer reaches at least 98% of the target, the consumer lag stays bounded, and end-to-end p99 stays under 1 s. At 800k and 900k only the first condition fails: the producer stops at about 695k ticks/s, the same as at 700k. The analyzer's lag stayed under 4,000 messages and its latency did not change, so these runs say nothing about the analyzer's limit. That limit is measured in E2.

### Latency by stage

Median p50 of 3 repeats, in ms.

| Target rate | Broker in (producer → analyzer) | Processing (in the analyzer) | Processing p99 | Broker out (analyzer → sink) |
|---|---|---|---|---|
| 5,000 | 10.0 | 0.11 | 0.51 | 8.4 |
| 50,000 | 9.4 | 0.37 | 1.52 | 11.2 |
| 100,000 | 4.6 | 0.43 | 1.58 | 11.1 |
| 200,000 | 7.8 | 0.55 | 2.06 | 10.6 |
| 300,000 | 7.5 | 0.68 | 2.68 | 10.3 |
| 400,000 | 7.6 | 0.83 | 2.91 | 10.2 |
| 500,000 | 7.9 | 0.98 | 3.33 | 9.8 |
| 600,000 | 8.1 | 1.19 | 3.77 | 9.7 |
| 700,000 | 8.1 | 1.29 | 3.83 | 9.7 |

Most of the latency is time spent getting through Redpanda and the Kafka clients' batching, not computation. Processing time grows with the rate, from 0.1 ms to 1.3 ms at p50, but stays a small share of the total.

The p99 at 5k ticks/s (about 46 ms) is higher than at the higher rates (about 30 ms). With few messages per second, batches fill slowly; this is a likely explanation but it has not been measured separately.

## E2: analyzer throughput on a pre-filled topic

The producer first wrote the whole week to the topic (at about 700k ticks/s). Only then were the analyzer and the sink started, and the analyzer read everything from the beginning. Its rate is measured from the first to the last message it read.

| Run | Analyzer rate (ticks/s) | Analyzer CPU avg / max | Analyzer max memory |
|---|---|---|---|
| 1 | 1,379,371 | 240% / 300% | 44 MB |
| 2 | 1,375,127 | 240% / 300% | 46 MB |
| 3 | 1,375,671 | 241% / 300% | 46 MB |
| **Median** | **1,375,671** | | |

All 3 runs produced the same 1,855,645 windows and 7,769 BUY / 3,580 SELL signals as the live runs over the whole week. The latency of these runs only measures how long ticks waited in the topic, so it is not reported.

The analyzer used nearly all of its 3 cores here, so on this machine and with 4 partitions its limit is about 1.38 million ticks/s: twice what the producer can deliver. In the live runs it needed about 0.2 cores per 100k ticks/s (143% CPU at 700k ticks/s).

## E4: scaling with analyzer cores

Pre-filled topic (as in E2), whole week, 8 partitions, analyzer pinned to 1, 2 or 3 P-cores. 3 repeats each, all VALID.

| Analyzer cores | Analyzer rate, ticks/s (range) | Speedup | Efficiency | Analyzer max memory |
|---|---|---|---|---|
| 1 (`ANALYZER_CPUS=0`) | 505,075 (505,004–506,240) | 1.00× | 100% | 2.8 GB |
| 2 (`0-1`) | 992,189 (989,893–993,514) | 1.96× | 98% | 1.9 GB |
| 3 (`0-2`) | 1,372,479 (1,372,403–1,374,314) | 2.72× | 91% | 60 MB |

All 9 runs produced 1,855,645 windows, 7,769 BUY and 3,580 SELL, the same as with 4 partitions. With 3 cores the rate equals E2's (4 partitions).

**Memory under overload:** with 1 or 2 cores the analyzer's memory grew steadily, about 20 MB/s, and was not released before the run ended. Most likely the async output writer, which has no limit on queued results, falls behind when every core is busy processing. Not confirmed with a profiler.

## E5: each exchange alone

The week split by symbol suffix (`data/trades_week_<EX>.csv`). Per exchange: a pre-filled run over the whole file and a run at 50k ticks/s over the first 7.5M ticks (NL: whole file, 6.0M). Defaults otherwise. Medians of 3 runs, all VALID, all 50k runs sustainable.

| Exchange | Trades | Symbols | Pre-filled rate, ticks/s (range) | 50k: p50 / p99 ms | 50k: max lag | Windows | BUY / SELL |
|---|---|---|---|---|---|---|---|
| ETR | 31,996,003 | 2,126 | 1,382,934 (1,381,936–1,392,167) | 15.2 / 22.0 | 79 | 556,759 | 2,854 / 1,037 |
| FR | 21,367,417 | 2,285 | 1,359,447 (1,358,260–1,364,503) | 16.6 / 24.1 | 387 | 1,043,637 | 3,970 / 2,079 |
| NL | 6,028,250 | 559 | 1,281,163 (1,275,044–1,292,271) | 15.4 / 19.1 | 291 | 255,249 | 945 / 464 |
| All (E2, E3) | 59,391,670 | 4,970 | 1,375,671 | 19.1 / 29.1 | 329 | 1,855,645 | 7,769 / 3,580 |

Windows, signals and late trades (4,371 + 1,135 + 0 = 5,506) of the three add up exactly to the combined run. The NL pre-filled run lasts under 5 s, so start-up probably explains its lower rate.

## E6: long-tail distribution

Trades per symbol over the week: 4,970 symbols, busiest A169S8.ETR with 162,379 trades, median 10,510. The top 1% of symbols make 13.5% of trades, the top 10% 54.7%, the bottom 50% only 5.0%; 1,387 symbols have under 100 trades and 595 under 10.

**Partition load** (4 partitions, FNV-1a symbol hash). Predicted from symbol counts; measured by writing the week to a 4-partition topic and taking `rpk topic describe -p` high watermarks minus 178,733 heartbeats per partition:

| Partition | Symbols | Ticks predicted | Ticks measured |
|---|---|---|---|
| 0 | 1,268 | 15,847,610 | 15,847,610 |
| 1 | 1,233 | 14,193,356 | 14,193,356 |
| 2 | 1,215 | 14,668,237 | 14,668,237 |
| 3 | 1,254 | 14,682,467 | 14,682,467 |

Busiest partition 1.067× the average (8 partitions, predicted: 1.090×).

**Latency by symbol tier**, from one 400k ticks/s run covering the whole week (`results/e3-rate-400000/20261113-221428`), first 30 s excluded:

| Tier | Symbols | Share of trades | Windows | e2e p50 ms | e2e p99 ms | Processing p99 ms |
|---|---|---|---|---|---|---|
| Top 1% | 50 | 13.5% | 21,856 | 19.3 | 29.7 | 2.94 |
| Top 1–10% | 447 | 41.2% | 224,939 | 18.7 | 29.9 | 2.93 |
| 10–50% | 1,988 | 40.3% | 959,707 | 19.2 | 30.1 | 2.93 |
| Bottom 50% | 2,485 | 5.0% | 266,157 | 19.3 | 30.2 | 2.96 |

At 50k and 700k ticks/s the tiers' p50 also differ by under 1 ms. Windows close on the partition watermark, which every tick and the per-second heartbeats move, so a quiet symbol's window closes as soon as a busy one's (`TestHeartbeatClosesQuietSymbols`).

## Resources

| Component | CPU at 700k ticks/s | Max memory |
|---|---|---|
| Analyzer | 143% | 39 MB (46 MB when draining the topic) |
| Producer | 172% | 30 MB |
| Redpanda | 5.5% | 886 MB (limited to 1 GB) |
| Result sink | 12% (max 23%) | 19 MB |

The analyzer's memory does not grow with the rate (29 MB at 5k ticks/s, 39 MB at 700k), because it only keeps the open windows and two EMA values per symbol.

Redpanda's CPU is surprisingly low. The sampler reads the CPU of the container's main process, so this may undercount Redpanda's threads; treat it as a lower bound.

## Effect of the producer change

Before these runs the producer sent every tick to Kafka with its own call in the paced modes, and reached at most about 480k ticks/s, so 500k ticks/s and above failed. It now sends ticks in chunks of up to 256, and sends what it has collected before every pause. A tick therefore waits in the producer only while the next ones are read, which is not included in the measured latency. The earlier sweep (in `results/before-producer-chunking/`) compared with this one:

| Target rate | Producer reached, before → after | p50 ms, before → after | p99 ms, before → after |
|---|---|---|---|
| 5,000 | 5,000 → 5,000 | 17.7 → 17.9 | 42.8 → 46.5 |
| 100,000 | 99,999 → 99,999 | 17.4 → 17.4 | 27.9 → 29.6 |
| 400,000 | 399,998 → 400,003 | 18.6 → 19.1 | 28.9 → 30.1 |
| 500,000 | 483,107 → 499,996 | 18.7 → 19.2 | 28.8 → 29.6 |
| 700,000 | 481,199 → 698,690 | 18.7 → 19.7 | 29.2 → 30.7 |

Latency stayed about the same; the change raised the producer's ceiling from about 480k to about 695k ticks/s, which moved the highest sustainable tested rate from 400k to 700k ticks/s.

## Overall assessment

The results support the main claims: the system gives correct results, is fast, and scales with cores. Its weaknesses come mostly from the test setup, not from the design.

**Strengths**

- **Large margin over real load.** The busiest second in the data has about 4,600 trades. The pipeline sustained 700,000 ticks/s, about 150 times that, and the analyzer alone 1.38 million.
- **Low, stable latency.** End-to-end p99 stays at 29 to 31 ms from 50k to 700k ticks/s. The analyzer takes only 0.1 to 1.3 ms of it; the rest is the two trips through the broker.
- **Correctness shown in several independent ways.** Results match the offline tool exactly and agree with an independent implementation. The only differences come from 5,506 trades that arrive 18 hours late and are dropped as late, as designed. All 63 runs gave identical counts, and the three exchanges run separately add up exactly to the combined run.
- **Near-linear scaling.** Two analyzer cores give 1.96× the 1-core rate and three give 2.72×.
- **Uneven data causes no problems.** The busiest partition holds only 7% more ticks than the average, and quiet symbols get their results as fast as busy ones.
- **Careful method.** Repeated runs, a validity check on every run, warmup excluded, pinned cores and a recorded environment. The producer bottleneck was found and fixed instead of being reported as the system's limit.

**Weaknesses a critical reader would point out**

1. **One laptop and a development-mode broker.** The absolute numbers would not carry over to a real deployment.
2. **The live limit is only bounded.** The live pipeline's limit lies between 700k and 1.38M ticks/s, so 700k should be presented as a lower bound, not as the system's limit.
3. **Only constant input rates.** Real trading is bursty, and no run replays the real pattern. This is the largest gap relative to how the system would actually be used.
4. **Unbounded memory under overload.** When the analyzer is short of cores, its memory grows about 20 MB/s (E4). It appears only under overload, but it is a real robustness flaw.
5. **At most 3 analyzer cores.** "Scales well" has been shown only up to 3 cores.

**Cheap ways to strengthen the results**

- **A replay at 10× real speed** (`MODE=realtime SPEED=10`, a few minutes). This addresses weakness 3 and shows latency under real bursts, including the moments when many windows close at once.
- **A limit on the output writer's queue**, then the 1-core E4 case run again. The memory problem could then be reported as found and fixed.

## Limitations

- **One machine.** Producer, broker, analyzer and sink share one laptop. They are pinned to separate cores, but share memory bandwidth, caches and the power budget.
- **Load generator ceiling.** The live pipeline could not be pushed beyond about 695k ticks/s, so its true sustainable limit lies somewhere between 700k and the 1.38M the analyzer reaches on a pre-filled topic.
- **Redpanda in development mode** with one core and 1 GB, so broker-side numbers would be different in production.
- **Constant rate.** These runs replay the data at a constant rate. The real data is bursty; a replay at 10× historical speed (`MODE=realtime SPEED=10`, peaks around 46k ticks/s) would show latency under the real pattern, and has not been run yet.
- **At most 3 analyzer cores** were tested; the rest of the laptop's cores are slower E-cores.
- **Not yet measured:** per-partition lag and latency, synthetic heavier skew, the empty-window `carry` policy, and other grace periods.

## Reproducing

```bash
powerprofilesctl set performance
bench/sweep-rate.sh | tee results/e3-sweep.log                               # E3, about 1.5 h
for i in 1 2 3; do INPUT=data/trades_only_week.csv PREFILL=1 bench/run.sh e2-drain; done   # E2, about 7 min
for i in 1 2 3; do for c in 0 0-1 0-2; do PREFILL=1 PARTITIONS=8 ANALYZER_CPUS=$c INPUT=data/trades_only_week.csv bench/run.sh e4-cores-$c; done; done   # E4, about 27 min
for ex in FR NL ETR; do { head -n 1 data/trades_only_week.csv; grep -E "^[^,]*\.$ex," data/trades_only_week.csv; } > data/trades_week_$ex.csv; done
for i in 1 2 3; do for ex in FR NL ETR; do                                   # E5, about 32 min
  PREFILL=1 INPUT=data/trades_week_$ex.csv bench/run.sh e5-drain-$ex
  RATE=50000 MAX_TICKS=7500000 INPUT=data/trades_week_$ex.csv bench/run.sh e5-rate-$ex
done; done
powerprofilesctl set balanced
```
