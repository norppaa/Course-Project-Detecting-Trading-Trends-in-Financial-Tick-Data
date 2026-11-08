package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Columns of results.csv after the nine shared with the offline tool.
const (
	colProduced = 9 + iota
	colIngested
	colEmitted
	colReceived
	resultCols
)

// lagTolerance is how much the highest lag may rise between the two halves of
// a run and still count as bounded, in messages. It absorbs normal batching.
const lagTolerance = 10000

// achievedRateShare is how close to the target a rate-mode run must get.
const achievedRateShare = 0.98

type stage struct {
	name   string
	values []int64 // microseconds
}

type latencySecond struct {
	second int64
	values []int64
}

type statsSample struct {
	at    time.Time
	ticks int64
	rate  float64
	lag   int64
}

type analyzerLog struct {
	samples                          []statsSample
	messages, ticks, heartbeats, eos int64
	bad, late, windows, buys, sells  int64
	ticksPerSecond                   float64
	found                            bool
}

type producerLog struct {
	delivered, failed, controlSent, controlFailed int64
	rate                                          float64
	found                                         bool
}

type resourceStats struct {
	cpuSum, cpuMax, rssMax float64
	count                  int
}

var (
	reStats = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) \[Stats\] ticks (\d+)\s+([\d.]+)/s \| lag (\d+)`)
	reMsgs  = regexp.MustCompile(`Messages\s+: (\d+) \(ticks (\d+), heartbeats (\d+), eos (\d+), bad (\d+)\)`)
	reLate  = regexp.MustCompile(`Late \(dropped\)\s+: (\d+)`)
	reWins  = regexp.MustCompile(`Windows emitted\s+: (\d+)`)
	reSigs  = regexp.MustCompile(`BUY / SELL\s+: (\d+) / (\d+)`)
	reRate  = regexp.MustCompile(`input active \S+, (\d+) ticks/s`)

	reDelivered = regexp.MustCompile(`Ticks Delivered\s+: (\d+)`)
	reFailed    = regexp.MustCompile(`Ticks Failed\s+: (\d+)`)
	reControl   = regexp.MustCompile(`Control Messages\s+: (\d+) sent, (\d+) failed`)
	reOverall   = regexp.MustCompile(`Overall Avg Rate\s+: (\d+)`)
)

func runAnalyze(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	dir := fs.String("dir", "", "run folder created by run.sh")
	warmup := fs.Duration("warmup", 30*time.Second, "ignore results received in this first part of the run")
	p99Limit := fs.Duration("p99-limit", time.Second, "end-to-end p99 limit for a sustainable run")
	compare := fs.String("compare", "", "offline tool output to compare the results with")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("-dir is required")
	}

	f, err := os.Create(filepath.Join(*dir, "summary.txt"))
	if err != nil {
		return err
	}
	defer f.Close()
	out := bufio.NewWriter(io.MultiWriter(f, os.Stdout))
	defer out.Flush()

	cfg := readConfig(filepath.Join(*dir, "config.env"))
	fmt.Fprintf(out, "Run: %s\n", *dir)
	for _, k := range []string{"INPUT", "MODE", "RATE", "SPEED", "PARTITIONS", "GRACE", "POLICY", "HEARTBEAT", "MAX_TICKS"} {
		fmt.Fprintf(out, "  %-11s %s\n", k, cfg[k])
	}

	prod, err := readProducerLog(filepath.Join(*dir, "producer.log"))
	if err != nil {
		return err
	}
	ana, err := readAnalyzerLog(filepath.Join(*dir, "analyzer.log"))
	if err != nil {
		return err
	}
	sink, err := readSinkSummary(filepath.Join(*dir, "sink.json"))
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "\nThroughput\n")
	fmt.Fprintf(out, "  producer  %.0f ticks/s\n", prod.rate)
	fmt.Fprintf(out, "  analyzer  %.0f ticks/s while input was flowing\n", ana.ticksPerSecond)
	maxLag := int64(0)
	for _, s := range ana.samples {
		maxLag = max(maxLag, s.lag)
	}
	fmt.Fprintf(out, "  max lag   %d messages\n", maxLag)

	lat, seconds, used, skipped, effWarmup, err := readLatencies(filepath.Join(*dir, "results.csv"), *warmup)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "\nLatency, ms (%d results used, first %v ignored: %d)\n", used, effWarmup, skipped)
	if effWarmup != *warmup {
		fmt.Fprintf(out, "  NOTE: run is too short for the %v warmup, so none was applied\n", *warmup)
	}
	fmt.Fprintf(out, "  %-12s %10s %10s %10s %10s %10s\n", "stage", "count", "p50", "p95", "p99", "max")
	var e2eP99 float64
	var e2eCount int
	for _, st := range lat {
		slices.Sort(st.values)
		if len(st.values) == 0 {
			fmt.Fprintf(out, "  %-12s %10d %10s %10s %10s %10s\n", st.name, 0, "-", "-", "-", "-")
			continue
		}
		p99 := percentile(st.values, 99)
		if st.name == "end-to-end" {
			e2eP99, e2eCount = p99, len(st.values)
		}
		fmt.Fprintf(out, "  %-12s %10d %10.2f %10.2f %10.2f %10.2f\n", st.name, len(st.values),
			ms(percentile(st.values, 50)), ms(percentile(st.values, 95)), ms(p99), ms(float64(st.values[len(st.values)-1])))
	}

	if err := writeLatencySeries(filepath.Join(*dir, "latency_series.csv"), seconds); err != nil {
		return err
	}
	if err := writeStatsSeries(filepath.Join(*dir, "throughput_series.csv"), ana.samples); err != nil {
		return err
	}

	res, resErr := readResources(filepath.Join(*dir, "resources.csv"))
	if resErr == nil {
		fmt.Fprintf(out, "\nResources (100%% CPU = one core)\n")
		fmt.Fprintf(out, "  %-10s %10s %10s %10s\n", "component", "avg CPU %", "max CPU %", "max MB")
		names := make([]string, 0, len(res))
		for n := range res {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, n := range names {
			r := res[n]
			fmt.Fprintf(out, "  %-10s %10.1f %10.1f %10.1f\n", n, r.cpuSum/float64(r.count), r.cpuMax, r.rssMax)
		}
	}

	fmt.Fprintf(out, "\nCounts\n")
	fmt.Fprintf(out, "  producer  delivered %d, failed %d, control %d (failed %d)\n", prod.delivered, prod.failed, prod.controlSent, prod.controlFailed)
	fmt.Fprintf(out, "  analyzer  ticks %d, heartbeats %d, eos %d, bad %d, late %d, windows %d, BUY %d, SELL %d\n",
		ana.ticks, ana.heartbeats, ana.eos, ana.bad, ana.late, ana.windows, ana.buys, ana.sells)
	fmt.Fprintf(out, "  sink      indicators %d, signals %d, bad %d\n", sink.Indicators, sink.Signals, sink.Bad)

	fmt.Fprintf(out, "\nChecks\n")
	valid := true
	check := func(ok bool, format string, a ...any) {
		state := "ok  "
		if !ok {
			state = "FAIL"
			valid = false
		}
		fmt.Fprintf(out, "  [%s] %s\n", state, fmt.Sprintf(format, a...))
	}
	check(prod.found && ana.found, "producer and analyzer finished and printed summaries")
	check(prod.failed == 0 && prod.controlFailed == 0, "producer had no failed messages")
	check(prod.delivered == ana.ticks, "analyzer read every tick (%d delivered, %d read)", prod.delivered, ana.ticks)
	check(ana.bad == 0 && sink.Bad == 0, "no undecodable messages")
	check(ana.windows == sink.Indicators, "sink got every window (%d emitted, %d received)", ana.windows, sink.Indicators)
	check(ana.buys+ana.sells == sink.Signals, "sink got every signal (%d emitted, %d received)", ana.buys+ana.sells, sink.Signals)
	check(ana.eos > 0, "end-of-stream arrived")
	if resErr != nil {
		check(false, "resource samples available: %v", resErr)
	}

	if *compare != "" {
		diff, onlyOne, dups, err := compareWithOffline(filepath.Join(*dir, "results.csv"), *compare)
		if err != nil {
			return err
		}
		check(diff == 0 && onlyOne == 0 && dups == 0, "same results as offline tool (%d differ, %d missing on one side, %d duplicated)", diff, onlyOne, dups)
	}

	if cfg["MODE"] == "rate" {
		fmt.Fprintf(out, "\nSustainable at this rate?\n")
		target, _ := strconv.ParseFloat(cfg["RATE"], 64)
		sustainable := true
		verdict := func(ok bool, format string, a ...any) {
			state := "yes"
			if !ok {
				state = "NO "
				sustainable = false
			}
			fmt.Fprintf(out, "  [%s] %s\n", state, fmt.Sprintf(format, a...))
		}
		verdict(prod.rate >= target*achievedRateShare, "producer reached the target (%.0f of %.0f ticks/s)", prod.rate, target)
		first, second := lagHalves(ana.samples)
		verdict(second <= first+lagTolerance, "lag stayed bounded (max %d in first half, %d in second)", first, second)
		verdict(e2eCount > 0 && e2eP99 <= float64(p99Limit.Microseconds()), "end-to-end p99 within %v (%.2f ms)", *p99Limit, ms(e2eP99))
		if sustainable {
			fmt.Fprintln(out, "  Result: sustainable")
		} else {
			fmt.Fprintln(out, "  Result: NOT sustainable")
		}
	}

	if valid {
		fmt.Fprintln(out, "\nRun is VALID")
	} else {
		fmt.Fprintln(out, "\nRun is INVALID, do not use its numbers")
	}
	return nil
}

func ms(us float64) float64 { return us / 1000 }

// percentile returns the nearest-rank percentile of sorted values.
func percentile(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(float64(len(sorted))*p/100+0.999999) - 1
	return float64(sorted[max(0, min(rank, len(sorted)-1))])
}

// lagHalves returns the highest lag in each half of the part of the run where
// ticks were still arriving.
func lagHalves(samples []statsSample) (first, second int64) {
	end := len(samples)
	for end > 1 && samples[end-1].ticks == samples[end-2].ticks {
		end--
	}
	for i, s := range samples[:end] {
		if i < end/2 {
			first = max(first, s.lag)
		} else {
			second = max(second, s.lag)
		}
	}
	return first, second
}

func readConfig(path string) map[string]string {
	cfg := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg
	}
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			cfg[k] = v
		}
	}
	return cfg
}

func readProducerLog(path string) (producerLog, error) {
	var p producerLog
	err := eachLine(path, func(l string) {
		if m := reDelivered.FindStringSubmatch(l); m != nil {
			p.delivered, p.found = atoi64(m[1]), true
		} else if m := reFailed.FindStringSubmatch(l); m != nil {
			p.failed = atoi64(m[1])
		} else if m := reControl.FindStringSubmatch(l); m != nil {
			p.controlSent, p.controlFailed = atoi64(m[1]), atoi64(m[2])
		} else if m := reOverall.FindStringSubmatch(l); m != nil {
			p.rate = float64(atoi64(m[1]))
		}
	})
	return p, err
}

func readAnalyzerLog(path string) (analyzerLog, error) {
	var a analyzerLog
	err := eachLine(path, func(l string) {
		if m := reStats.FindStringSubmatch(l); m != nil {
			at, _ := time.ParseInLocation("2006/01/02 15:04:05", m[1], time.Local)
			rate, _ := strconv.ParseFloat(m[3], 64)
			a.samples = append(a.samples, statsSample{at: at, ticks: atoi64(m[2]), rate: rate, lag: atoi64(m[4])})
		} else if m := reMsgs.FindStringSubmatch(l); m != nil {
			a.messages, a.ticks, a.heartbeats, a.eos, a.bad = atoi64(m[1]), atoi64(m[2]), atoi64(m[3]), atoi64(m[4]), atoi64(m[5])
			a.found = true
		} else if m := reLate.FindStringSubmatch(l); m != nil {
			a.late = atoi64(m[1])
		} else if m := reWins.FindStringSubmatch(l); m != nil {
			a.windows = atoi64(m[1])
		} else if m := reSigs.FindStringSubmatch(l); m != nil {
			a.buys, a.sells = atoi64(m[1]), atoi64(m[2])
		} else if m := reRate.FindStringSubmatch(l); m != nil {
			a.ticksPerSecond = float64(atoi64(m[1]))
		}
	})
	return a, err
}

func readSinkSummary(path string) (sinkSummary, error) {
	var s sinkSummary
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}

func eachLine(path string, fn func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		fn(sc.Text())
	}
	return sc.Err()
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// readLatencies returns each latency stage for results received after the
// warmup. If the run is too short for the warmup to leave anything useful,
// no warmup is applied, and the warmup actually used is returned.
func readLatencies(path string, warmup time.Duration) ([]*stage, []*latencySecond, int, int, time.Duration, error) {
	var first, last int64
	err := eachResult(path, func(r []int64) {
		if first == 0 || r[3] < first {
			first = r[3]
		}
		last = max(last, r[3])
	})
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}
	if last-first <= 2*warmup.Microseconds() {
		warmup = 0
	}

	breakdown := []struct {
		name     string
		from, to int
	}{
		{"broker-in", 0, 1},
		{"processing", 1, 2},
		{"broker-out", 2, 3},
		{"end-to-end", 0, 3},
	}
	stages := make([]*stage, len(breakdown))
	for i, b := range breakdown {
		stages[i] = &stage{name: b.name}
	}
	bySecond := map[int64]*latencySecond{}
	used, skipped := 0, 0
	cutoff := first + warmup.Microseconds()
	err = eachResult(path, func(r []int64) {
		if r[3] < cutoff {
			skipped++
			return
		}
		used++
		for i, b := range breakdown {
			if r[b.from] != 0 && r[b.to] != 0 {
				stages[i].values = append(stages[i].values, r[b.to]-r[b.from])
			}
		}
		if r[0] != 0 {
			sec := (r[3] - first) / 1_000_000
			ls := bySecond[sec]
			if ls == nil {
				ls = &latencySecond{second: sec}
				bySecond[sec] = ls
			}
			ls.values = append(ls.values, r[3]-r[0])
		}
	})
	series := make([]*latencySecond, 0, len(bySecond))
	for _, ls := range bySecond {
		series = append(series, ls)
	}
	slices.SortFunc(series, func(a, b *latencySecond) int { return int(a.second - b.second) })
	return stages, series, used, skipped, warmup, err
}

// eachResult calls fn with produced, ingested, emitted and received times (µs,
// 0 if missing) of every row in results.csv.
func eachResult(path string, fn func([]int64)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	r.FieldsPerRecord = resultCols
	r.ReuseRecord = true
	if _, err := r.Read(); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	times := make([]int64, 4)
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for i, c := range []int{colProduced, colIngested, colEmitted, colReceived} {
			times[i] = atoi64(rec[c])
		}
		fn(times)
	}
}

func writeLatencySeries(path string, series []*latencySecond) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "second,results,p50_ms,p99_ms,max_ms")
	for _, s := range series {
		slices.Sort(s.values)
		fmt.Fprintf(w, "%d,%d,%.2f,%.2f,%.2f\n", s.second, len(s.values),
			ms(percentile(s.values, 50)), ms(percentile(s.values, 99)), ms(float64(s.values[len(s.values)-1])))
	}
	return w.Flush()
}

func writeStatsSeries(path string, samples []statsSample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "second,ticks,ticks_per_second,lag")
	for _, s := range samples {
		fmt.Fprintf(w, "%d,%d,%.0f,%d\n", int64(s.at.Sub(samples[0].at).Seconds()), s.ticks, s.rate, s.lag)
	}
	return w.Flush()
}

func readResources(path string) (map[string]*resourceStats, error) {
	res := map[string]*resourceStats{}
	header := true
	err := eachLine(path, func(l string) {
		if header {
			header = false
			return
		}
		f := strings.Split(l, ",")
		if len(f) != 4 {
			return
		}
		cpu, _ := strconv.ParseFloat(f[2], 64)
		rss, _ := strconv.ParseFloat(f[3], 64)
		r := res[f[1]]
		if r == nil {
			r = &resourceStats{}
			res[f[1]] = r
		}
		r.cpuSum += cpu
		r.cpuMax = max(r.cpuMax, cpu)
		r.rssMax = max(r.rssMax, rss)
		r.count++
	})
	if err == nil && len(res) == 0 {
		err = errors.New("no samples")
	}
	return res, err
}

// compareWithOffline compares the nine result columns for each (symbol, window).
func compareWithOffline(resultsPath, offlinePath string) (diff, onlyOne, dups int, err error) {
	load := func(path string) (map[string]string, int, error) {
		rows := map[string]string{}
		repeated := 0
		header := true
		err := eachLine(path, func(l string) {
			if header {
				header = false
				return
			}
			f := strings.SplitN(l, ",", 10)
			if len(f) < 9 {
				return
			}
			key := f[0] + "," + f[2]
			if _, ok := rows[key]; ok {
				repeated++
			}
			rows[key] = strings.Join(f[:9], ",")
		})
		return rows, repeated, err
	}
	online, dups, err := load(resultsPath)
	if err != nil {
		return 0, 0, 0, err
	}
	offline, _, err := load(offlinePath)
	if err != nil {
		return 0, 0, 0, err
	}
	for k, v := range online {
		other, ok := offline[k]
		switch {
		case !ok:
			onlyOne++
		case other != v:
			diff++
		}
	}
	for k := range offline {
		if _, ok := online[k]; !ok {
			onlyOne++
		}
	}
	return diff, onlyOne, dups, nil
}
