// Command offline runs the stream analyzer engine directly over a cleaned
// CSV file (output of scripts/clean_data.go), without Redpanda. Rows are
// processed in file order, which is the same order the producer replays
// them in, so the results should match the streaming pipeline exactly.
//
// Uses:
//   - correctness: diff results against the ground truth
//   - performance: raw engine throughput, separate from broker overhead
//   - dataset analysis: how out of order trading times are (to pick Grace)
//
// Example:
//
//	go run ./analyzer/cmd/offline -input data/trades_08-11-21.csv \
//	    -output data/results_08-11-21.csv -grace 5s
package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"trading-trends/analyzer/engine"
)

// Column order written by scripts/clean_data.go.
const (
	colID = iota
	colSecType
	colDate
	colTime
	colLast
	colTradingTime
	colTradingDate
	numCols
)

type counters struct {
	rows, noPrice, badPrice, badColumns       int64
	missingTradingTime, missingTradingDate    int64
	usedSystemTime, badTimestamp, priceEvents int64
	outOfOrder                                int64
	lateHist                                  [6]int64 // see lateBuckets
	maxLateness                               int64
	examples                                  map[string]int
}

var lateBuckets = []struct {
	limit int64
	label string
}{
	{1 * engine.Second, "< 1 s"},
	{10 * engine.Second, "1-10 s"},
	{60 * engine.Second, "10-60 s"},
	{5 * engine.Minute, "1-5 min"},
	{60 * engine.Minute, "5-60 min"},
	{1 << 62, ">= 60 min"},
}

func main() {
	input := flag.String("input", "data/trades_cleaned.csv", "cleaned CSV (or .csv.gz)")
	output := flag.String("output", "", "results CSV (default: stdout, '-' for stdout, 'none' to discard)")
	grace := flag.Duration("grace", 5*time.Second, "allowed out-of-orderness of event time")
	policy := flag.String("policy", "skip", "empty-window policy: skip | carry")
	suppress := flag.Bool("suppress-first", false, "suppress the advisory in a symbol's first window")
	fallback := flag.Bool("fallback-system-time", false, "use system Date/Time when Trading time is missing")
	signalsOnly := flag.Bool("signals-only", false, "write only windows with a BUY/SELL advisory")
	flag.Parse()

	cfg := engine.Config{Grace: grace.Microseconds(), SuppressFirstSignal: *suppress}
	switch *policy {
	case "skip":
		cfg.Policy = engine.SkipEmpty
	case "carry":
		cfg.Policy = engine.CarryLastClose
	default:
		fatalf("unknown -policy %q (want skip or carry)", *policy)
	}

	in, closeIn := openInput(*input)
	defer closeIn()
	out, closeOut := openOutput(*output)
	defer closeOut()

	if out != nil {
		out.WriteString("symbol,sec_type,window_start,window_end,close,has_tick,ema38,ema100,signal\n")
	}
	var line []byte
	emit := func(r engine.WindowResult) {
		if out == nil || (*signalsOnly && r.Signal == engine.None) {
			return
		}
		line = line[:0]
		line = append(line, r.Symbol...)
		line = append(line, ',')
		line = append(line, r.SecType)
		line = append(line, ',')
		line = appendTime(line, r.WindowStart)
		line = append(line, ',')
		line = appendTime(line, r.WindowEnd)
		line = append(line, ',')
		line = strconv.AppendFloat(line, r.Close, 'g', -1, 64)
		line = append(line, ',')
		line = strconv.AppendBool(line, r.HasTick)
		line = append(line, ',')
		line = strconv.AppendFloat(line, r.EMA38, 'g', -1, 64)
		line = append(line, ',')
		line = strconv.AppendFloat(line, r.EMA100, 'g', -1, 64)
		line = append(line, ',')
		line = append(line, r.Signal.String()...)
		line = append(line, '\n')
		out.Write(line)
	}

	eng := engine.New(cfg, emit)
	var tp engine.TimeParser
	c := counters{examples: map[string]int{}}
	symbols := map[string]struct{}{}
	var maxSeen int64
	var fields [numCols]string

	start := time.Now()
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		s := sc.Text()
		if s == "" || strings.HasPrefix(s, "ID,") || s[0] == '#' {
			continue
		}
		c.rows++
		if !split(s, fields[:]) {
			c.badColumns++
			c.example("bad column count", s)
			continue
		}

		last := fields[colLast]
		if last == "" {
			c.noPrice++
			continue
		}
		price, err := strconv.ParseFloat(last, 64)
		if err != nil || price <= 0 {
			c.badPrice++
			c.example("bad or zero price", s)
			continue
		}

		date, clock := fields[colTradingDate], fields[colTradingTime]
		if clock == "" {
			c.missingTradingTime++
			if !*fallback {
				c.example("missing trading time (dropped)", s)
				continue
			}
			c.usedSystemTime++
			date, clock = fields[colDate], fields[colTime]
		} else if date == "" {
			c.missingTradingDate++
			date = fields[colDate]
		}
		ts, err := tp.Parse(date, clock)
		if err != nil {
			c.badTimestamp++
			c.example("unparseable timestamp", s)
			continue
		}

		c.priceEvents++
		if ts < maxSeen {
			c.outOfOrder++
			lateness := maxSeen - ts
			if lateness > c.maxLateness {
				c.maxLateness = lateness
			}
			for i, b := range lateBuckets {
				if lateness < b.limit {
					c.lateHist[i]++
					break
				}
			}
		} else {
			maxSeen = ts
		}
		symbols[fields[colID]] = struct{}{}

		var secType byte
		if st := fields[colSecType]; st != "" {
			secType = st[0]
		}
		eng.Process(engine.Tick{Symbol: fields[colID], SecType: secType, Price: price, EventTime: ts})
	}
	if err := sc.Err(); err != nil {
		fatalf("read error: %v", err)
	}
	eng.Flush()
	elapsed := time.Since(start)

	report(c, eng.Stats(), len(symbols), elapsed, cfg)
}

// split fills dst with exactly len(dst) comma-separated fields of s.
// Substrings share memory with s, so no allocation per field.
func split(s string, dst []string) bool {
	for i := 0; i < len(dst)-1; i++ {
		j := strings.IndexByte(s, ',')
		if j < 0 {
			return false
		}
		dst[i], s = s[:j], s[j+1:]
	}
	if strings.IndexByte(s, ',') >= 0 {
		return false
	}
	dst[len(dst)-1] = strings.TrimRight(s, "\r")
	return true
}

// appendTime formats event time (µs, naive wall clock) as DD-MM-YYYY HH:MM:SS.
func appendTime(b []byte, us int64) []byte {
	return time.UnixMicro(us).UTC().AppendFormat(b, "02-01-2006 15:04:05")
}

func (c *counters) example(kind, row string) {
	if c.examples[kind] < 3 {
		fmt.Fprintf(os.Stderr, "  example [%s]: %s\n", kind, row)
	}
	c.examples[kind]++
}

func report(c counters, s engine.Stats, nSym int, elapsed time.Duration, cfg engine.Config) {
	pct := func(n, d int64) float64 {
		if d == 0 {
			return 0
		}
		return 100 * float64(n) / float64(d)
	}
	w := os.Stderr
	fmt.Fprintf(w, "\n=== input ===\n")
	fmt.Fprintf(w, "rows                         %12d\n", c.rows)
	fmt.Fprintf(w, "  without price (not trades) %12d  (%.1f%%)\n", c.noPrice, pct(c.noPrice, c.rows))
	fmt.Fprintf(w, "  bad or zero price          %12d\n", c.badPrice)
	fmt.Fprintf(w, "  wrong column count         %12d\n", c.badColumns)
	fmt.Fprintf(w, "  missing trading time       %12d  (system time used for %d)\n", c.missingTradingTime, c.usedSystemTime)
	fmt.Fprintf(w, "  missing trading date       %12d  (system date used)\n", c.missingTradingDate)
	fmt.Fprintf(w, "  unparseable timestamp      %12d\n", c.badTimestamp)
	fmt.Fprintf(w, "price events used            %12d\n", c.priceEvents)
	fmt.Fprintf(w, "distinct symbols             %12d\n", nSym)

	fmt.Fprintf(w, "\n=== event-time disorder (vs. max trading time seen so far) ===\n")
	fmt.Fprintf(w, "out-of-order events          %12d  (%.2f%% of price events)\n", c.outOfOrder, pct(c.outOfOrder, c.priceEvents))
	for i, b := range lateBuckets {
		fmt.Fprintf(w, "  behind by %-10s        %12d\n", b.label, c.lateHist[i])
	}
	fmt.Fprintf(w, "max lateness                 %12v\n", time.Duration(c.maxLateness)*time.Microsecond)

	fmt.Fprintf(w, "\n=== engine (grace %v) ===\n", time.Duration(cfg.Grace)*time.Microsecond)
	fmt.Fprintf(w, "ticks accepted               %12d\n", s.TicksAccepted)
	fmt.Fprintf(w, "ticks late (dropped)         %12d  (%.3f%%)\n", s.TicksLate, pct(s.TicksLate, s.TicksAccepted+s.TicksLate))
	fmt.Fprintf(w, "windows emitted              %12d\n", s.WindowsEmitted)
	fmt.Fprintf(w, "BUY advisories               %12d\n", s.BuySignals)
	fmt.Fprintf(w, "SELL advisories              %12d\n", s.SellSignals)
	fmt.Fprintf(w, "elapsed                      %12v  (%.2f M rows/s incl. parsing and output)\n",
		elapsed.Round(time.Millisecond), float64(c.rows)/elapsed.Seconds()/1e6)
}

func openInput(path string) (io.Reader, func()) {
	f, err := os.Open(path)
	if err != nil {
		fatalf("open input: %v", err)
	}
	r := bufio.NewReaderSize(f, 1<<20)
	if !strings.HasSuffix(path, ".gz") {
		return r, func() { f.Close() }
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		fatalf("gzip: %v", err)
	}
	return gz, func() { gz.Close(); f.Close() }
}

// openOutput returns nil when output is discarded.
func openOutput(path string) (*bufio.Writer, func()) {
	switch path {
	case "none":
		return nil, func() {}
	case "", "-":
		w := bufio.NewWriterSize(os.Stdout, 1<<20)
		return w, func() { w.Flush() }
	}
	f, err := os.Create(path)
	if err != nil {
		fatalf("create output: %v", err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	return w, func() {
		if err := w.Flush(); err != nil {
			fatalf("write output: %v", err)
		}
		if err := f.Close(); err != nil {
			fatalf("close output: %v", err)
		}
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "offline: "+format+"\n", a...)
	os.Exit(1)
}
