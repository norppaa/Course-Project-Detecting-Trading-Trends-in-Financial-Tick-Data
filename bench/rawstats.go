package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

// runRawStats counts events and trades in raw DEBS day files and finds the
// busiest second of each, by system time (when Infront received the event).
// That is the arrival rate a live system would have to absorb, so it is the
// number to compare the benchmark rates with.
func runRawStats(args []string) error {
	fs := flag.NewFlagSet("rawstats", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("usage: bench rawstats <raw day file>...")
	}
	var allEvents, allTrades int64
	var peakEvents, peakTrades int64
	for _, path := range fs.Args() {
		s, err := rawFileStats(path)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n  events %d, trades %d (%.1f events per trade)\n", path, s.events, s.trades, ratio(s.events, s.trades))
		fmt.Printf("  busiest second: %d events (%s), %d trades (%s)\n", s.peakEvents, s.peakEventsAt, s.peakTrades, s.peakTradesAt)
		allEvents += s.events
		allTrades += s.trades
		peakEvents = max(peakEvents, s.peakEvents)
		peakTrades = max(peakTrades, s.peakTrades)
	}
	if fs.NArg() > 1 {
		fmt.Printf("all files\n  events %d, trades %d (%.1f events per trade)\n", allEvents, allTrades, ratio(allEvents, allTrades))
		fmt.Printf("  busiest second: %d events, %d trades\n", peakEvents, peakTrades)
	}
	return nil
}

type rawStats struct {
	events, trades             int64
	peakEvents, peakTrades     int64
	peakEventsAt, peakTradesAt string
}

func rawFileStats(path string) (rawStats, error) {
	var s rawStats
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer f.Close()

	// Raw rows: ID, SecType, Date, Time, ..., Last at index 21.
	events := map[string]int64{}
	trades := map[string]int64{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		l := sc.Text()
		if l == "" || l[0] == '#' || strings.HasPrefix(l, "ID,SecType") {
			continue
		}
		cols := strings.SplitN(l, ",", 23)
		if len(cols) < 23 || cols[0] == "" {
			continue
		}
		s.events++
		date, tm := cols[2], cols[3]
		if len(tm) < 8 {
			continue // no usable system time; counted but not placed in a second
		}
		sec := date + " " + tm[:8]
		events[sec]++
		if cols[21] != "" {
			s.trades++
			trades[sec]++
		}
	}
	if err := sc.Err(); err != nil {
		return s, err
	}
	s.peakEvents, s.peakEventsAt = busiest(events)
	s.peakTrades, s.peakTradesAt = busiest(trades)
	return s, nil
}

func busiest(m map[string]int64) (int64, string) {
	var best int64
	var at string
	for k, v := range m {
		if v > best || (v == best && k < at) {
			best, at = v, k
		}
	}
	return best, at
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
