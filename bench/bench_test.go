package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trading-trends/pkg/models"
)

func TestPercentile(t *testing.T) {
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i + 1)
	}
	for _, c := range []struct {
		p    float64
		want float64
	}{{50, 50}, {95, 95}, {99, 99}, {100, 100}} {
		if got := percentile(values, c.p); got != c.want {
			t.Errorf("p%g = %g, want %g", c.p, got, c.want)
		}
	}
	if got := percentile([]int64{7}, 99); got != 7 {
		t.Errorf("single value: got %g", got)
	}
}

func TestParseProcStat(t *testing.T) {
	line := "123 (my (odd) proc) S 1 1 1 0 -1 4194560 10 0 0 0 250 50 0 0 20 0 8 0 100 4096 77 18446744073709551615"
	ticks, rss, err := parseProcStat(line)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ticks != 300 || rss != 77 {
		t.Errorf("got ticks=%d rss=%d, want 300 and 77", ticks, rss)
	}
}

func TestReadAnalyzerLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "analyzer.log")
	log := `2026/11/08 19:49:50 [Stats] ticks 1052655     210530/s | lag 12      | late 0     | windows 10
2026/11/08 19:49:55 Messages        : 2019420 (ticks 2000000, heartbeats 19416, eos 4, bad 1)
2026/11/08 19:49:55 Late (dropped)  : 3
2026/11/08 19:49:55 Windows emitted : 70683
2026/11/08 19:49:55 BUY / SELL      : 3971 / 5
2026/11/08 19:49:55 Elapsed         : 25.235s (input active 5.468s, 365779 ticks/s)
`
	if err := os.WriteFile(path, []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	a, err := readAnalyzerLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.samples) != 1 || a.samples[0].ticks != 1052655 || a.samples[0].lag != 12 {
		t.Errorf("samples = %+v", a.samples)
	}
	if !a.found || a.ticks != 2000000 || a.eos != 4 || a.bad != 1 || a.late != 3 ||
		a.windows != 70683 || a.buys != 3971 || a.sells != 5 || a.ticksPerSecond != 365779 {
		t.Errorf("summary = %+v", a)
	}
}

func TestLagHalvesIgnoresIdleEnd(t *testing.T) {
	samples := []statsSample{
		{ticks: 10, lag: 5}, {ticks: 20, lag: 9},
		{ticks: 30, lag: 2}, {ticks: 40, lag: 7},
		{ticks: 40, lag: 900}, {ticks: 40, lag: 900},
	}
	first, second := lagHalves(samples)
	if first != 9 || second != 7 {
		t.Errorf("got %d and %d, want 9 and 7", first, second)
	}
}

func TestAppendResultMatchesOfflineColumns(t *testing.T) {
	start := time.Date(2021, 11, 8, 7, 0, 0, 0, time.UTC)
	r := models.WindowIndicator{
		Symbol: "IECRP.FR", SecType: "I", WindowStart: start, WindowEnd: start.Add(5 * time.Minute),
		LastPrice: 121.94, HasTick: true, EMA38: 6.25, EMA100: 2.5,
	}
	r.ProducedAt = time.UnixMicro(100)
	got := string(appendResult(nil, &r, time.UnixMicro(400)))
	want := "IECRP.FR,I,08-11-2021 07:00:00,08-11-2021 07:05:00,121.94,true,6.25,2.5,NONE,100,,,400\n"
	if got != want {
		t.Errorf("got  %s want %s", got, want)
	}
	if n := strings.Count(strings.TrimSpace(resultsHeader), ",") + 1; n != resultCols {
		t.Errorf("header has %d columns, want %d", n, resultCols)
	}
}

func TestCompareWithOffline(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	online := write("online.csv", "header\n"+
		"A,I,w1,e1,1,true,2,3,NONE,1,2,3,4\n"+
		"B,I,w1,e1,1,true,2,3,BUY,1,2,3,4\n"+
		"C,I,w1,e1,1,true,2,3,NONE,1,2,3,4\n")
	offline := write("offline.csv", "header\n"+
		"A,I,w1,e1,1,true,2,3,NONE\n"+
		"B,I,w1,e1,1,true,2,3,NONE\n"+
		"D,I,w1,e1,1,true,2,3,NONE\n")
	diff, onlyOne, dups, err := compareWithOffline(online, offline)
	if err != nil {
		t.Fatal(err)
	}
	if diff != 1 || onlyOne != 2 || dups != 0 {
		t.Errorf("diff=%d onlyOne=%d dups=%d, want 1, 2, 0", diff, onlyOne, dups)
	}
}
