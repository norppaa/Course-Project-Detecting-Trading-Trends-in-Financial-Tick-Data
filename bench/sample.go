package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const clockTicksPerSecond = 100 // USER_HZ, 100 on Linux

type process struct {
	name      string
	pid       int
	lastTicks int64
	seen      bool
}

// runSample writes CPU and memory of the given processes (name=pid) to a CSV
// file once per interval. 100% CPU means one fully used core.
func runSample(args []string) error {
	fs := flag.NewFlagSet("sample", flag.ContinueOnError)
	out := fs.String("out", "resources.csv", "output CSV")
	interval := fs.Duration("interval", time.Second, "sampling interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("give at least one name=pid")
	}

	var procs []*process
	for _, arg := range fs.Args() {
		name, pidText, ok := strings.Cut(arg, "=")
		pid, err := strconv.Atoi(pidText)
		if !ok || err != nil {
			return fmt.Errorf("bad process %q, want name=pid", arg)
		}
		procs = append(procs, &process{name: name, pid: pid})
	}

	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "unix_ms,name,cpu_percent,rss_mb")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pageSize := int64(os.Getpagesize())
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	last := time.Now()
	for _, p := range procs {
		if ticks, _, err := readProcStat(p.pid); err == nil {
			p.lastTicks, p.seen = ticks, true
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-tick.C:
			elapsed := now.Sub(last).Seconds()
			last = now
			for _, p := range procs {
				ticks, rssPages, err := readProcStat(p.pid)
				if err != nil { // the process has exited
					continue
				}
				if p.seen {
					cpu := float64(ticks-p.lastTicks) / clockTicksPerSecond / elapsed * 100
					rss := float64(rssPages*pageSize) / (1 << 20)
					fmt.Fprintf(w, "%d,%s,%.1f,%.1f\n", now.UnixMilli(), p.name, cpu, rss)
				}
				p.lastTicks, p.seen = ticks, true
			}
			w.Flush()
		}
	}
}

// readProcStat returns a process's total CPU ticks and resident pages.
func readProcStat(pid int) (ticks, rssPages int64, err error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	return parseProcStat(string(b))
}

// parseProcStat reads utime, stime and rss. The command name is in parentheses
// and may contain spaces, so fields are counted from after the last ')'.
func parseProcStat(s string) (ticks, rssPages int64, err error) {
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, 0, errors.New("malformed stat line")
	}
	f := strings.Fields(s[i+1:])
	// f[0] is field 3 of proc(5): utime=14, stime=15, rss=24.
	if len(f) < 22 {
		return 0, 0, errors.New("short stat line")
	}
	utime, err1 := strconv.ParseInt(f[11], 10, 64)
	stime, err2 := strconv.ParseInt(f[12], 10, 64)
	rssPages, err3 := strconv.ParseInt(f[21], 10, 64)
	if err := errors.Join(err1, err2, err3); err != nil {
		return 0, 0, err
	}
	return utime + stime, rssPages, nil
}
