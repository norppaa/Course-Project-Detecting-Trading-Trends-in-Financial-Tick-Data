package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"trading-trends/pkg/models"
)

// resultsHeader starts with the same columns as the offline tool's output, so
// the two can be compared row by row.
const resultsHeader = "symbol,sec_type,window_start,window_end,close,has_tick,ema38,ema100,signal," +
	"produced_us,ingested_us,emitted_us,received_us\n"

// sinkSummary is written to sink.json at the end of a run.
type sinkSummary struct {
	Indicators int64 `json:"indicators"`
	Signals    int64 `json:"signals"`
	Bad        int64 `json:"bad"`
}

type received struct {
	value []byte
	at    time.Time
}

func runSink(args []string) error {
	fs := flag.NewFlagSet("sink", flag.ContinueOnError)
	brokers := fs.String("brokers", "localhost:19092", "comma-separated broker addresses")
	topic := fs.String("topic", "trading-signals", "output topic of the analyzer")
	outDir := fs.String("out", ".", "folder for results.csv and sink.json")
	finishFile := fs.String("finish-file", "", "the run may end once this file exists")
	idle := fs.Duration("idle", 10*time.Second, "after the finish file exists, stop after this long without results")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *finishFile == "" {
		return errors.New("-finish-file is required")
	}
	brokerList := strings.Split(*brokers, ",")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	partitions, err := listPartitions(ctx, brokerList, *topic)
	if err != nil {
		return err
	}

	// Each partition stamps messages as soon as they are read. Decoding and
	// writing happen in one goroutine and can't affect the timestamps.
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	in := make(chan received, 16384)
	var readers sync.WaitGroup
	var lastUnixNano syncedTime
	for _, p := range partitions {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokerList,
			Topic:     *topic,
			Partition: p,
			MinBytes:  1,
			MaxBytes:  16 << 20,
			MaxWait:   50 * time.Millisecond,
		})
		if err := r.SetOffset(kafka.FirstOffset); err != nil {
			return err
		}
		readers.Add(1)
		go func() {
			defer readers.Done()
			defer r.Close()
			for {
				msg, err := r.ReadMessage(readCtx)
				if err != nil {
					return
				}
				now := time.Now()
				lastUnixNano.set(now)
				in <- received{value: msg.Value, at: now}
			}
		}()
	}

	go func() {
		waitUntilFinished(readCtx, *finishFile, *idle, &lastUnixNano)
		cancelRead()
	}()
	go func() {
		readers.Wait()
		close(in)
	}()

	return writeResults(in, *outDir)
}

// waitUntilFinished returns once the finish file exists and no result has
// arrived for idle.
func waitUntilFinished(ctx context.Context, path string, idle time.Duration, last *syncedTime) {
	var finishedAt time.Time
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			if finishedAt.IsZero() {
				if _, err := os.Stat(path); err != nil {
					continue
				}
				finishedAt = now
			}
			quietSince := last.get()
			if quietSince.Before(finishedAt) {
				quietSince = finishedAt
			}
			if now.Sub(quietSince) >= idle {
				return
			}
		}
	}
}

func writeResults(in <-chan received, outDir string) error {
	f, err := os.Create(filepath.Join(outDir, "results.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.WriteString(resultsHeader); err != nil {
		return err
	}

	var sum sinkSummary
	var line []byte
	for m := range in {
		var ind models.WindowIndicator
		if err := json.Unmarshal(m.value, &ind); err != nil {
			sum.Bad++
			continue
		}
		switch ind.Kind {
		case models.KindSignal:
			sum.Signals++
		case models.KindIndicator:
			sum.Indicators++
			line = appendResult(line[:0], &ind, m.at)
			if _, err := w.Write(line); err != nil {
				return err
			}
		default:
			sum.Bad++
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}

	b, err := json.Marshal(sum)
	if err != nil {
		return err
	}
	fmt.Printf("[Sink] indicators %d, signals %d, bad %d\n", sum.Indicators, sum.Signals, sum.Bad)
	return os.WriteFile(filepath.Join(outDir, "sink.json"), b, 0o644)
}

func appendResult(b []byte, r *models.WindowIndicator, receivedAt time.Time) []byte {
	const windowFormat = "02-01-2006 15:04:05" // same as the offline tool
	signal := r.Signal
	if signal == "" {
		signal = "NONE"
	}
	b = append(b, r.Symbol...)
	b = append(b, ',')
	b = append(b, r.SecType...)
	b = append(b, ',')
	b = r.WindowStart.UTC().AppendFormat(b, windowFormat)
	b = append(b, ',')
	b = r.WindowEnd.UTC().AppendFormat(b, windowFormat)
	b = append(b, ',')
	b = strconv.AppendFloat(b, r.LastPrice, 'g', -1, 64)
	b = append(b, ',')
	b = strconv.AppendBool(b, r.HasTick)
	b = append(b, ',')
	b = strconv.AppendFloat(b, r.EMA38, 'g', -1, 64)
	b = append(b, ',')
	b = strconv.AppendFloat(b, r.EMA100, 'g', -1, 64)
	b = append(b, ',')
	b = append(b, signal...)
	for _, t := range []time.Time{r.ProducedAt, r.IngestedAt, r.EmittedAt, receivedAt} {
		b = append(b, ',')
		if !t.IsZero() {
			b = strconv.AppendInt(b, t.UnixMicro(), 10)
		}
	}
	return append(b, '\n')
}

// listPartitions reads the topic's partitions without being able to create it.
func listPartitions(ctx context.Context, brokers []string, topic string) ([]int, error) {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second}
	res, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, err
	}
	for _, t := range res.Topics {
		if t.Name != topic {
			continue
		}
		if t.Error != nil {
			return nil, fmt.Errorf("topic %s: %w", topic, t.Error)
		}
		ids := make([]int, len(t.Partitions))
		for i, p := range t.Partitions {
			ids[i] = p.ID
		}
		return ids, nil
	}
	return nil, fmt.Errorf("topic %s not found", topic)
}

// syncedTime is a time shared between goroutines.
type syncedTime struct {
	mu sync.Mutex
	t  time.Time
}

func (s *syncedTime) set(t time.Time) {
	s.mu.Lock()
	s.t = t
	s.mu.Unlock()
}

func (s *syncedTime) get() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.t
}
