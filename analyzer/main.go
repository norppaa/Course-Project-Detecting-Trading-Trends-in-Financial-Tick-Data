// Command analyzer is the online stream analyzer (component 3).
//
// It consumes ticks from the "market-ticks" topic, computes Query 1
// (EMA38/EMA100 per symbol over 5-minute event-time tumbling windows) and
// Query 2 (BUY/SELL crossover advisories), and publishes the results as JSON
// to the output topic(s). All query logic lives in ./engine and is shared
// with the offline tool in ./cmd/offline, so both give the same results.
//
// See analyzer/README.md for the options and the message contract.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"trading-trends/pkg/models"
)

func main() {
	cfg, err := LoadConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("[Analyzer] %v", err)
	}
	logConfig(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sink := newKafkaSink(cfg)
	d := newDispatcher(cfg, sink)
	in := newInput(cfg)

	// Readers run until ctx is cancelled.
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		if err := in.run(ctx, d); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Analyzer] input stopped: %v", err)
			stop()
		}
	}()

	// Periodic metrics and idle detection.
	start := time.Now()
	m := &metrics{start: start, last: start}
	iw := &idleWatcher{d: d, after: cfg.IdleFlush}
	tick := time.NewTicker(250 * time.Millisecond)
	nextLog := start.Add(cfg.MetricsInterval)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case now := <-tick.C:
			if iw.check(now) {
				log.Printf("[Analyzer] no input for %v: closed all open windows", cfg.IdleFlush)
			}
			if cfg.MetricsInterval > 0 && !now.Before(nextLog) {
				m.log(now, d, in, sink)
				nextLog = now.Add(cfg.MetricsInterval)
			}
		}
	}
	tick.Stop()

	log.Println("[Analyzer] shutting down...")
	readers.Wait()
	in.close()
	d.close() // drain worker queues (open windows stay open: no flush on exit)
	if err := sink.Close(); err != nil {
		log.Printf("[Analyzer] closing writer: %v", err)
	}
	m.log(time.Now(), d, in, sink)
	m.summary(d)
}

func logConfig(cfg *Config) {
	mode := "group " + cfg.GroupID
	if cfg.Partitions != "" {
		mode = "partitions " + cfg.Partitions
	}
	ind := cfg.IndicatorTopic
	if ind == "" {
		ind = "(not published)"
	}
	policy := "skip"
	if cfg.Engine.Policy != 0 {
		policy = "carry"
	}
	log.Println("==================================================")
	log.Println("               STREAM ANALYZER                    ")
	log.Println("==================================================")
	log.Printf("Brokers         : %v", cfg.Brokers)
	log.Printf("Input topic     : %s (%s, start %s)", cfg.InputTopic, mode, cfg.StartAt)
	log.Printf("Indicator topic : %s", ind)
	log.Printf("Signal topic    : %s", cfg.SignalTopic)
	log.Printf("Grace           : %v", time.Duration(cfg.Engine.Grace)*time.Microsecond)
	log.Printf("Empty windows   : %s", policy)
	log.Printf("Suppress first  : %t", cfg.Engine.SuppressFirstSignal)
	log.Printf("Idle flush      : %v", cfg.IdleFlush)
	log.Println("--------------------------------------------------")
}

// ------------------------------------------------------------------ input

// input reads market-ticks either through a consumer group (one reader,
// Kafka assigns partitions) or from explicit partitions (one reader each).
type input struct {
	cfg     *Config
	mu      sync.Mutex
	readers []*kafka.Reader
}

func newInput(cfg *Config) *input { return &input{cfg: cfg} }

func (in *input) readerConfig() kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:        in.cfg.Brokers,
		Topic:          in.cfg.InputTopic,
		MinBytes:       1,
		MaxBytes:       16 << 20,
		MaxWait:        100 * time.Millisecond,
		QueueCapacity:  1024,
		ReadBackoffMin: 10 * time.Millisecond,
		ReadBackoffMax: 200 * time.Millisecond,
	}
}

func (in *input) startOffset() int64 {
	if in.cfg.StartAt == "last" {
		return kafka.LastOffset
	}
	return kafka.FirstOffset
}

func (in *input) run(ctx context.Context, d *dispatcher) error {
	if in.cfg.Partitions == "" {
		rc := in.readerConfig()
		rc.GroupID = in.cfg.GroupID
		rc.StartOffset = in.startOffset()
		rc.CommitInterval = time.Second
		r := kafka.NewReader(rc)
		in.add(r)
		return consume(ctx, r, d)
	}

	parts, err := in.resolvePartitions(ctx)
	if err != nil {
		return err
	}
	log.Printf("[Analyzer] reading partitions %v of %s", parts, in.cfg.InputTopic)
	errc := make(chan error, len(parts))
	for _, p := range parts {
		rc := in.readerConfig()
		rc.Partition = p
		r := kafka.NewReader(rc)
		if err := r.SetOffset(in.startOffset()); err != nil {
			return err
		}
		in.add(r)
		go func() { errc <- consume(ctx, r, d) }()
	}
	var first error
	for range parts {
		if err := <-errc; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// consume reads messages until ctx is cancelled and hands them to workers.
func consume(ctx context.Context, r *kafka.Reader, d *dispatcher) error {
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		m := inMsg{value: msg.Value, receivedAt: time.Now().UTC()}
		for _, h := range msg.Headers {
			switch h.Key {
			case models.HeaderKind:
				m.kind = string(h.Value)
			case models.HeaderProducedAt:
				if t, err := time.Parse(time.RFC3339Nano, string(h.Value)); err == nil {
					m.producedAt = t
				}
			}
		}
		d.dispatch(msg.Partition, m)
	}
}

func (in *input) resolvePartitions(ctx context.Context) ([]int, error) {
	if in.cfg.Partitions != "all" {
		return parsePartitions(in.cfg.Partitions)
	}
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		for _, b := range in.cfg.Brokers {
			conn, err := kafka.DialContext(ctx, "tcp", b)
			if err != nil {
				lastErr = err
				continue
			}
			ps, err := conn.ReadPartitions(in.cfg.InputTopic)
			conn.Close()
			if err != nil || len(ps) == 0 {
				lastErr = fmt.Errorf("topic %s: %v", in.cfg.InputTopic, err)
				continue
			}
			out := make([]int, 0, len(ps))
			for _, p := range ps {
				out = append(out, p.ID)
			}
			sort.Ints(out)
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, fmt.Errorf("listing partitions: %w", lastErr)
}

func (in *input) add(r *kafka.Reader) {
	in.mu.Lock()
	in.readers = append(in.readers, r)
	in.mu.Unlock()
}

// lag sums the consumer lag of all readers (messages behind the log end).
func (in *input) lag() int64 {
	in.mu.Lock()
	defer in.mu.Unlock()
	var n int64
	for _, r := range in.readers {
		if l := r.Stats().Lag; l > 0 {
			n += l
		}
	}
	return n
}

func (in *input) close() {
	in.mu.Lock()
	defer in.mu.Unlock()
	for _, r := range in.readers {
		r.Close()
	}
}

// ------------------------------------------------------------------ output

type kafkaSink struct {
	w      *kafka.Writer
	sent   atomic.Int64
	failed atomic.Int64
}

func newKafkaSink(cfg *Config) *kafkaSink {
	s := &kafkaSink{}
	s.w = &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.Hash{}, // results for a symbol stay in order
		BatchSize:              cfg.BatchSize,
		BatchTimeout:           cfg.BatchTimeout,
		RequiredAcks:           kafka.RequireOne,
		Async:                  true,
		AllowAutoTopicCreation: true,
		Completion: func(msgs []kafka.Message, err error) {
			if err != nil {
				s.failed.Add(int64(len(msgs)))
				log.Printf("[Analyzer] failed to publish %d results: %v", len(msgs), err)
				return
			}
			s.sent.Add(int64(len(msgs)))
		},
	}
	return s
}

func (s *kafkaSink) Send(msgs []outMsg) error {
	km := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		km[i] = kafka.Message{
			Topic:   m.topic,
			Key:     []byte(m.key),
			Value:   m.value,
			Headers: []kafka.Header{{Key: models.HeaderKind, Value: []byte(m.kind)}},
		}
	}
	// Async writer: returns immediately, errors arrive in Completion.
	return s.w.WriteMessages(context.Background(), km...)
}

func (s *kafkaSink) Close() error { return s.w.Close() }

// ------------------------------------------------------------------ metrics

type metrics struct {
	start, last time.Time
	lastMsgs    int64
}

func (m *metrics) totals(d *dispatcher) (t workerStats, minWM, maxWM int64) {
	for i, w := range d.all() {
		s := w.stats()
		t.Messages += s.Messages
		t.Ticks += s.Ticks
		t.Heartbeats += s.Heartbeats
		t.BadMessages += s.BadMessages
		t.SinkErrors += s.SinkErrors
		t.Engine.TicksLate += s.Engine.TicksLate
		t.Engine.WindowsEmitted += s.Engine.WindowsEmitted
		t.Engine.BuySignals += s.Engine.BuySignals
		t.Engine.SellSignals += s.Engine.SellSignals
		t.LatencySumUs += s.LatencySumUs
		t.LatencyCount += s.LatencyCount
		t.LatencyMaxUs = max(t.LatencyMaxUs, s.LatencyMaxUs)
		if i == 0 || s.Engine.Watermark < minWM {
			minWM = s.Engine.Watermark
		}
		maxWM = max(maxWM, s.Engine.Watermark)
	}
	return
}

func (m *metrics) log(now time.Time, d *dispatcher, in *input, s *kafkaSink) {
	t, minWM, _ := m.totals(d)
	dt := now.Sub(m.last).Seconds()
	rate := 0.0
	if dt > 0 {
		rate = float64(t.Messages-m.lastMsgs) / dt
	}
	m.last, m.lastMsgs = now, t.Messages
	avgLat := time.Duration(0)
	if t.LatencyCount > 0 {
		avgLat = time.Duration(t.LatencySumUs/t.LatencyCount) * time.Microsecond
	}
	wm := "-"
	if minWM > 0 {
		wm = time.UnixMicro(minWM).UTC().Format("02-01 15:04:05")
	}
	log.Printf("[Stats] in %-9d %8.0f msg/s | lag %-7d | late %-5d | windows %-8d | BUY %-6d SELL %-6d | watermark %s | out %d (err %d) | proc avg %v max %v",
		t.Messages, rate, in.lag(), t.Engine.TicksLate, t.Engine.WindowsEmitted,
		t.Engine.BuySignals, t.Engine.SellSignals, wm, s.sent.Load(), s.failed.Load()+t.SinkErrors,
		avgLat, time.Duration(t.LatencyMaxUs)*time.Microsecond)
}

func (m *metrics) summary(d *dispatcher) {
	t, _, _ := m.totals(d)
	el := time.Since(m.start)
	// Throughput over the time input was actually flowing (first to last
	// message), so idle time before the producer starts or after it ends
	// does not dilute it.
	active := time.Duration(d.lastInput.Load() - d.firstInput.Load())
	rate := 0.0
	if active > 0 {
		rate = float64(t.Messages) / active.Seconds()
	}
	log.Println("==================================================")
	log.Printf("Partitions      : %d", len(d.all()))
	log.Printf("Messages        : %d (ticks %d, heartbeats %d, bad %d)", t.Messages, t.Ticks, t.Heartbeats, t.BadMessages)
	log.Printf("Late (dropped)  : %d", t.Engine.TicksLate)
	log.Printf("Windows emitted : %d", t.Engine.WindowsEmitted)
	log.Printf("BUY / SELL      : %d / %d", t.Engine.BuySignals, t.Engine.SellSignals)
	log.Printf("Elapsed         : %v (input active %v, %.0f msg/s)", el.Round(time.Millisecond), active.Round(time.Millisecond), rate)
	log.Println("==================================================")
}
