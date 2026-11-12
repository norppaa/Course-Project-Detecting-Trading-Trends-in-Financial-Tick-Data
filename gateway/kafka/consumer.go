package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"trading-trends/gateway/model"
	"trading-trends/gateway/server"
)

// ConsumerConfig holds configuration options for the Kafka signal consumer.
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	// GroupID of the consumer group. The gateway is a broadcaster: every
	// instance must see every partition, so each instance should use its own
	// group. Empty = a unique id per process.
	GroupID string
	// StartAt is "last" (only results produced after the gateway started)
	// or "first" (replay the whole topic to rebuild the chart history).
	StartAt string
}

// ConsumerStats are counters for the debug panel.
type ConsumerStats struct {
	Topic          string    `json:"topic"`
	GroupID        string    `json:"group_id"`
	StartAt        string    `json:"start_at"`
	Messages       int64     `json:"messages"`
	Indicators     int64     `json:"indicators"`
	Signals        int64     `json:"signals"`
	DecodeErrors   int64     `json:"decode_errors"`
	ReadErrors     int64     `json:"read_errors"`
	LastMessageAt  time.Time `json:"last_message_at,omitzero"`
	LatencySamples int64     `json:"latency_samples"`
	LatencyAvgMs   float64   `json:"latency_avg_ms"` // producer send -> gateway receive
	LatencyMaxMs   float64   `json:"latency_max_ms"`
	LatencyLastMs  float64   `json:"latency_last_ms"`
	AnalyzerAvgMs  float64   `json:"analyzer_to_gateway_avg_ms"` // analyzer emit -> gateway receive
}

// SignalConsumer consumes analyzer output from Kafka and hands it to the Hub.
type SignalConsumer struct {
	cfg    ConsumerConfig
	hub    *server.Hub
	reader *kafka.Reader

	messages, indicators, signals atomic.Int64
	decodeErrors, readErrors      atomic.Int64

	mu            sync.Mutex
	lastMessageAt time.Time
	latN          int64
	latSum        float64
	latMax        float64
	latLast       float64
	anaN          int64
	anaSum        float64
}

// NewSignalConsumer creates a new SignalConsumer.
func NewSignalConsumer(cfg ConsumerConfig, hub *server.Hub) *SignalConsumer {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:19092"}
	}
	if cfg.Topic == "" {
		cfg.Topic = "trading-signals"
	}
	if cfg.GroupID == "" {
		host, _ := os.Hostname()
		cfg.GroupID = fmt.Sprintf("web-gateway-%s-%d", host, time.Now().UnixNano())
	}
	start := kafka.FirstOffset
	if strings.EqualFold(cfg.StartAt, "last") {
		start = kafka.LastOffset
		cfg.StartAt = "last"
	} else {
		cfg.StartAt = "first"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		MaxWait:        250 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    start,
	})

	return &SignalConsumer{
		cfg:    cfg,
		hub:    hub,
		reader: reader,
	}
}

// Start begins consuming messages in a blocking loop until ctx is canceled.
func (c *SignalConsumer) Start(ctx context.Context) {
	log.Printf("[Kafka Consumer] Consuming topic '%s' via %s (group %s, start %s)",
		c.cfg.Topic, strings.Join(c.cfg.Brokers, ","), c.cfg.GroupID, c.cfg.StartAt)

	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.readErrors.Add(1)
			log.Printf("[Kafka Consumer] Error reading message (retrying in 2s): %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		received := time.Now().UTC()
		n := c.messages.Add(1)

		var event model.SignalEvent
		if err := json.Unmarshal(m.Value, &event); err != nil || event.Symbol == "" {
			c.decodeErrors.Add(1)
			log.Printf("[Kafka Consumer] Skipping undecodable message at partition %d offset %d: %v",
				m.Partition, m.Offset, err)
			continue
		}
		// The Kafka header is authoritative for the message kind.
		for _, h := range m.Headers {
			if h.Key == "kind" && len(h.Value) > 0 {
				event.Kind = strings.ToLower(string(h.Value))
			}
		}
		if event.Kind == model.KindSignal {
			c.signals.Add(1)
		} else {
			c.indicators.Add(1)
		}
		c.recordLatency(received, event)

		c.hub.Broadcast(event)

		if n%10000 == 0 {
			log.Printf("[Kafka Consumer] %d messages consumed (latest: %s %s @ %.4f, window end %s)",
				n, event.Kind, event.Symbol, event.Close, event.WindowEnd.Format("2006-01-02 15:04"))
		}
	}
}

func (c *SignalConsumer) recordLatency(received time.Time, ev model.SignalEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastMessageAt = received
	if !ev.ProducedAt.IsZero() {
		ms := float64(received.Sub(ev.ProducedAt).Microseconds()) / 1000
		c.latN++
		c.latSum += ms
		c.latLast = ms
		if ms > c.latMax {
			c.latMax = ms
		}
	}
	if !ev.EmittedAt.IsZero() {
		c.anaN++
		c.anaSum += float64(received.Sub(ev.EmittedAt).Microseconds()) / 1000
	}
}

// Stats returns a snapshot of the consumer counters.
func (c *SignalConsumer) Stats() ConsumerStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := ConsumerStats{
		Topic:          c.cfg.Topic,
		GroupID:        c.cfg.GroupID,
		StartAt:        c.cfg.StartAt,
		Messages:       c.messages.Load(),
		Indicators:     c.indicators.Load(),
		Signals:        c.signals.Load(),
		DecodeErrors:   c.decodeErrors.Load(),
		ReadErrors:     c.readErrors.Load(),
		LastMessageAt:  c.lastMessageAt,
		LatencySamples: c.latN,
		LatencyMaxMs:   c.latMax,
		LatencyLastMs:  c.latLast,
	}
	if c.latN > 0 {
		s.LatencyAvgMs = c.latSum / float64(c.latN)
	}
	if c.anaN > 0 {
		s.AnalyzerAvgMs = c.anaSum / float64(c.anaN)
	}
	return s
}

// Close closes the underlying Kafka reader.
func (c *SignalConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
