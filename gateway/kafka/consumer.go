package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"trading-trends/gateway/model"
	"trading-trends/gateway/server"
)

// ConsumerConfig holds configuration options for the Kafka signal consumer.
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// SignalConsumer consumes trading signals from Kafka and broadcasts them to the Hub.
type SignalConsumer struct {
	cfg    ConsumerConfig
	hub    *server.Hub
	reader *kafka.Reader
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
		cfg.GroupID = "web-gateway-group"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset, // Start from latest signals
	})

	return &SignalConsumer{
		cfg:    cfg,
		hub:    hub,
		reader: reader,
	}
}

// Start begins consuming messages in a blocking loop until ctx is canceled.
func (c *SignalConsumer) Start(ctx context.Context) {
	log.Printf("[Kafka Consumer] Starting consumer on topic '%s' via brokers %v",
		c.cfg.Topic, strings.Join(c.cfg.Brokers, ","))

	var msgCount int64

	for {
		select {
		case <-ctx.Done():
			log.Println("[Kafka Consumer] Context canceled, stopping reader...")
			return
		default:
		}

		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Kafka Consumer] Error reading message (retrying in 2s): %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		msgCount++
		var event model.SignalEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("[Kafka Consumer] Failed to unmarshal message at offset %d: %v", m.Offset, err)
			continue
		}

		// Broadcast to WebSocket clients
		c.hub.Broadcast(event)

		if event.Signal == "BUY" || event.Signal == "SELL" {
			log.Printf("[Kafka Consumer] Alert detected for %s: %s (Close=%.4f, EMA38=%.4f, EMA100=%.4f)",
				event.Symbol, event.Signal, event.Close, event.EMA38, event.EMA100)
		} else if msgCount%500 == 0 {
			log.Printf("[Kafka Consumer] Processed %d signal events (latest: %s @ %.4f)",
				msgCount, event.Symbol, event.Close)
		}
	}
}

// Close closes the underlying Kafka reader.
func (c *SignalConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

