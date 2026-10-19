package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"trading-trends/pkg/models"
)

// Publisher defines the interface for streaming ticks to a broker.
type Publisher interface {
	Publish(ctx context.Context, tick *models.MarketTick) error
	Flush(ctx context.Context) error
	Close() error
	Stats() (sent int64, errs int64)
}

// KafkaPublisher publishes market ticks to a Kafka/Redpanda topic.
type KafkaPublisher struct {
	writer     *kafka.Writer
	sentCount  int64
	errorCount int64
	buffer     []kafka.Message
	bufferCap  int
	lastFlush  time.Time
}

// NewKafkaPublisher creates a new high-throughput batched Kafka publisher.
func NewKafkaPublisher(brokers []string, topic string, batchSize int, batchTimeout time.Duration, compression string) *KafkaPublisher {
	var compCodec kafka.Compression
	switch compression {
	case "snappy":
		compCodec = kafka.Snappy
	case "gzip":
		compCodec = kafka.Gzip
	case "lz4":
		compCodec = kafka.Lz4
	case "zstd":
		compCodec = kafka.Zstd
	default:
		compCodec = kafka.Snappy
	}

	p := &KafkaPublisher{
		bufferCap: batchSize,
		buffer:    make([]kafka.Message, 0, batchSize),
		lastFlush: time.Now(),
	}

	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Guarantees strict FIFO per Symbol key
		MaxAttempts:  5,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		Compression:  compCodec,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				atomic.AddInt64(&p.errorCount, int64(len(messages)))
				log.Printf("[KafkaPublisher] Error producing %d messages: %v", len(messages), err)
			}
		},
	}

	return p
}

// Publish serializes and adds a tick to the output batch buffer.
func (p *KafkaPublisher) Publish(ctx context.Context, tick *models.MarketTick) error {
	payload, err := json.Marshal(tick)
	if err != nil {
		return fmt.Errorf("failed to marshal tick: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(tick.ID), // Symbol is the partition key
		Value: payload,
		Time:  tick.Timestamp,
	}

	p.buffer = append(p.buffer, msg)
	atomic.AddInt64(&p.sentCount, 1)

	// Flush buffer if capacity is reached
	if len(p.buffer) >= p.bufferCap {
		return p.Flush(ctx)
	}

	return nil
}

// Flush dispatches any queued messages in the local buffer to the Kafka writer.
func (p *KafkaPublisher) Flush(ctx context.Context) error {
	if len(p.buffer) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, len(p.buffer))
	copy(msgs, p.buffer)
	p.buffer = p.buffer[:0]
	p.lastFlush = time.Now()

	return p.writer.WriteMessages(ctx, msgs...)
}

// Close flushes buffered messages and closes the Kafka writer.
func (p *KafkaPublisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = p.Flush(ctx)
	return p.writer.Close()
}

// Stats returns the total messages sent and production errors encountered.
func (p *KafkaPublisher) Stats() (sent int64, errs int64) {
	return atomic.LoadInt64(&p.sentCount), atomic.LoadInt64(&p.errorCount)
}
