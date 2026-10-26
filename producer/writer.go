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

// KafkaPublisher publishes market ticks to a Kafka/Redpanda topic.
// Batching is left to kafka.Writer, which flushes a batch when it is full
// or when batchTimeout expires, so slow replay modes are not delayed.
type KafkaPublisher struct {
	writer    *kafka.Writer
	queued    atomic.Int64
	delivered atomic.Int64
	failed    atomic.Int64
}

// NewKafkaPublisher creates an asynchronous, Snappy-compressed Kafka publisher.
func NewKafkaPublisher(brokers []string, topic string, batchSize int, batchTimeout time.Duration) *KafkaPublisher {
	p := &KafkaPublisher{}
	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Guarantees strict FIFO per Symbol key
		MaxAttempts:  5,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				p.failed.Add(int64(len(messages)))
				log.Printf("[KafkaPublisher] Error producing %d messages: %v", len(messages), err)
				return
			}
			p.delivered.Add(int64(len(messages)))
		},
	}
	return p
}

// Publish serializes a tick and hands it to the async Kafka writer.
func (p *KafkaPublisher) Publish(ctx context.Context, tick *models.MarketTick) error {
	payload, err := json.Marshal(tick)
	if err != nil {
		return fmt.Errorf("failed to marshal tick: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(tick.ID), // Symbol is the partition key
		Value: payload,
		Time:  tick.Timestamp,
	})
	if err != nil {
		return err
	}
	p.queued.Add(1)
	return nil
}

// Close sends any pending batches, waits for them to complete, and closes the writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

// Stats returns how many messages were queued, confirmed by the broker, and failed.
func (p *KafkaPublisher) Stats() (queued, delivered, failed int64) {
	return p.queued.Load(), p.delivered.Load(), p.failed.Load()
}
