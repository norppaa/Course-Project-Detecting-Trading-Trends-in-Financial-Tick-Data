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

	partitions    int
	controlSent   atomic.Int64
	controlFailed atomic.Int64
}

// NewKafkaPublisher creates an asynchronous, Snappy-compressed Kafka publisher.
func NewKafkaPublisher(brokers []string, topic string, batchSize int, batchTimeout time.Duration, partitions int) *KafkaPublisher {
	p := &KafkaPublisher{partitions: partitions}
	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &controlBalancer{}, // Guarantees strict FIFO per Symbol key
		MaxAttempts:  5,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			var controls int64
			for _, m := range messages {
				if isControl(m) {
					controls++
				}
			}
			ticks := int64(len(messages)) - controls
			if err != nil {
				p.failed.Add(ticks)
				p.controlFailed.Add(controls)
				log.Printf("[KafkaPublisher] Error producing %d messages: %v", len(messages), err)
				return
			}
			p.delivered.Add(ticks)
			p.controlSent.Add(controls)
		},
	}
	return p
}

// Publish serializes ticks and hands them to the async Kafka writer in one call.
func (p *KafkaPublisher) Publish(ctx context.Context, ticks ...*models.MarketTick) error {
	msgs, err := buildMessages(ticks)
	if err != nil {
		p.failed.Add(int64(len(ticks)))
		return err
	}
	stampProducedAt(msgs, time.Now())

	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		p.failed.Add(int64(len(msgs)))
		return err
	}
	p.queued.Add(int64(len(msgs)))
	return nil
}

// PublishControl sends a control message to every partition, behind all ticks
// published so far.
func (p *KafkaPublisher) PublishControl(ctx context.Context, kind string, value []byte, eventTime time.Time) error {
	msgs := buildControlMessages(kind, value, eventTime, p.partitions, time.Now())
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		p.controlFailed.Add(int64(len(msgs)))
		return err
	}
	return nil
}

// buildMessages converts ticks into Kafka messages keyed by symbol.
func buildMessages(ticks []*models.MarketTick) ([]kafka.Message, error) {
	msgs := make([]kafka.Message, len(ticks))
	for i, tick := range ticks {
		payload, err := json.Marshal(tick)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tick: %w", err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(tick.ID), // Symbol is the partition key
			Value: payload,
			Time:  tick.Timestamp,
		}
	}
	return msgs, nil
}

// stampProducedAt sets the wall-clock send time that the analyzer copies into
// its output for end-to-end latency measurement. Messages share one header
// slice, so a chunk costs a single timestamp format.
func stampProducedAt(msgs []kafka.Message, now time.Time) {
	headers := []kafka.Header{{
		Key:   models.HeaderProducedAt,
		Value: []byte(now.UTC().Format(time.RFC3339Nano)),
	}}
	for i := range msgs {
		msgs[i].Headers = headers
	}
}

// Close sends any pending batches, waits for them to complete, and closes the writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

// ControlStats returns how many control messages were confirmed and failed.
func (p *KafkaPublisher) ControlStats() (sent, failed int64) {
	return p.controlSent.Load(), p.controlFailed.Load()
}

// Stats returns how many ticks were queued, confirmed by the broker, and failed.
func (p *KafkaPublisher) Stats() (queued, delivered, failed int64) {
	return p.queued.Load(), p.delivered.Load(), p.failed.Load()
}
