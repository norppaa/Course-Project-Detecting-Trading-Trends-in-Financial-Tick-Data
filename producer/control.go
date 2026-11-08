package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"trading-trends/pkg/models"
)

// Control messages (heartbeat, end-of-stream) must reach every partition and
// must not overtake ticks already queued, so they go through the same
// kafka.Writer as the ticks. The writer routes only through its Balancer and
// Message.Partition must not be set when writing, so a control message carries
// its target partition number in its key.

// controlBalancer routes control messages to the partition named in their key
// and everything else by symbol hash.
type controlBalancer struct {
	symbols kafka.Hash
}

func (b *controlBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if isControl(msg) {
		if p, err := strconv.Atoi(string(msg.Key)); err == nil {
			return p
		}
	}
	return b.symbols.Balance(msg, partitions...)
}

func isControl(msg kafka.Message) bool {
	for _, h := range msg.Headers {
		if h.Key == models.HeaderKind {
			return true
		}
	}
	return false
}

// buildControlMessages creates one copy of a control message per partition.
func buildControlMessages(kind string, value []byte, eventTime time.Time, partitions int, now time.Time) []kafka.Message {
	headers := []kafka.Header{
		{Key: models.HeaderKind, Value: []byte(kind)},
		{Key: models.HeaderProducedAt, Value: []byte(now.UTC().Format(time.RFC3339Nano))},
	}
	msgs := make([]kafka.Message, partitions)
	for p := range msgs {
		msgs[p] = kafka.Message{
			Key:     []byte(strconv.Itoa(p)),
			Value:   value,
			Time:    eventTime,
			Headers: headers,
		}
	}
	return msgs
}

// heartbeatValue serializes the payload of a heartbeat.
func heartbeatValue(eventTime time.Time) ([]byte, error) {
	b, err := json.Marshal(models.Heartbeat{EventTime: eventTime})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal heartbeat: %w", err)
	}
	return b, nil
}

// heartbeatClock decides when a heartbeat is due. It follows event time, so
// heartbeats are placed the same way in the data at any replay speed.
type heartbeatClock struct {
	interval time.Duration // 0 = heartbeats disabled
	latest   time.Time     // highest event time seen
	lastSent time.Time     // event time of the previous heartbeat
}

// observe records a tick's event time and returns the event time to send in a
// heartbeat, if one is due.
func (c *heartbeatClock) observe(ts time.Time) (time.Time, bool) {
	if ts.IsZero() || !ts.After(c.latest) {
		return time.Time{}, false
	}
	c.latest = ts
	if c.interval <= 0 {
		return time.Time{}, false
	}
	if c.lastSent.IsZero() {
		c.lastSent = ts
		return time.Time{}, false
	}
	if ts.Sub(c.lastSent) < c.interval {
		return time.Time{}, false
	}
	c.lastSent = ts
	return ts, true
}

// Retry settings for reading the topic's partition count at startup. The topic
// may still be being created, or the broker may still be starting.
const (
	partitionLookupAttempts = 30
	partitionLookupWait     = time.Second
	partitionLookupTimeout  = 5 * time.Second
)

// topicPartitionCount asks the brokers how many partitions the topic has,
// retrying for a while. It uses a metadata request that cannot create the
// topic, so a missing topic is an error and not a new topic with the broker's
// default partition count.
func topicPartitionCount(ctx context.Context, brokers []string, topic string) (int, error) {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: partitionLookupTimeout}
	return retryPartitionCount(ctx, partitionLookupAttempts, partitionLookupWait, func(ctx context.Context) (int, error) {
		res, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			return 0, err
		}
		for _, t := range res.Topics {
			if t.Name != topic {
				continue
			}
			if t.Error != nil {
				return 0, t.Error
			}
			if len(t.Partitions) > 0 {
				return len(t.Partitions), nil
			}
		}
		return 0, fmt.Errorf("topic %q has no partitions", topic)
	})
}

// retryPartitionCount calls lookup until it succeeds, attempts run out, or ctx
// is cancelled.
func retryPartitionCount(ctx context.Context, attempts int, wait time.Duration, lookup func(context.Context) (int, error)) (int, error) {
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(wait):
			}
		}
		n, err := lookup(ctx)
		if err == nil {
			return n, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}
	return 0, lastErr
}
