package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"trading-trends/pkg/models"
)

func TestStampProducedAt(t *testing.T) {
	msgs, err := buildMessages([]*models.MarketTick{
		{ID: "RDSA.NL", Price: 18.45},
		{ID: "SIE.ETR", Price: 140.2},
	})
	if err != nil {
		t.Fatalf("buildMessages: %v", err)
	}

	// A non-UTC time with nanoseconds: the header must keep the exact instant.
	now := time.Date(2026, 11, 8, 12, 0, 0, 123456789, time.FixedZone("EET", 2*60*60))
	stampProducedAt(msgs, now)

	for i, m := range msgs {
		if len(m.Headers) != 1 || m.Headers[0].Key != models.HeaderProducedAt {
			t.Fatalf("message %d: unexpected headers %v", i, m.Headers)
		}
		// Same parsing as the analyzer's consume loop.
		got, err := time.Parse(time.RFC3339Nano, string(m.Headers[0].Value))
		if err != nil {
			t.Fatalf("message %d: parse header: %v", i, err)
		}
		if !got.Equal(now) {
			t.Errorf("message %d: got %v, want %v", i, got, now)
		}
	}
}

func TestControlBalancerRouting(t *testing.T) {
	partitions := []int{0, 1, 2, 3}
	b := &controlBalancer{}
	msgs := buildControlMessages(models.KindHeartbeat, nil, time.Now(), len(partitions), time.Now())
	if len(msgs) != len(partitions) {
		t.Fatalf("got %d messages, want one per partition", len(msgs))
	}
	for want, m := range msgs {
		if got := b.Balance(m, partitions...); got != want {
			t.Errorf("control message routed to %d, want %d", got, want)
		}
	}

	// Ticks must keep the plain symbol hash, even if the symbol looks like a number.
	tick := kafka.Message{Key: []byte("3")}
	if got, want := b.Balance(tick, partitions...), (&kafka.Hash{}).Balance(tick, partitions...); got != want {
		t.Errorf("tick routed to %d, want %d", got, want)
	}
}

func TestBuildControlMessagesHeaders(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 5, time.UTC)
	msgs := buildControlMessages(models.KindEndOfStream, nil, time.Time{}, 1, now)
	headers := map[string]string{}
	for _, h := range msgs[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers[models.HeaderKind] != models.KindEndOfStream {
		t.Errorf("kind header = %q", headers[models.HeaderKind])
	}
	got, err := time.Parse(time.RFC3339Nano, headers[models.HeaderProducedAt])
	if err != nil || !got.Equal(now) {
		t.Errorf("produced_at = %v (err %v), want %v", got, err, now)
	}
}

func TestHeartbeatClock(t *testing.T) {
	base := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	c := heartbeatClock{interval: time.Second}

	steps := []struct {
		ts  time.Time
		due bool
	}{
		{at(0), false},       // first tick only starts the clock
		{at(500), false},     // interval not reached
		{at(1000), true},     // interval reached
		{at(900), false},     // out of order
		{time.Time{}, false}, // no timestamp
		{at(1999), false},
		{at(5000), true}, // a long gap gives one heartbeat
	}
	for i, s := range steps {
		got, due := c.observe(s.ts)
		if due != s.due || (due && !got.Equal(s.ts)) {
			t.Errorf("step %d: got (%v, %t), want due=%t at %v", i, got, due, s.due, s.ts)
		}
	}
	if !c.latest.Equal(at(5000)) {
		t.Errorf("latest = %v", c.latest)
	}

	off := heartbeatClock{}
	off.observe(at(0))
	if _, due := off.observe(at(10000)); due {
		t.Error("heartbeat sent while disabled")
	}
	if !off.latest.Equal(at(10000)) {
		t.Error("latest must be tracked even with heartbeats off, end-of-stream uses it")
	}
}

func TestRetryPartitionCount(t *testing.T) {
	calls := 0
	n, err := retryPartitionCount(context.Background(), 5, time.Millisecond, func(context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("not ready")
		}
		return 4, nil
	})
	if err != nil || n != 4 || calls != 3 {
		t.Errorf("got n=%d err=%v calls=%d, want 4, nil, 3", n, err, calls)
	}

	want := errors.New("still missing")
	calls = 0
	_, err = retryPartitionCount(context.Background(), 3, time.Millisecond, func(context.Context) (int, error) {
		calls++
		return 0, want
	})
	if !errors.Is(err, want) || calls != 3 {
		t.Errorf("got err=%v calls=%d, want last error after 3 calls", err, calls)
	}
}

// A broker that accepts connections but never answers must not hang startup.
func TestTopicPartitionCountStopsOnSilentBroker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := topicPartitionCount(ctx, []string{ln.Addr().String()}, "ticks")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected an error from a silent broker")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lookup did not stop when the context ended")
	}
}
