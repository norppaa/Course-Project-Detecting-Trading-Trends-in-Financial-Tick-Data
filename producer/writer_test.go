package main

import (
	"testing"
	"time"

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
