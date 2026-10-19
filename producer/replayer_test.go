package main

import (
	"context"
	"testing"
	"time"

	"trading-trends/pkg/models"
)

func TestMaxSpeedPacer(t *testing.T) {
	p := NewMaxSpeedPacer()
	ctx := context.Background()

	tick := &models.MarketTick{ID: "AAPL"}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if err := p.Wait(ctx, tick); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Errorf("MaxSpeedPacer took too long: %v", elapsed)
	}
}

func TestFixedRatePacer(t *testing.T) {
	rate := 1000 // 1000 msg/sec -> 100 msgs should take ~100ms
	p, err := NewFixedRatePacer(rate)
	if err != nil {
		t.Fatalf("failed to create pacer: %v", err)
	}

	ctx := context.Background()
	tick := &models.MarketTick{ID: "AAPL"}

	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := p.Wait(ctx, tick); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)
	// Expect ~100ms, allow some tolerance (70ms to 200ms)
	if elapsed < 70*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Errorf("FixedRatePacer timing out of range: got %v, expected ~100ms", elapsed)
	}
}

func TestSimulatedRealtimePacer(t *testing.T) {
	speed := 100.0 // 100x speedup
	p, err := NewSimulatedRealtimePacer(speed, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create pacer: %v", err)
	}

	ctx := context.Background()
	baseTime := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)

	// Two events 10 seconds apart in event-time
	// At 100x speedup, wall time delay should be 10s / 100 = 100ms
	tick1 := &models.MarketTick{ID: "AAPL", Timestamp: baseTime}
	tick2 := &models.MarketTick{ID: "AAPL", Timestamp: baseTime.Add(10 * time.Second)}

	start := time.Now()
	if err := p.Wait(ctx, tick1); err != nil {
		t.Fatalf("tick1 wait error: %v", err)
	}
	if err := p.Wait(ctx, tick2); err != nil {
		t.Fatalf("tick2 wait error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 80*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Errorf("SimulatedRealtimePacer timing out of range: got %v, expected ~100ms", elapsed)
	}
}
