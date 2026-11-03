package main

import (
	"context"
	"fmt"
	"time"

	"trading-trends/pkg/models"
)

// Pacer controls the rate at which ticks are dispatched to the message broker.
type Pacer interface {
	Wait(ctx context.Context, tick *models.MarketTick) error
}

// newPacer builds the pacer for the configured replay mode.
func newPacer(cfg *Config) (Pacer, error) {
	switch cfg.ReplayMode {
	case "rate":
		return NewFixedRatePacer(cfg.Rate)
	case "realtime":
		return NewSimulatedRealtimePacer(cfg.Speed, 1*time.Second)
	default:
		return NewMaxSpeedPacer(), nil
	}
}

// sleepUntil blocks until the wall-clock deadline or until ctx is cancelled.
// Waits shorter than a millisecond are skipped; because pacers schedule against
// a fixed start time, skipped time is made up on later ticks rather than lost.
func sleepUntil(ctx context.Context, deadline time.Time) error {
	d := time.Until(deadline)
	if d < time.Millisecond {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// MaxSpeedPacer emits events as fast as possible without throttling.
type MaxSpeedPacer struct{}

func NewMaxSpeedPacer() *MaxSpeedPacer {
	return &MaxSpeedPacer{}
}

func (p *MaxSpeedPacer) Wait(ctx context.Context, _ *models.MarketTick) error {
	return ctx.Err()
}

// FixedRatePacer throttles output to a target number of events per second.
type FixedRatePacer struct {
	targetRate int64     // messages per second
	checkEvery int64     // only check the clock every N messages (~every 10ms)
	sentCount  int64     // total messages paced
	startTime  time.Time // set on the first Wait
}

// NewFixedRatePacer creates a pacer targeting `rate` messages per second.
func NewFixedRatePacer(rate int) (*FixedRatePacer, error) {
	if rate <= 0 {
		return nil, fmt.Errorf("rate must be positive, got %d", rate)
	}
	return &FixedRatePacer{
		targetRate: int64(rate),
		checkEvery: max(1, int64(rate)/100),
	}, nil
}

func (p *FixedRatePacer) Wait(ctx context.Context, _ *models.MarketTick) error {
	if p.startTime.IsZero() {
		p.startTime = time.Now()
	}
	p.sentCount++
	if p.sentCount%p.checkEvery != 0 {
		return ctx.Err()
	}
	expected := time.Duration(p.sentCount) * time.Second / time.Duration(p.targetRate)
	return sleepUntil(ctx, p.startTime.Add(expected))
}

// SimulatedRealtimePacer replays events according to their historical timestamp deltas,
// scaled by a speedup factor (e.g. 10x, 100x).
type SimulatedRealtimePacer struct {
	speedMultiplier float64
	maxWaitGap      time.Duration // cap on the wall-clock wait between two events
	wallStart       time.Time     // wall time of the first event
	wallOffset      time.Duration // scheduled wall time of lastEvent, relative to wallStart
	lastEvent       time.Time
}

// NewSimulatedRealtimePacer creates a pacer that scales historical timestamps by speedMultiplier.
// maxWaitGap caps long market inactive gaps (e.g. overnight) to prevent hanging.
func NewSimulatedRealtimePacer(speedMultiplier float64, maxWaitGap time.Duration) (*SimulatedRealtimePacer, error) {
	if speedMultiplier <= 0 {
		return nil, fmt.Errorf("speedMultiplier must be positive, got %f", speedMultiplier)
	}
	if maxWaitGap <= 0 {
		maxWaitGap = 1 * time.Second
	}
	return &SimulatedRealtimePacer{
		speedMultiplier: speedMultiplier,
		maxWaitGap:      maxWaitGap,
	}, nil
}

func (p *SimulatedRealtimePacer) Wait(ctx context.Context, tick *models.MarketTick) error {
	if tick.Timestamp.IsZero() {
		return ctx.Err()
	}
	if p.wallStart.IsZero() {
		p.wallStart = time.Now()
		p.lastEvent = tick.Timestamp
		return ctx.Err()
	}
	// Out-of-order or same-time ticks are sent immediately
	if !tick.Timestamp.After(p.lastEvent) {
		return ctx.Err()
	}

	gap := time.Duration(float64(tick.Timestamp.Sub(p.lastEvent)) / p.speedMultiplier)
	p.wallOffset += min(gap, p.maxWaitGap)
	p.lastEvent = tick.Timestamp
	return sleepUntil(ctx, p.wallStart.Add(p.wallOffset))
}
