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
	Close() error
}

// MaxSpeedPacer emits events as fast as possible without throttling.
type MaxSpeedPacer struct{}

func NewMaxSpeedPacer() *MaxSpeedPacer {
	return &MaxSpeedPacer{}
}

func (p *MaxSpeedPacer) Wait(ctx context.Context, _ *models.MarketTick) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (p *MaxSpeedPacer) Close() error {
	return nil
}

// FixedRatePacer throttles output to a target number of events per second.
type FixedRatePacer struct {
	targetRate int       // messages per second
	batchCheck int64     // check interval (e.g. every 100 messages)
	sentCount  int64     // total messages paced
	startTime  time.Time // reference start time
}

// NewFixedRatePacer creates a pacer targeting `rate` messages per second.
func NewFixedRatePacer(rate int) (*FixedRatePacer, error) {
	if rate <= 0 {
		return nil, fmt.Errorf("rate must be positive, got %d", rate)
	}

	batch := int64(100)
	if rate < 100 {
		batch = 1
	} else if rate > 50000 {
		batch = 500
	}

	return &FixedRatePacer{
		targetRate: rate,
		batchCheck: batch,
		startTime:  time.Now(),
	}, nil
}

func (p *FixedRatePacer) Wait(ctx context.Context, _ *models.MarketTick) error {
	p.sentCount++
	if p.sentCount%p.batchCheck != 0 {
		return nil
	}

	// Calculate where we should be in time
	expectedDuration := time.Duration(p.sentCount) * time.Second / time.Duration(p.targetRate)
	actualDuration := time.Since(p.startTime)

	if actualDuration < expectedDuration {
		sleepTime := expectedDuration - actualDuration
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepTime):
		}
	}

	return nil
}

func (p *FixedRatePacer) Close() error {
	return nil
}

// SimulatedRealtimePacer replays events according to their historical timestamp deltas,
// scaled by a speedup factor (e.g. 10x, 100x).
type SimulatedRealtimePacer struct {
	speedMultiplier float64
	maxWaitGap      time.Duration
	lastEventTime   time.Time
	lastWallTime    time.Time
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
		return nil
	}

	now := time.Now()
	if p.lastEventTime.IsZero() {
		p.lastEventTime = tick.Timestamp
		p.lastWallTime = now
		return nil
	}

	// Only sleep if this tick is strictly after the previous tick
	if tick.Timestamp.After(p.lastEventTime) {
		eventDelta := tick.Timestamp.Sub(p.lastEventTime)
		wallDeltaNeeded := time.Duration(float64(eventDelta) / p.speedMultiplier)

		if wallDeltaNeeded > p.maxWaitGap {
			wallDeltaNeeded = p.maxWaitGap
		}

		expectedWallTime := p.lastWallTime.Add(wallDeltaNeeded)
		sleepDuration := time.Until(expectedWallTime)

		if sleepDuration > 500*time.Microsecond {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleepDuration):
			}
		}

		p.lastEventTime = tick.Timestamp
		p.lastWallTime = time.Now()
	}

	return nil
}

func (p *SimulatedRealtimePacer) Close() error {
	return nil
}
